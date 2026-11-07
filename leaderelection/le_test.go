package leaderelection

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"
)

func startElector(t *testing.T, client *fake.Clientset, name string,
	started, stopped func()) (context.CancelFunc, <-chan struct{}) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	le, err := New(client, Config{
		LeaseName:        "app",
		Identity:         name, // 显式身份：默认 hostname 会让同进程双 elector 互认
		LeaseDuration:    2 * time.Second,
		RenewDeadline:    time.Second,
		RetryPeriod:      200 * time.Millisecond,
		OnStartedLeading: func(_ context.Context) { started() },
		OnStoppedLeading: stopped,
	})
	if err != nil {
		t.Fatalf("New(%s): %v", name, err)
	}
	go func() {
		le.Run(ctx)
		close(done)
	}()
	return cancel, done
}

func waitCond(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("condition not met in time")
}

func TestLeadershipHandoff(t *testing.T) {
	client := fake.NewSimpleClientset()

	var mu sync.Mutex
	var led1, led2, stopped2 bool

	cancel1, done1 := startElector(t, client, "e1",
		func() { mu.Lock(); led1 = true; mu.Unlock() },
		func() {})
	waitCond(t, 5*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return led1
	})

	// e2 在 e1 持有期间不得当选
	cancel2, done2 := startElector(t, client, "e2",
		func() { mu.Lock(); led2 = true; mu.Unlock() },
		func() { mu.Lock(); stopped2 = true; mu.Unlock() })
	time.Sleep(1 * time.Second)
	mu.Lock()
	if led2 {
		mu.Unlock()
		t.Fatal("e2 became leader while e1 holds the lease")
	}
	mu.Unlock()

	// e1 退场后租约到期（2s），e2 应接棒；此后无竞争者，e2 持续持有不会失权
	cancel1()
	<-done1
	waitCond(t, 15*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return led2
	})
	cancel2()
	<-done2
	// Run 因 ctx 取消退出时应触发失权回调
	waitCond(t, 2*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return stopped2
	})
}

// Run 因调用方取消 ctx 退出时，退出原因应上抛为 ctx.Err()（#42）。
func TestRunReturnsContextCanceled(t *testing.T) {
	client := fake.NewSimpleClientset()
	started := make(chan struct{})
	done := make(chan error, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		done <- Run(ctx, client, Config{
			LeaseName:        "app",
			Identity:         "e1",
			LeaseDuration:    2 * time.Second,
			RenewDeadline:    time.Second,
			RetryPeriod:      200 * time.Millisecond,
			OnStartedLeading: func(context.Context) { close(started) },
			OnStoppedLeading: func() {},
		})
	}()
	select {
	case <-started:
	case <-time.After(10 * time.Second):
		t.Fatal("not elected in time")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run() = %v, want context.Canceled", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not return after ctx cancel")
	}
}

// Run 因失权退出时，退出原因应上抛为 ErrLostLeadership（#42）：
// 当选后令所有 Lease update 失败，续约窗口内重试耗尽即放弃领导权。
func TestRunReturnsErrLostLeadership(t *testing.T) {
	client := fake.NewSimpleClientset()
	var failRenew atomic.Bool
	client.PrependReactor("update", "leases",
		func(_ clienttesting.Action) (bool, runtime.Object, error) {
			if failRenew.Load() {
				return true, nil, errors.New("lease update boom")
			}
			return false, nil, nil
		})
	done := make(chan error, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		done <- Run(ctx, client, Config{
			LeaseName:        "app",
			Identity:         "e1",
			LeaseDuration:    2 * time.Second,
			RenewDeadline:    time.Second,
			RetryPeriod:      200 * time.Millisecond,
			OnStartedLeading: func(context.Context) { failRenew.Store(true) },
			OnStoppedLeading: func() {},
		})
	}()
	select {
	case err := <-done:
		if !errors.Is(err, ErrLostLeadership) {
			t.Fatalf("Run() = %v, want ErrLostLeadership", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("Run did not return after leadership loss")
	}
}

func TestConfigValidation(t *testing.T) {
	client := fake.NewSimpleClientset()
	if _, err := New(client, Config{}); err == nil {
		t.Fatal("empty lease name should fail")
	}
	if _, err := New(client, Config{LeaseName: "app"}); err == nil {
		t.Fatal("missing OnStartedLeading should fail")
	}
	if _, err := New(client, Config{
		LeaseName:        "app",
		OnStartedLeading: func(context.Context) {},
	}); err == nil {
		t.Fatal("missing OnStoppedLeading should fail")
	}
	if _, err := New(client, Config{
		LeaseName:        "app",
		LeaseDuration:    time.Second,
		RenewDeadline:    2 * time.Second, // 违反 renew < lease
		OnStartedLeading: func(context.Context) {},
		OnStoppedLeading: func() {},
	}); err == nil {
		t.Fatal("renewDeadline >= leaseDuration should fail")
	}
	if _, err := New(client, Config{
		LeaseName:        "app",
		LeaseDuration:    15 * time.Second,
		RenewDeadline:    5 * time.Second,
		RetryPeriod:      5 * time.Second, // 违反 retry < renew
		OnStartedLeading: func(context.Context) {},
		OnStoppedLeading: func() {},
	}); err == nil {
		t.Fatal("retryPeriod >= renewDeadline should fail")
	}
	if _, err := New(client, Config{
		LeaseName:        "app",
		LeaseDuration:    15 * time.Second,
		RenewDeadline:    6 * time.Second,
		RetryPeriod:      5 * time.Second, // renew == retry*1.2，无裕度也拒绝
		OnStartedLeading: func(context.Context) {},
		OnStoppedLeading: func() {},
	}); err == nil {
		t.Fatal("renewDeadline <= retryPeriod*1.2 should fail")
	}
	if _, err := New(client, Config{
		LeaseName:        "app",
		LeaseDuration:    15 * time.Second,
		RenewDeadline:    9 * time.Second,
		RetryPeriod:      8 * time.Second, // 8s*1.2=9.6s > 9s
		OnStartedLeading: func(context.Context) {},
		OnStoppedLeading: func() {},
	}); err == nil {
		t.Fatal("renewDeadline < retryPeriod*1.2 should fail")
	}
	// 全零走默认值（15s/10s/2s），满足全部约束
	if _, err := New(client, Config{
		LeaseName:        "app",
		OnStartedLeading: func(context.Context) {},
		OnStoppedLeading: func() {},
	}); err != nil {
		t.Fatalf("default durations should pass: %v", err)
	}
}
