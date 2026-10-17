package leaderelection

import (
	"context"
	"sync"
	"testing"
	"time"

	"k8s.io/client-go/kubernetes/fake"
)

func startElector(t *testing.T, client *fake.Clientset, name string,
	started, stopped func()) (context.CancelFunc, <-chan struct{}) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	le, err := New(client, Config{
		LeaseName:       "app",
		Identity:        name, // 显式身份：默认 hostname 会让同进程双 elector 互认
		LeaseDuration:   2 * time.Second,
		RenewDeadline:   time.Second,
		RetryPeriod:     200 * time.Millisecond,
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

func TestConfigValidation(t *testing.T) {
	client := fake.NewSimpleClientset()
	if _, err := New(client, Config{}); err == nil {
		t.Fatal("empty lease name should fail")
	}
	if _, err := New(client, Config{LeaseName: "app"}); err == nil {
		t.Fatal("missing OnStartedLeading should fail")
	}
	if _, err := New(client, Config{
		LeaseName:       "app",
		LeaseDuration:   time.Second,
		RenewDeadline:   2 * time.Second, // 违反 renew < lease
		OnStartedLeading: func(context.Context) {},
	}); err == nil {
		t.Fatal("renewDeadline >= leaseDuration should fail")
	}
}
