package leaderelection

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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

// #81：续约失败放弃领导权（失权）与 ctx 取消并存时，失权不得被掩蔽为
// ctx.Err()——否则失权副本按「优雅关停」零退出（workflow#61 同构错误面
// 掩蔽）。本用例在 OnStoppedLeading 触发瞬间取消 ctx，构造确定性并存窗口：
// client-go 失权退出时先触发 OnStoppedLeading 再从 Run 返回，若失权判定
// 发生在返回后读 ctx.Err()，失权必被掩蔽成 context.Canceled。
func TestLostLeadershipNotMaskedByCtxCancel(t *testing.T) {
	client := fake.NewSimpleClientset()
	var failRenew atomic.Bool
	client.PrependReactor("update", "leases",
		func(_ clienttesting.Action) (bool, runtime.Object, error) {
			if failRenew.Load() {
				return true, nil, errors.New("lease update boom")
			}
			return false, nil, nil
		})
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
			OnStartedLeading: func(context.Context) { failRenew.Store(true); close(started) },
			OnStoppedLeading: cancel, // 失权回调触发瞬间 SIGTERM（ctx 取消）——两错误面并存
		})
	}()
	select {
	case <-started:
	case <-time.After(10 * time.Second):
		t.Fatal("not elected in time")
	}
	select {
	case err := <-done:
		if !errors.Is(err, ErrLostLeadership) {
			t.Fatalf("Run() = %v, want ErrLostLeadership (lost leadership must not be masked by ctx cancel)", err)
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

// electThenCancel 当选后取消 ctx（优雅关停），返回 Run 返回后的 Lease，
// 供断言关停时 Lease 的释放行为（#62）。
func electThenCancel(t *testing.T, client *fake.Clientset, releaseOnCancel bool) *coordinationv1.Lease {
	t.Helper()
	started := make(chan struct{})
	done := make(chan error, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		done <- Run(ctx, client, Config{
			LeaseName:       "app",
			Identity:        "e1",
			LeaseDuration:   2 * time.Second,
			RenewDeadline:   time.Second,
			RetryPeriod:     200 * time.Millisecond,
			ReleaseOnCancel: releaseOnCancel,
			OnStartedLeading: func(context.Context) {
				close(started)
			},
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
	lease, err := client.CoordinationV1().Leases("default").
		Get(context.Background(), "app", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get lease after shutdown: %v", err)
	}
	return lease
}

// #62：ReleaseOnCancel: true 时优雅关停（ctx 取消）应立即释放 Lease——
// holderIdentity 清空，继任者无需等租约自然过期。
func TestReleaseOnCancelClearsLeaseHolder(t *testing.T) {
	client := fake.NewSimpleClientset()
	lease := electThenCancel(t, client, true)
	if lease.Spec.HolderIdentity != nil && *lease.Spec.HolderIdentity != "" {
		t.Fatalf("holderIdentity = %q, want cleared with ReleaseOnCancel", *lease.Spec.HolderIdentity)
	}
}

// #62 负对照：缺省（false）保持既有行为——关停不释放 Lease，
// holderIdentity 仍为本副本，继任者须等租约自然过期（最长 LeaseDuration）。
func TestNoReleaseOnCancelKeepsLeaseHolder(t *testing.T) {
	client := fake.NewSimpleClientset()
	lease := electThenCancel(t, client, false)
	if lease.Spec.HolderIdentity == nil || *lease.Spec.HolderIdentity != "e1" {
		t.Fatalf("holderIdentity = %v, want %q kept by default (no release on cancel)", lease.Spec.HolderIdentity, "e1")
	}
}

// #62：ReleaseOnCancel 生效后继任者应在 RetryPeriod 量级内接管（而非等
// LeaseDuration 自然过期）——「关停空窗从 15s 量级缩到重试间隔」的直接判据。
// 故意放大 LeaseDuration 到 10s：若释放未生效，e2 在测试窗口内不可能当选。
func TestReleaseOnCancelSpeedsTakeover(t *testing.T) {
	client := fake.NewSimpleClientset()
	started1 := make(chan struct{})
	done1 := make(chan error, 1)
	ctx1, cancel1 := context.WithCancel(context.Background())
	defer cancel1()
	go func() {
		done1 <- Run(ctx1, client, Config{
			LeaseName:        "app",
			Identity:         "e1",
			LeaseDuration:    10 * time.Second,
			RenewDeadline:    2 * time.Second,
			RetryPeriod:      200 * time.Millisecond,
			ReleaseOnCancel:  true,
			OnStartedLeading: func(context.Context) { close(started1) },
			OnStoppedLeading: func() {},
		})
	}()
	select {
	case <-started1:
	case <-time.After(10 * time.Second):
		t.Fatal("e1 not elected in time")
	}

	started2 := make(chan struct{})
	done2 := make(chan error, 1)
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	go func() {
		done2 <- Run(ctx2, client, Config{
			LeaseName:        "app",
			Identity:         "e2",
			LeaseDuration:    10 * time.Second,
			RenewDeadline:    2 * time.Second,
			RetryPeriod:      200 * time.Millisecond,
			OnStartedLeading: func(context.Context) { close(started2) },
			OnStoppedLeading: func() {},
		})
	}()
	// e1 持有期间 e2 不得当选
	time.Sleep(1 * time.Second)
	select {
	case <-started2:
		t.Fatal("e2 became leader while e1 holds the lease")
	case <-time.After(50 * time.Millisecond):
	}

	cancel1() // e1 优雅关停并释放 Lease
	select {
	case err := <-done1:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run(e1) = %v, want context.Canceled", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Run(e1) did not return after ctx cancel")
	}

	// 释放后 e2 应在 RetryPeriod 量级内接管（远小于 10s 的 LeaseDuration）
	select {
	case <-started2:
	case <-time.After(3 * time.Second):
		t.Fatal("e2 did not take over within RetryPeriod scale after release")
	}
	cancel2()
	select {
	case err := <-done2:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run(e2) = %v, want context.Canceled", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Run(e2) did not return after ctx cancel")
	}
}

// #61：从未当选的副本在 ctx 取消时，Run 应返回 ctx.Err()（而非
// ErrLostLeadership），且 OnStoppedLeading 仍会触发（client-go 语义：
// elector 退出即回调，不区分是否曾当选）——推荐用法据此用 Run 返回值
// 区分退出原因，优雅关停不走 log.Fatal。
func TestRunReturnsContextCanceledWhenNeverElected(t *testing.T) {
	client := fake.NewSimpleClientset()
	// e1 先当选并持续续约，保证 e2 全程未当选
	started1 := make(chan struct{})
	done1 := make(chan error, 1)
	ctx1, cancel1 := context.WithCancel(context.Background())
	defer cancel1()
	go func() {
		done1 <- Run(ctx1, client, Config{
			LeaseName:        "app",
			Identity:         "e1",
			LeaseDuration:    2 * time.Second,
			RenewDeadline:    time.Second,
			RetryPeriod:      200 * time.Millisecond,
			OnStartedLeading: func(context.Context) { close(started1) },
			OnStoppedLeading: func() {},
		})
	}()
	select {
	case <-started1:
	case <-time.After(10 * time.Second):
		t.Fatal("e1 not elected in time")
	}

	var stopped2 atomic.Bool
	done2 := make(chan error, 1)
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	go func() {
		done2 <- Run(ctx2, client, Config{
			LeaseName:        "app",
			Identity:         "e2",
			LeaseDuration:    2 * time.Second,
			RenewDeadline:    time.Second,
			RetryPeriod:      200 * time.Millisecond,
			OnStartedLeading: func(context.Context) { t.Error("e2 should never be elected") },
			OnStoppedLeading: func() { stopped2.Store(true) },
		})
	}()
	// 留出 e2 进入 acquire 循环且确认未当选的时间，再取消
	time.Sleep(500 * time.Millisecond)
	if stopped2.Load() {
		t.Fatal("e2 unexpectedly exited before ctx cancel")
	}
	cancel2()
	select {
	case err := <-done2:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run(e2) = %v, want context.Canceled (never elected, graceful cancel)", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Run(e2) did not return after ctx cancel")
	}
	// elector 因从未当选而退出时 OnStoppedLeading 也必须触发（钉住 client-go
	// 语义：该回调 = elector 结束，≠ 失权）
	if !stopped2.Load() {
		t.Fatal("OnStoppedLeading should fire on elector exit even if never elected")
	}
}
