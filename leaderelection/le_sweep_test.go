package leaderelection

import (
	"context"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
)

// sweepTestConfig 构造一份满足既有校验的最小合法配置（时长全整秒，#101）。
func sweepTestConfig(mutate func(*Config)) Config {
	cfg := Config{
		LeaseName:        "app",
		LeaseDuration:    2 * time.Second,
		RenewDeadline:    time.Second,
		RetryPeriod:      200 * time.Millisecond,
		OnStartedLeading: func(context.Context) {},
		OnStoppedLeading: func() {},
	}
	if mutate != nil {
		mutate(&cfg)
	}
	return cfg
}

// #100 回归：默认 Identity 必须进程唯一——同主机两进程 / 同 Pod 两容器
// 两个 elector 的默认 identity 不得相同。此前默认取 hostname（主机级标识），
// 同 hostname 两副本的 identity 相同 ⇒ client-go 的「租约已被他人持有且
// 未过期」拦截被 IsLeader()（比对 holder identity）短路 ⇒ 两副本同时当选，
// 兜底互斥被打破且无告警。
func TestNewElector_DefaultIdentityProcessUnique(t *testing.T) {
	client := fake.NewSimpleClientset()
	_, rec1, err := newElector(client, sweepTestConfig(nil))
	if err != nil {
		t.Fatalf("newElector(1): %v", err)
	}
	_, rec2, err := newElector(client, sweepTestConfig(nil))
	if err != nil {
		t.Fatalf("newElector(2): %v", err)
	}
	id1 := rec1.Interface.(*resourcelock.LeaseLock).LockConfig.Identity
	id2 := rec2.Interface.(*resourcelock.LeaseLock).LockConfig.Identity
	if id1 == id2 {
		t.Fatalf("默认 identity 相同 = %q（#100：同主机两进程会互认身份双双当选，默认值必须进程唯一）", id1)
	}
	host, _ := os.Hostname()
	if !strings.HasPrefix(id1, host+"_") {
		t.Fatalf("默认 identity %q 应以 hostname 为前缀（主机可读性口径）", id1)
	}
}

// #101 回归：LeaseDuration 必须整秒且 ≥1s——亚秒租约被 client-go 截断为
// 0 秒写入 Lease 资源 ⇒ 竞争者 isLeaseValid 恒判「已过期」⇒ 不同 identity
// 的两个副本也同时当选（静默脑裂）；1500ms 这类非整秒会被截短。构造期
// fail-fast，与既有 renew/retty 关系校验同款。
func TestNewElector_RejectsSubSecondLease(t *testing.T) {
	client := fake.NewSimpleClientset()
	cases := []struct {
		name     string
		duration time.Duration
		wantErr  bool
	}{
		{"500ms 截断为 0 秒必须拒绝", 500 * time.Millisecond, true},
		{"900ms 截断为 0 秒必须拒绝", 900 * time.Millisecond, true},
		{"1500ms 非整秒被截短必须拒绝", 1500 * time.Millisecond, true},
		{"1s 合法", time.Second, false},
		{"2s 合法", 2 * time.Second, false},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			_, _, err := newElector(client, sweepTestConfig(func(c *Config) {
				c.LeaseDuration = tt.duration
				// RenewDeadline/RetryPeriod 仍满足既有关系校验（renew < lease
				// 在 500ms/400ms 组合下成立，让本用例确实落在粒度校验上）
				c.RenewDeadline = tt.duration * 4 / 5
				c.RetryPeriod = tt.duration / 5
			}))
			if tt.wantErr && err == nil {
				t.Fatalf("LeaseDuration=%v 应被构造期拒绝（#101：亚秒租约被 client-go 截断为 0 秒 ⇒ 破互斥）", tt.duration)
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("LeaseDuration=%v 不应被拒绝: %v", tt.duration, err)
			}
		})
	}
}

// #100 行为回归：默认 identity 下两个 elector 也必须正常互斥（修复前
// 同 hostname 让「租约已被他人持有」判定失效，两副本同时当选）。真选主
// 协议（fake clientset + client-go tryAcquireOrRenew），统计
// OnStartedLeading 触发次数。
func TestDefaultIdentity_MutualExclusionStillHolds(t *testing.T) {
	client := fake.NewSimpleClientset()
	var mu sync.Mutex
	leaders := map[string]bool{}
	stopped := make(chan struct{})

	cfgFor := func(tag string) Config {
		return sweepTestConfig(func(c *Config) {
			c.OnStartedLeading = func(context.Context) {
				mu.Lock()
				leaders[tag] = true
				mu.Unlock()
				<-stopped
			}
		})
	}
	cfgA, cfgB := cfgFor("A"), cfgFor("B")

	eA, _, err := newElector(client, cfgA)
	if err != nil {
		t.Fatalf("newElector(A): %v", err)
	}
	eB, _, err := newElector(client, cfgB)
	if err != nil {
		t.Fatalf("newElector(B): %v", err)
	}
	ctxA, cancelA := context.WithCancel(context.Background())
	ctxB, cancelB := context.WithCancel(context.Background())
	doneA := make(chan struct{})
	doneB := make(chan struct{})
	go func() { eA.Run(ctxA); close(doneA) }()
	go func() { eB.Run(ctxB); close(doneB) }()

	// 至少一方当选即视为选主已完成（谁当选不关心，关心的是不同时）
	waitLeadership(t, 10*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(leaders) > 0
	})
	// 给「另一副本也同时当选」留出暴露窗口（修复前脑裂在本窗口内必现）
	time.Sleep(3 * time.Second)

	cancelA()
	cancelB()
	close(stopped)
	<-doneA
	<-doneB

	mu.Lock()
	defer mu.Unlock()
	if len(leaders) != 1 {
		t.Fatalf("OnStartedLeading 触发副本 = %v（#100：默认 identity 下多副本仍必须单活，互斥不得被同 hostname 打破）", leaders)
	}
}

func waitLeadership(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("leadership condition not met in time")
}
