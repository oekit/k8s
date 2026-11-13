package leaderelection

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"
	clientleaderelection "k8s.io/client-go/tools/leaderelection"
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

// #108 单元钉：renewRecorder.observe 的取消豁免判据必须与 workflow#61 的
// isCancellationErr 完全同口径——Canceled 与 DeadlineExceeded 两个 sentinel
// 都不得记成续约失败；真失权（连接拒绝、租约被抢、窗口耗尽）仍入账；
// 成功续约与主动释放清账。
func TestRenewRecorderExemptsCancellationSentinels(t *testing.T) {
	record := resourcelock.LeaderElectionRecord{HolderIdentity: "e1"}
	rec := &renewRecorder{}
	rec.observe(record, nil) // 首次成功 acquire → 持有态
	if !rec.leading.Load() || rec.lost() {
		t.Fatal("precondition: recorder should be leading after successful acquire")
	}
	// DeadlineExceeded（直接值与 wrapped）都豁免——#108：WithTimeout 关停时
	// 在途续约以此收场（请求 ctx 派生自调用方 ctx），不是 Canceled
	rec.observe(record, context.DeadlineExceeded)
	if rec.lost() {
		t.Fatal("context.DeadlineExceeded must not be recorded as renew failure (#108)")
	}
	rec.observe(record, fmt.Errorf("lease update: %w", context.DeadlineExceeded))
	if rec.lost() {
		t.Fatal("wrapped context.DeadlineExceeded must not be recorded as renew failure (#108)")
	}
	// Canceled 一侧（#81 既有行为不回退）
	rec.observe(record, fmt.Errorf("lease update: %w", context.Canceled))
	if rec.lost() {
		t.Fatal("context.Canceled must not be recorded as renew failure (#81)")
	}
	// 真失权仍入账
	rec.observe(record, errors.New("connection refused"))
	if !rec.lost() {
		t.Fatal("real renew failure must be recorded as lost leadership signal")
	}
	// 成功续约清账
	rec.observe(record, nil)
	if rec.lost() {
		t.Fatal("successful renew must clear the failure signal")
	}
	// ReleaseOnCancel 主动释放（holder 置空）同样清账
	rec.observe(record, errors.New("connection refused"))
	if !rec.lost() {
		t.Fatal("precondition: renewed failure should be recorded")
	}
	rec.observe(resourcelock.LeaderElectionRecord{}, nil)
	if rec.lost() {
		t.Fatal("release must clear the failure signal")
	}
}

// #108 回归（关停面）：调用方用 context.WithTimeout 包住 Run（限时选主、
// 上层超时传导），deadline 触发瞬间恰有一次在途续约——真实 client 的请求
// ctx 派生自调用方 ctx，该请求以 context.DeadlineExceeded 收场（不是
// Canceled）。此前 observe 只豁免 Canceled ⇒ 计划内的超时关停被记成失权，
// Run 返回 ErrLostLeadership 而非 ctx.Err()，按推荐用法即 log.Fatal + 失权
// 重启告警（#61/#81 要消灭的「退出原因错分类」反方向复发）。修复后 Run
// 必须返回 ctx.Err()。确定性构造：reactor 把持有期的 Update 挂住（模拟在途
// 续约），直到 WithTimeout 的 deadline 自然触发再放行并镜像真实 client 行为
// 返回 ctx.Err()——错误面与记账时序与生产一致；与 WithCancel 同构钉
// （TestRenewFailureWithConcurrentCancelSurfacesLostLeadership）互补，补齐
// 测试矩阵只覆盖 Canceled 一侧的缺口。
func TestRunWithTimeoutShutdownReturnsDeadlineExceeded(t *testing.T) {
	client := fake.NewSimpleClientset()
	var failRenew atomic.Bool
	inFlight := make(chan struct{}) // 持有期的续约 Update 已进入在途挂起
	var runCtx atomic.Value         // 调用方 ctx，供 reactor 镜像真实 client 的 ctx 传播
	client.PrependReactor("update", "leases",
		func(_ clienttesting.Action) (bool, runtime.Object, error) {
			if !failRenew.Load() {
				return false, nil, nil
			}
			select {
			case inFlight <- struct{}{}:
			default:
			}
			ctx := runCtx.Load().(context.Context)
			<-ctx.Done() // 挂住在途 Update，直到调用方 ctx done（deadline 触发）
			if err := ctx.Err(); err != nil {
				return true, nil, err // 真实 client：调用方 ctx 已 done 的在途请求以 ctx.Err() 收场
			}
			return true, nil, errors.New("lease update boom")
		})
	started := make(chan struct{})
	done := make(chan error, 1)
	// deadline 由 WithTimeout 真实触发（#108 场景本体）：当选后续约被挂住，
	// 等 deadline 自然到期即「deadline 触发瞬间恰有在途续约」
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	runCtx.Store(ctx)
	defer cancel()
	go func() {
		done <- Run(ctx, client, Config{
			LeaseName:        "app",
			Identity:         "e1",
			LeaseDuration:    2 * time.Second,
			RenewDeadline:    time.Second,
			RetryPeriod:      200 * time.Millisecond,
			OnStartedLeading: func(context.Context) { failRenew.Store(true); close(started) },
			OnStoppedLeading: func() {},
		})
	}()
	select {
	case <-started:
	case <-time.After(10 * time.Second):
		t.Fatal("not elected in time")
	}
	select {
	case <-inFlight:
	case <-time.After(10 * time.Second):
		t.Fatal("no in-flight renew update observed")
	}
	select {
	case err := <-done:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Run() = %v, want ctx.Err() = context.DeadlineExceeded (#108: WithTimeout shutdown with an in-flight renew must not be recorded as lost leadership)", err)
		}
		if errors.Is(err, ErrLostLeadership) {
			t.Fatalf("Run() = %v, must not be ErrLostLeadership (planned timeout shutdown is not a leadership loss)", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("Run did not return after deadline")
	}
}

// #105 回归：LeaderElectionConfig.Name 必须与 LeaseName 同值——它是
// client-go 选主指标（leaderOn/leaderOff/slowpathExercised）的 label 与
// Check() 报文里的 lease 名，与 LeaseMeta.Name（Lease 资源名）是**两个字段**。
// 此前未填 ⇒ NewLeaderElector 构造瞬间即以空串调 leaderOff，指标 label 恒为
// 空串（多 elector/多租户同进程无法区分是谁在选主）。经 client-go 包级
// MetricsProvider 钩子读出 oekit 实际填的 label 值（SetProvider 全局仅首次
// 生效，本包其余测试不装 provider；fake 客户端无网络动作）。
type sweepMetricRecorder struct {
	mu   sync.Mutex
	ons  []string
	offs []string
}

func (m *sweepMetricRecorder) On(name string) {
	m.mu.Lock()
	m.ons = append(m.ons, name)
	m.mu.Unlock()
}

func (m *sweepMetricRecorder) Off(name string) {
	m.mu.Lock()
	m.offs = append(m.offs, name)
	m.mu.Unlock()
}

func (m *sweepMetricRecorder) SlowpathExercised(string) {}

type sweepMetricProvider struct{ m *sweepMetricRecorder }

func (p sweepMetricProvider) NewLeaderMetric() clientleaderelection.LeaderMetric { return p.m }

func TestLeaderElectionConfigNameFeedsMetricLabel(t *testing.T) {
	rec := &sweepMetricRecorder{}
	clientleaderelection.SetProvider(sweepMetricProvider{rec})
	client := fake.NewSimpleClientset()
	cfg := sweepTestConfig(nil) // LeaseName "app"
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- Run(ctx, client, cfg)
	}()
	select {
	case err := <-done:
		t.Fatalf("Run returned early: %v", err)
	case <-time.After(1 * time.Second):
	}
	// 行为面：NewLeaderElector 构造瞬间即以 config.Name 调一次 leaderOff
	// （client-go leaderelection.go），当选时再调 leaderOn——读出指标实际
	// 收到的 label（client-go 的 LeaderElector.config 字段未导出，跨包不可
	// 白盒直读，MetricsProvider 钩子即其暴露面）
	rec.mu.Lock()
	defer rec.mu.Unlock()
	if len(rec.offs) == 0 || rec.offs[0] != cfg.LeaseName {
		t.Fatalf("leaderOff label = %v, want [%q] (#105: unset Name leaves metric label permanently empty, electors indistinguishable)", rec.offs, cfg.LeaseName)
	}
	if len(rec.ons) == 0 || rec.ons[len(rec.ons)-1] != cfg.LeaseName {
		t.Fatalf("leaderOn label = %v, want [%q] (#105)", rec.ons, cfg.LeaseName)
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
