// Package leaderelection 提供基于 Kubernetes Lease 的选主简化封装：
// 一个二进制多副本部署时单活运行（定时任务分片、controller、缓存刷新等
// 场景的兜底互斥），底层即 kube-controller-manager 同款 leader election
// 协议（coordination.k8s.io/lease），对 apiserver 仅有 Lease 的
// get/create/update 权限要求。
//
// # 快速接入
//
//	cs, _ := k8s.NewClientset(k8s.Config{})
//	err := leaderelection.Run(ctx, cs, leaderelection.Config{
//	    LeaseName: "my-app",
//	    OnStartedLeading: func(ctx context.Context) {
//	        // 当选：阻塞跑单活逻辑，直至 ctx 在失去领导权时被取消
//	        serve(ctx)
//	    },
//	    OnStoppedLeading: func() {}, // client-go 要求非 nil；退出原因用 Run 返回值区分
//	})
//	// 退出原因用 Run 返回值区分，勿在 OnStoppedLeading 里无条件 log.Fatal：
//	//   失权（续约失败放弃领导权）返回 ErrLostLeadership——此时才退出交由重启
//	//   （含续约失败与取消同窗：失权优先上抛，不被掩蔽成 ctx.Err()，#81）；
//	//   ctx 取消返回 ctx.Err()——优雅关停（含从未当选的副本，多副本部署里
//	//   它们是多数），正常退出即可；否则每次发布/缩容所有副本都以非零码
//	//   退出，把真实失权告警淹没。
//	if errors.Is(err, leaderelection.ErrLostLeadership) {
//	    log.Fatal("lost leadership, exiting for restart")
//	}
//
// Namespace 默认取 POD_NAMESPACE 环境变量，再回退 "default"；
// Identity 默认取「hostname_pid_随机后缀」（进程唯一，#100）——hostname
// 是主机级标识，同主机两进程 / 同 Pod 两容器会互认身份、双双当选，打破
// 兜底互斥；Deployment/StatefulSet 建议显式传 Pod 名。
//
// Run 返回后应尽快退出进程，不要复用同一进程以同一 Identity 再入选举
// （重新参与选主在语义上等同于未退出，等于主动放弃提前释放的价值）。
package leaderelection

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/oekit/logx"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
)

var log = logx.Delegating("k8s.leaderelection")

const (
	defaultLeaseDuration = 15 * time.Second
	defaultRenewDeadline = 10 * time.Second
	defaultRetryPeriod   = 2 * time.Second
	defaultNamespace     = "default"
)

// Config 是选主配置。
type Config struct {
	// LeaseName 必填：Lease 资源名，同一 LeaseName 的副本参与同一场选主。
	LeaseName string
	// Namespace 是 Lease 所在命名空间（默认 POD_NAMESPACE 环境变量 → "default"）。
	Namespace string
	// Identity 是候选人标识（默认「hostname_pid_随机后缀」，进程唯一，#100；
	// Deployment/StatefulSet 场景建议显式传 Pod 名——可读且有稳定的语义
	// 归属；同主机多进程 / 同 Pod 多容器场景**必须**用默认值或自行保证
	// 进程唯一，hostname 是主机级标识，两进程同 identity 会让 client-go
	// 的「租约已被他人持有」判定失效，两副本同时当选）。
	Identity string
	// LeaseDuration 是租约时长（默认 15s，须大于 RenewDeadline；**必须为
	// 整秒且 ≥1s**，#101——client-go 把租约按整秒写入 Lease 资源，亚秒值
	// 被截断为 0 秒 ⇒ 竞争者恒判「租约已过期」⇒ 不同 identity 的两个副本
	// 同时当选；1500ms 这类非整秒会被截短，对外声明的租约比实际短）。
	LeaseDuration time.Duration
	// RenewDeadline 是续约窗口（默认 10s；窗口内续约失败即放弃领导权，
	// 须满足 > RetryPeriod*1.2，见 New 的校验）。
	RenewDeadline time.Duration
	// RetryPeriod 是重试间隔（默认 2s；client-go 协议要求显著小于
	// RenewDeadline，否则续约窗口内只有一次重试机会）。
	RetryPeriod time.Duration
	// OnStartedLeading 必填：当选回调，在独立 goroutine 中运行，应阻塞至
	// 其入参 ctx 取消（ctx 在失去领导权时被取消）；快速返回不会结束选主，
	// 只会留下空转的续约循环，单活职责随即失效。
	OnStartedLeading func(ctx context.Context)
	// OnStoppedLeading 必填：elector 结束回调——client-go 在 LeaderElector
	// 退出时无条件触发，不区分退出原因：失去领导权、ctx 取消优雅关停、
	// 乃至从未当选就退出（client-go 文档明示 "even if it did not start
	// leading"）都会调用，因此它不等于「失去领导权」。是否失权请以 Run
	// 的返回值为准（ErrLostLeadership）；勿在此回调里无条件 log.Fatal——
	// 否则非 leader 副本的每次正常关停（滚动发布/缩容）都会以非零码退出。
	// client-go 的 LeaderElector 要求该回调非 nil，缺失会导致构造失败。
	OnStoppedLeading func()
	// OnNewLeader 新leader产生回调（含自己当选；identity 为新 leader 标识）。
	OnNewLeader func(identity string)
	// ReleaseOnCancel 控制 ctx 取消（优雅关停）时是否立即释放 Lease
	// （透传 client-go LeaderElectionConfig.ReleaseOnCancel，默认 false
	// 保持既有行为）。
	//
	// false（默认）：关停不释放 Lease，继任者须等租约自然过期才能接管——
	// 按 LeaseDuration 缺省 15s 计，每次滚动发布/缩容最长 15s 无人单活。
	//
	// true：ctx 取消后立即清空 Lease 的 holderIdentity，继任者在 RetryPeriod
	// 量级内即可接管（从释放到继任者当选仍有一小段选主空窗，但不再是
	// 15s 量级）。
	//
	// 启用前提（client-go 明示的安全约束）：取消 ctx 前必须保证受 Lease
	// 保护的单活逻辑已经完成/排空——释放后继任者可立刻当选，若本副本
	// 仍有在飞单活逻辑，会出现两个副本同时执行关键路径。
	ReleaseOnCancel bool
}

// newElector 是选主构造内核（Run 与测试消费；历史兼容薄壳 New 已按
// #90 的弃用承诺于 v0.3.0 删除），额外返回续约事件记录器（#81 失权判定用）。
func newElector(client kubernetes.Interface, cfg Config) (*leaderelection.LeaderElector, *renewRecorder, error) {
	if cfg.LeaseName == "" {
		return nil, nil, fmt.Errorf("k8s leaderelection: lease name is empty")
	}
	if cfg.OnStartedLeading == nil {
		return nil, nil, fmt.Errorf("k8s leaderelection: OnStartedLeading is required")
	}
	if cfg.OnStoppedLeading == nil {
		return nil, nil, fmt.Errorf("k8s leaderelection: OnStoppedLeading is required")
	}
	ns := cfg.Namespace
	if ns == "" {
		ns = os.Getenv("POD_NAMESPACE")
	}
	if ns == "" {
		ns = defaultNamespace
	}
	id := cfg.Identity
	if id == "" {
		h, err := os.Hostname()
		if err != nil {
			return nil, nil, fmt.Errorf("k8s leaderelection: hostname: %w", err)
		}
		// #100：默认 identity 必须**进程唯一**——hostname 是主机级标识，
		// 同主机两进程 / 同 Pod 两容器（sidecar 与主容器）identity 相同 ⇒
		// client-go 的「租约已被他人持有且未过期」拦截被 IsLeader()（比对
		// holder identity）短路 ⇒ 两副本同时当选，兜底互斥被打破且无告警。
		// 三层组合：hostname 区分主机、pid 区分同主机多进程、随机后缀兜底
		// pid 复用（容器内 pid 回绕）与跨主机撞名。
		id = h + "_" + strconv.Itoa(os.Getpid()) + "_" + randomSuffix()
	}
	leaseDuration, renewDeadline, retryPeriod := cfg.LeaseDuration, cfg.RenewDeadline, cfg.RetryPeriod
	if leaseDuration == 0 {
		leaseDuration = defaultLeaseDuration
	}
	if renewDeadline == 0 {
		renewDeadline = defaultRenewDeadline
	}
	if retryPeriod == 0 {
		retryPeriod = defaultRetryPeriod
	}
	// #101：租约必须为整秒且 ≥1s——client-go 把租约按整秒写入 Lease 资源
	// （resourcelock/leaselock.go: int32(LeaseDuration/time.Second)），亚秒值
	// 被截断为 0 秒 ⇒ 竞争者 isLeaseValid 恒判「已过期」⇒ 不同 identity 的
	// 两个副本也同时当选（静默脑裂，现象会被误判为业务 bug）。1500ms 这类
	// 非整秒会被截短 0.5s，对外声明的租约比实际短。构造期 fail-fast 与本
	// 函数其余时长校验同款。
	if leaseDuration < time.Second || leaseDuration%time.Second != 0 {
		return nil, nil, fmt.Errorf("k8s leaderelection: leaseDuration (%s) must be a whole number of seconds >= 1s (client-go writes the lease duration in whole seconds; sub-second values are truncated to 0 and break mutual exclusion)",
			leaseDuration)
	}
	if renewDeadline >= leaseDuration {
		return nil, nil, fmt.Errorf("k8s leaderelection: renewDeadline (%s) must be < leaseDuration (%s)",
			renewDeadline, leaseDuration)
	}
	// retryPeriod 须显著小于 renewDeadline（client-go 建议留 1.2 倍抖动裕度）：
	// 否则续约窗口内仅一次重试机会，网络抖动一次就放弃领导权。
	// retryPeriod*6/5 即 retryPeriod*1.2（整数运算，避免浮点转换）。
	if minRenew := retryPeriod * 6 / 5; renewDeadline <= minRenew {
		return nil, nil, fmt.Errorf("k8s leaderelection: renewDeadline (%s) must be > retryPeriod (%s) * 1.2",
			renewDeadline, retryPeriod)
	}

	lock := &resourcelock.LeaseLock{
		LeaseMeta: metav1.ObjectMeta{Name: cfg.LeaseName, Namespace: ns},
		Client:    client.CoordinationV1(),
		LockConfig: resourcelock.ResourceLockConfig{
			Identity: id,
		},
	}
	rec := &renewRecorder{Interface: lock}
	le, err := leaderelection.NewLeaderElector(leaderelection.LeaderElectionConfig{
		// #105：Name 是 client-go 选主指标（leaderOn/leaderOff/slowpathExercised）
		// 的 label 与 Check() 报文里的 lease 名，与 LeaseMeta.Name（Lease 资源名）
		// 是两个不同字段——此前未填，指标 label 恒为空串（多 elector/多租户
		// 同进程无法区分）。缺省与 LeaseName 同值（调用方经由 Config.LeaseName
		// 可配置）；LeaseName 构造期必填非空，label 不会为空。
		Name:          cfg.LeaseName,
		Lock:          rec,
		LeaseDuration: leaseDuration,
		RenewDeadline: renewDeadline,
		RetryPeriod:   retryPeriod,
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: cfg.OnStartedLeading,
			OnStoppedLeading: cfg.OnStoppedLeading,
			OnNewLeader:      cfg.OnNewLeader,
		},
		ReleaseOnCancel: cfg.ReleaseOnCancel,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("k8s leaderelection: new elector: %w", err)
	}
	log.Info("elector created", "lease", cfg.LeaseName, "namespace", ns, "identity", id)
	return le, rec, nil
}

// randomSuffix 生成 4 字节随机十六进制后缀（#100 默认 identity 的第三层）。
// crypto/rand 失败极罕见（OS 熵源不可用），退化为纳秒时间戳后缀——兜底层
// 只要求「同主机同 pid 复用窗口内不重」。
func randomSuffix() string {
	var b [4]byte
	if _, err := rand.Read(b[:]); err != nil {
		return strconv.FormatInt(time.Now().UnixNano(), 36)
	}
	return hex.EncodeToString(b[:])
}

// ErrLostLeadership 表示 elector 因失去领导权（续约窗口内续约失败）而退出。
// 失权意味着本副本已不适合继续单活职责，典型处理是进程退出交由重启，
// 而非把该错误当成功吞掉。
var ErrLostLeadership = errors.New("k8s leaderelection: lost leadership")

// renewRecorder 包装 resourcelock.Interface，在续约事件发生时刻记录续约
// 结果（#81）：失权判定不得放在 le.Run 返回之后事后读 ctx.Err()——「续约
// 失败放弃领导权」与「ctx 取消」两条退出路径并存时（续约开始失败后
// RenewDeadline 窗口内触发 SIGTERM/滚动重启），失权会被掩蔽为 ctx.Err()，
// 失权副本按「优雅关停」零退出（workflow#61 同构错误面掩蔽）。全部方法
// 由 le.Run 所在 goroutine 调用（acquire/renew/release/OnStoppedLeading 均
// 在该 goroutine 内），Run 返回后读取无并发；按共享状态口径仍以 atomic 保护。
type renewRecorder struct {
	resourcelock.Interface
	leading      atomic.Bool // 已成功 create/update 持有 Lease
	renewFailing atomic.Bool // 持有期间续约失败在飞（成功续约/主动释放即清零）
}

// Create 实现 resourcelock.Interface，结果按事件同刻入账。
func (r *renewRecorder) Create(ctx context.Context, ler resourcelock.LeaderElectionRecord) error {
	err := r.Interface.Create(ctx, ler)
	r.observe(ler, err)
	return err
}

// Update 实现 resourcelock.Interface，结果按事件同刻入账。
func (r *renewRecorder) Update(ctx context.Context, ler resourcelock.LeaderElectionRecord) error {
	err := r.Interface.Update(ctx, ler)
	r.observe(ler, err)
	return err
}

// isCancellationErr 判断错误链是否含 context 取消类 sentinel（workflow#61
// 分类口径：Canceled 与 DeadlineExceeded 同属取消类副作用，#108）。
func isCancellationErr(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

// observe 入账一次 create/update 的续约结果：holderIdentity 非空是持有方
// 的续约（acquire 首次成功后进入持有态），置空是 ReleaseOnCancel 的主动
// 释放——成功即清账，优雅交棒不算失权。取消类失败是关停副作用，与
// workflow 的 isCancellationErr 完全同口径：Canceled 与 DeadlineExceeded
// 两个 sentinel 都豁免（#108）——调用方 WithTimeout/WithDeadline 包住 Run
// 时，deadline 触发瞬间的在途续约以 context.DeadlineExceeded 收场（不是
// Canceled），此前只豁免 Canceled ⇒ 计划内的超时关停被记成失权，Run 返回
// ErrLostLeadership 而非 ctx.Err()，按本包推荐用法即 log.Fatal + 失权重启
// 告警。真失权（连接拒绝、冲突、租约被抢、续约窗口耗尽）不受影响，仍入账；
// 客户端请求级超时（如 rest.Config.Timeout）恰逢调用方 ctx 已 done 的窗口
// 里同样被豁免，但真实失权仍会经 OnStoppedLeading（ctx 尚在）路径如实上抛。
func (r *renewRecorder) observe(ler resourcelock.LeaderElectionRecord, err error) {
	if ler.HolderIdentity == "" {
		if err == nil {
			r.leading.Store(false)
			r.renewFailing.Store(false)
		}
		return
	}
	if err == nil {
		r.leading.Store(true)
		r.renewFailing.Store(false)
		return
	}
	if r.leading.Load() && !isCancellationErr(err) {
		r.renewFailing.Store(true)
	}
}

// lost 判定失权信号：持有期间存在未被成功续约/主动释放清除的续约失败，
// 即「续约失败与 ctx 取消同窗」的失权（#81）。
func (r *renewRecorder) lost() bool {
	return r.leading.Load() && r.renewFailing.Load()
}

// Run 参与选主并阻塞：当选后 OnStartedLeading 在独立 goroutine 中运行；
// 失去领导权或 ctx 取消时返回，退出原因通过返回值上抛——失权退出（续约
// 失败放弃领导权）返回 ErrLostLeadership，ctx 取消返回 ctx.Err()（含从未
// 当选就因取消退出的场景）。失权与 ctx 取消并存时失权优先（真实失败优先
// 于取消副作用，workflow#61 分类口径，#81）：续约失败与取消同窗（续约
// 已在失败、RenewDeadline 窗口内触发关停）按失权上抛，ctx 取消信息仅留
// 在报文，errors.Is(err, context.Canceled) 不成立——失权副本不得被消费方
// 按「优雅关停」零退出吞掉告警。OnStoppedLeading 是「elector 退出」回调
// （client-go 无条件触发，不区分失权/关停，见 Config.OnStoppedLeading），
// 先于返回触发；退出原因以 Run 的返回值为准。Run 内部已管理 goroutine，
// 禁止裸并发调用同一 elector。
func Run(ctx context.Context, client kubernetes.Interface, cfg Config) error {
	// 失权判定放在事件同刻（#81）：① OnStoppedLeading 触发瞬间 ctx 尚在，
	// 即 client-go 已放弃领导权（续约失败语义）；② 续约失败在飞由
	// renewRecorder 在续约失败事件时刻入账，覆盖「续约失败与取消同窗」——
	// 此前判定放在 le.Run 返回后读 ctx.Err()，两条退出路径并存时失权被
	// 掩蔽成 ctx.Err()，失权副本按「优雅关停」零退出。
	var stoppedWhileAlive atomic.Bool
	if cfg.OnStoppedLeading != nil {
		stopped := cfg.OnStoppedLeading
		cfg.OnStoppedLeading = func() {
			if ctx.Err() == nil {
				stoppedWhileAlive.Store(true)
			}
			stopped()
		}
	}
	le, rec, err := newElector(client, cfg)
	if err != nil {
		return err
	}
	le.Run(ctx)
	if stoppedWhileAlive.Load() || rec.lost() {
		if ctxErr := ctx.Err(); ctxErr != nil {
			// 失权优先于取消副作用；ctx 取消信息保留在报文里。
			return fmt.Errorf("%w (ctx also canceled: %v)", ErrLostLeadership, ctxErr)
		}
		return ErrLostLeadership
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		return ctxErr
	}
	return ErrLostLeadership
}
