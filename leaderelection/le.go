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
//	        // 当选：在这里跑单活逻辑；ctx 在失去领导权时被取消
//	        serve(ctx)
//	    },
//	    OnStoppedLeading: func() {
//	        log.Fatal("lost leadership, exiting for restart") // 典型：直接退出交由重启
//	    },
//	})
//
// Namespace 默认取 POD_NAMESPACE 环境变量，再回退 "default"；
// Identity 默认取 hostname。
package leaderelection

import (
	"context"
	"fmt"
	"os"
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
	// Identity 是候选人标识（默认 hostname；StatefulSet 场景建议显式传
	// Pod 名保证唯一）。
	Identity string
	// LeaseDuration 是租约时长（默认 15s，须大于 RenewDeadline）。
	LeaseDuration time.Duration
	// RenewDeadline 是续约窗口（默认 10s；窗口内续约失败即放弃领导权）。
	RenewDeadline time.Duration
	// RetryPeriod 是重试间隔（默认 2s）。
	RetryPeriod time.Duration
	// OnStartedLeading 必填：当选回调；其入参 ctx 在失去领导权时被取消，
	// 长驻逻辑必须监听该 ctx（非阻塞回调，返回即认为启动完成）。
	OnStartedLeading func(ctx context.Context)
	// OnStoppedLeading 失去领导权回调（必填建议：log.Fatal 退出交由重启）。
	OnStoppedLeading func()
	// OnNewLeader 新leader产生回调（含自己当选；identity 为新 leader 标识）。
	OnNewLeader func(identity string)
}

// New 创建 LeaderElector（不阻塞；调用方持有并自行 Run）。
func New(client kubernetes.Interface, cfg Config) (*leaderelection.LeaderElector, error) {
	if cfg.LeaseName == "" {
		return nil, fmt.Errorf("k8s leaderelection: lease name is empty")
	}
	if cfg.OnStartedLeading == nil {
		return nil, fmt.Errorf("k8s leaderelection: OnStartedLeading is required")
	}
	if cfg.OnStoppedLeading == nil {
		log.Warn("OnStoppedLeading not set; lost leadership will be silent")
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
			return nil, fmt.Errorf("k8s leaderelection: hostname: %w", err)
		}
		id = h
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
	if renewDeadline >= leaseDuration {
		return nil, fmt.Errorf("k8s leaderelection: renewDeadline (%s) must be < leaseDuration (%s)",
			renewDeadline, leaseDuration)
	}

	lock := &resourcelock.LeaseLock{
		LeaseMeta: metav1.ObjectMeta{Name: cfg.LeaseName, Namespace: ns},
		Client:    client.CoordinationV1(),
		LockConfig: resourcelock.ResourceLockConfig{
			Identity: id,
		},
	}
	le, err := leaderelection.NewLeaderElector(leaderelection.LeaderElectionConfig{
		Lock:            lock,
		LeaseDuration:   leaseDuration,
		RenewDeadline:   renewDeadline,
		RetryPeriod:     retryPeriod,
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: cfg.OnStartedLeading,
			OnStoppedLeading: cfg.OnStoppedLeading,
			OnNewLeader:      cfg.OnNewLeader,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("k8s leaderelection: new elector: %w", err)
	}
	log.Info("elector created", "lease", cfg.LeaseName, "namespace", ns, "identity", id)
	return le, nil
}

// Run 参与选主并阻塞：当选后 OnStartedLeading 运行；失去领导权或 ctx 取消
// 时返回。Run 内部已管理 goroutine，禁止裸并发调用同一 elector。
func Run(ctx context.Context, client kubernetes.Interface, cfg Config) error {
	le, err := New(client, cfg)
	if err != nil {
		return err
	}
	if cfg.OnStoppedLeading == nil {
		// 没有失权回调时，Run 返回即退出，调用方必须处理返回值
		defer log.Info("leadership run finished", "lease", cfg.LeaseName)
	}
	le.Run(ctx)
	return nil
}
