// Package k8s 提供 Kubernetes 客户端封装：RESTConfig/Clientset/DynamicClient
// 构建（显式 kubeconfig → KUBECONFIG 显式意图 → in-cluster 自动 →
// ~/.kube/config 逐级回退）与 Lease 选主简化封装
// （leaderelection 子包，适配主备/单活场景）。
//
// 零可观测性副作用：不自动注入任何 tracing/logger 后端，SDK 内部统一
// logx.Delegating("k8s") 懒解析。
//
// # 快速接入
//
//	// 集群内（Pod）自动走 ServiceAccount；开发机走 KUBECONFIG/~/.kube/config。
//	// 显式 KUBECONFIG 环境变量即显式意图（与 kubectl 同语义），优先于
//	// in-cluster 自动探测；in-cluster 试加载失败自动回退 kubeconfig（#79）。
//	cs, err := k8s.NewClientset(k8s.Config{})
//
//	// 显式 kubeconfig
//	cs, err := k8s.NewClientset(k8s.Config{KubeconfigPath: "/path/to/kubeconfig"})
//
// 选主（同 kube-apiserver Lease 协议，二进制/多副本单活）：
//
//	err := k8s.leaderelection.Run(ctx, cs, k8s.leaderelection.Config{
//	    LeaseName: "my-app",
//	    OnStartedLeading: func(ctx context.Context) { serve(ctx) },
//	    OnStoppedLeading: func() {}, // client-go 要求非 nil；退出原因用 Run 返回值区分
//	})
//	// 失权（续约失败放弃领导权）返回 ErrLostLeadership——此时才退出交由重启
//	// （含续约失败与取消同窗：失权优先上抛，不被掩蔽成 ctx.Err()，#81）；
//	// ctx 取消返回 ctx.Err()——优雅关停（含从未当选的副本），正常退出即可，
//	// 勿无条件 log.Fatal（否则每次发布/缩容所有副本都非零退出）
//	if errors.Is(err, k8s.leaderelection.ErrLostLeadership) {
//	    log.Fatal("lost leadership, exiting for restart")
//	}
package k8s
