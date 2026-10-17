// Package k8s 提供 Kubernetes 客户端封装：RESTConfig/Clientset/DynamicClient
// 构建（in-cluster 自动优先，回退 kubeconfig）与 Lease 选主简化封装
// （leaderelection 子包，适配主备/单活场景）。
//
// 零可观测性副作用：不自动注入任何 tracing/logger 后端，SDK 内部统一
// logx.Delegating("k8s") 懒解析。
//
// # 快速接入
//
//	// 集群内（Pod）自动走 ServiceAccount；开发机走 KUBECONFIG/~/.kube/config
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
//	    OnStoppedLeading: func() { log.Fatal("lost leadership") },
//	})
package k8s
