# k8s

> Kubernetes 客户端封装——客户端构建 / Lease 选主

> 🌐 项目主页：**https://oekit.github.io/** · 名称由来：**O**pen **E**asy **Kit**（开源 · 易用 · 工具集合）
> 🤖 AI Agent：本仓规范与硬规则见 [AGENTS.md](AGENTS.md)；机器可读索引见 [llms.txt](https://oekit.github.io/llms.txt)（接口签名以源码为准）。

## 安装

```bash
go get github.com/oekit/k8s@v0.1.0
```

## 这个仓库做什么

`k8s` 是 **Kubernetes 客户端封装**：一套配置解析出 `rest.Config` 并构建
`Clientset`/`DynamicClient`（in-cluster 自动优先，逐级回退 kubeconfig），
外加 Lease 选主简化封装（`leaderelection`）适配主备/单活场景。

零可观测性副作用：不自动注入任何 tracing/logger 后端；SDK 内部统一
`logx.Delegating("k8s")` 懒解析。

## 组成

| 包 | import 路径 | 做什么 |
|---|---|---|
| `k8s`（模块根包） | `github.com/oekit/k8s` | `RESTConfig`（解析顺序：显式 KubeconfigPath → in-cluster → KUBECONFIG → ~/.kube/config；QPS/Burst 默认 50/100、UserAgent 默认 oekit-k8s）、`NewClientset`、`NewDynamicClient`（CRD/GVR）、`InCluster()` 环境探测 |
| `leaderelection` | `github.com/oekit/k8s/leaderelection` | Lease 选主简化封装：`New`/`Run`（阻塞当选）+ `OnStartedLeading`/`OnStoppedLeading`/`OnNewLeader` 回调；Namespace 默认 POD_NAMESPACE → default；仅要求 Lease get/create/update 权限 |

## 快速使用

```go
import (
    k8s "github.com/oekit/k8s"
    "github.com/oekit/k8s/leaderelection"
)

// 集群内自动走 ServiceAccount；开发机走 KUBECONFIG/~/.kube/config
cs, err := k8s.NewClientset(k8s.Config{})
pods, _ := cs.CoreV1().Pods("default").List(ctx, metav1.ListOptions{})

// 主备单活：只有 leader 跑业务，失权即退出交由重启
err = leaderelection.Run(ctx, cs, leaderelection.Config{
    LeaseName: "my-app",
    OnStartedLeading: func(ctx context.Context) { serve(ctx) },
    OnStoppedLeading: func() { log.Fatal("lost leadership") },
})
```

### 客户端限流

client-go 默认 QPS 5/Burst 10 对 controller 类负载偏保守；本仓默认
QPS 50/Burst 100，可用 `Config{QPS: ..., Burst: ...}` 覆盖。
`Timeout` 默认 0（不设单请求超时）——Watch/长流式调用必须保持 0。

## 边界

- 不封装 informer/cache 体系（v0.1.0 YAGNI，直接用 client-go 原生）
- 不做 kubectl 类资源运维封装
- 测试：配置解析用临时 kubeconfig，选主用 fake clientset——不依赖真实 apiserver

## License

MIT
