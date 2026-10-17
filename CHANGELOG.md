# Changelog — oekit/k8s

所有 notable 变更记录如下。格式参考 [Keep a Changelog](https://keepachangelog.com/)。

## [v0.1.1] — 2026-10-17

- fix(deps): 内部依赖地板 logx v0.2.0 → v0.5.0；go 维持 1.26.0——k8s.io/client-go 生态声明 go1.26，暂不做生态级回退

## [v0.1.0] — 2026-10-17

首个版本。

### 目录结构

```
github.com/oekit/k8s                   # package k8s — 客户端构建（模块根）
└── leaderelection/                    # package leaderelection — Lease 选主简化封装
```

### 能力面

- **k8s（根包）**：`RESTConfig` 解析顺序显式 kubeconfig → in-cluster →
  KUBECONFIG → ~/.kube/config（无可用心源显式报错 fail-loud）；QPS/Burst
  默认 50/100（client-go 默认 5/10 对 controller 偏保守）；`UserAgent`
  默认 oekit-k8s 便于 apiserver 审计归因；`Timeout` 默认 0 保证 Watch 安全；
  `NewClientset`/`NewDynamicClient`/`InCluster()`/`DefaultKubeconfigPath()`
- **leaderelection**：coordination.k8s.io/lease 选主（kube-controller-manager
  同款协议），`New` 校验 LeaseName/OnStartedLeading 必填与 renew < lease，
  `Run` 阻塞当选；Namespace 默认 POD_NAMESPACE → default，Identity 默认
  hostname；OnStartedLeading/OnStoppedLeading/OnNewLeader 回调直通
- 测试：临时 kubeconfig 解析、KUBECONFIG 环境变量回退、无可用心源报错、
  fake clientset 双 elector 选主与失权接棒（不依赖真实 apiserver），`-race` 全绿
