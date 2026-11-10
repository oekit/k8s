# Changelog — oekit/k8s

所有 notable 变更记录如下。格式参考 [Keep a Changelog](https://keepachangelog.com/)。

## [Unreleased]

- fix(k8s): InCluster 弱判定截断回退链（#79）——InCluster 判定对齐 rest.InClusterConfig，显式 KUBECONFIG 优先于 in-cluster，SA 试加载失败回退 kubeconfig 并告警

关联问题单：oekit/k8s#79。

- fix(k8s): QPS/Burst 负值被静默改写为默认 50/100（#80）——负值透传即禁用客户端限流（client-go 契约），0 仍取默认

关联问题单：oekit/k8s#80。

## [v0.2.2] — 2026-11-11

- - build: 依赖地板对齐（floor sync，无代码变更）

## [v0.2.1] — 2026-11-10

- - build: 依赖地板对齐（floor sync，无代码变更）

## [v0.2.0] — 2026-11-09

- feat(leaderelection): 暴露 ReleaseOnCancel——优雅关停立即释放 Lease（#62）
- docs(leaderelection): OnStoppedLeading 语义修正——优雅关停/从未当选不再 log.Fatal（#61）

关联问题单：k8s#61、k8s#62。

## [v0.1.10] — 2026-11-08

- build: 依赖地板对齐——本版把 oekit 依赖地板抬到各依赖当前最新已发布版本（matrix-tools/scripts/align_floors.py 维护轮）

## [v0.1.9] — 2026-11-07

- build: 依赖地板对齐 concurrency v0.28.1（+registry nacos v0.1.9 嵌套）

## [v0.1.8] — 2026-11-07

- fix(leaderelection): New 补校验 retryPeriod < renewDeadline×1.2（#39）
- fix(leaderelection): OnStoppedLeading 必填——nil 返回参数错误，文档同步（#41）
- fix(leaderelection): Run 上抛失权/取消原因——新增 ErrLostLeadership 哨兵，不再恒 nil（#42）
- docs(leaderelection): OnStartedLeading「阻塞回调」语义修正——独立 goroutine 中阻塞至 ctx 取消（#43）

关联问题单：k8s#39、k8s#41、k8s#42、k8s#43。

## [v0.1.7] — 2026-11-06

- build: 矩阵依赖地板对齐（第二轮发版后置）

## [v0.1.6] — 2026-11-06

- build: 矩阵依赖地板对齐（维护轮 #233）

## [v0.1.4] — 2026-10-23

- build: 矩阵内依赖地板对齐（align_floors 不动点）：logx v0.7.0 → v0.7.1（#23）

## [v0.1.3] — 2026-10-19

- build: 矩阵内依赖地板对齐（align_floors 不动点）：logx v0.6.0 → v0.7.0

## [v0.1.2] — 2026-10-17

- fix(deps): logx 地板 v0.5.0 → v0.6.0（零代码改动）

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
