package k8s_test

// doc.go「快速接入」选主示例的可编译性守卫（#104）：示例此前用了非法限定符
// k8s.leaderelection——leaderelection 是独立子包，Go 不允许经父包限定符访问
// 子包，逐字照抄编译不过（undefined: k8s.leaderelection）；而注释不是可执行
// 的 Example，CI 抓不到。本文件把示例按修复后的真实 import 路径做成 Example
// 函数：无 Output 注释 ⇒ go test 仅编译不执行（不触发 NewClientset 的
// kubeconfig 探测与选主网络动作），从此示例可编译性由 CI 守住。与 README
// 「快速使用」、leaderelection 包文档的同一写法三处对齐。

import (
	"context"
	"errors"
	"log"

	"github.com/oekit/k8s"
	"github.com/oekit/k8s/leaderelection"
)

// serve 是示例里的单活逻辑占位（阻塞至失去领导权时 ctx 被取消）。
func serve(ctx context.Context) { <-ctx.Done() }

func Example_quickStart() {
	cs, err := k8s.NewClientset(k8s.Config{})
	if err != nil {
		log.Fatal(err)
	}
	runErr := leaderelection.Run(context.Background(), cs, leaderelection.Config{
		LeaseName:        "my-app",
		OnStartedLeading: serve,
		OnStoppedLeading: func() {}, // client-go 要求非 nil；退出原因用 Run 返回值区分
	})
	if errors.Is(runErr, leaderelection.ErrLostLeadership) {
		log.Fatal("lost leadership, exiting for restart")
	}
}
