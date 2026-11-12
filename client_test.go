package k8s

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/tools/clientcmd"
)

const testKubeconfig = `apiVersion: v1
kind: Config
clusters:
- cluster:
    server: https://192.168.1.10:6443
  name: test
contexts:
- context:
    cluster: test
    user: test
  name: test
current-context: test
users:
- name: test
  user:
    token: abc
`

func writeKubeconfig(t *testing.T) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "kubeconfig")
	if err := os.WriteFile(p, []byte(testKubeconfig), 0o600); err != nil {
		t.Fatalf("write kubeconfig: %v", err)
	}
	return p
}

func TestInCluster(t *testing.T) {
	t.Setenv("KUBERNETES_SERVICE_HOST", "")
	t.Setenv("KUBERNETES_SERVICE_PORT", "")
	if InCluster() {
		t.Fatal("InCluster should be false without env")
	}
	// 半注入环境（env 注入但 ServiceAccount token 不可加载）不得判为集群内，
	// 否则 kubeconfig 回退链被截断（#79）。
	t.Setenv("KUBERNETES_SERVICE_HOST", "10.0.0.1")
	t.Setenv("KUBERNETES_SERVICE_PORT", "443")
	if _, statErr := os.Stat("/var/run/secrets/kubernetes.io/serviceaccount/token"); statErr != nil {
		if InCluster() {
			t.Fatal("InCluster should be false in half-injected env (env set but SA token unreadable)")
		}
		return
	}
	if !InCluster() {
		t.Fatal("InCluster should be true with env and loadable SA token")
	}
}

// #79：半注入环境（in-cluster env 注入但 ServiceAccount token 不可加载）
// 下有效 KUBECONFIG 不得被无视——KUBECONFIG 显式设置即显式意图（kubectl
// 同语义），优先于 in-cluster 自动探测。
func TestRESTConfigKubeconfigEnvWinsOverInClusterEnv(t *testing.T) {
	p := writeKubeconfig(t)
	t.Setenv("KUBECONFIG", p)
	t.Setenv("KUBERNETES_SERVICE_HOST", "10.0.0.1")
	t.Setenv("KUBERNETES_SERVICE_PORT", "443")
	rc, err := RESTConfig(Config{})
	if err != nil {
		t.Fatalf("RESTConfig with valid KUBECONFIG in half-injected env: %v", err)
	}
	if rc.Host != "https://192.168.1.10:6443" {
		t.Fatalf("Host = %q, want KUBECONFIG host", rc.Host)
	}
}

// #79：半注入环境下 in-cluster 试加载失败不得截断回退链——继续走
// ~/.kube/config 默认规则，而非直接报错。（clientcmd 在包初始化时固化
// RecommendedHomeFile，HOME 覆盖不生效，这里直接注入该导出变量。）
func TestRESTConfigHalfInClusterEnvFallsBackToHomeKubeconfig(t *testing.T) {
	homeKubeconfig := filepath.Join(t.TempDir(), "config")
	if err := os.WriteFile(homeKubeconfig, []byte(testKubeconfig), 0o600); err != nil {
		t.Fatalf("write kubeconfig: %v", err)
	}
	origHomeFile := clientcmd.RecommendedHomeFile
	clientcmd.RecommendedHomeFile = homeKubeconfig
	defer func() { clientcmd.RecommendedHomeFile = origHomeFile }()
	t.Setenv("KUBECONFIG", "")
	t.Setenv("KUBERNETES_SERVICE_HOST", "10.0.0.1")
	t.Setenv("KUBERNETES_SERVICE_PORT", "443")
	rc, err := RESTConfig(Config{})
	if err != nil {
		t.Fatalf("RESTConfig should fall back to ~/.kube/config in half-injected env: %v", err)
	}
	if rc.Host != "https://192.168.1.10:6443" {
		t.Fatalf("Host = %q, want ~/.kube/config host", rc.Host)
	}
}

func TestRESTConfigFromKubeconfigPath(t *testing.T) {
	p := writeKubeconfig(t)
	rc, err := RESTConfig(Config{KubeconfigPath: p})
	if err != nil {
		t.Fatalf("RESTConfig: %v", err)
	}
	if rc.Host != "https://192.168.1.10:6443" {
		t.Fatalf("Host = %q", rc.Host)
	}
	if rc.QPS != defaultQPS || rc.Burst != defaultBurst {
		t.Fatalf("QPS/Burst = %v/%v; want defaults", rc.QPS, rc.Burst)
	}
	if rc.UserAgent != defaultUserAgent {
		t.Fatalf("UserAgent = %q", rc.UserAgent)
	}
	if rc.Timeout != 0 {
		t.Fatalf("Timeout = %v; want 0 (stream-safe default)", rc.Timeout)
	}

	rc2, err := RESTConfig(Config{KubeconfigPath: p, QPS: 10, Burst: 20, Timeout: 5_000_000_000, UserAgent: "svc"})
	if err != nil {
		t.Fatalf("RESTConfig overrides: %v", err)
	}
	if rc2.QPS != 10 || rc2.Burst != 20 || rc2.UserAgent != "svc" {
		t.Fatalf("overrides lost: %+v", rc2)
	}
}

func TestRESTConfigFromKubeconfigEnv(t *testing.T) {
	p := writeKubeconfig(t)
	t.Setenv("KUBECONFIG", p)
	rc, err := RESTConfig(Config{})
	if err != nil {
		t.Fatalf("RESTConfig via KUBECONFIG: %v", err)
	}
	if rc.Host != "https://192.168.1.10:6443" {
		t.Fatalf("Host = %q", rc.Host)
	}
}

// #80：QPS/Burst 负值是 client-go 契约的「禁用客户端限流」（迁移自裸
// client-go 的 QPS:-1/Burst:-1 场景），必须透传，不得被默认 50/100 改写。
func TestRESTConfigNegativeQPSBurstDisablesRateLimit(t *testing.T) {
	p := writeKubeconfig(t)
	rc, err := RESTConfig(Config{KubeconfigPath: p, QPS: -1, Burst: -1})
	if err != nil {
		t.Fatalf("RESTConfig: %v", err)
	}
	if rc.QPS != -1 || rc.Burst != -1 {
		t.Fatalf("QPS/Burst = %v/%v, want -1/-1 (client-go: negative disables client-side rate limiting)", rc.QPS, rc.Burst)
	}
}

// #92：Timeout 负值无 client-go 契约（rest.Config 负 Timeout 只会让请求
// 即刻超时），与 QPS/Burst 的「负值=禁用限流」不同——按 0 处理（不设
// 单请求超时），语义已在 godoc 明示，不得静默透传负值。
func TestRESTConfigNegativeTimeoutTreatedAsUnset(t *testing.T) {
	p := writeKubeconfig(t)
	rc, err := RESTConfig(Config{KubeconfigPath: p, Timeout: -time.Second})
	if err != nil {
		t.Fatalf("RESTConfig: %v", err)
	}
	if rc.Timeout != 0 {
		t.Fatalf("Timeout = %v, want 0 (negative has no client-go contract, treated as unset, #92)", rc.Timeout)
	}
}

func TestRESTConfigNoSource(t *testing.T) {
	// 无 kubeconfig、无 in-cluster 环境时必须显式报错
	t.Setenv("KUBECONFIG", filepath.Join(t.TempDir(), "missing"))
	t.Setenv("KUBERNETES_SERVICE_HOST", "")
	t.Setenv("KUBERNETES_SERVICE_PORT", "")
	if _, err := RESTConfig(Config{}); err == nil {
		t.Fatal("RESTConfig without any source should fail")
	}
}

func TestNewDynamicClient(t *testing.T) {
	p := writeKubeconfig(t)
	dc, err := NewDynamicClient(Config{KubeconfigPath: p})
	if err != nil {
		t.Fatalf("NewDynamicClient: %v", err)
	}
	if dc == nil {
		t.Fatal("dynamic client nil")
	}
}

// #99 回归：负值 = 禁用客户端限流是**两字段联动**语义——任一字段为负即
// QPS/Burst 都置 -1（client-go 的禁用契约是 QPS:-1/Burst:-1 成对）。
// 此前两字段各自独立补默认/透传，单独设 Burst:-1 产出 QPS=50/Burst=-1：
// NewClientset 构造期被 client-go 拒绝（burst is required to be greater
// than 0...），NewDynamicClient 更糟——限流器建成 rate.NewLimiter(50,-1)，
// 构造成功但每个请求都被挡回。
func TestRESTConfigSingleNegativeFieldLinksBothToDisabled(t *testing.T) {
	p := writeKubeconfig(t)
	cases := []struct {
		name string
		cfg  Config
	}{
		{"仅 Burst:-1", Config{KubeconfigPath: p, Burst: -1}},
		{"仅 QPS:-1", Config{KubeconfigPath: p, QPS: -1}},
		{"QPS 正值 + Burst:-1", Config{KubeconfigPath: p, QPS: 100, Burst: -1}},
		{"QPS:-1 + Burst 正值", Config{KubeconfigPath: p, QPS: -1, Burst: 200}},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			rc, err := RESTConfig(tt.cfg)
			if err != nil {
				t.Fatalf("RESTConfig: %v", err)
			}
			if rc.QPS != -1 || rc.Burst != -1 {
				t.Fatalf("QPS/Burst = %v/%v, want -1/-1（#99：禁用限流必须两字段联动，产出的配置要在 client-go 侧可用）", rc.QPS, rc.Burst)
			}
		})
	}
}

// #99 功能回归：单独设 Burst:-1 时产出的配置在 client-go 侧可用——
// dynamic 客户端能正常发请求（修复前限流器 rate.NewLimiter(50,-1) 把每个
// 请求都挡回：rate: Wait(n=1) exceeds limiter's burst -1）。
func TestNewDynamicClient_SingleNegativeBurstIssuesRequests(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"apiVersion":"v1","kind":"PodList","items":[]}`))
	}))
	t.Cleanup(srv.Close)

	p := filepath.Join(t.TempDir(), "kubeconfig")
	kc := strings.Replace(testKubeconfig, "https://192.168.1.10:6443", srv.URL, 1)
	if err := os.WriteFile(p, []byte(kc), 0o600); err != nil {
		t.Fatalf("write kubeconfig: %v", err)
	}

	dc, err := NewDynamicClient(Config{KubeconfigPath: p, Burst: -1})
	if err != nil {
		t.Fatalf("NewDynamicClient(Burst:-1): %v", err)
	}
	pods, err := dc.Resource(schema.GroupVersionResource{Version: "v1", Resource: "pods"}).
		Namespace("default").List(context.Background(), metav1.ListOptions{})
	if err != nil {
		t.Fatalf("dynamic List err = %v（#99：禁用客户端限流的配置必须可用，不得被限流器挡回）", err)
	}
	if pods == nil {
		t.Fatal("List returned nil list")
	}
}
