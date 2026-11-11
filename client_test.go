package k8s

import (
	"os"
	"path/filepath"
	"testing"
	"time"

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
