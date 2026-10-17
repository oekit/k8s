package k8s

import (
	"os"
	"path/filepath"
	"testing"
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
	t.Setenv("KUBERNETES_SERVICE_HOST", "10.0.0.1")
	t.Setenv("KUBERNETES_SERVICE_PORT", "443")
	if !InCluster() {
		t.Fatal("InCluster should be true with env")
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
