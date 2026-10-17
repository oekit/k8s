package k8s

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/oekit/logx"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

var log = logx.Delegating("k8s")

const (
	defaultQPS        = float32(50)
	defaultBurst      = 100
	defaultUserAgent  = "oekit-k8s"
	defaultKubeconfig = ".kube/config"
)

// Config 是客户端构建配置。
type Config struct {
	// KubeconfigPath 显式指定 kubeconfig 路径；设置后不再探测 in-cluster。
	// 为空时：集群内环境（POD）自动使用 ServiceAccount，本机回退
	// KUBECONFIG 环境变量，再回退 ~/.kube/config。
	KubeconfigPath string `yaml:"kubeconfigPath"`
	// QPS/Burst 是客户端限流（默认 50/100——client-go 默认 5/10 对
	// controller 类负载偏保守）。
	QPS   float32 `yaml:"qps"`
	Burst int     `yaml:"burst"`
	// Timeout 是单请求超时；0 表示不设（Watch/长流式调用必须保持 0）。
	Timeout time.Duration `yaml:"timeout"`
	// UserAgent 是请求 UA（默认 "oekit-k8s"，便于 apiserver 审计归因）。
	UserAgent string `yaml:"userAgent"`
	// InsecureSkipTLSVerify 跳过 TLS 校验（仅开发/联调，勿用于生产）。
	InsecureSkipTLSVerify bool `yaml:"insecureSkipTLSVerify"`
}

// InCluster 判断当前进程是否运行在集群内（POD 环境注入了非空的
// KUBERNETES_SERVICE_HOST/PORT）。
func InCluster() bool {
	return os.Getenv("KUBERNETES_SERVICE_HOST") != "" && os.Getenv("KUBERNETES_SERVICE_PORT") != ""
}

// RESTConfig 按配置解析出 *rest.Config（连接参数已应用）。
//
// 解析顺序：显式 KubeconfigPath > in-cluster（ServiceAccount）>
// KUBECONFIG 环境变量 > ~/.kube/config。
func RESTConfig(cfg Config) (*rest.Config, error) {
	var rc *rest.Config
	var err error
	switch {
	case cfg.KubeconfigPath != "":
		rc, err = clientcmd.BuildConfigFromFlags("", cfg.KubeconfigPath)
		if err != nil {
			return nil, fmt.Errorf("k8s: load kubeconfig %s: %w", cfg.KubeconfigPath, err)
		}
		log.Info("kubeconfig loaded", "path", cfg.KubeconfigPath)
	case InCluster():
		rc, err = rest.InClusterConfig()
		if err != nil {
			return nil, fmt.Errorf("k8s: in-cluster config: %w", err)
		}
		log.Info("in-cluster config loaded")
	default:
		// 默认加载规则：KUBECONFIG（可多路径冒号分隔）→ ~/.kube/config
		loadingRules := clientcmd.NewDefaultClientConfigLoadingRules()
		rc, err = clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
			loadingRules, &clientcmd.ConfigOverrides{}).ClientConfig()
		if err != nil {
			return nil, fmt.Errorf("k8s: no usable kubeconfig (set KUBECONFIG or run in-cluster): %w", err)
		}
		log.Info("kubeconfig loaded from default rules")
	}
	if cfg.QPS > 0 {
		rc.QPS = cfg.QPS
	} else if rc.QPS == 0 {
		rc.QPS = defaultQPS
	}
	if cfg.Burst > 0 {
		rc.Burst = cfg.Burst
	} else if rc.Burst == 0 {
		rc.Burst = defaultBurst
	}
	if cfg.Timeout > 0 {
		rc.Timeout = cfg.Timeout
	}
	ua := cfg.UserAgent
	if ua == "" {
		ua = defaultUserAgent
	}
	rc.UserAgent = ua
	if cfg.InsecureSkipTLSVerify {
		rc.Insecure = true
	}
	return rc, nil
}

// NewClientset 构建带全部内置资源组的客户端（典型入口）。
func NewClientset(cfg Config) (*kubernetes.Clientset, error) {
	rc, err := RESTConfig(cfg)
	if err != nil {
		return nil, err
	}
	cs, err := kubernetes.NewForConfig(rc)
	if err != nil {
		return nil, fmt.Errorf("k8s: new clientset: %w", err)
	}
	return cs, nil
}

// NewDynamicClient 构建非资源型动态客户端（CRD/GVR 操作）。
func NewDynamicClient(cfg Config) (dynamic.Interface, error) {
	rc, err := RESTConfig(cfg)
	if err != nil {
		return nil, err
	}
	dc, err := dynamic.NewForConfig(rc)
	if err != nil {
		return nil, fmt.Errorf("k8s: new dynamic client: %w", err)
	}
	return dc, nil
}

// DefaultKubeconfigPath 返回当前用户家目录下的默认 kubeconfig 路径
// （HOME 不可用时返回空串）。
func DefaultKubeconfigPath() string {
	home, err := os.UserHomeDir()
	if err != nil || home == "" {
		return ""
	}
	return filepath.Join(home, defaultKubeconfig)
}
