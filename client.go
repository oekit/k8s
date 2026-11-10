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
	// 为空时：显式 KUBECONFIG 环境变量（显式意图，与 kubectl 同语义）优先，
	// 其次集群内环境（POD）自动使用 ServiceAccount，最后回退 ~/.kube/config；
	// in-cluster 试加载失败不截断回退链，自动回退 kubeconfig 并告警（#79）。
	KubeconfigPath string `yaml:"kubeconfigPath"`
	// QPS/Burst 是客户端限流：0 = 未配置（默认 50/100——client-go 默认 5/10
	// 对 controller 类负载偏保守）；正数 = 覆盖；负值 = 禁用客户端限流
	// （client-go 契约，迁移自裸 client-go 的 QPS:-1/Burst:-1 场景，#80）。
	QPS   float32 `yaml:"qps"`
	Burst int     `yaml:"burst"`
	// Timeout 是单请求超时；0 表示不设（Watch/长流式调用必须保持 0）。
	Timeout time.Duration `yaml:"timeout"`
	// UserAgent 是请求 UA（默认 "oekit-k8s"，便于 apiserver 审计归因）。
	UserAgent string `yaml:"userAgent"`
	// InsecureSkipTLSVerify 跳过 TLS 校验（仅开发/联调，勿用于生产）。
	InsecureSkipTLSVerify bool `yaml:"insecureSkipTLSVerify"`
}

// InCluster 判断当前进程是否具备可用的集群内 ServiceAccount 配置：除 POD
// 环境注入的非空 KUBERNETES_SERVICE_HOST/PORT 外，还要求 ServiceAccount
// token 可加载（与 client-go rest.InClusterConfig 的判定对齐）。只看两个
// env 的弱判定在半注入环境（CI runner Pod、kubectl exec 残留 export 等）
// 会误判为集群内，截断 kubeconfig 回退链（#79）。
func InCluster() bool {
	if os.Getenv("KUBERNETES_SERVICE_HOST") == "" || os.Getenv("KUBERNETES_SERVICE_PORT") == "" {
		return false
	}
	_, err := rest.InClusterConfig()
	return err == nil
}

// RESTConfig 按配置解析出 *rest.Config（连接参数已应用）。
//
// 解析顺序：显式 KubeconfigPath > KUBECONFIG 环境变量（显式意图，与
// kubectl 同语义）> in-cluster（ServiceAccount）> ~/.kube/config。
// in-cluster 试加载失败不截断回退链，自动回退 kubeconfig 并告警（#79）。
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
	case os.Getenv("KUBECONFIG") != "":
		// KUBECONFIG 显式设置 = 显式意图（与 kubectl 同语义），优先于
		// in-cluster 自动探测：半注入/借壳环境（CI runner Pod、kubectl exec
		// 残留 export）里挂载的 kubeconfig 才是本进程想用的凭据；反向
		// （SA token 可读但身份是环境自带 SA）也不会静默用错凭证（#79）。
		rc, err = loadDefaultKubeconfig()
		if err != nil {
			return nil, err
		}
	case InCluster():
		rc, err = rest.InClusterConfig()
		if err != nil {
			// InCluster 判定已与 rest.InClusterConfig 对齐，此处仅剩判定后
			// token 被卸载之类的竞态：不截断回退链，退回 kubeconfig 并告警（#79）。
			log.Warn("in-cluster config unavailable, falling back to kubeconfig", logx.ErrKey, err)
			inClusterErr := err
			rc, err = loadDefaultKubeconfig()
			if err != nil {
				return nil, fmt.Errorf("k8s: in-cluster config (%v) and kubeconfig fallback: %w", inClusterErr, err)
			}
		} else {
			log.Info("in-cluster config loaded")
		}
	default:
		rc, err = loadDefaultKubeconfig()
		if err != nil {
			return nil, err
		}
	}
	// 0 = 未配置取默认；非 0 一律透传——负值是 client-go 契约的
	// 「禁用客户端限流」，不得吞成默认值（#80）。
	if cfg.QPS != 0 {
		rc.QPS = cfg.QPS
	} else if rc.QPS == 0 {
		rc.QPS = defaultQPS
	}
	if cfg.Burst != 0 {
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

// loadDefaultKubeconfig 按默认规则加载 kubeconfig：KUBECONFIG 环境变量
// （可多路径冒号分隔）→ ~/.kube/config。
func loadDefaultKubeconfig() (*rest.Config, error) {
	loadingRules := clientcmd.NewDefaultClientConfigLoadingRules()
	rc, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
		loadingRules, &clientcmd.ConfigOverrides{}).ClientConfig()
	if err != nil {
		return nil, fmt.Errorf("k8s: no usable kubeconfig (set KUBECONFIG or run in-cluster): %w", err)
	}
	log.Info("kubeconfig loaded from default rules")
	return rc, nil
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
