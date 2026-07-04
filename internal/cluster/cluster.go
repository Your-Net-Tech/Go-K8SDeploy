package cluster

import (
	"fmt"
	"os/exec"
	"path/filepath"
	"sync"
	"time"
)

type Cluster struct {
	Name       string `yaml:"name"`
	Context    string `yaml:"context"`      // kubeconfig context
	Kubeconfig string `yaml:"kubeconfig"`   // optional path
	Namespace  string `yaml:"namespace"`
	Server     string `yaml:"server"`       // optional, for display
	Primary    bool   `yaml:"primary"`      // is this the primary cluster for multi-cluster deploys?
	Insecure   bool   `yaml:"insecure"`
	Labels     map[string]string `yaml:"labels,omitempty"`
}

type Registry struct {
	mu       sync.RWMutex
	clusters map[string]*Cluster
}

func NewRegistry() *Registry {
	return &Registry{
		clusters: map[string]*Cluster{},
	}
}

func (r *Registry) Add(c Cluster) error {
	if c.Name == "" {
		return fmt.Errorf("cluster name required")
	}
	if c.Context == "" && c.Kubeconfig == "" {
		return fmt.Errorf("cluster %s: context or kubeconfig required", c.Name)
	}
	c.Context = setDefault(c.Context, "default")
	c.Namespace = setDefault(c.Namespace, "default")
	r.mu.Lock()
	defer r.mu.Unlock()
	r.clusters[c.Name] = &c
	return nil
}

func (r *Registry) Get(name string) (*Cluster, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	c, ok := r.clusters[name]
	return c, ok
}

func (r *Registry) List() []*Cluster {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]*Cluster, 0, len(r.clusters))
	for _, c := range r.clusters {
		out = append(out, c)
	}
	return out
}

func (r *Registry) Primary() *Cluster {
	for _, c := range r.List() {
		if c.Primary {
			return c
		}
	}
	clusters := r.List()
	if len(clusters) > 0 {
		return clusters[0]
	}
	return nil
}

func (c *Cluster) Run(args ...string) (string, error) {
	cmdArgs := []string{}
	if c.Kubeconfig != "" {
		cmdArgs = append(cmdArgs, "--kubeconfig", c.Kubeconfig)
	}
	if c.Context != "" {
		cmdArgs = append(cmdArgs, "--context", c.Context)
	}
	if c.Insecure {
		cmdArgs = append(cmdArgs, "--insecure-skip-tls-verify")
	}
	cmdArgs = append(cmdArgs, args...)

	cmd := exec.Command("kubectl", cmdArgs...)
	out, err := cmd.CombinedOutput()
	return string(out), err
}

func (c *Cluster) Apply(manifest string) error {
	_, err := c.Run("apply", "-f", manifest)
	return err
}

func (c *Cluster) DryRun(manifest string) error {
	_, err := c.Run("apply", "--dry-run=client", "-f", manifest)
	return err
}

func (c *Cluster) WaitReady(namespace string, timeout time.Duration) error {
	_, err := c.Run("wait", "--for=condition=Ready",
		"pod", "-l", "app", "-n", namespace,
		"--timeout", timeout.String())
	return err
}

func (c *Cluster) Diff(manifest string) (string, error) {
	out, err := c.Run("diff", "-f", manifest, "--field-manager=k8s-deploy")
	return out, err
}

func (c *Cluster) Describe() string {
	kubeconfig := c.Kubeconfig
	if kubeconfig == "" {
		kubeconfig = "(default ~/.kube/config)"
	}
	if !filepath.IsAbs(kubeconfig) && kubeconfig != "(default ~/.kube/config)" {
		kubeconfig = "~/" + kubeconfig
	}
	return fmt.Sprintf("%s (%s, ns=%s, kubeconfig=%s)",
		c.Name, c.Context, c.Namespace, kubeconfig)
}

func setDefault(s, def string) string {
	if s == "" {
		return def
	}
	return s
}