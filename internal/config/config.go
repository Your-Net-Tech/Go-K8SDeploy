package config

import (
	"os"
	"time"

	"gopkg.in/yaml.v3"

	"k8s-deploy/internal/cluster"
)

type Config struct {
	Project    string               `yaml:"project"`
	Clusters   []cluster.Cluster    `yaml:"clusters"`
	Cluster    Cluster              `yaml:"cluster"` // legacy single cluster support
	Registry   Registry             `yaml:"registry"`
	Services   []Service            `yaml:"services"`
	Manifests  []string             `yaml:"manifests"`
	Pipeline   Pipeline             `yaml:"pipeline"`
	Approval   Approval             `yaml:"approval"`
	RBAC       RBAC                 `yaml:"rbac"`
	Strategy   string               `yaml:"strategy"` // single, multi-active, multi-passive
}

type Cluster struct {
	Context   string `yaml:"context"`
	Namespace string `yaml:"namespace"`
}

type Registry struct {
	Address  string `yaml:"address"`
	Insecure bool   `yaml:"insecure"`
	PushTo   string `yaml:"push_to"` // which cluster hosts the registry
}

type Service struct {
	Name       string            `yaml:"name"`
	Dockerfile string            `yaml:"dockerfile"`
	Context    string            `yaml:"context"`
	Image      string            `yaml:"image"`
	BuildArgs  map[string]string `yaml:"build_args,omitempty"`
}

type Pipeline struct {
	HealthCheckTimeout time.Duration `yaml:"health_check_timeout"`
	HealthCheckPoll    time.Duration `yaml:"health_check_poll"`
	RollbackOnFailure  bool          `yaml:"rollback_on_failure"`
	KeepBuildHistory   int           `yaml:"keep_build_history"`
	BuildParallel      bool          `yaml:"build_parallel"`
}

type Approval struct {
	Required    bool     `yaml:"required"`
	Approvers   []string `yaml:"approvers"` // telegram chat_ids or usernames
	Channels    []string `yaml:"channels"`  // telegram, slack
	Timeout     time.Duration `yaml:"timeout"`
	AutoApprove bool          `yaml:"auto_approve"` // for testing
}

type RBAC struct {
	Enabled     bool      `yaml:"enabled"`
	DefaultRole string    `yaml:"default_role"` // viewer, developer, admin
	Roles       []Role    `yaml:"roles"`
}

type Role struct {
	Name      string   `yaml:"name"`
	Allow     []string `yaml:"allow"`     // verbs: apply, rollback, view, etc.
	Resources []string `yaml:"resources"` // which manifests/apps
}

func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	cfg := &Config{
		Pipeline: Pipeline{
			HealthCheckTimeout: 5 * time.Minute,
			HealthCheckPoll:    10 * time.Second,
			RollbackOnFailure:  true,
			KeepBuildHistory:   50,
		},
		Approval: Approval{
			Timeout: 30 * time.Minute,
		},
		Strategy: "single",
	}
	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, err
	}

	// Compat: se Cluster antigo, copia para Clusters
	if len(cfg.Clusters) == 0 && cfg.Cluster.Context != "" {
		cfg.Clusters = []cluster.Cluster{{
			Name:      "default",
			Context:   cfg.Cluster.Context,
			Namespace: cfg.Cluster.Namespace,
			Primary:   true,
		}}
	}
	if len(cfg.Clusters) == 0 {
		cfg.Clusters = []cluster.Cluster{{
			Name: "default", Context: "default",
			Namespace: "default", Primary: true,
		}}
	}
	return cfg, nil
}