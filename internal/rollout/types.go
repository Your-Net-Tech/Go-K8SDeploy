// Package rollout implementa o engine de progressive delivery.
//
// Define a propria definicao de Rollout (em vez de usar Argo Rollouts CRD).
// Um Rollout eh um conjunto de strategies aplicadas em ordem sobre um Service.
package rollout

import (
	"fmt"
	"time"
)

type Strategy string

const (
	StrategyRecreate Strategy = "recreate"
	StrategyRolling  Strategy = "rolling"
	StrategyCanary   Strategy = "canary"
	StrategyBlueGreen Strategy = "blueGreen"
	StrategyABTest    Strategy = "abTest"
)

type Rollout struct {
	APIVersion string `yaml:"apiVersion"`
	Kind       string `yaml:"kind"`
	Metadata    Metadata `yaml:"metadata"`

	Spec Spec `yaml:"spec"`
}

type Metadata struct {
	Name      string `yaml:"name"`
	Namespace string `yaml:"namespace"`
	Labels    map[string]string `yaml:"labels,omitempty"`
}

type Spec struct {
	// Strategy: qual tipo de rollout fazer
	Strategy Strategy `yaml:"strategy"`

	// Service: qual Service do K8s este Rollout controla
	Service Service `yaml:"service"`

	// Replicas: numero desejado de pods
	Replicas int `yaml:"replicas"`

	// Selector: seleciona os pods (app=foo)
	Selector map[string]string `yaml:"selector"`

	// Template: pod template completo (igual Deployment.spec.template)
	// Suportado como embedded string yaml para flexibilidade
	Template string `yaml:"template"`

	// Strategy-specific configs
	Rolling    *RollingUpdate    `yaml:"rollingUpdate,omitempty"`
	Canary     *CanaryStrategy   `yaml:"canary,omitempty"`
	BlueGreen  *BlueGreenStrategy `yaml:"blueGreen,omitempty"`
	ABTest     *ABTestStrategy   `yaml:"abTest,omitempty"`

	// Analysis automacao
	Analysis *Analysis `yaml:"analysis,omitempty"`

	// Hooks para pre/post
	PreDeploy  []Hook `yaml:"preDeploy,omitempty"`
	PostDeploy []Hook `yaml:"postDeploy,omitempty"`
}

type Service struct {
	Name string `yaml:"name"`
	Port int    `yaml:"port"`
}

type RollingUpdate struct {
	MaxSurge       string `yaml:"maxSurge"`
	MaxUnavailable string `yaml:"maxUnavailable"`
}

type CanaryStrategy struct {
	// Steps em ordem de progressao
	Steps []Step `yaml:"steps"`

	// Trafico peso (0-100)
	TrafficSplit TrafficSplit `yaml:"trafficSplit"`

	// Verificacao entre steps
	Analysis *Analysis `yaml:"analysis,omitempty"`

	// Pause final antes de promover pra 100%
	FinalPauseDuration time.Duration `yaml:"finalPauseDuration"`
}

type Step struct {
	// Percentual de trafego da versao NOVA (stable fica com 100-percent)
	SetWeight int `yaml:"setWeight"`

	// Pausar por duracao
	Pause *Pause `yaml:"pause,omitempty"`
}

type Pause struct {
	Duration     time.Duration `yaml:"duration,omitempty"`
	UntilApproved bool         `yaml:"untilApproved,omitempty"`
	Message      string        `yaml:"message,omitempty"`
}

type TrafficSplit struct {
	// Onde fazer traffic split
	// Ingress: NGINX Ingress com annotations
	// Service: direto no Service selector (blue/green swap)
	// Manual: usuario controla o peso
	Method string `yaml:"method"` // ingress, service, manual

	// Nome do Ingress (se method=ingress)
	IngressName string `yaml:"ingressName,omitempty"`
}

type BlueGreenStrategy struct {
	// Service fica apontando pra versao "active"
	// Quando promover, troca
	PreviewService string `yaml:"previewService"`
	ActiveService  string `yaml:"activeService"`

	// Verificacao pos-deploy
	PreviewReplicaCount int `yaml:"previewReplicaCount"`
	AutoPromotionEnabled bool `yaml:"autoPromotionEnabled"`
	AutoPromotionSeconds int `yaml:"autoPromotionSeconds"`
}

type ABTestStrategy struct {
	// A/B por header
	HeaderName string `yaml:"headerName"`

	// Cabecalhos que vao pra versao B
	MatchHeaders map[string]string `yaml:"matchHeaders"`

	// Cabecalhos que vao pra versao A (default)
	DefaultHeaders map[string]string `yaml:"defaultHeaders"`
}

type Analysis struct {
	// Conditions declarativas PRÓPRIAS (sem Prometheus/sem terceiros)
	Conditions []Condition `yaml:"conditions"`

	// Failure limit
	FailureLimit int `yaml:"failureLimit"`

	// Intervalo de check
	Interval time.Duration `yaml:"interval"`

	// Hooks
	Webhooks []WebhookCheck `yaml:"webhooks,omitempty"`
}

// Condition é uma check própria sem dependência externa
type Condition struct {
	Type         string            `yaml:"type"`         // failed_restarts, crashed, oom_killed, error_log_matches, http_probes
	Max          int               `yaml:"max,omitempty"`
	PodCount     int               `yaml:"pod_count,omitempty"`
	Pattern      string            `yaml:"pattern,omitempty"`
	URL          string            `yaml:"url,omitempty"`
	Method       string            `yaml:"method,omitempty"`
	StatusCodes  []int             `yaml:"status_codes,omitempty"`
	Timeout      time.Duration      `yaml:"timeout,omitempty"`
	Interval     time.Duration      `yaml:"interval,omitempty"`
	Count        int               `yaml:"count,omitempty"`
	Headers      map[string]string `yaml:"headers,omitempty"`
	Body         string            `yaml:"body,omitempty"`
}

type AnalysisTemplate struct {
	Name      string `yaml:"name"`
	Template  string `yaml:"template"`
	Count     int `yaml:"count"`
	SuccessCondition string `yaml:"successCondition"`
}

type WebhookCheck struct {
	Name    string            `yaml:"name"`
	URL     string            `yaml:"url"`
	Method  string            `yaml:"method"`
	Headers map[string]string `yaml:"headers,omitempty"`
	Body    string            `yaml:"body,omitempty"`
	Timeout time.Duration      `yaml:"timeout"`
}

type Hook struct {
	Type string `yaml:"type"` // exec, http, prometheus
	Spec string `yaml:"spec"`
}

// RolloutState persiste onde o rollout estah
type State struct {
	RolloutRef string `json:"rollout"`
	Phase      Phase  `json:"phase"`
	CurrentStep int   `json:"currentStep"`
	StartedAt   time.Time `json:"startedAt"`
	UpdatedAt   time.Time `json:"updatedAt"`
	Revision    int    `json:"revision"`
	AnalysisRun *AnalysisRun `json:"analysisRun,omitempty"`
}

type Phase string

const (
	PhaseHealthy    Phase = "Healthy"
	PhaseProgressing Phase = "Progressing"
	PhasePaused      Phase = "Paused"
	PhaseDegraded    Phase = "Degraded"
	PhaseFailed      Phase = "Failed"
)

type AnalysisRun struct {
	Name string `json:"name"`
	Status string `json:"status"` // running, success, failed
	StartedAt time.Time `json:"startedAt"`
	FinishedAt *time.Time `json:"finishedAt,omitempty"`
	Message string `json:"message,omitempty"`
}

func (r *Rollout) Validate() error {
	if r.Metadata.Name == "" {
		return fmt.Errorf("metadata.name required")
	}
	if r.Spec.Strategy == "" {
		r.Spec.Strategy = StrategyRolling
	}
	if r.Spec.Replicas <= 0 {
		r.Spec.Replicas = 1
	}
	if r.Spec.Service.Name == "" {
		return fmt.Errorf("spec.service.name required")
	}
	return nil
}

func (r *Rollout) String() string {
	return fmt.Sprintf("Rollout{%s/%s, strategy=%s, replicas=%d}",
		r.Metadata.Namespace, r.Metadata.Name,
		r.Spec.Strategy, r.Spec.Replicas)
}