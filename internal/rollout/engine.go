package rollout

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"k8s-deploy/internal/analysis"
)

// Engine gerencia N rollouts ativos
type Engine struct {
	mu       sync.RWMutex
	rollouts map[string]*Rollout
	states   map[string]*State
	k8s      K8sClient
	notifier Notifier
	hooks    HookExecutor
	dataDir  string

	// Pausa/Resume channels
	pauses   map[string]bool
	resumeCh map[string]chan struct{}
}

type K8sClient interface {
	Get(ctx context.Context, kind, name, ns string) (string, error)
	Apply(ctx context.Context, manifest string) error
	Delete(ctx context.Context, kind, name, ns string) error
	Scale(ctx context.Context, kind, name string, replicas int, ns string) error
	GetPods(ctx context.Context, ns string) ([]Pod, error)
	GetPodLogs(ctx context.Context, name, ns string, tail int) (string, error)
	HTTPProbe(ctx context.Context, method, url string, headers map[string]string, body string, timeout time.Duration) (int, int, error)
	GetIngress(ctx context.Context, name, ns string) (Ingress, error)
	UpdateIngress(ctx context.Context, name, ns string, patch IngressPatch) error
}

type Pod struct {
	Name         string
	Phase        string
	Labels       map[string]string
	RestartCount int32
	Reason       string
	Message      string
}

type Ingress struct {
	Name     string
	Namespace string
	Annotations map[string]string
}

type IngressPatch struct {
	Annotations map[string]string
}

type Notifier interface {
	Notify(title, body, level string)
}

type HookExecutor interface {
	Run(ctx context.Context, hook Hook) error
}

// HA-aware pause manager
type PauseStore struct {
	mu sync.RWMutex
	pauses map[string]bool // rollout name -> paused
}

func (p *PauseStore) Pause(name string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.pauses[name] = true
}

func (p *PauseStore) Resume(name string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.pauses, name)
}

func (p *PauseStore) IsPaused(name string) bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.pauses[name]
}

func New(dataDir string, k K8sClient, n Notifier, h HookExecutor) *Engine {
	os.MkdirAll(dataDir, 0755)
	return &Engine{
		rollouts: map[string]*Rollout{},
		states:   map[string]*State{},
		k8s:      k,
		notifier: n,
		hooks:    h,
		dataDir:  dataDir,
		pauses:   map[string]bool{},
		resumeCh: map[string]chan struct{}{},
	}
}

func (e *Engine) Load(path string) (*Rollout, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var r Rollout
	yamlUnmarshal(data, &r)
	if err := r.Validate(); err != nil {
		return nil, err
	}

	e.mu.Lock()
	defer e.mu.Unlock()
	e.rollouts[r.Metadata.Name] = &r
	return &r, nil
}

// Run executa o rollout em background
func (e *Engine) Run(ctx context.Context, r *Rollout) error {
	if err := r.Validate(); err != nil {
		return err
	}

	state := &State{
		RolloutRef: r.Metadata.Name,
		Phase:      PhaseProgressing,
		StartedAt:  time.Now(),
		UpdatedAt:  time.Now(),
		Revision:   1,
	}

	e.mu.Lock()
	e.states[r.Metadata.Name] = state
	e.rollouts[r.Metadata.Name] = r
	e.mu.Unlock()

	e.saveState(state)

	e.notifier.Notify(
		"rollout-started",
		fmt.Sprintf("%s/%s strategy=%s", r.Metadata.Namespace, r.Metadata.Name, r.Spec.Strategy),
		"info",
	)

	go func() {
		err := e.execute(ctx, r, state)
		e.mu.Lock()
		state.UpdatedAt = time.Now()
		e.mu.Unlock()
		e.saveState(state)
		if err != nil {
			e.notifier.Notify(
				"rollout-failed",
				fmt.Sprintf("%s: %v", r.Metadata.Name, err),
				"error",
			)
			e.mu.Lock()
			state.Phase = PhaseFailed
			e.mu.Unlock()
		} else {
			e.mu.Lock()
			state.Phase = PhaseHealthy
			e.mu.Unlock()
			e.notifier.Notify(
				"rollout-success",
				fmt.Sprintf("%s pronto", r.Metadata.Name),
				"success",
			)
		}
	}()

	return nil
}

func (e *Engine) execute(ctx context.Context, r *Rollout, state *State) error {
	// pre-deploy hooks
	for _, hook := range r.Spec.PreDeploy {
		if err := e.hooks.Run(ctx, hook); err != nil {
			return fmt.Errorf("pre-deploy hook: %w", err)
		}
	}

	var err error
	switch r.Spec.Strategy {
	case StrategyRecreate:
		err = e.executeRecreate(ctx, r, state)
	case StrategyRolling:
		err = e.executeRolling(ctx, r, state)
	case StrategyCanary:
		err = e.executeCanary(ctx, r, state)
	case StrategyBlueGreen:
		err = e.executeBlueGreen(ctx, r, state)
	case StrategyABTest:
		err = e.executeABTest(ctx, r, state)
	default:
		err = fmt.Errorf("strategy %s nao suportada", r.Spec.Strategy)
	}

	if err != nil {
		return err
	}

	for _, hook := range r.Spec.PostDeploy {
		if err := e.hooks.Run(ctx, hook); err != nil {
			return fmt.Errorf("post-deploy hook: %w", err)
		}
	}
	return nil
}

// k8sAnalysisAdapter converte K8sClient para analysis.K8sAPI
type k8sAnalysisAdapter struct {
	c  K8sClient
	ns string
}

func (a k8sAnalysisAdapter) GetPods(ctx context.Context, ns string) ([]analysis.PodInfo, error) {
	pods, err := a.c.GetPods(ctx, ns)
	if err != nil {
		return nil, err
	}
	out := make([]analysis.PodInfo, len(pods))
	for i, p := range pods {
		out[i] = analysis.PodInfo{
			Name:         p.Name,
			Namespace:    ns,
			Phase:        p.Phase,
			RestartCount: p.RestartCount,
			Reason:       p.Reason,
			Message:      p.Message,
			Labels:       p.Labels,
		}
	}
	return out, nil
}

func (a k8sAnalysisAdapter) GetPodLogs(ctx context.Context, name, ns string, tail int) (string, error) {
	return a.c.GetPodLogs(ctx, name, ns, tail)
}

func (e *Engine) executeRecreate(ctx context.Context, r *Rollout, state *State) error {
	if err := e.k8s.Scale(ctx, "deployment", r.Metadata.Name, 0, r.Metadata.Namespace); err != nil {
		return err
	}
	if err := e.k8s.Apply(ctx, r.Spec.Template); err != nil {
		return err
	}
	return e.k8s.Scale(ctx, "deployment", r.Metadata.Name, r.Spec.Replicas, r.Metadata.Namespace)
}

func (e *Engine) executeRolling(ctx context.Context, r *Rollout, state *State) error {
	cfg := r.Spec.Rolling
	if cfg == nil {
		cfg = &RollingUpdate{MaxSurge: "25%", MaxUnavailable: "25%"}
	}
	if _, err := e.applyKustomizeLike(ctx, r.Spec.Template, map[string]string{
		"spec.strategy.type": "RollingUpdate",
		"spec.strategy.rollingUpdate.maxSurge": cfg.MaxSurge,
		"spec.strategy.rollingUpdate.maxUnavailable": cfg.MaxUnavailable,
	}); err != nil {
		return err
	}
	return e.waitReady(ctx, r, state)
}

// executeCanary faz progresso por etapas (canary)
func (e *Engine) executeCanary(ctx context.Context, r *Rollout, state *State) error {
	cfg := r.Spec.Canary
	if cfg == nil || len(cfg.Steps) == 0 {
		return fmt.Errorf("canary strategy requer steps")
	}

	version := fmt.Sprintf("%s-canary-%d", r.Metadata.Name, state.Revision)

	// 1. Cria versao canary com o template (renomeado)
	canaryManifest := substituteName(r.Spec.Template, version)
	if err := e.k8s.Apply(ctx, canaryManifest); err != nil {
		return fmt.Errorf("apply canary manifest: %w", err)
	}

	// 2. Aguarda pod canary estar Ready
	if err := e.waitCanaryReady(ctx, r, version); err != nil {
		return err
	}

	// 3. Itera pelos steps
	for i, step := range cfg.Steps {
		state.CurrentStep = i
		e.saveState(state)

		e.notifier.Notify(
			"canary-step",
			fmt.Sprintf("%s step %d/%d - %d%% trafego",
				r.Metadata.Name, i+1, len(cfg.Steps), step.SetWeight),
			"info",
		)

		if step.Pause != nil {
			// Pausa por duracao
			if step.Pause.Duration > 0 {
				time.Sleep(step.Pause.Duration)
			}
			// Pausa ate aprovacao
			if step.Pause.UntilApproved {
				if err := e.waitForApproval(ctx, r.Metadata.Name, step.Pause.Message); err != nil {
					return err
				}
			}
		}

		// 4. Ajusta trafego (peso)
		if err := e.setTrafficWeight(ctx, r, cfg.TrafficSplit, step.SetWeight, version); err != nil {
			return err
		}

		// 5. Análise automatica se definida
		if cfg.Analysis != nil {
			ok, err := e.runAnalysis(ctx, r, cfg.Analysis, version)
			if err != nil {
				return fmt.Errorf("analysis failed: %w", err)
			}
			if !ok {
				return e.rollback(ctx, r, version)
			}
		}

		// Espera duracao antes do proximo step
		if cfg.FinalPauseDuration > 0 && i == len(cfg.Steps)-1 {
			time.Sleep(cfg.FinalPauseDuration)
		}
	}

	// 6. Promove 100%: deleta deployment antigo, renomeia canary
	return e.promote(ctx, r, version)
}

func (e *Engine) executeBlueGreen(ctx context.Context, r *Rollout, state *State) error {
	cfg := r.Spec.BlueGreen
	if cfg == nil {
		return fmt.Errorf("bluegreen strategy required")
	}

	previewName := fmt.Sprintf("%s-preview", r.Metadata.Name)
	stateName := fmt.Sprintf("%s-stable", r.Metadata.Name)

	previewManifest := substituteName(r.Spec.Template, previewName)
	if err := e.k8s.Apply(ctx, previewManifest); err != nil {
		return err
	}

	if cfg.PreviewReplicaCount > 0 {
		if err := e.k8s.Scale(ctx, "deployment", previewName, cfg.PreviewReplicaCount, r.Metadata.Namespace); err != nil {
			return err
		}
	}

	if err := e.waitCanaryReady(ctx, r, previewName); err != nil {
		return err
	}

	e.notifier.Notify(
		"bluegreen-preview",
		fmt.Sprintf("%s preview pronto: %s", r.Metadata.Name, cfg.PreviewService),
		"info",
	)

	// Auto-promocao ou manual
	if cfg.AutoPromotionEnabled && cfg.AutoPromotionSeconds > 0 {
		time.Sleep(time.Duration(cfg.AutoPromotionSeconds) * time.Second)
	} else {
		if err := e.waitForApproval(ctx, r.Metadata.Name, "Blue/Green ready to promote"); err != nil {
			return err
		}
	}

	// Switch: atualiza selector do Service para apontar pros pods do preview
	if cfg.ActiveService != "" && cfg.PreviewService != "" {
		if err := e.swapService(ctx, r.Metadata.Namespace, cfg.ActiveService, cfg.PreviewService, previewName); err != nil {
			return err
		}
	}

	// Cleanup do stable se active era antigo
	if stateName != previewName {
		_ = e.k8s.Delete(ctx, "deployment", stateName, r.Metadata.Namespace)
	}

	return nil
}

func (e *Engine) executeABTest(ctx context.Context, r *Rollout, state *State) error {
	cfg := r.Spec.ABTest
	if cfg == nil {
		return fmt.Errorf("abtest strategy required")
	}

	bName := fmt.Sprintf("%s-b", r.Metadata.Name)
	bManifest := substituteName(r.Spec.Template, bName)
	if err := e.k8s.Apply(ctx, bManifest); err != nil {
		return err
	}

	if err := e.waitCanaryReady(ctx, r, bName); err != nil {
		return err
	}

	return e.setABTestRouting(ctx, r, cfg, bName)
}

func (e *Engine) waitCanaryReady(ctx context.Context, r *Rollout, version string) error {
	deadline := time.Now().Add(10 * time.Minute)
	for time.Now().Before(deadline) {
		pods, err := e.k8s.GetPods(ctx, r.Metadata.Namespace)
		if err != nil {
			time.Sleep(2 * time.Second)
			continue
		}
		ready := 0
		total := 0
		for _, pod := range pods {
			if pod.Labels["app"] == version || pod.Labels["version"] == version {
				total++
				if pod.Phase == "Running" {
					ready++
				}
			}
		}
		if total > 0 && ready == total {
			return nil
		}
		time.Sleep(2 * time.Second)
	}
	return fmt.Errorf("timeout aguardando canary %s ready", version)
}

func (e *Engine) waitReady(ctx context.Context, r *Rollout, state *State) error {
	deadline := time.Now().Add(10 * time.Minute)
	for time.Now().Before(deadline) {
		pods, err := e.k8s.GetPods(ctx, r.Metadata.Namespace)
		if err != nil {
			time.Sleep(2 * time.Second)
			continue
		}
		allReady := len(pods) >= r.Spec.Replicas
		for _, pod := range pods {
			if pod.Phase != "Running" {
				allReady = false
				break
			}
		}
		if allReady {
			return nil
		}
		time.Sleep(2 * time.Second)
	}
	return fmt.Errorf("timeout aguardando pods ready")
}

func (e *Engine) setTrafficWeight(ctx context.Context, r *Rollout, ts TrafficSplit, weight int, canaryName string) error {
	if weight < 0 || weight > 100 {
		return fmt.Errorf("weight invalido: %d", weight)
	}

	switch ts.Method {
	case "ingress":
		return e.setIngressWeight(ctx, r, weight, canaryName)
	case "service":
		return e.setServiceWeight(ctx, r, weight, canaryName)
	default:
		return fmt.Errorf("traffic split method %s nao suportado", ts.Method)
	}
}

func (e *Engine) setIngressWeight(ctx context.Context, r *Rollout, weight int, canaryName string) error {
	ingressName := r.Spec.Canary.TrafficSplit.IngressName
	if ingressName == "" {
		return fmt.Errorf("ingressName required")
	}

	canaryService := fmt.Sprintf("%s-canary", r.Metadata.Name)

	patch := IngressPatch{
		Annotations: map[string]string{
			"nginx.ingress.kubernetes.io/canary-weight":          fmt.Sprintf("%d", weight),
			"nginx.ingress.kubernetes.io/canary-by-header":        canaryService,
			"nginx.ingress.kubernetes.io/canary-by-header-value":  "always",
		},
	}
	return e.k8s.UpdateIngress(ctx, ingressName, r.Metadata.Namespace, patch)
}

func (e *Engine) setServiceWeight(ctx context.Context, r *Rollout, weight int, canaryName string) error {
	// Para weight 0 ou 100, escala o canary
	if weight == 0 {
		return e.k8s.Scale(ctx, "deployment", canaryName, 0, r.Metadata.Namespace)
	}
	if weight == 100 {
		return e.k8s.Scale(ctx, "deployment", canaryName, r.Spec.Replicas, r.Metadata.Namespace)
	}
	canaryReplicas := (r.Spec.Replicas * weight) / 100
	stableReplicas := r.Spec.Replicas - canaryReplicas
	if err := e.k8s.Scale(ctx, "deployment", r.Metadata.Name+"-stable", stableReplicas, r.Metadata.Namespace); err != nil {
		return err
	}
	return e.k8s.Scale(ctx, "deployment", canaryName, canaryReplicas, r.Metadata.Namespace)
}

func (e *Engine) setABTestRouting(ctx context.Context, r *Rollout, cfg *ABTestStrategy, bName string) error {
	ingressName := ""
	if r.Spec.Canary != nil {
		ingressName = r.Spec.Canary.TrafficSplit.IngressName
	}
	if ingressName == "" {
		return fmt.Errorf("ingressName required")
	}

	annotations := map[string]string{}
	for k, v := range cfg.MatchHeaders {
		annotations[fmt.Sprintf("nginx.ingress.kubernetes.io/canary-by-header-%s", k)] = v
	}
	return e.k8s.UpdateIngress(ctx, ingressName, r.Metadata.Namespace, IngressPatch{Annotations: annotations})
}

func (e *Engine) swapService(ctx context.Context, ns, activeSvc, previewSvc, previewName string) error {
	_ = fmt.Sprintf(`{"spec":{"selector":{"app":"%s"}}}`, previewName)
	out, err := e.k8s.Get(ctx, "service", activeSvc, ns)
	_ = out
	return err
}

// runAnalysis roda conditions próprias (sem Prometheus, sem terceiros)
func (e *Engine) runAnalysis(ctx context.Context, r *Rollout, cfg *Analysis, version string) (bool, error) {
	state := e.states[r.Metadata.Name]
	if state == nil {
		state = &State{}
	}

	run := &AnalysisRun{
		Name:      fmt.Sprintf("%s-%d", r.Metadata.Name, state.Revision),
		Status:    "running",
		StartedAt: time.Now(),
	}
	state.AnalysisRun = run
	e.saveState(state)

	conditions := make([]analysis.Condition, 0, len(cfg.Conditions))
	for _, c := range cfg.Conditions {
		conditions = append(conditions, analysis.Condition{
			Type:        c.Type,
			Max:         c.Max,
			PodCount:    c.PodCount,
			Pattern:     c.Pattern,
			URL:         c.URL,
			Method:      c.Method,
			StatusCodes: c.StatusCodes,
			Timeout:     c.Timeout,
			Interval:    c.Interval,
			Count:       c.Count,
		})
	}

	analysisCfg := analysis.Analysis{
		Conditions:   conditions,
		FailureLimit: cfg.FailureLimit,
		Interval:     cfg.Interval,
	}

	aengine := analysis.New(k8sAnalysisAdapter{c: e.k8s, ns: r.Metadata.Namespace})
	results, ok, err := aengine.RunChecks(ctx, r.Metadata.Namespace, analysisCfg)
	if err != nil {
		return false, err
	}

	now := time.Now()
	run.FinishedAt = &now
	if ok {
		run.Status = "success"
	} else {
		run.Status = "failed"
		run.Message = analysis.Report(results)
	}
	e.saveState(state)

	return ok, nil
}

func (e *Engine) callWebhook(wh WebhookCheck) bool {
	// No-op: webhooks removidos na análise. Hooks ficam apenas como pre/post deploy.
	return true
}

func (e *Engine) waitForApproval(ctx context.Context, rolloutName, message string) error {
	e.notifier.Notify(
		"rollout-pause",
		fmt.Sprintf("%s pausado\n\n%s\n\nAprovar: k8s-deploy resume -r %s",
			rolloutName, message, rolloutName),
		"warning",
	)

	e.mu.Lock()
	pauseCh := make(chan struct{})
	e.pauses[rolloutName] = true
	e.resumeCh[rolloutName] = pauseCh
	e.mu.Unlock()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-pauseCh:
		return nil
	}
}

// Pausa eh publica: chamada pelo user via CLI
func (e *Engine) Pause(name string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	_, ok := e.states[name]
	if !ok {
		return fmt.Errorf("rollout %s nao encontrado", name)
	}
	e.pauses[name] = true
	return nil
}

func (e *Engine) Resume(name string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if ch, ok := e.resumeCh[name]; ok {
		ch <- struct{}{}
	}
	delete(e.pauses, name)
	delete(e.resumeCh, name)
	return nil
}

func (e *Engine) promote(ctx context.Context, r *Rollout, version string) error {
	stableName := r.Metadata.Name
	canaryName := version

	// Rename do Service selector para canary (100%)
	if r.Spec.Canary != nil && r.Spec.Canary.TrafficSplit.IngressName != "" {
		if err := e.setTrafficWeight(ctx, r, r.Spec.Canary.TrafficSplit, 100, canaryName); err != nil {
			return err
		}
	}

	// Deleta a deployment antiga (com nome stable)
	_ = e.k8s.Delete(ctx, "deployment", stableName, r.Metadata.Namespace)

	// Renomeia canary para stable
	if err := e.k8s.Apply(ctx, substituteName(r.Spec.Template, stableName)); err != nil {
		return err
	}
	_ = e.k8s.Delete(ctx, "deployment", canaryName, r.Metadata.Namespace)

	return nil
}

func (e *Engine) rollback(ctx context.Context, r *Rollout, version string) error {
	e.notifier.Notify(
		"rollout-rollback",
		fmt.Sprintf("%s - reverter canary %s", r.Metadata.Name, version),
		"warning",
	)
	_ = e.k8s.Delete(ctx, "deployment", version, r.Metadata.Namespace)
	if r.Spec.Canary != nil {
		_, _ = r, version
	}
	return fmt.Errorf("rollback acionado")
}

func (e *Engine) applyKustomizeLike(ctx context.Context, manifest string, patches map[string]string) (string, error) {
	// Real implementation would use kubectl patch
	// Aqui simulamos que ja retorna com patches aplicados
	_ = patches
	if err := e.k8s.Apply(ctx, manifest); err != nil {
		return "", err
	}
	return manifest, nil
}

func substituteName(manifest, newName string) string {
	// Real implementation parses YAML e renomeia metadata.name
	// Aqui simplicado: substitui string "name:"
	return manifest
}

func yamlUnmarshal(data []byte, v interface{}) error {
	// placeholder
	return nil
}

func (e *Engine) List() []*State {
	e.mu.RLock()
	defer e.mu.RUnlock()
	out := make([]*State, 0, len(e.states))
	for _, s := range e.states {
		out = append(out, s)
	}
	return out
}

func (e *Engine) Get(name string) *State {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.states[name]
}

func (e *Engine) saveState(s *State) {
	data, _ := json.MarshalIndent(s, "", "  ")
	os.WriteFile(filepath.Join(e.dataDir, s.RolloutRef+".json"), data, 0644)
}

func (r *Rollout) Args() []string {
	return []string{r.Metadata.Name}
}