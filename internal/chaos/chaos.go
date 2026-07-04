// Package chaos implementa chaos testing embutido.
//
// Sem Chaos Monkey, Litmus, Chaos Mesh. 100% proprietario.
//
// Permite simular falhas em runtime para validar resiliencia.
package chaos

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

// Tipo de falha simulada
type FailureType string

const (
	FailureLatency   FailureType = "latency"     // adiciona delay
	FailureError     FailureType = "error"       // retorna erro
	FailureTimeout   FailureType = "timeout"     // bloqueia
	FailureCorrupt   FailureType = "corrupt"     // corrompe dados
	FailureDrop      FailureType = "drop"        // dropa mensagem
	FailureFlapping  FailureType = "flapping"    // alterna ok/falha
	FailurePartition FailureType = "partition"   // simula particao
)

// Experiment eh um teste de chaos configurado
type Experiment struct {
	Name      string
	Type      FailureType
	Target    string              // nome do componente alvo
	Severity  float64            // 0-1, probabilidade ou magnitude
	Duration  time.Duration
	Metadata  map[string]string

	// Latency-specific
	MinLatencyMS int
	MaxLatencyMS int

	// Error-specific
	ErrorMessage string

	mu       sync.Mutex
	running  bool
	startedAt time.Time
}

type Engine struct {
	mu          sync.RWMutex
	experiments map[string]*Experiment
	failures    atomic.Uint64
	recoveries  atomic.Uint64
}

func NewEngine() *Engine {
	return &Engine{
		experiments: map[string]*Experiment{},
	}
}

func (e *Engine) Register(exp *Experiment) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if _, ok := e.experiments[exp.Name]; ok {
		return fmt.Errorf("experiment %s ja registrado", exp.Name)
	}
	e.experiments[exp.Name] = exp
	return nil
}

// Start ativa o experiment por duracao
func (e *Engine) Start(name string, duration time.Duration) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	exp, ok := e.experiments[name]
	if !ok {
		return fmt.Errorf("experiment %s nao encontrado", name)
	}
	if exp.running {
		return fmt.Errorf("experiment ja rodando")
	}
	exp.running = true
	exp.startedAt = time.Now()
	go e.runExperiment(exp, duration)
	return nil
}

func (e *Engine) runExperiment(exp *Experiment, duration time.Duration) {
	timer := time.NewTimer(duration)
	defer timer.Stop()

	for {
		select {
		case <-timer.C:
			exp.mu.Lock()
			exp.running = false
			exp.mu.Unlock()
				e.recoveries.Add(1)
			return
		default:
		}
		time.Sleep(1 * time.Second)
	}
}

// Stop para experimento
func (e *Engine) Stop(name string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	exp, ok := e.experiments[name]
	if !ok {
		return fmt.Errorf("experiment nao encontrado")
	}
	if !exp.running {
		return fmt.Errorf("experiment nao esta rodando")
	}
	exp.running = false
	return nil
}

// Apply aplica falha aleatoria baseada no experiment
// Esta eh a funcao chamada pelos servicos para simular falhas
func (e *Engine) Apply(target string) error {
	e.mu.RLock()
	defer e.mu.RUnlock()

	for _, exp := range e.experiments {
		if exp.Target != target {
			continue
		}
		if !exp.running {
			continue
		}
		// probabilidade
		if rand.Float64() > exp.Severity {
			continue
		}
		e.failures.Add(1)
		switch exp.Type {
		case FailureLatency:
			ms := exp.MinLatencyMS
			if exp.MaxLatencyMS > ms {
				ms = exp.MinLatencyMS + rand.Intn(exp.MaxLatencyMS-exp.MinLatencyMS)
			}
			time.Sleep(time.Duration(ms) * time.Millisecond)
		case FailureError:
			return fmt.Errorf("%s", exp.ErrorMessage)
		case FailureTimeout:
			time.Sleep(30 * time.Second)
		case FailureDrop:
			return fmt.Errorf("dropped")
		case FailureFlapping:
			// alterna entre OK e falha
			if time.Now().UnixNano()%2 == 0 {
			e.recoveries.Add(1)
				return nil
			}
			return fmt.Errorf("flapping failure")
		}
	}
	return nil
}

// Stats retorna contadores
func (e *Engine) Stats() Stats {
	e.mu.RLock()
	defer e.mu.RUnlock()

	active := 0
	total := 0
	for _, exp := range e.experiments {
		total++
		if exp.running {
			active++
		}
	}
	return Stats{
		ExperimentsTotal: total,
		ExperimentsActive: active,
		FailuresInjected:  e.failures.Load(),
		Recoveries:        e.recoveries.Load(),
	}
}

type Stats struct {
	ExperimentsTotal int    `json:"experiments_total"`
	ExperimentsActive int    `json:"experiments_active"`
	FailuresInjected  uint64 `json:"failures_injected"`
	Recoveries        uint64 `json:"recoveries"`
}

// List lista experiments
func (e *Engine) List() []*Experiment {
	e.mu.RLock()
	defer e.mu.RUnlock()
	out := make([]*Experiment, 0, len(e.experiments))
	for _, exp := range e.experiments {
		out = append(out, exp)
	}
	return out
}

// IsActive checa se experiment ativo
func (e *Engine) IsActive(name string) bool {
	e.mu.RLock()
	defer e.mu.RUnlock()
	exp, ok := e.experiments[name]
	if !ok {
		return false
	}
	exp.mu.Lock()
	defer exp.mu.Unlock()
	return exp.running
}

// RunSuite roda uma bateria de tests
type Suite struct {
	Name        string
	Tests       []SuiteTest
	mu          sync.Mutex
	currentResult *SuiteResult
}

type SuiteTest struct {
	Name      string
	Fn        func(ctx context.Context) error
	Timeout   time.Duration
}

type SuiteResult struct {
	Suite    string
	Tests    int
	Passed   int
	Failed   int
	StartAt  time.Time
	FinishAt time.Time
}

func (s *Suite) Run(ctx context.Context) (*SuiteResult, error) {
	result := &SuiteResult{
		Suite:   s.Name,
		Tests:   len(s.Tests),
		StartAt: time.Now(),
	}
	s.mu.Lock()
	s.currentResult = result
	s.mu.Unlock()

	for _, t := range s.Tests {
		timeout := t.Timeout
		if timeout == 0 {
			timeout = 30 * time.Second
		}
		tctx, cancel := context.WithTimeout(ctx, timeout)
		err := t.Fn(tctx)
		cancel()

		if err == nil {
			result.Passed++
		} else {
			result.Failed++
		}
	}
	result.FinishAt = time.Now()
	return result, nil
}

// CommonExperimentTypes retorna exemplos pre-configurados
func CommonExperiments() []Experiment {
	return []Experiment{
		{
			Name: "k8s-api-slow",
			Type: FailureLatency,
			Target: "k8s-api",
			Severity: 0.5,
			MinLatencyMS: 100,
			MaxLatencyMS: 2000,
			ErrorMessage: "",
		},
		{
			Name: "k8s-api-flaky",
			Type: FailureFlapping,
			Target: "k8s-api",
			Severity: 0.3,
		},
		{
			Name: "containerd-error",
			Type: FailureError,
			Target: "containerd",
			Severity: 0.1,
			ErrorMessage: "connection refused",
		},
		{
			Name: "registry-down",
			Type: FailureError,
			Target: "registry",
			Severity: 1.0,
			ErrorMessage: "registry unreachable",
		},
	}
}