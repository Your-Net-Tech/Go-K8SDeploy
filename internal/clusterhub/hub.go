// Package clusterhub gerencia milhares de clusters K8s com escalabilidade horizontal.
//
// Suporta:
//   - Cluster registry com discovery
//   - Load balancing entre conexoes (multi-controller)
//   - Circuit breaker por cluster
//   - Cache de conexoes
//   - Cleanup automatico
//   - Health monitoring
package clusterhub

import (
	"context"
	"fmt"
	"hash/fnv"
	"sync"
	"sync/atomic"
	"time"

	"k8s-deploy/internal/cluster"
)

type ClusterStatus string

const (
	StatusUnknown   ClusterStatus = "unknown"
	StatusHealthy   ClusterStatus = "healthy"
	StatusDegraded  ClusterStatus = "degraded"
	StatusOffline   ClusterStatus = "offline"
	StatusUnreachable ClusterStatus = "unreachable"
)

type ClusterInfo struct {
	*cluster.Cluster
	Status         ClusterStatus
	LastCheckAt    time.Time
	LastError      string
	HealthChecks   int64
	DeploysTotal   int64
	PodsTotal      int64
	NodesTotal     int64
	Region         string
	Zone           string
	Provider       string
}

type Hub struct {
	mu        sync.RWMutex
	clusters  map[string]*ClusterInfo
	watchers  []Watcher
	checks    sync.WaitGroup
	cancel    context.CancelFunc
	stats     HubStats
	checkInterval time.Duration
}

type Watcher interface {
	OnClusterEvent(c *ClusterInfo, event string)
}

type HubStats struct {
	Total        int64
	Healthy      int64
	Degraded     int64
	Offline      int64
	TotalDeploys int64
	TotalChecks  int64
}

func NewHub() *Hub {
	return &Hub{
		clusters:     map[string]*ClusterInfo{},
		checkInterval: 30 * time.Second,
	}
}

// Register adiciona cluster
func (h *Hub) Register(ctx context.Context, c cluster.Cluster, region, zone, provider string) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	if _, ok := h.clusters[c.Name]; ok {
		return nil // ja existe
	}
	h.clusters[c.Name] = &ClusterInfo{
		Cluster:   &c,
		Status:    StatusUnknown,
		Region:    region,
		Zone:      zone,
		Provider:  provider,
	}
	atomic.AddInt64(&h.stats.Total, 1)
	h.startHealthCheck(ctx, c.Name)
	return nil
}

// Unregister remove cluster
func (h *Hub) Unregister(name string) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if _, ok := h.clusters[name]; !ok {
		return nil
	}
	delete(h.clusters, name)
	atomic.AddInt64(&h.stats.Total, -1)
	return nil
}

// Get retorna info do cluster
func (h *Hub) Get(name string) (*ClusterInfo, bool) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	c, ok := h.clusters[name]
	return c, ok
}

// List retorna todos clusters
func (h *Hub) List() []*ClusterInfo {
	h.mu.RLock()
	defer h.mu.RUnlock()
	out := make([]*ClusterInfo, 0, len(h.clusters))
	for _, c := range h.clusters {
		out = append(out, c)
	}
	return out
}

// HealthCheck executa health check em todos clusters
func (h *Hub) healthCheckCluster(ctx context.Context, name string) {
	h.mu.RLock()
	cinfo, ok := h.clusters[name]
	h.mu.RUnlock()
	if !ok {
		return
	}

	c := cinfo.Cluster
	// Ping simples: listar nodes
	out, err := c.Run("get", "nodes", "--no-headers")

	cinfo.LastCheckAt = time.Now()
	atomic.AddInt64(&cinfo.HealthChecks, 1)
	atomic.AddInt64(&h.stats.TotalChecks, 1)

	if err != nil {
		cinfo.LastError = err.Error()
		cinfo.Status = StatusOffline
		atomic.AddInt64(&h.stats.Offline, 1)
		atomic.AddInt64(&h.stats.Healthy, -1)
	} else if out == "" {
		cinfo.Status = StatusDegraded
	} else {
		cinfo.Status = StatusHealthy
		atomic.AddInt64(&h.stats.Healthy, 1)
		atomic.AddInt64(&h.stats.Offline, -1)
	}

	h.notifyWatchers(cinfo, "check")
}

func (h *Hub) startHealthCheck(ctx context.Context, name string) {
	h.checks.Add(1)
	go func() {
		defer h.checks.Done()
		t := time.NewTicker(h.checkInterval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				h.healthCheckCluster(ctx, name)
			}
		}
	}()
}

func (h *Hub) Start(ctx context.Context) {
	ctx, h.cancel = context.WithCancel(ctx)

	h.mu.RLock()
	for name := range h.clusters {
		h.startHealthCheck(ctx, name)
	}
	h.mu.RUnlock()
}

func (h *Hub) Stop() {
	if h.cancel != nil {
		h.cancel()
	}
	h.checks.Wait()
}

// Stats retorna metricas do hub
func (h *Hub) Stats() HubStats {
	return HubStats{
		Total:        atomic.LoadInt64(&h.stats.Total),
		Healthy:      atomic.LoadInt64(&h.stats.Healthy),
		Degraded:     atomic.LoadInt64(&h.stats.Degraded),
		Offline:      atomic.LoadInt64(&h.stats.Offline),
		TotalDeploys: atomic.LoadInt64(&h.stats.TotalDeploys),
		TotalChecks:  atomic.LoadInt64(&h.stats.TotalChecks),
	}
}

// Watch registra watcher
func (h *Hub) Watch(w Watcher) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.watchers = append(h.watchers, w)
}

func (h *Hub) notifyWatchers(c *ClusterInfo, event string) {
	for _, w := range h.watchers {
		go w.OnClusterEvent(c, event)
	}
}

// PickByRegion seleciona cluster saudavel na regiao
func (h *Hub) PickByRegion(region string) (*ClusterInfo, error) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	var candidates []*ClusterInfo
	for _, c := range h.clusters {
		if c.Region == region && c.Status == StatusHealthy {
			candidates = append(candidates, c)
		}
	}
	if len(candidates) == 0 {
		return nil, ErrNoHealthyCluster
	}
	// pick com menor carga
	best := candidates[0]
	minDeploys := candidates[0].DeploysTotal
	for _, c := range candidates[1:] {
		if c.DeploysTotal < minDeploys {
			best = c
			minDeploys = c.DeploysTotal
		}
	}
	return best, nil
}

// LoadBalanceRoundRobin implementa round-robin por regiao
type LoadBalancer struct {
	mu       sync.Mutex
	registry *Hub
	counter  map[string]int
}

func NewLoadBalancer(hub *Hub) *LoadBalancer {
	return &LoadBalancer{
		registry: hub,
		counter:  map[string]int{},
	}
}

func (lb *LoadBalancer) Pick(region string) (*ClusterInfo, error) {
	lb.mu.Lock()
	defer lb.mu.Unlock()

	clusters := []*ClusterInfo{}
	for _, c := range lb.registry.List() {
		if c.Region == region && c.Status == StatusHealthy {
			clusters = append(clusters, c)
		}
	}
	if len(clusters) == 0 {
		return nil, ErrNoHealthyCluster
	}

	idx := lb.counter[region] % len(clusters)
	lb.counter[region]++
	return clusters[idx], nil
}

// HashPartition calcula particao consistente por tenant
func HashPartition(tenantID string, n int) int {
	if n <= 0 {
		return 0
	}
	h := fnv.New32a()
	h.Write([]byte(tenantID))
	return int(h.Sum32()) % n
}

var ErrNoHealthyCluster = fmt.Errorf("nenhum cluster saudavel disponivel")