package ws

import (
	"sync"
	"sync/atomic"
	"time"
)

// RateLimiter com token bucket
type RateLimiter struct {
	mu       sync.Mutex
	tokens   float64
	capacity float64
	rate     float64
	lastRefill time.Time
}

func NewRateLimiter(rps, burst int) *RateLimiter {
	return &RateLimiter{
		tokens:   float64(burst),
		capacity: float64(burst),
		rate:     float64(rps),
		lastRefill: time.Now(),
	}
}

func (r *RateLimiter) Allow() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	now := time.Now()
	elapsed := now.Sub(r.lastRefill).Seconds()
	r.lastRefill = now
	r.tokens += elapsed * r.rate
	if r.tokens > r.capacity {
		r.tokens = r.capacity
	}
	if r.tokens >= 1 {
		r.tokens--
		return true
	}
	return false
}

// Breaker throttling interno: N falhas consecutivas abrem
type Breaker struct {
	mu         sync.Mutex
	threshold  int
	failures   int
	open       bool
	lastOpen   time.Time
	cooldown   time.Duration
}

func NewBreaker(threshold int) *Breaker {
	return &Breaker{
		threshold: threshold,
		cooldown:  5 * time.Second,
	}
}

// Allow checa se o breaker esta fechado
func (b *Breaker) Allow() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.open {
		if time.Since(b.lastOpen) > b.cooldown {
			b.open = false
			b.failures = 0
			return true
		}
		return false
	}
	return true
}

// RecordFailure registra falha
func (b *Breaker) RecordFailure() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.failures++
	if b.failures >= b.threshold {
		b.open = true
		b.lastOpen = time.Now()
	}
}

// RecordSuccess reseta falhas
func (b *Breaker) RecordSuccess() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.failures = 0
}

func (b *Breaker) IsOpen() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.open
}

// HeartbeatCounter conta heartbeat por cliente
type HeartbeatCounter struct {
	counters sync.Map
}

func NewHeartbeatCounter() *HeartbeatCounter {
	return &HeartbeatCounter{}
}

func (h *HeartbeatCounter) Tick(id string) uint64 {
	v, _ := h.counters.LoadOrStore(id, &atomic.Uint64{})
	return v.(*atomic.Uint64).Add(1)
}

func (h *HeartbeatCounter) Get(id string) uint64 {
	v, ok := h.counters.Load(id)
	if !ok {
		return 0
	}
	return v.(*atomic.Uint64).Load()
}

func (h *HeartbeatCounter) Reset(id string) {
	h.counters.Delete(id)
}