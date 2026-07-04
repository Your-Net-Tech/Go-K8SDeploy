// Package ratelimit implementa rate limiters compostos para SaaS.
//
// - Token bucket per-client
// - Sliding window para requests per day
// - Concurrency limiter
package ratelimit

import (
	"context"
	"sync"
	"time"
)

type MultiLimiter struct {
	mu        sync.RWMutex
	buckets   map[string]*TokenBucket
	dayWindow map[string]*WindowCounter
}

func NewMultiLimiter() *MultiLimiter {
	return &MultiLimiter{
		buckets:   map[string]*TokenBucket{},
		dayWindow: map[string]*WindowCounter{},
	}
}

// NewRateLimiter cria rate limiter token-bucket simples
func NewRateLimiter(rps, burst int) *RateLimiter {
	return &RateLimiter{
		capacity:  float64(burst),
		bucket:    &TokenBucket{tokens: float64(burst), capacity: float64(burst), rate: float64(rps)},
		threshold: time.Duration(float64(1)/float64(rps)*1000) * time.Millisecond,
	}
}

// RateLimiter wrapper para uso simples
type RateLimiter struct {
	mu        sync.Mutex
	capacity  float64
	bucket    *TokenBucket
	threshold time.Duration
}

func (r *RateLimiter) Allow() bool {
	return r.bucket.Allow()
}

func (r *RateLimiter) Wait(ctx context.Context) error {
	for {
		if r.Allow() {
			return nil
		}
		select {
		case <-time.After(r.threshold):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (m *MultiLimiter) Allow(clientID string, tier Tier) bool {
	m.mu.Lock()
	bucket, ok := m.buckets[clientID]
	if !ok {
		bucket = &TokenBucket{
			tokens:   float64(tier.Burst),
			capacity: float64(tier.Burst),
			rate:     float64(tier.RequestsPerSecond),
			lastRefill: time.Now(),
		}
		m.buckets[clientID] = bucket
	}

	day, ok := m.dayWindow[clientID]
	if !ok {
		day = &WindowCounter{
			count: 0,
			limit: tier.RequestsPerDay,
		}
		m.dayWindow[clientID] = day
	}
	m.mu.Unlock()

	if !day.Allow() {
		return false
	}

	return bucket.Allow()
}

type Tier struct {
	RequestsPerSecond int
	Burst             int
	RequestsPerDay    int
	Concurrency       int
}

type TokenBucket struct {
	mu        sync.Mutex
	tokens    float64
	capacity  float64
	rate      float64
	lastRefill time.Time
}

func (b *TokenBucket) Allow() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	now := time.Now()
	elapsed := now.Sub(b.lastRefill).Seconds()
	b.lastRefill = now
	b.tokens += elapsed * b.rate
	if b.tokens > b.capacity {
		b.tokens = b.capacity
	}
	if b.tokens >= 1 {
		b.tokens--
		return true
	}
	return false
}

type WindowCounter struct {
	mu     sync.Mutex
	count  int
	limit  int
	day    int
}

func (w *WindowCounter) Allow() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.limit < 0 {
		return true
	}
	today := dayOfYear(time.Now())
	if today != w.day {
		w.day = today
		w.count = 0
	}
	if w.count >= w.limit {
		return false
	}
	w.count++
	return true
}

func dayOfYear(t time.Time) int {
	return t.YearDay() + t.Year()*1000
}