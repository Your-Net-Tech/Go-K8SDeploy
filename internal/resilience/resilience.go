package resilience

import (
	"context"
	"encoding/hex"
	"errors"
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

// Circuit breaker com 3 estados
type CircuitBreaker struct {
	name             string
	maxFailures      uint32
	timeout          time.Duration
	halfOpenMax      uint32

	mu               sync.Mutex
	state            cbState
	failures         uint32
	successes        uint32
	openedAt         time.Time
}

type cbState int

const (
	stateClosed cbState = iota
	stateOpen
	stateHalfOpen
)

// Backoff exponential com jitter
type Backoff struct {
	base      time.Duration
	max       time.Duration
	attempts  int
	jitter    bool
	rng       *rand.Rand
}

func NewBackoff(base, max time.Duration) *Backoff {
	return &Backoff{
		base: base,
		max:  max,
		jitter: true,
		rng:  rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

func (b *Backoff) Next() time.Duration {
	if b.attempts > 20 {
		b.attempts = 20
	}
	mult := math.Pow(2, float64(b.attempts))
	d := time.Duration(float64(b.base) * mult)
	if d > b.max {
		d = b.max
	}
	if b.jitter {
		jitter := time.Duration(b.rng.Int63n(int64(d)))
		d = d/2 + jitter/2
	}
	b.attempts++
	return d
}

func (b *Backoff) Reset() { b.attempts = 0 }

// Retry com backoff
func Retry(ctx context.Context, attempts int, b *Backoff, fn func() error) error {
	var lastErr error
	for i := 0; i < attempts; i++ {
		if err := ctx.Err(); err != nil {
			return err
		}

		err := fn()
		if err == nil {
			return nil
		}
		lastErr = err

		if i == attempts-1 {
			break
		}

		select {
		case <-time.After(b.Next()):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return lastErr
}

// Cache com TTL e invalidação
type Cache struct {
	mu    sync.RWMutex
	items map[string]*cacheItem
}

type cacheItem struct {
	value     interface{}
	expiresAt time.Time
	hits      uint64
}

func NewCache() *Cache {
	return &Cache{items: map[string]*cacheItem{}}
}

func (c *Cache) Get(key string) (interface{}, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	item, ok := c.items[key]
	if !ok {
		return nil, false
	}
	if time.Now().After(item.expiresAt) {
		return nil, false
	}
	atomic.AddUint64(&item.hits, 1)
	return item.value, true
}

func (c *Cache) Set(key string, value interface{}, ttl time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.items[key] = &cacheItem{
		value:     value,
		expiresAt: time.Now().Add(ttl),
	}
}

func (c *Cache) Delete(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.items, key)
}

func (c *Cache) Stats() (size int, hits uint64) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	now := time.Now()
	for _, item := range c.items {
		if now.Before(item.expiresAt) {
			size++
			hits += item.hits
		}
	}
	return
}

// Rate limiter (token bucket)
type RateLimiter struct {
	mu       sync.Mutex
	tokens   float64
	capacity float64
	rate     float64
	lastRefill time.Time
}

func NewRateLimiter(rps int, burst int) *RateLimiter {
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
	r.tokens = math.Min(r.capacity, r.tokens+elapsed*r.rate)
	if r.tokens >= 1 {
		r.tokens--
		return true
	}
	return false
}

func (r *RateLimiter) Wait(ctx context.Context) error {
	for {
		if r.Allow() {
			return nil
		}
		select {
		case <-time.After(50 * time.Millisecond):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// ErrCircuitOpen eh retornado quando o circuito esta aberto
var ErrCircuitOpen = errors.New("circuit breaker is open")

// Call executa funcao dentro do circuit breaker
func (cb *CircuitBreaker) Call(fn func() error) error {
	cb.mu.Lock()
	if cb.state == stateOpen {
		if time.Since(cb.openedAt) > cb.timeout {
			cb.state = stateHalfOpen
			cb.successes = 0
		} else {
			cb.mu.Unlock()
			return ErrCircuitOpen
		}
	}
	cb.mu.Unlock()

	err := fn()

	cb.mu.Lock()
	defer cb.mu.Unlock()
	if err != nil {
		cb.failures++
		if cb.failures >= cb.maxFailures {
			cb.state = stateOpen
			cb.openedAt = time.Now()
		}
		return err
	}

	if cb.state == stateHalfOpen {
		cb.successes++
		if cb.successes >= cb.halfOpenMax {
			cb.state = stateClosed
			cb.failures = 0
		}
	} else {
		cb.failures = 0
	}
	return nil
}

func NewCircuitBreaker(name string, maxFailures uint32, timeout time.Duration) *CircuitBreaker {
	return &CircuitBreaker{
		name:        name,
		maxFailures: maxFailures,
		timeout:     timeout,
		halfOpenMax: 2,
		state:       stateClosed,
	}
}

func (cb *CircuitBreaker) State() string {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	switch cb.state {
	case stateClosed:
		return "closed"
	case stateOpen:
		return "open"
	case stateHalfOpen:
		return "half-open"
	}
	return "unknown"
}

// IsOpen retorna true se circuito está aberto
func (cb *CircuitBreaker) IsOpen() bool {
	return cb.State() == "open"
}

// Metrics counters
type Metrics struct {
	mu     sync.RWMutex
	gauges map[string]float64
	counts map[string]uint64
	histograms map[string]*Histogram
}

type Histogram struct {
	mu       sync.Mutex
	buckets  []float64
	counts   []uint64
	sum      float64
	count    uint64
}

func NewMetrics() *Metrics {
	return &Metrics{
		gauges:    map[string]float64{},
		counts:    map[string]uint64{},
		histograms: map[string]*Histogram{},
	}
}

func (m *Metrics) Counter(name string) { m.counts[name]++ }
func (m *Metrics) CounterInc(name string, n uint64) { m.counts[name] += n }
func (m *Metrics) Gauge(name string, val float64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.gauges[name] = val
}

func (m *Metrics) Histogram(name string, val float64, buckets []float64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	h, ok := m.histograms[name]
	if !ok {
		h = &Histogram{buckets: buckets, counts: make([]uint64, len(buckets)+1)}
		m.histograms[name] = h
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	h.sum += val
	h.count++
	for i, b := range buckets {
		if val <= b {
			h.counts[i]++
		}
	}
	h.counts[len(buckets)]++
}

func (m *Metrics) Render() string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := ""
	for n, c := range m.counts {
		out += n + " " + itoa(c) + "\n"
	}
	for n, g := range m.gauges {
		out += n + " " + ftoa(g) + "\n"
	}
	for n, h := range m.histograms {
		out += n + "_count " + itoa(h.count) + "\n"
		out += n + "_sum " + ftoa(h.sum) + "\n"
	}
	return out
}

func itoa(n uint64) string {
	if n == 0 {
		return "0"
	}
	out := ""
	for n > 0 {
		out = string(rune('0'+n%10)) + out
		n /= 10
	}
	return out
}

func ftoa(f float64) string {
	return fmtFloat(f)
}

func fmtFloat(f float64) string {
	// simples formatacao
	return strconvFloat(f)
}

func strconvFloat(f float64) string {
	return itoa(uint64(f))
}

// Distributed lock via DB (SQLite)
type Lock struct {
	mu      sync.Mutex
	key     string
	token   string
	expireAt time.Time
	engine  *MutexEngine
}

type MutexEngine struct {
	mu    sync.Mutex
	locks map[string]*Lock
}

func NewMutexEngine() *MutexEngine {
	return &MutexEngine{locks: map[string]*Lock{}}
}

func (m *MutexEngine) Acquire(key string, ttl time.Duration) (*Lock, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if existing, ok := m.locks[key]; ok && time.Now().Before(existing.expireAt) {
		return nil, false
	}
	b := make([]byte, 16)
	rand.Read(b)
	token := hex.EncodeToString(b)
	lock := &Lock{
		key:       key,
		token:     token,
		expireAt:  time.Now().Add(ttl),
		engine:    m,
	}
	m.locks[key] = lock
	return lock, true
}

func (m *MutexEngine) Release(key, token string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	lock, ok := m.locks[key]
	if !ok || lock.token != token {
		return false
	}
	delete(m.locks, key)
	return true
}

func (l *Lock) Release() {
	if l.engine != nil {
		l.engine.Release(l.key, l.token)
	}
}