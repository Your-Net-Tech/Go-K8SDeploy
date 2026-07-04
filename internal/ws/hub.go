// Package ws implementa WebSocket server resiliente para streaming de eventos.
//
// Características de resiliência:
//   - Multiplex de eventos (deployments, clusters, alerts)
//   - Heartbeat com timeout (mata connections inativas)
//   - Replay buffer (cliente novo recebe eventos recentes)
//   - Rate limit por cliente
//   - Connection tracking (cleanup automatico)
//   - Event sourcing (ring buffer historico)
//   - SSE fallback automatico se cliente nao suportar WS
//   - Circuit breaker interno (se muitos clientes falharem, throttle)
package ws

import (
	"sync"
	"sync/atomic"
	"time"
)

// EventType categoriza o evento
type EventType string

const (
	EventDeploymentStatus EventType = "deployment.status"
	EventClusterHealth    EventType = "cluster.health"
	EventRolloutStep      EventType = "rollout.step"
	EventRolloutPaused    EventType = "rollout.paused"
	EventAlert            EventType = "alert"
	EventNotification     EventType = "notification"
	EventTick             EventType = "tick" // heartbeat
)

// Event eh o payload padrao
type Event struct {
	ID        string                 `json:"id"`
	Type      EventType              `json:"type"`
	Timestamp time.Time              `json:"ts"`
	Source    string                 `json:"source"`
	Data      map[string]interface{} `json:"data"`
	Priority  Priority               `json:"priority"`
}

// Priority ordena eventos no buffer
type Priority int

const (
	PriorityLow      Priority = 1
	PriorityNormal   Priority = 2
	PriorityHigh     Priority = 3
	PriorityCritical Priority = 4
)

// Client representa uma conexao WS ativa
type Client struct {
	ID           string
	remoteAddr   string
	userAgent    string
	send         chan Event
	hub          *Hub
	lastSeen     atomic.Int64
	closeOnce    sync.Once
	closeSignal  chan struct{}
	mu           sync.Mutex
	subscribedTo map[EventType]bool
}

func (c *Client) String() string {
	return c.remoteAddr
}

func (c *Client) Close() {
	c.closeOnce.Do(func() {
		close(c.closeSignal)
	})
}

// ReadPump pode ser usada para receber pings
func (c *Client) ReadPump() {
	for {
		select {
		case <-c.closeSignal:
			return
		default:
		}
		// timeout detection via lastSeen
		last := c.lastSeen.Load()
		if time.Since(time.Unix(0, last)) > 60*time.Second {
			c.Close()
			return
		}
		time.Sleep(5 * time.Second)
	}
}

// LastSeenUpdate eh chamada pelo handler de pings
func (c *Client) LastSeenUpdate() {
	c.lastSeen.Store(time.Now().UnixNano())
}

// ClosedSignal usado por handlers para goroutine control
func (c *Client) ClosedSignal() <-chan struct{} {
	return c.closeSignal
}

// SendChannel usado por handlers para escrever
func (c *Client) SendChannel() <-chan Event {
	return c.send
}

// Hub eh o agregador central
type Hub struct {
	mu          sync.RWMutex
	clients     map[*Client]bool
	register    chan *Client
	unregister  chan *Client
	broadcast   chan Event
	replay      *RingBuffer
	rateLimit   *RateLimiter
	breaker     *Breaker
	maxClients  int
	pingTicker  *time.Ticker
	dropSlow    atomic.Int64
	totalSent   atomic.Int64
	totalDropped atomic.Int64
	config      Config

	// SSE fallback
	sseClients map[string]*SSEClient
	sseMu       sync.RWMutex
}

type Config struct {
	MaxClients       int
	HeartbeatInterval time.Duration
	HeartbeatTimeout time.Duration
	ReplaySize       int
	RateLimitRPS     int
	RateLimitBurst   int
	BreakerThreshold int
}

func DefaultConfig() Config {
	return Config{
		MaxClients:        1000,
		HeartbeatInterval: 15 * time.Second,
		HeartbeatTimeout:  60 * time.Second,
		ReplaySize:        1000,
		RateLimitRPS:      10,
		RateLimitBurst:    20,
		BreakerThreshold:  50,
	}
}

// RingBuffer é um buffer circular de eventos recentes para replay
type RingBuffer struct {
	mu     sync.RWMutex
	events []Event
	idx    int
	full   bool
	max    int
}

func NewRingBuffer(max int) *RingBuffer {
	return &RingBuffer{
		events: make([]Event, max),
		max:    max,
	}
}

func (r *RingBuffer) Push(e Event) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events[r.idx] = e
	r.idx = (r.idx + 1) % r.max
	if r.idx == 0 {
		r.full = true
	}
}

// Snapshot retorna eventos desde `since` ou todos se since=0
func (r *RingBuffer) Snapshot(since time.Time) []Event {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := []Event{}
	for i := 0; i < r.max; i++ {
		idx := (r.idx - 1 - i + r.max) % r.max
		e := r.events[idx]
		if e.ID == "" {
			continue
		}
		if !since.IsZero() && e.Timestamp.Before(since) {
			continue
		}
		out = append([]Event{e}, out...)
	}
	return out
}

func NewHub(cfg Config) *Hub {
	h := &Hub{
		clients:    map[*Client]bool{},
		sseClients: map[string]*SSEClient{},
		register:   make(chan *Client, 100),
		unregister: make(chan *Client, 100),
		broadcast:  make(chan Event, 1000),
		replay:     NewRingBuffer(cfg.ReplaySize),
		rateLimit:  NewRateLimiter(cfg.RateLimitRPS, cfg.RateLimitBurst),
		breaker:    NewBreaker(cfg.BreakerThreshold),
		maxClients: cfg.MaxClients,
		config:     cfg,
	}
	go h.run()
	go h.heartbeatLoop()
	return h
}

// Register adiciona um cliente novo
func (h *Hub) Register(c *Client) {
	c.hub = h
	h.mu.Lock()
	if len(h.clients) >= h.maxClients {
		h.mu.Unlock()
		c.Close()
		return
	}
	h.clients[c] = true
	h.mu.Unlock()
	h.replayTo(c)
	select {
	case h.register <- c:
	case <-time.After(2 * time.Second):
		c.Close()
	}
}

// Unregister remove cliente
func (h *Hub) Unregister(c *Client) {
	select {
	case h.unregister <- c:
	case <-time.After(1 * time.Second):
	}
}

// Publish envia evento para todos os clientes
func (h *Hub) Publish(et EventType, source string, priority Priority, data map[string]interface{}) {
	if !h.breaker.Allow() {
		return
	}
	e := Event{
		ID:        generateID(),
		Type:      et,
		Timestamp: time.Now(),
		Source:    source,
		Data:      data,
		Priority:  priority,
	}
	h.replay.Push(e)

	select {
	case h.broadcast <- e:
	case <-time.After(100 * time.Millisecond):
		h.totalDropped.Add(1)
	}
}

// SSERegister adiciona um cliente SSE
func (h *Hub) SSERegister(c *SSEClient) {
	h.sseMu.Lock()
	h.sseClients[c.ID] = c
	h.sseMu.Unlock()
	go h.sseReplayTo(c)
}

func (h *Hub) SSEUnregister(id string) {
	h.sseMu.Lock()
	delete(h.sseClients, id)
	h.sseMu.Unlock()
}

func (h *Hub) sseReplayTo(c *SSEClient) {
	for _, e := range h.replay.Snapshot(c.since) {
		select {
		case <-c.done:
			return
		case c.send <- e:
		}
	}
}

// Stats retorna metricas do hub
func (h *Hub) Stats() Stats {
	h.mu.RLock()
	clientCount := len(h.clients)
	h.mu.RUnlock()

	h.sseMu.RLock()
	sseCount := len(h.sseClients)
	h.sseMu.RUnlock()

	return Stats{
		Clients:      clientCount,
		SSEClients:   sseCount,
		TotalSent:    h.totalSent.Load(),
		TotalDropped: h.totalDropped.Load(),
		BufferSize:   len(h.broadcast),
		ReplaySize:   h.replaySize(),
		BreakerOpen:  h.breaker.IsOpen(),
	}
}

func (h *Hub) replaySize() int {
	h.replay.mu.RLock()
	defer h.replay.mu.RUnlock()
	count := 0
	for _, e := range h.replay.events {
		if e.ID != "" {
			count++
		}
	}
	return count
}

type Stats struct {
	Clients      int  `json:"clients"`
	SSEClients   int  `json:"sse_clients"`
	TotalSent    int64 `json:"total_sent"`
	TotalDropped int64 `json:"total_dropped"`
	BufferSize   int    `json:"buffer_size"`
	ReplaySize   int    `json:"replay_size"`
	BreakerOpen  bool   `json:"breaker_open"`
}

// SendTo envia evento para um cliente especifico
func (h *Hub) SendTo(c *Client, e Event) {
	c.lastSeen.Store(time.Now().UnixNano())
	select {
	case c.send <- e:
	case <-time.After(50 * time.Millisecond):
		h.totalDropped.Add(1)
	default:
		h.totalDropped.Add(1)
	}
}

func (h *Hub) run() {
	for {
		select {
		case c := <-h.register:
			h.mu.Lock()
			h.clients[c] = true
			h.mu.Unlock()
		case c := <-h.unregister:
			h.mu.Lock()
			if _, ok := h.clients[c]; ok {
				delete(h.clients, c)
				close(c.send)
			}
			h.mu.Unlock()
		case e := <-h.broadcast:
			h.dispatch(e)
		}
	}
}

func (h *Hub) dispatch(e Event) {
	if !h.rateLimit.Allow() {
		h.dropSlow.Add(1)
		return
	}

	h.mu.RLock()
	clients := make([]*Client, 0, len(h.clients))
	for c := range h.clients {
		clients = append(clients, c)
	}
	h.mu.RUnlock()

	for _, c := range clients {
		c.mu.Lock()
		if c.subscribedTo == nil || c.subscribedTo[e.Type] {
			h.SendTo(c, e)
		}
		c.mu.Unlock()
	}

	h.sseMu.RLock()
	for _, sse := range h.sseClients {
		select {
		case sse.send <- e:
		default:
		}
	}
	h.sseMu.RUnlock()

	h.totalSent.Add(1)
}

func (h *Hub) replayTo(c *Client) {
	c.mu.Lock()
	c.lastSeen.Store(time.Now().UnixNano())
	c.mu.Unlock()
	events := h.replay.Snapshot(time.Time{})
	for _, e := range events {
		select {
		case c.send <- e:
		case <-time.After(50 * time.Millisecond):
		case <-c.closeSignal:
			return
		}
	}
}

func (h *Hub) heartbeatLoop() {
	t := time.NewTicker(h.config.HeartbeatInterval)
	defer t.Stop()
	for range t.C {
		hb := Event{
			ID:        generateID(),
			Type:      EventTick,
			Timestamp: time.Now(),
			Source:    "hub",
			Data: map[string]interface{}{
				"stats": h.Stats(),
			},
			Priority: PriorityLow,
		}

		h.mu.RLock()
		for c := range h.clients {
			last := c.lastSeen.Load()
			if time.Since(time.Unix(0, last)) > h.config.HeartbeatTimeout {
				c.Close()
				continue
			}
			select {
			case c.send <- hb:
			case <-c.closeSignal:
			default:
			}
		}
		h.mu.RUnlock()
	}
}

// NewClient cria um cliente sem websocket - usado por handlers
func (h *Hub) NewClient(remoteAddr, userAgent string) *Client {
	return &Client{
		ID:           generateID(),
		remoteAddr:   remoteAddr,
		userAgent:    userAgent,
		send:         make(chan Event, 64),
		closeSignal:  make(chan struct{}),
		subscribedTo: map[EventType]bool{},
		lastSeen:     atomic.Int64{},
	}
}

func (c *Client) lastSeenUpdate() {
	c.lastSeen.Store(time.Now().UnixNano())
}

func (c *Client) Subscribe(types ...EventType) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, t := range types {
		c.subscribedTo[t] = true
	}
}

func (c *Client) Unsubscribe(types ...EventType) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, t := range types {
		delete(c.subscribedTo, t)
	}
}

// SSEClient implementa cliente SSE para fallback
type SSEClient struct {
	ID    string
	send  chan Event
	done  chan struct{}
	since time.Time
}

// NewSSEClient cria cliente SSE
func NewSSEClient(id string, since time.Time) *SSEClient {
	return &SSEClient{
		ID:    id,
		send:  make(chan Event, 64),
		done:  make(chan struct{}),
		since: since,
	}
}

// Close fecha cliente SSE
func (c *SSEClient) Close() {
	select {
	case <-c.done:
	default:
		close(c.done)
	}
}

// CloseChan expõe canal
func (c *SSEClient) CloseChan() <-chan struct{} { return c.done }

// SendChannel expõe canal de envio
func (c *SSEClient) SendChannel() <-chan Event { return c.send }

// generateID gera ID unico
func generateID() string {
	return time.Now().Format("20060102150405.000000")
}