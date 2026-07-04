package wshandler

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"k8s-deploy/internal/ws"

	"github.com/gorilla/websocket"
)

// Handler faz upgrade HTTP -> WebSocket
type Handler struct {
	hub      *ws.Hub
	upgrader websocket.Upgrader
	config   ws.Config
}

func New(hub *ws.Hub, cfg ws.Config) *Handler {
	return &Handler{
		hub: hub,
		upgrader: websocket.Upgrader{
			ReadBufferSize:  1024,
			WriteBufferSize: 4096,
			CheckOrigin:     func(r *http.Request) bool { return true },
			HandshakeTimeout: 10 * time.Second,
		},
		config: cfg,
	}
}

// ServeHTTP upgrade e gerencia cliente
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Suporte a SSE fallback: ?mode=sse
	if r.URL.Query().Get("mode") == "sse" {
		h.serveSSE(w, r)
		return
	}

	conn, err := h.upgrader.Upgrade(w, r, nil)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	client := h.hub.NewClient(r.RemoteAddr, r.UserAgent())
	h.hub.Register(client)
	defer func() {
		h.hub.Unregister(client)
		conn.Close()
	}()

	conn.SetReadLimit(int64(8192))
	conn.SetReadDeadline(time.Now().Add(h.config.HeartbeatTimeout))
	conn.SetPongHandler(func(string) error {
		client.LastSeenUpdate()
		conn.SetReadDeadline(time.Now().Add(h.config.HeartbeatTimeout))
		return nil
	})

	// read pump: gerencia pings do client
	go h.readPump(conn, client)

	// write pump: envia eventos pro client
	h.writePump(conn, client)
}

func (h *Handler) LastSeenUpdate(c *ws.Client) {
	c.LastSeenUpdate()
}

// Wrapper para lastSeenUpdate pois eh privado no pacote original
// Como Client.lastSeen eh exportada mas lastSeenUpdate nao, expomos via handler

func (h *Handler) readPump(conn *websocket.Conn, client *ws.Client) {
	defer conn.Close()
	for {
		// leitura de mensagens do client (subscriptions, pings custom)
		conn.SetReadDeadline(time.Now().Add(h.config.HeartbeatTimeout))
		_, msg, err := conn.ReadMessage()
		if err != nil {
			return
		}

		var cmd struct {
			Action string   `json:"action"`
			Types  []ws.EventType `json:"types,omitempty"`
		}
		if err := json.Unmarshal(msg, &cmd); err != nil {
			continue
		}

		switch cmd.Action {
		case "subscribe":
			client.Subscribe(cmd.Types...)
		case "unsubscribe":
			client.Unsubscribe(cmd.Types...)
		case "ping":
			client.LastSeenUpdate()
		}
	}
}

func (h *Handler) writePump(conn *websocket.Conn, client *ws.Client) {
	ticker := time.NewTicker(h.config.HeartbeatInterval)
	defer func() {
		ticker.Stop()
		conn.Close()
	}()

	for {
		select {
		case <-client.ClosedSignal():
			return
		case e := <-client.SendChannel():
			conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			data, _ := json.Marshal(e)
			if err := conn.WriteMessage(websocket.TextMessage, data); err != nil {
				return
			}
		case <-ticker.C:
			conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if err := conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
			client.LastSeenUpdate()
		}
	}
}

// serveSSE: Server-Sent Events fallback
func (h *Handler) serveSSE(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "SSE nao suportado", 500)
		return
	}

	sinceParam := r.URL.Query().Get("since")
	since := time.Time{}
	if sinceParam != "" {
		if t, err := time.Parse(time.RFC3339, sinceParam); err == nil {
			since = t
		}
	}

	sseID := fmt.Sprintf("sse-%d", time.Now().UnixNano())
	sseClient := ws.NewSSEClient(sseID, since)
	h.hub.SSERegister(sseClient)
	defer h.hub.SSEUnregister(sseID)

	ctx := r.Context()
	ticker := time.NewTicker(h.config.HeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case e := <-sseClient.SendChannel():
			data, _ := json.Marshal(e)
			fmt.Fprintf(w, "id: %s\nevent: %s\ndata: %s\n\n", e.ID, e.Type, data)
			flusher.Flush()
		case <-ticker.C:
			fmt.Fprintf(w, ": heartbeat %d\n\n", time.Now().Unix())
			flusher.Flush()
		}
	}
}

// ConnectionLimiter limita numero de conexoes
type ConnectionLimiter struct {
	mu      sync.Mutex
	max     int
	current int
}

func NewConnectionLimiter(max int) *ConnectionLimiter {
	return &ConnectionLimiter{max: max}
}

func (c *ConnectionLimiter) Acquire() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.current >= c.max {
		return false
	}
	c.current++
	return true
}

func (c *ConnectionLimiter) Release() {
	c.mu.Lock()
	c.current--
	c.mu.Unlock()
}

func (c *ConnectionLimiter) Count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.current
}