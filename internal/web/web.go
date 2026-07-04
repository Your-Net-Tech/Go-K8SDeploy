package web

import (
	"context"
	"embed"
	"fmt"
	"io/fs"
	"net/http"
	"os/exec"
	"time"

	"k8s-deploy/internal/cluster"
	"k8s-deploy/internal/notify"
	"k8s-deploy/internal/ws"
	"k8s-deploy/internal/wshandler"
	"k8s-deploy/state"
)

//go:embed static/*
var staticFiles embed.FS

type Server struct {
	port     string
	store    *state.Store
	clusters *cluster.Registry
	notifier *notify.Notifier
	hub      *ws.Hub
	wsHandler *wshandler.Handler
	srv      *http.Server
}

func New(port string, store *state.Store, clusters *cluster.Registry, n *notify.Notifier) *Server {
	cfg := ws.DefaultConfig()
	hub := ws.NewHub(cfg)
	handler := wshandler.New(hub, cfg)
	return &Server{
		port:      port,
		store:     store,
		clusters:  clusters,
		notifier:  n,
		hub:       hub,
		wsHandler: handler,
	}
}

func (s *Server) Start(ctx context.Context) error {
	mux := http.NewServeMux()

	mux.HandleFunc("/ws", s.wsHandler.ServeHTTP)
	mux.HandleFunc("/api/status", s.apiStatus)
	mux.HandleFunc("/api/deployments", s.apiDeployments)
	mux.HandleFunc("/api/clusters", s.apiClusters)
	mux.HandleFunc("/api/ws-stats", s.apiWSStats)
	mux.HandleFunc("/api/trigger", s.apiTrigger)
	mux.HandleFunc("/api/health", s.apiHealth)

	staticFS, _ := fs.Sub(staticFiles, "static")
	mux.Handle("/", http.FileServer(http.FS(staticFS)))

	s.srv = &http.Server{Addr: ":" + s.port, Handler: s.srvLog(mux)}
	s.notifier.Send("ui-dashboard", fmt.Sprintf("started on :%s with WebSocket", s.port), "info")

	// Background loop publica eventos de cluster health
	go s.publishClusterHealth(ctx)

	go func() {
		<-ctx.Done()
		s.srv.Shutdown(ctx)
	}()
	return s.srv.ListenAndServe()
}

func (s *Server) publishClusterHealth(ctx context.Context) {
	t := time.NewTicker(10 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			healthy := 0
			total := 0
			for _, c := range s.clusters.List() {
				total++
				if _, err := c.Run("get", "nodes", "--no-headers"); err == nil {
					healthy++
				}
			}
			s.hub.Publish(ws.EventClusterHealth, "cluster-monitor", ws.PriorityNormal, map[string]interface{}{
				"healthy": healthy,
				"total":   total,
			})
		}
	}
}

func (s *Server) srvLog(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		h.ServeHTTP(w, r)
		fmt.Printf("[HTTP] %s %s %s\n", r.Method, r.URL.Path, time.Since(start).Round(time.Millisecond))
	})
}

func (s *Server) apiHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(`{"status":"ok"}`))
}

func (s *Server) apiWSStats(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, `{"ws_clients":%d}`, s.hub.Stats().Clients+s.hub.Stats().SSEClients)
}

func (s *Server) apiStatus(w http.ResponseWriter, r *http.Request) {
	clusters := s.clusters.List()
	deps, _ := s.store.ListDeployments("", 1)
	healthy := 0
	for _, c := range clusters {
		if _, err := c.Run("get", "nodes", "--no-headers"); err == nil {
			healthy++
		}
	}
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, `{"clusters_total": %d, "clusters_healthy": %d, "last_deployment": %q, "ws_stats": %+v}`,
		len(clusters), healthy, lastDepInfo(deps), s.hub.Stats())
}

func lastDepInfo(deps []*state.Deployment) string {
	if len(deps) == 0 {
		return "none"
	}
	d := deps[0]
	return fmt.Sprintf("rev %d: %s", d.Revision, d.Status)
}

func (s *Server) apiDeployments(w http.ResponseWriter, r *http.Request) {
	project := r.URL.Query().Get("project")
	deps, err := s.store.ListDeployments(project, 20)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte("["))
	for i, d := range deps {
		if i > 0 {
			w.Write([]byte(","))
		}
		fmt.Fprintf(w, `{"id":%d,"project":%q,"rev":%d,"status":%q,"started":%q,"error":%q}`,
			d.ID, d.Project, d.Revision, d.Status,
			d.StartedAt.Format("2006-01-02 15:04:05"),
			d.Error)

		// Publicar como evento WS
		s.hub.Publish(ws.EventDeploymentStatus, "deploy-api", ws.PriorityNormal, map[string]interface{}{
			"id":      d.ID,
			"project": d.Project,
			"rev":     d.Revision,
			"status":  d.Status,
		})
	}
	w.Write([]byte("]"))
}

func (s *Server) apiClusters(w http.ResponseWriter, r *http.Request) {
	clusters := s.clusters.List()
	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte("["))
	for i, c := range clusters {
		if i > 0 {
			w.Write([]byte(","))
		}
		_, err := c.Run("get", "nodes", "--no-headers")
		fmt.Fprintf(w, `{"name":%q,"context":%q,"namespace":%q,"primary":%t,"healthy":%t}`,
			c.Name, c.Context, c.Namespace, c.Primary, err == nil)
	}
	w.Write([]byte("]"))
}

func (s *Server) apiTrigger(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", 405)
		return
	}
	project := r.URL.Query().Get("project")
	if project == "" {
		http.Error(w, "project required", 400)
		return
	}
	go func() {
		cmd := exec.Command("k8s-deploy", "apply", "-p", project)
		out, err := cmd.CombinedOutput()
		if err != nil {
			s.hub.Publish(ws.EventAlert, "ui-trigger", ws.PriorityHigh, map[string]interface{}{
				"project": project,
				"error":   string(out),
			})
			s.notifier.Send("ui-trigger-failed",
				fmt.Sprintf("%s: %v\n%s", project, err, string(out)), "error")
		} else {
			s.hub.Publish(ws.EventNotification, "ui-trigger", ws.PriorityNormal, map[string]interface{}{
				"project": project,
				"result":  string(out),
			})
		}
	}()
	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(`{"status":"started"}`))
}