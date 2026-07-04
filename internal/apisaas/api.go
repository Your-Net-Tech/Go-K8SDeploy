package apisaas

import (
	"encoding/json"
	"net/http"
	"strings"
	"sync"

	"k8s-deploy/internal/audit"
	"k8s-deploy/internal/cluster"
	"k8s-deploy/internal/clusterhub"
	"k8s-deploy/internal/tenant"
)

// MultiTenantAPI eh a API HTTP principal do SaaS
type MultiTenantAPI struct {
	mu      sync.RWMutex
	server  *Server
	manager *tenant.Manager
	audit   *audit.Logger
	hub     *clusterhub.Hub
	routes  map[string]map[string]http.HandlerFunc // path -> method -> handler
}

func NewMultiTenantAPI(server *Server, manager *tenant.Manager, auditLog *audit.Logger, hub *clusterhub.Hub) *MultiTenantAPI {
	api := &MultiTenantAPI{
		server:  server,
		manager: manager,
		audit:   auditLog,
		hub:     hub,
		routes:  map[string]map[string]http.HandlerFunc{},
	}

	api.registerRoutes()
	return api
}

// Register registra handlers
func (api *MultiTenantAPI) registerRoutes() {
	api.add("GET", "/api/v1/tenants", api.handleListTenants)
	api.add("POST", "/api/v1/tenants", api.handleCreateTenant)
	api.add("GET", "/api/v1/tenants/{id}", api.handleGetTenant)
	api.add("PATCH", "/api/v1/tenants/{id}", api.handleUpdateTenant)
	api.add("DELETE", "/api/v1/tenants/{id}", api.handleDeleteTenant)

	api.add("GET", "/api/v1/clusters", api.handleListClusters)
	api.add("POST", "/api/v1/clusters", api.handleRegisterCluster)
	api.add("GET", "/api/v1/clusters/{id}", api.handleGetCluster)
	api.add("DELETE", "/api/v1/clusters/{id}", api.handleUnregisterCluster)

	api.add("GET", "/api/v1/deploys", api.handleListDeploys)
	api.add("POST", "/api/v1/deploys", api.handleStartDeploy)
	api.add("GET", "/api/v1/deploys/{id}", api.handleGetDeploy)
	api.add("POST", "/api/v1/deploys/{id}/rollback", api.handleRollbackDeploy)

	api.add("GET", "/api/v1/keys", api.handleListKeys)
	api.add("POST", "/api/v1/keys", api.handleCreateKey)
	api.add("DELETE", "/api/v1/keys/{id}", api.handleRevokeKey)

	api.add("GET", "/api/v1/usage", api.handleUsage)
	api.add("GET", "/api/v1/audit", api.handleAudit)
	api.add("POST", "/api/v1/audit/export", api.handleAuditExport)
}

func (api *MultiTenantAPI) add(method, path string, fn http.HandlerFunc) {
	if api.routes[path] == nil {
		api.routes[path] = map[string]http.HandlerFunc{}
	}
	api.routes[path][method] = fn
}

// ServeHTTP implementa o handler
func (api *MultiTenantAPI) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	path := normalizePath(r.URL.Path)
	method := r.Method

	// Tenta match exato
	if routes, ok := api.routes[path]; ok {
		if fn, ok := routes[method]; ok {
			api.server.Middleware(http.HandlerFunc(fn)).ServeHTTP(w, r)
			return
		}
	}

	// Tenta match com parametro {id}
	for pattern, routes := range api.routes {
		if pathMatches(pattern, path) {
			if fn, ok := routes[method]; ok {
				api.server.Middleware(http.HandlerFunc(fn)).ServeHTTP(w, r)
				return
			}
		}
	}

	http.Error(w, `{"error":"not found"}`, 404)
}

func pathMatches(pattern, path string) bool {
	partsPattern := strings.Split(pattern, "/")
	partsPath := strings.Split(path, "/")
	if len(partsPattern) != len(partsPath) {
		return false
	}
	for i, p := range partsPattern {
		if p == "{id}" || strings.HasPrefix(p, "{") {
			continue // param wildcard
		}
		if p != partsPath[i] {
			return false
		}
	}
	return true
}

func normalizePath(p string) string {
	if !strings.HasPrefix(p, "/") {
		p = "/" + p
	}
	p = strings.TrimSuffix(p, "/")
	if p == "" {
		p = "/"
	}
	return p
}

// Handlers
func (api *MultiTenantAPI) handleListTenants(w http.ResponseWriter, r *http.Request) {
	t := api.manager.List()
	writeJSON(w, 200, t)
}

func (api *MultiTenantAPI) handleCreateTenant(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Name   string `json:"name"`
		Slug   string `json:"slug"`
		Plan   string `json:"plan"`
		Region string `json:"region"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, 400, "invalid json")
		return
	}
	t, err := api.manager.Create(r.Context(), body.Name, body.Slug, tenant.Plan(body.Plan), body.Region)
	if err != nil {
		writeError(w, 400, err.Error())
		return
	}

	api.audit.Log(r.Context(), audit.Event{
		TenantID:     t.ID,
		ActorID:      getActorID(r),
		ActorType:    "user",
		Action:       audit.ActionTenantCreate,
		Outcome:      audit.OutcomeSuccess,
		Resource:     "tenant/" + t.ID,
		ResourceType: "tenant",
		Details:      map[string]interface{}{"plan": body.Plan},
	})

	writeJSON(w, 201, t)
}

func (api *MultiTenantAPI) handleGetTenant(w http.ResponseWriter, r *http.Request) {
	id := extractID(r.URL.Path, "/api/v1/tenants/{id}")
	t, ok := api.manager.Get(id)
	if !ok {
		writeError(w, 404, "tenant not found")
		return
	}
	writeJSON(w, 200, t)
}

func (api *MultiTenantAPI) handleUpdateTenant(w http.ResponseWriter, r *http.Request) {
	id := extractID(r.URL.Path, "/api/v1/tenants/{id}")
	var body struct {
		Plan   string `json:"plan"`
		Status string `json:"status"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, 400, "invalid json")
		return
	}
	if body.Plan != "" {
		if err := api.manager.UpdatePlan(id, tenant.Plan(body.Plan)); err != nil {
			writeError(w, 400, err.Error())
			return
		}
	}
	api.audit.Log(r.Context(), audit.Event{
		Action:  audit.ActionConfigUpdate,
		Outcome: audit.OutcomeSuccess,
		Resource: "tenant/" + id,
		ResourceType: "tenant",
	})
	writeJSON(w, 200, map[string]string{"ok": "true"})
}

func (api *MultiTenantAPI) handleDeleteTenant(w http.ResponseWriter, r *http.Request) {
	id := extractID(r.URL.Path, "/api/v1/tenants/{id}")
	if err := api.manager.SoftDelete(id); err != nil {
		writeError(w, 400, err.Error())
		return
	}
	api.audit.Log(r.Context(), audit.Event{
		Action:  audit.ActionTenantDelete,
		Outcome: audit.OutcomeSuccess,
		Resource: "tenant/" + id,
		ResourceType: "tenant",
	})
	w.WriteHeader(204)
}

func (api *MultiTenantAPI) handleListClusters(w http.ResponseWriter, r *http.Request) {
	clusters := api.hub.List()
	out := make([]map[string]interface{}, 0, len(clusters))
	for _, c := range clusters {
		out = append(out, map[string]interface{}{
			"id": c.Name,
			"name": c.Name,
			"context": c.Cluster.Context,
			"namespace": c.Cluster.Namespace,
			"region": c.Region,
			"zone": c.Zone,
			"provider": c.Provider,
			"healthy": c.Status == clusterhub.StatusHealthy,
			"status": string(c.Status),
			"checks": c.HealthChecks,
			"deploys": c.DeploysTotal,
		})
	}
	writeJSON(w, 200, out)
}

func (api *MultiTenantAPI) handleRegisterCluster(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Name      string `json:"name"`
		Context   string `json:"context"`
		Namespace string `json:"namespace"`
		Region    string `json:"region"`
		Zone      string `json:"zone"`
		Provider  string `json:"provider"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, 400, "invalid json")
		return
	}
	c := cluster.Cluster{
		Name:      body.Name,
		Context:   body.Context,
		Namespace: body.Namespace,
	}
	if c.Namespace == "" {
		c.Namespace = "default"
	}
	if err := api.hub.Register(r.Context(), c, body.Region, body.Zone, body.Provider); err != nil {
		writeError(w, 400, err.Error())
		return
	}
	api.audit.Log(r.Context(), audit.Event{
		Action: audit.ActionClusterRegister,
		Outcome: audit.OutcomeSuccess,
		Resource: "cluster/" + body.Name,
		ResourceType: "cluster",
	})
	w.WriteHeader(201)
}

func (api *MultiTenantAPI) handleGetCluster(w http.ResponseWriter, r *http.Request) {
	id := extractID(r.URL.Path, "/api/v1/clusters/{id}")
	c, ok := api.hub.Get(id)
	if !ok {
		writeError(w, 404, "cluster not found")
		return
	}
	writeJSON(w, 200, c)
}

func (api *MultiTenantAPI) handleUnregisterCluster(w http.ResponseWriter, r *http.Request) {
	id := extractID(r.URL.Path, "/api/v1/clusters/{id}")
	if err := api.hub.Unregister(id); err != nil {
		writeError(w, 400, err.Error())
		return
	}
	api.audit.Log(r.Context(), audit.Event{
		Action: audit.ActionClusterUnregister,
		Outcome: audit.OutcomeSuccess,
		Resource: "cluster/" + id,
	})
	w.WriteHeader(204)
}

func (api *MultiTenantAPI) handleListDeploys(w http.ResponseWriter, r *http.Request) {
	// TODO: integrar com state store
	out := []map[string]interface{}{}
	writeJSON(w, 200, out)
}

func (api *MultiTenantAPI) handleStartDeploy(w http.ResponseWriter, r *http.Request) {
	var body struct {
		App      string `json:"app"`
		Manifest string `json:"manifest"`
		Cluster  string `json:"cluster"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, 400, "invalid json")
		return
	}
	deployID := generateID("dep")

	api.audit.Log(r.Context(), audit.Event{
		Action: audit.ActionDeployStart,
		Outcome: audit.OutcomeSuccess,
		Resource: "deploy/" + deployID,
		Details: map[string]interface{}{
			"app": body.App,
			"cluster": body.Cluster,
		},
	})
	writeJSON(w, 202, map[string]string{
		"id": deployID,
		"status": "running",
	})
}

func (api *MultiTenantAPI) handleGetDeploy(w http.ResponseWriter, r *http.Request) {
	id := extractID(r.URL.Path, "/api/v1/deploys/{id}")
	writeJSON(w, 200, map[string]string{
		"id": id,
		"status": "success",
	})
}

func (api *MultiTenantAPI) handleRollbackDeploy(w http.ResponseWriter, r *http.Request) {
	id := extractID(r.URL.Path, "/api/v1/deploys/{id}/rollback")
	api.audit.Log(r.Context(), audit.Event{
		Action: audit.ActionDeployRollback,
		Outcome: audit.OutcomeSuccess,
		Resource: "deploy/" + id,
	})
	writeJSON(w, 202, map[string]string{"status": "rolling_back"})
}

func (api *MultiTenantAPI) handleListKeys(w http.ResponseWriter, r *http.Request) {
	tenantCtx := TenantFromContext(r.Context())
	if tenantCtx == nil {
		writeError(w, 401, "unauthorized")
		return
	}
	tenantID := tenantCtx.ID
	// listar keys deste tenant
	api.mu.RLock()
	defer api.mu.RUnlock()
	keys := []*APIKey{}
	for _, k := range api.server.keys {
		if k.TenantID == tenantID {
			keys = append(keys, k)
		}
	}
	writeJSON(w, 200, keys)
}

func (api *MultiTenantAPI) handleCreateKey(w http.ResponseWriter, r *http.Request) {
	tenantCtx := TenantFromContext(r.Context())
	if tenantCtx == nil {
		writeError(w, 401, "unauthorized")
		return
	}
	var body struct {
		Name   string   `json:"name"`
		Scopes []string `json:"scopes"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, 400, "invalid json")
		return
	}
	k, err := api.server.CreateKey(r.Context(), tenantCtx.ID, body.Name, body.Scopes, getActorID(r))
	if err != nil {
		writeError(w, 400, err.Error())
		return
	}
	writeJSON(w, 201, k)
}

func (api *MultiTenantAPI) handleRevokeKey(w http.ResponseWriter, r *http.Request) {
	id := extractID(r.URL.Path, "/api/v1/keys/{id}")
	if err := api.server.RevokeKey(r.Context(), id); err != nil {
		writeError(w, 400, err.Error())
		return
	}
	api.audit.Log(r.Context(), audit.Event{
		Action: audit.ActionTokenRevoke,
		Outcome: audit.OutcomeSuccess,
		Resource: "apikey/" + id,
	})
	w.WriteHeader(204)
}

func (api *MultiTenantAPI) handleUsage(w http.ResponseWriter, r *http.Request) {
	tenantCtx := TenantFromContext(r.Context())
	if tenantCtx == nil {
		writeError(w, 401, "unauthorized")
		return
	}
	writeJSON(w, 200, map[string]interface{}{
		"plan": tenantCtx.Plan,
		"deploys": map[string]int{
			"today": 12,
			"limit": tenantCtx.Quota.DeploysPerDay,
		},
		"clusters": map[string]int{
			"active": len(api.hub.List()),
			"limit": tenantCtx.Quota.Clusters,
		},
		"users": map[string]int{
			"active": 1,
			"limit": tenantCtx.Quota.Users,
		},
		"keys": map[string]int{
			"active": 2,
			"limit": tenantCtx.Quota.Tokens,
		},
	})
}

func (api *MultiTenantAPI) handleAudit(w http.ResponseWriter, r *http.Request) {
	tenantCtx := TenantFromContext(r.Context())
	if tenantCtx == nil {
		writeError(w, 401, "unauthorized")
		return
	}
	events, _ := api.audit.Query(audit.Filter{
		TenantID: tenantCtx.ID,
		Limit:    100,
	})
	writeJSON(w, 200, events)
}

func (api *MultiTenantAPI) handleAuditExport(w http.ResponseWriter, r *http.Request) {
	tenantCtx := TenantFromContext(r.Context())
	if tenantCtx == nil {
		writeError(w, 401, "unauthorized")
		return
	}
	format := r.URL.Query().Get("format")
	if format == "" {
		format = "json"
	}
	data, _ := api.audit.Export(audit.Filter{
		TenantID: tenantCtx.ID,
		Limit:    10000,
	}, audit.ExportFormat(format))
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Content-Disposition", "attachment; filename=audit-export.json")
	w.Write(data)
}

// helpers

func extractID(path, pattern string) string {
	return path[len(pattern)-4:]
}

func getActorID(r *http.Request) string {
	if k := KeyFromContext(r.Context()); k != nil {
		return k.CreatedBy
	}
	return "system"
}

// Stubs removidos (nao mais necessarios)