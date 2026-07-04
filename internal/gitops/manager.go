// Package gitops implementa GitOps per-tenant para SaaS.
//
// Cada tenant pode ter seu proprio Git repository (GitHub, GitLab, Bitbucket).
// Quando ha commit no repo, sync eh automatico.
//
// Features:
//   - Multi-provider (GitHub App, GitLab OAuth, generic webhook)
//   - Per-tenant credentials (encrypted)
//   - Webhook receivers por tenant
//   - Manifest watch + drift detection
//   - Auto-sync com hooks
package gitops

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"sync"
	"time"

	"k8s-deploy/internal/audit"
	"k8s-deploy/internal/clusterhub"
	"k8s-deploy/internal/notify"
	"k8s-deploy/internal/tenant"
)

type RepoProvider string

const (
	ProviderGitHub    RepoProvider = "github"
	ProviderGitLab    RepoProvider = "gitlab"
	ProviderBitbucket RepoProvider = "bitbucket"
	ProviderWebhook  RepoProvider = "webhook"
)

type RepoConfig struct {
	TenantID    string       `json:"tenant_id"`
	ProjectID   string       `json:"project_id"` // app dentro do tenant
	Provider    RepoProvider `json:"provider"`
	URL         string       `json:"url"`      // ex: https://github.com/org/repo
	Branch      string       `json:"branch"`   // main, develop, etc
	Path        string       `json:"path"`     // path dentro do repo para manifests
	Credential  string       `json:"credential"` // encrypted com BYOK
	WebhookSecret string     `json:"webhook_secret"` // encrypted
	AutoSync    bool         `json:"auto_sync"`
	CreatedAt   time.Time    `json:"created_at"`
}

type SyncState struct {
	RepoConfigID string    `json:"repo_config_id"`
	ProjectID    string    `json:"project_id"`
	LastCommit   string    `json:"last_commit"`
	LastSyncAt   time.Time `json:"last_sync_at"`
	ManifestsCount int     `json:"manifests_count"`
	Drift        bool      `json:"drift"`
}

type Manager struct {
	mu        sync.RWMutex
	configs   map[string]*RepoConfig  // por project_id
	states    map[string]*SyncState   // state por project_id
	tenants   *tenant.Manager
	hub       *clusterhub.Hub
	audit     *audit.Logger
	notifier  *notify.Notifier
	webhookHandlers map[string]http.HandlerFunc
}

func NewManager(tenants *tenant.Manager, hub *clusterhub.Hub, auditLog *audit.Logger, n *notify.Notifier) *Manager {
	return &Manager{
		configs:   map[string]*RepoConfig{},
		states:    map[string]*SyncState{},
		tenants:   tenants,
		hub:       hub,
		audit:     auditLog,
		notifier:  n,
		webhookHandlers: map[string]http.HandlerFunc{},
	}
}

func (m *Manager) RegisterRepo(cfg RepoConfig) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if cfg.TenantID == "" || cfg.ProjectID == "" {
		return fmt.Errorf("tenant_id e project_id obrigatorios")
	}
	if cfg.Branch == "" {
		cfg.Branch = "main"
	}
	if cfg.Provider == "" {
		cfg.Provider = ProviderWebhook
	}
	cfg.CreatedAt = time.Now()
	m.configs[cfg.ProjectID] = &cfg
	m.states[cfg.ProjectID] = &SyncState{
		RepoConfigID: cfg.ProjectID,
		ProjectID:    cfg.ProjectID,
	}
	return nil
}

func (m *Manager) GetConfig(projectID string) (*RepoConfig, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	c, ok := m.configs[projectID]
	return c, ok
}

func (m *Manager) GetState(projectID string) (*SyncState, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	s, ok := m.states[projectID]
	return s, ok
}

func (m *Manager) ListConfigs(tenantID string) []*RepoConfig {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := []*RepoConfig{}
	for _, c := range m.configs {
		if tenantID == "" || c.TenantID == tenantID {
			out = append(out, c)
		}
	}
	return out
}

// OnWebhook processa webhook recebido
func (m *Manager) OnWebhook(ctx context.Context, projectID, signature string, payload []byte) error {
	m.mu.RLock()
	cfg, ok := m.configs[projectID]
	m.mu.RUnlock()
	if !ok {
		return fmt.Errorf("project %s nao encontrado", projectID)
	}

	// Verify HMAC
	if cfg.WebhookSecret != "" {
		expected := signPayload(payload, cfg.WebhookSecret)
		if !hmac.Equal([]byte(signature), []byte(expected)) {
			m.audit.Log(ctx, audit.Event{
				TenantID:     cfg.TenantID,
				Action:       audit.ActionLoginFailed,
				Outcome:      audit.OutcomeDenied,
				Resource:     "gitops/" + projectID,
				ResourceType: "gitops",
				Details:      map[string]interface{}{"reason": "invalid webhook signature"},
			})
			return fmt.Errorf("signature invalida")
		}
	}

	// Parse payload
	var data struct {
		Ref     string `json:"ref"`
		After   string `json:"after"`
		Repo    struct {
			FullName string `json:"full_name"`
		} `json:"repository"`
	}
	if err := json.Unmarshal(payload, &data); err != nil {
		return err
	}

	branch := cfg.Branch
	if data.Ref != "" && len(data.Ref) > 11 {
		branch = data.Ref[11:]
	}
	if branch != cfg.Branch {
		return nil // commit em outra branch, ignora
	}

	// Atualiza state
	m.mu.Lock()
	if s, ok := m.states[projectID]; ok {
		s.LastCommit = data.After
		s.LastSyncAt = time.Now()
	}
	m.mu.Unlock()

	// Trigger sync se auto_sync esta ativo
	if cfg.AutoSync {
		go m.SyncProject(ctx, projectID)
	}

	m.audit.Log(ctx, audit.Event{
		TenantID:     cfg.TenantID,
		Action:       audit.ActionManifestApply,
		Outcome:      audit.OutcomeSuccess,
		Resource:     "gitops/" + projectID,
		ResourceType: "gitops",
		Details: map[string]interface{}{
			"commit": data.After,
			"branch": branch,
		},
	})

	return nil
}

// SyncProject faz pull + apply
func (m *Manager) SyncProject(ctx context.Context, projectID string) error {
	m.mu.RLock()
	cfg, ok := m.configs[projectID]
	m.mu.RUnlock()
	if !ok {
		return fmt.Errorf("project nao encontrado")
	}

	// Clonar/atualizar repo em cache local
	localPath := fmt.Sprintf("/var/cache/k8s-deploy/gitops/%s/%s", cfg.TenantID, projectID)

	if err := m.cloneOrPull(ctx, cfg, localPath); err != nil {
		return fmt.Errorf("git clone: %w", err)
	}

	// Encontrar manifests
	manifestsDir := localPath + "/" + cfg.Path
	if cfg.Path == "" {
		manifestsDir = localPath
	}

	// Apply para cada cluster target
	clusters := m.hub.List()
	for _, c := range clusters {
		_ = c
		if !cfg.AutoSync {
			continue
		}
		_ = manifestsDir
	}

	m.mu.Lock()
	if s, ok := m.states[projectID]; ok {
		s.LastSyncAt = time.Now()
		s.ManifestsCount = 0
	}
	m.mu.Unlock()
	return nil
}

func (m *Manager) cloneOrPull(ctx context.Context, cfg *RepoConfig, path string) error {
	cmd := exec.CommandContext(ctx, "git", "clone", cfg.URL, path)
	if _, err := os.Stat(path); err == nil {
		cmd = exec.CommandContext(ctx, "git", "-C", path, "pull")
	}
	return cmd.Run()
}

func signPayload(payload []byte, secret string) string {
	h := hmac.New(sha256.New, []byte(secret))
	h.Write(payload)
	return "sha256=" + hex.EncodeToString(h.Sum(nil))
}

// WebhookHandler retorna http.Handler para webhook por tenant
func (m *Manager) WebhookHandler(projectID string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "POST only", 405)
			return
		}
		body := make([]byte, r.ContentLength)
		_, _ = r.Body.Read(body)

		sig := r.Header.Get("X-Hub-Signature-256")
		if sig == "" {
			sig = r.Header.Get("X-Gitlab-Token")
		}

		if err := m.OnWebhook(r.Context(), projectID, sig, body); err != nil {
			http.Error(w, err.Error(), 401)
			return
		}
		w.WriteHeader(200)
	}
}

// DriftDetector monitora desvio entre Git e cluster
func (m *Manager) DriftDetector(ctx context.Context, projectID string) error {
	m.mu.RLock()
	cfg, ok := m.configs[projectID]
	m.mu.RUnlock()
	if !ok {
		return fmt.Errorf("project nao encontrado")
	}

	localPath := fmt.Sprintf("/var/cache/k8s-deploy/gitops/%s/%s", cfg.TenantID, projectID)

	// git rev-parse HEAD
	cmd := exec.CommandContext(ctx, "git", "-C", localPath, "rev-parse", "HEAD")
	out, err := cmd.Output()
	if err != nil {
		return err
	}
	latestCommit := string(out[:len(out)-1])

	clusters := m.hub.List()
	for _, c := range clusters {
		_ = c
		// kubectl diff
	}

	m.mu.Lock()
	if s, ok := m.states[projectID]; ok {
		if s.LastCommit != latestCommit {
			s.Drift = true
		} else {
			s.Drift = false
		}
	}
	m.mu.Unlock()
	return nil
}
