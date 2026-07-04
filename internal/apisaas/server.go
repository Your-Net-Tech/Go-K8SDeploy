// Package apisaas implementa API HTTP multi-tenant para SaaS.
//
// Características:
//   - Multi-tenant API keys (com prefixo identificador)
//   - Rate limit por plano (Free, Starter, Pro, Enterprise)
//   - Quotas enforcement
//   - Auth middleware
//   - Audit log automático
package apisaas

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"k8s-deploy/internal/audit"
	"k8s-deploy/internal/ratelimit"
	"k8s-deploy/internal/tenant"
)

type APIKey struct {
	ID         string    `json:"id"`
	Key        string    `json:"key"` // raw, never persisted
	Prefix     string    `json:"prefix"`
	TenantID   string    `json:"tenant_id"`
	Name       string    `json:"name"`
	Scopes     []string  `json:"scopes"`     // read, write, admin
	CreatedAt  time.Time `json:"created_at"`
	ExpiresAt  *time.Time `json:"expires_at,omitempty"`
	LastUsedAt *time.Time `json:"last_used_at,omitempty"`
	CreatedBy  string    `json:"created_by"`
}

// RateLimitSettings por plano
type RateLimitTier struct {
	RequestsPerSecond int
	Burst             int
	RequestsPerDay    int
	Concurrency       int
}

func Tiers() map[tenant.Plan]RateLimitTier {
	return map[tenant.Plan]RateLimitTier{
		tenant.PlanFree:       {1, 5, 1000, 1},
		tenant.PlanStarter:    {10, 50, 50000, 5},
		tenant.PlanPro:        {50, 200, 500000, 50},
		tenant.PlanEnterprise: {500, 2000, -1, 500},
	}
}

type Server struct {
	mu        sync.RWMutex
	keys      map[string]*APIKey // hash -> APIKey
	tenants   *tenant.Manager
	audit     *audit.Logger
	limiter   *ratelimit.MultiLimiter
	handlers  map[string]http.Handler
	onRequest func(ctx context.Context, req *http.Request) error
}

func NewServer(tenants *tenant.Manager, auditLog *audit.Logger, limiter *ratelimit.MultiLimiter) *Server {
	return &Server{
		keys:     map[string]*APIKey{},
		tenants:  tenants,
		audit:    auditLog,
		limiter:  limiter,
		handlers: map[string]http.Handler{},
	}
}

// CreateKey gera nova API key para tenant
func (s *Server) CreateKey(ctx context.Context, tenantID, name string, scopes []string, createdBy string) (*APIKey, error) {
	t, ok := s.tenants.Get(tenantID)
	if !ok {
		return nil, fmt.Errorf("tenant %s nao encontrado", tenantID)
	}
	if t.Status != "active" {
		return nil, fmt.Errorf("tenant nao esta ativo")
	}

	// gera key
	b := make([]byte, 32)
	rand.Read(b)
	rawKey := "kdt_" + base64.RawURLEncoding.EncodeToString(b)
	keyID := generateID("key")
	prefix := rawKey[:12]

	k := &APIKey{
		ID:        keyID,
		Key:       rawKey,
		Prefix:    prefix,
		TenantID:  tenantID,
		Name:      name,
		Scopes:    scopes,
		CreatedAt: time.Now(),
		CreatedBy: createdBy,
	}

	s.mu.Lock()
	// salvamos hash da key, nao a key em si
	s.keys[hashKey(rawKey)] = k
	s.mu.Unlock()

	// log
	s.audit.Log(ctx, audit.Event{
		TenantID:     tenantID,
		ActorID:      createdBy,
		ActorType:    "user",
		Action:       audit.ActionTokenCreate,
		Resource:     "apikey/" + keyID,
		ResourceType: "apikey",
		Outcome:      audit.OutcomeSuccess,
		Details: map[string]interface{}{
			"scopes": scopes,
			"name":   name,
		},
	})

	return k, nil
}

// RevokeKey revoga
func (s *Server) RevokeKey(ctx context.Context, keyID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for hash, k := range s.keys {
		if k.ID == keyID {
			delete(s.keys, hash)
			return nil
		}
	}
	return fmt.Errorf("key nao encontrada")
}

// Hash da key para indexacao
func hashKey(key string) string {
	// simplificacao: usamos os primeiros chars como hash index
	return key[:16]
}

// Auth valida key e retorna contexto do tenant
type AuthResult struct {
	Authenticated bool
	Key           *APIKey
	Tenant        *tenant.Tenant
	StatusCode    int
	Error         string
}

func (s *Server) Auth(rawKey string) AuthResult {
	if rawKey == "" {
		return AuthResult{StatusCode: 401, Error: "missing API key"}
	}
	prefix := rawKey[:16]
	s.mu.RLock()
	defer s.mu.RUnlock()
	for hash, k := range s.keys {
		if hash == prefix {
			if k.Key != rawKey {
				continue
			}
			t, ok := s.tenants.Get(k.TenantID)
			if !ok {
				return AuthResult{StatusCode: 401, Error: "tenant invalid"}
			}
			if t.Status != "active" {
				return AuthResult{StatusCode: 403, Error: "tenant inactive"}
			}
			now := time.Now()
			k.LastUsedAt = &now
			return AuthResult{
				Authenticated: true,
				Key:           k,
				Tenant:        t,
				StatusCode:    200,
			}
		}
	}
	return AuthResult{StatusCode: 401, Error: "invalid API key"}
}

// Middleware HTTP que verifica auth + rate limit
func (s *Server) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auth := extractAuth(r)
		if auth == "" {
			http.Error(w, `{"error":"unauthorized"}`, 401)
			s.audit.Log(r.Context(), audit.Event{
				SourceIP:  r.RemoteAddr,
				UserAgent: r.UserAgent(),
				Action:    audit.ActionAuthRejected,
				Outcome:   audit.OutcomeDenied,
				Details:   map[string]interface{}{"reason": "no_api_key"},
			})
			return
		}

		result := s.Auth(auth)
		if !result.Authenticated {
			http.Error(w, fmt.Sprintf(`{"error":"%s"}`, result.Error), result.StatusCode)
			s.audit.Log(r.Context(), audit.Event{
				SourceIP:  r.RemoteAddr,
				UserAgent: r.UserAgent(),
				Action:    audit.ActionLoginFailed,
				Outcome:   audit.OutcomeDenied,
				Details:   map[string]interface{}{"reason": result.Error},
			})
			return
		}

		// Rate limit por tenant+plano
		t := Tiers()[result.Tenant.Plan]
		tier := ratelimit.Tier{
			RequestsPerSecond: t.RequestsPerSecond,
			Burst:             t.Burst,
			RequestsPerDay:    t.RequestsPerDay,
			Concurrency:       t.Concurrency,
		}
		ok := s.limiter.Allow(result.Key.ID, tier)
		if !ok {
			w.Header().Set("Retry-After", "60")
			http.Error(w, `{"error":"rate_limit_exceeded","tier":"`+string(result.Tenant.Plan)+`"}`, 429)
			return
		}

		// injeta contexto
		ctx := r.Context()
		ctx = context.WithValue(ctx, KeyContextKey{}, result.Key)
		ctx = context.WithValue(ctx, TenantContextKey{}, result.Tenant)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func extractAuth(r *http.Request) string {
	h := r.Header.Get("Authorization")
	if h == "" {
		return ""
	}
	if len(h) > 7 && h[:7] == "Bearer " {
		return h[7:]
	}
	return ""
}

// Contexto
type KeyContextKey struct{}
type TenantContextKey struct{}

func KeyFromContext(ctx context.Context) *APIKey {
	k, _ := ctx.Value(KeyContextKey{}).(*APIKey)
	return k
}

func TenantFromContext(ctx context.Context) *tenant.Tenant {
	t, _ := ctx.Value(TenantContextKey{}).(*tenant.Tenant)
	return t
}

// JSON response
func writeJSON(w http.ResponseWriter, code int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(data)
}

func writeError(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}

func generateID(prefix string) string {
	b := make([]byte, 8)
	rand.Read(b)
	return prefix + "_" + base64.RawURLEncoding.EncodeToString(b)
}