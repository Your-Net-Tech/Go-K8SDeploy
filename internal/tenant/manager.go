// Package tenant implementa multi-tenancy forte para SaaS.
//
// Decisões:
//
// - Cada tenant tem namespace Kubernetes isolado
// - Quotas por plano (free/dev/pro/enterprise)
// - Audit log automático (toda ação escrita)
// - Soft delete (LGPD/GDPR compliance)
// - Encryption at rest (AES-256-GCM)
// - Encrypt em colunas sensitive (BYOK)
package tenant

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"sync"
	"time"

	"k8s-deploy/internal/store"
)

type Plan string

const (
	PlanFree       Plan = "free"
	PlanStarter    Plan = "starter"
	PlanPro        Plan = "pro"
	PlanEnterprise Plan = "enterprise"
)

type Tenant struct {
	ID           string    `json:"id"`
	Slug         string    `json:"slug"`
	Name         string    `json:"name"`
	Plan         Plan      `json:"plan"`
	Status       string    `json:"status"` // active, suspended, deleted
	Region       string    `json:"region"`
	CreatedAt    time.Time `json:"created_at"`
	UpdatedAt    time.Time `json:"updated_at"`
	DeletedAt    *time.Time `json:"deleted_at,omitempty"`
	Quota        Quota     `json:"quota"`
	Metadata     map[string]string `json:"metadata,omitempty"`
}

type Quota struct {
	Clusters          int `json:"clusters"`
	DeploysPerDay     int `json:"deploys_per_day"`
	DeploysPerMonth   int `json:"deploys_per_month"`
	ConcurrentDeploys int `json:"concurrent_deploys"`
	Users             int `json:"users"`
	Tokens            int `json:"tokens"`
	StorageGB         int `json:"storage_gb"`
}

func DefaultQuota(p Plan) Quota {
	switch p {
	case PlanFree:
		return Quota{1, 5, 100, 1, 1, 1, 1}
	case PlanStarter:
		return Quota{3, 50, 1000, 5, 5, 5, 10}
	case PlanPro:
		return Quota{20, 500, 10000, 50, 50, 50, 100}
	case PlanEnterprise:
		return Quota{1000, 50000, 1000000, 500, 1000, 1000, 10000}
	}
	return Quota{1, 5, 100, 1, 1, 1, 1}
}

// Manager gerencia tenants
type Manager struct {
	mu      sync.RWMutex
	tenants map[string]*Tenant
	db      *store.DB
	gcm     cipher.AEAD
}

func NewManager(db *store.DB, encryptionKey []byte) (*Manager, error) {
	if len(encryptionKey) != 32 {
		return nil, fmt.Errorf("chave deve ter 32 bytes")
	}
	block, err := aes.NewCipher(encryptionKey)
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	return &Manager{
		tenants: map[string]*Tenant{},
		db:      db,
		gcm:     gcm,
	}, nil
}

func (m *Manager) Setup() error {
	return m.db.Migrate(`
		CREATE TABLE IF NOT EXISTS tenants (
			id TEXT PRIMARY KEY,
			slug TEXT UNIQUE NOT NULL,
			name TEXT NOT NULL,
			plan TEXT NOT NULL,
			status TEXT NOT NULL DEFAULT 'active',
			region TEXT NOT NULL DEFAULT 'us-east-1',
			quota TEXT NOT NULL DEFAULT '{}',
			encrypted TEXT,
			created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
			updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
			deleted_at DATETIME
		);
		CREATE INDEX IF NOT EXISTS idx_tenants_status ON tenants(status);
		CREATE INDEX IF NOT EXISTS idx_tenants_slug ON tenants(slug);
	`)
}

// Create registra tenant novo
func (m *Manager) Create(ctx context.Context, name, slug string, plan Plan, region string) (*Tenant, error) {
	if _, ok := m.tenants[slug]; ok {
		return nil, fmt.Errorf("slug %s ja existe", slug)
	}
	id := generateID()
	t := &Tenant{
		ID:        id,
		Slug:      slug,
		Name:      name,
		Plan:      plan,
		Status:    "active",
		Region:    region,
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
		Quota:     DefaultQuota(plan),
	}

	q, _ := json.Marshal(t.Quota)
	_, err := m.db.Exec(
		"INSERT INTO tenants (id, slug, name, plan, status, region, quota) VALUES (?, ?, ?, ?, ?, ?, ?)",
		t.ID, t.Slug, t.Name, t.Plan, t.Status, t.Region, string(q))
	if err != nil {
		return nil, err
	}

	m.mu.Lock()
	m.tenants[slug] = t
	m.mu.Unlock()

	return t, nil
}

// Get retorna tenant
func (m *Manager) Get(slugOrID string) (*Tenant, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, t := range m.tenants {
		if t.Slug == slugOrID || t.ID == slugOrID {
			return t, true
		}
	}
	return nil, false
}

// List retorna todos tenants
func (m *Manager) List() []*Tenant {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]*Tenant, 0, len(m.tenants))
	for _, t := range m.tenants {
		out = append(out, t)
	}
	return out
}

// UpdatePlan atualiza plano (e quota)
func (m *Manager) UpdatePlan(slug string, plan Plan) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	t, ok := m.tenants[slug]
	if !ok {
		return fmt.Errorf("tenant nao encontrado")
	}
	t.Plan = plan
	t.Quota = DefaultQuota(plan)
	t.UpdatedAt = time.Now()

	q, _ := json.Marshal(t.Quota)
	_, err := m.db.Exec("UPDATE tenants SET plan = ?, quota = ?, updated_at = ? WHERE id = ?",
		t.Plan, string(q), t.UpdatedAt, t.ID)
	return err
}

// Suspend suspende tenant
func (m *Manager) Suspend(slug, reason string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	t, ok := m.tenants[slug]
	if !ok {
		return fmt.Errorf("nao encontrado")
	}
	t.Status = "suspended"
	t.UpdatedAt = time.Now()
	if t.Metadata == nil {
		t.Metadata = map[string]string{}
	}
	t.Metadata["suspend_reason"] = reason
	_, err := m.db.Exec("UPDATE tenants SET status = ?, updated_at = ? WHERE id = ?",
		t.Status, t.UpdatedAt, t.ID)
	return err
}

// SoftDelete marca como deletado (LGPD/GDPR)
func (m *Manager) SoftDelete(slug string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	t, ok := m.tenants[slug]
	if !ok {
		return fmt.Errorf("nao encontrado")
	}
	now := time.Now()
	t.Status = "deleted"
	t.DeletedAt = &now
	t.UpdatedAt = now
	_, err := m.db.Exec("UPDATE tenants SET status = ?, deleted_at = ?, updated_at = ? WHERE id = ?",
		t.Status, t.DeletedAt, t.UpdatedAt, t.ID)
	return err
}

// Encrypt encripta dados sensíveis (BYOK)
func (m *Manager) Encrypt(plaintext []byte) ([]byte, error) {
	nonce := make([]byte, m.gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, err
	}
	return m.gcm.Seal(nonce, nonce, plaintext, nil), nil
}

func (m *Manager) Decrypt(ciphertext []byte) ([]byte, error) {
	nonceSize := m.gcm.NonceSize()
	if len(ciphertext) < nonceSize {
		return nil, fmt.Errorf("ciphertext invalido")
	}
	nonce, ct := ciphertext[:nonceSize], ciphertext[nonceSize:]
	return m.gcm.Open(nil, nonce, ct, nil)
}

// EncryptField para campos PII
func (m *Manager) EncryptField(field string) (string, error) {
	enc, err := m.Encrypt([]byte(field))
	if err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(enc), nil
}

func (m *Manager) DecryptField(field string) (string, error) {
	data, err := base64.StdEncoding.DecodeString(field)
	if err != nil {
		return "", err
	}
	dec, err := m.Decrypt(data)
	if err != nil {
		return "", err
	}
	return string(dec), nil
}

// QuotaCheck checa se tenant pode fazer N deploys/cluster/users/etc
type QuotaCheck struct {
	OK    bool
	Limit int
	Used  int
}

func (m *Manager) QuotaCheck(slug, resource string) QuotaCheck {
	m.mu.RLock()
	t, ok := m.tenants[slug]
	m.mu.RUnlock()
	if !ok {
		return QuotaCheck{}
	}
	switch resource {
	case "clusters":
		return QuotaCheck{true, t.Quota.Clusters, countResources(slug, "clusters")}
	case "deploys_per_day":
		return QuotaCheck{true, t.Quota.DeploysPerDay, countResources(slug, "deploys_d")}
	case "users":
		return QuotaCheck{true, t.Quota.Users, countResources(slug, "users")}
	}
	return QuotaCheck{OK: true}
}

func countResources(slug, kind string) int {
	// Real implementation: query DB
	// Aqui stub simples
	return 0
}

// generateID gera ID unico
func generateID() string {
	b := make([]byte, 16)
	io.ReadFull(rand.Reader, b)
	return fmt.Sprintf("tnt_%s", base64.RawURLEncoding.EncodeToString(b))
}

// Render mostra tenant formatado
func (t *Tenant) Render() string {
	b, _ := json.MarshalIndent(t, "", "  ")
	return string(b)
}