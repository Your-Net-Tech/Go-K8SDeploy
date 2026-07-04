package approval

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"k8s-deploy/internal/notify"
)

type Request struct {
	ID         string    `json:"id"`
	Project    string    `json:"project"`
	Revision   int       `json:"revision"`
	Summary    string    `json:"summary"`
	Diff       string    `json:"diff"`
	Requester  string    `json:"requester"`
	RequestedAt time.Time `json:"requested_at"`
	Status     string    `json:"status"` // pending, approved, rejected, expired
	DecidedAt  *time.Time `json:"decided_at,omitempty"`
	DecidedBy  string    `json:"decided_by,omitempty"`
	Notes      string    `json:"notes,omitempty"`
	TTL        time.Duration `json:"ttl,omitempty"`
}

type Manager struct {
	mu       sync.RWMutex
	requests map[string]*Request
	dataDir  string
	notifier *notify.Notifier
	notify   <-chan string // telegram updates
}

func New(dataDir string, n *notify.Notifier) *Manager {
	return &Manager{
		requests: map[string]*Request{},
		dataDir:  dataDir,
		notifier: n,
	}
}

func (m *Manager) loadFromDisk() error {
	path := filepath.Join(m.dataDir, "approvals.json")
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	var requests []*Request
	if err := json.Unmarshal(data, &requests); err != nil {
		return err
	}
	for _, r := range requests {
		m.requests[r.ID] = r
	}
	return nil
}

func (m *Manager) saveToDisk() error {
	os.MkdirAll(m.dataDir, 0755)
	path := filepath.Join(m.dataDir, "approvals.json")
	var list []*Request
	for _, r := range m.requests {
		list = append(list, r)
	}
	data, err := json.MarshalIndent(list, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0644)
}

// RequestApproval cria uma requisicao que precisa de aprovacao
func (m *Manager) RequestApproval(ctx context.Context, project string, revision int, summary, diff, requester string, ttl time.Duration) (*Request, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.requests == nil {
		m.requests = map[string]*Request{}
	}
	if err := m.loadFromDisk(); err != nil {
		return nil, err
	}

	req := &Request{
		ID:          fmt.Sprintf("apr-%d-%d", revision, time.Now().Unix()),
		Project:     project,
		Revision:    revision,
		Summary:     summary,
		Diff:        diff,
		Requester:   requester,
		RequestedAt: time.Now(),
		Status:      "pending",
		TTL:         ttl,
	}
	m.requests[req.ID] = req
	m.saveToDisk()

	m.notifier.Send(
		fmt.Sprintf("APROVACAO NECESSARIA: %s rev %d", project, revision),
		fmt.Sprintf("%s\n\n%s\n\nAprovar: /approve %s\nRejeitar: /reject %s",
			summary, diff, req.ID, req.ID),
		"warning",
	)

	go m.expireAfter(req.ID, ttl)
	return req, nil
}

// WaitUntilDecided bloqueia ate a requisicao ser decidida ou expirar
func (m *Manager) WaitUntilDecided(ctx context.Context, id string) (*Request, error) {
	m.mu.RLock()
	req, ok := m.requests[id]
	m.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("request %s nao encontrado", id)
	}

	for {
		select {
		case <-ctx.Done():
			return req, ctx.Err()
		case <-time.After(2 * time.Second):
			m.mu.RLock()
			current := m.requests[id]
			m.mu.RUnlock()
			if current == nil {
				return nil, fmt.Errorf("request removido")
			}
			if current.Status != "pending" {
				return current, nil
			}
			if current.TTL > 0 && time.Since(current.RequestedAt) > current.TTL {
				m.mu.Lock()
				m.requests[id].Status = "expired"
				m.saveToDisk()
				m.mu.Unlock()
				m.notifier.Send(
					fmt.Sprintf("APROVACAO EXPIRADA: %s", current.Project),
					fmt.Sprintf("Request %s expirou sem decisao", id),
					"warning",
				)
				return m.requests[id], nil
			}
		}
	}
}

func (m *Manager) Approve(id, decider, notes string) error {
	return m.decide(id, decider, notes, "approved")
}

func (m *Manager) Reject(id, decider, notes string) error {
	return m.decide(id, decider, notes, "rejected")
}

func (m *Manager) decide(id, decider, notes, status string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	req, ok := m.requests[id]
	if !ok {
		return fmt.Errorf("request %s nao encontrado", id)
	}
	if req.Status != "pending" {
		return fmt.Errorf("request ja decidido: %s", req.Status)
	}

	now := time.Now()
	req.Status = status
	req.DecidedAt = &now
	req.DecidedBy = decider
	req.Notes = notes

	if err := m.saveToDisk(); err != nil {
		return err
	}

	emoji := "✅"
	if status == "rejected" {
		emoji = "❌"
	}
	m.notifier.Send(
		fmt.Sprintf("%s %s rev %d", emoji, req.Project, req.Revision),
		fmt.Sprintf("%s por %s\nNotas: %s", status, decider, notes),
		"info",
	)
	return nil
}

func (m *Manager) List() []*Request {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]*Request, 0, len(m.requests))
	for _, r := range m.requests {
		out = append(out, r)
	}
	return out
}

func (m *Manager) Pending() []*Request {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var out []*Request
	for _, r := range m.requests {
		if r.Status == "pending" {
			out = append(out, r)
		}
	}
	return out
}

func (m *Manager) Get(id string) (*Request, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	r, ok := m.requests[id]
	return r, ok
}

func (m *Manager) expireAfter(id string, ttl time.Duration) {
	if ttl <= 0 {
		return
	}
	time.Sleep(ttl)
	m.mu.Lock()
	defer m.mu.Unlock()
	if req, ok := m.requests[id]; ok && req.Status == "pending" {
		req.Status = "expired"
		req.DecidedAt = ptrTime(time.Now())
		m.saveToDisk()
	}
}

func ptrTime(t time.Time) *time.Time { return &t }