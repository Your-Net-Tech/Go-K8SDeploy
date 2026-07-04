package rbac

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"k8s-deploy/internal/config"
)

type Principal struct {
	Subject  string   `yaml:"subject"`  // user id, e.g. "joao@telegram"
	Name     string   `yaml:"name"`
	Roles    []string `yaml:"roles"`
	Tokens   []string `yaml:"tokens,omitempty"`
}

type Engine struct {
	mu       sync.RWMutex
	principals map[string]*Principal
	roles       map[string]*config.Role
	defaultRole string
	dataDir     string
}

func New(dataDir string, cfg config.RBAC) *Engine {
	roles := map[string]*config.Role{}
	for i, r := range cfg.Roles {
		roles[r.Name] = &cfg.Roles[i]
	}

	return &Engine{
		principals:  map[string]*Principal{},
		roles:       roles,
		defaultRole: cfg.DefaultRole,
		dataDir:     dataDir,
	}
}

func (e *Engine) loadFromDisk() error {
	path := filepath.Join(e.dataDir, "rbac.json")
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	var principals []*Principal
	if err := json.Unmarshal(data, &principals); err != nil {
		return err
	}
	for _, p := range principals {
		e.principals[p.Subject] = p
	}
	return nil
}

func (e *Engine) saveToDisk() error {
	os.MkdirAll(e.dataDir, 0755)
	path := filepath.Join(e.dataDir, "rbac.json")
	var list []*Principal
	for _, p := range e.principals {
		list = append(list, p)
	}
	data, err := json.MarshalIndent(list, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0644)
}

func (e *Engine) AddPrincipal(p Principal) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if p.Subject == "" {
		return fmt.Errorf("subject required")
	}
	if err := e.loadFromDisk(); err != nil {
		return err
	}
	e.principals[p.Subject] = &p
	return e.saveToDisk()
}

func (e *Engine) Authorize(subject, verb, resource string) bool {
	e.mu.RLock()
	defer e.mu.RUnlock()

	p, ok := e.principals[subject]
	if !ok {
		return false
	}

	for _, rname := range p.Roles {
		r, ok := e.roles[rname]
		if !ok {
			continue
		}
		allowed := false
		for _, v := range r.Allow {
			if v == verb || v == "*" {
				allowed = true
				break
			}
		}
		if !allowed {
			continue
		}
		for _, res := range r.Resources {
			if res == resource || res == "*" {
				return true
			}
		}
	}
	return false
}

func (e *Engine) AuthenticateToken(token string) (string, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	for _, p := range e.principals {
		for _, t := range p.Tokens {
			if secureEqual(t, token) {
				return p.Subject, true
			}
		}
	}
	return "", false
}

func (e *Engine) GenerateToken(subject string) (string, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	p, ok := e.principals[subject]
	if !ok {
		return "", fmt.Errorf("principal %s nao encontrado", subject)
	}
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	tok := "kdt_" + hex.EncodeToString(b)
	p.Tokens = append(p.Tokens, tok)
	return tok, e.saveToDisk()
}

func (e *Engine) ListRoles() []string {
	e.mu.RLock()
	defer e.mu.RUnlock()
	out := make([]string, 0, len(e.roles))
	for n := range e.roles {
		out = append(out, n)
	}
	return out
}

func secureEqual(a, b string) bool {
	return subtle.ConstantTimeCompare([]byte(a), []byte(b)) == 1
}

func (e *Engine) String() string {
	return fmt.Sprintf("RBAC{roles=%d, principals=%d, default=%s}",
		len(e.roles), len(e.principals), e.defaultRole)
}

func (e *Engine) Principal(subject string) *Principal {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.principals[subject]
}

func VerbIn(verb string, allowed []string) bool {
	for _, a := range allowed {
		if strings.EqualFold(a, verb) {
			return true
		}
	}
	return false
}