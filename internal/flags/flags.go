// Package flags implementa feature flags sem dependencia de terceiros.
//
// Sem LaunchDarkly, Unleash, Split.io, Statsig. 100% proprietario.
//
// Features sao armazenadas em memoria + SQLite.
// Podem ter targeting rules, rollout percentual, variacoes.
package flags

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"k8s-deploy/internal/store"
)

type Flag struct {
	Key         string            `yaml:"key" json:"key"`
	Description string            `yaml:"description" json:"description"`
	Enabled     bool              `yaml:"enabled" json:"enabled"`
	Type        FlagType          `yaml:"type" json:"type"` // bool, string, int, json
	DefaultValue interface{}      `yaml:"default_value" json:"default_value"`
	Variations  []Variation       `yaml:"variations" json:"variations"`
	Targeting   *Targeting        `yaml:"targeting,omitempty" json:"targeting,omitempty"`
	Rules       []Rule            `yaml:"rules,omitempty" json:"rules,omitempty"`
	CreatedAt   time.Time         `yaml:"created_at" json:"created_at"`
	UpdatedAt   time.Time         `yaml:"updated_at" json:"updated_at"`
	Tags        []string          `yaml:"tags,omitempty" json:"tags,omitempty"`
}

type FlagType string

const (
	TypeBool   FlagType = "bool"
	TypeString FlagType = "string"
	TypeInt    FlagType = "int"
	TypeJSON   FlagType = "json"
)

type Variation struct {
	Key   string      `json:"key"`
	Value interface{} `json:"value"`
}

// Targeting eh a regra de quem recebe o que
type Targeting struct {
	// Rollout percentual de uma variacao
	// 0-100, usuarios no % recebem essa variacao
	Rollouts []Rollout `yaml:"rollouts" json:"rollouts"`
}

type Rollout struct {
	Variation string  `yaml:"variation" json:"variation"` // chave da variacao
	Percent   float64 `yaml:"percent" json:"percent"`     // 0-100
	// User targeting: lista de chaves que entram nessa variacao
	Users []string `yaml:"users,omitempty" json:"users,omitempty"`
}

// Rule permite conditions complexas
type Rule struct {
	Name      string            `yaml:"name" json:"name"`
	Variation string            `yaml:"variation" json:"variation"`
	Match     []MatchCondition  `yaml:"match" json:"match"`
}

type MatchCondition struct {
	Attribute string      `yaml:"attribute" json:"attribute"` // ex: "user_id", "namespace"
	Op        string      `yaml:"op" json:"op"`               // eq, neq, in, contains, regex
	Value     interface{} `yaml:"value" json:"value"`
}

// Context usado na avaliacao
type Context struct {
	UserID    string            `json:"user_id"`
	Namespace string            `json:"namespace"`
	Service   string            `json:"service"`
	Version   string            `json:"version"`
	Attrs     map[string]string `json:"attrs"`
}

// Event log para auditoria
type Event struct {
	Time    time.Time `json:"time"`
	Flag    string    `json:"flag"`
	User    string    `json:"user"`
	Result  string    `json:"result"`
	Reason  string    `json:"reason"`
}

// Engine eh o motor de feature flags
type Engine struct {
	mu     sync.RWMutex
	flags  map[string]*Flag
	db     *store.DB
	events []Event
	maxEvents int
}

// NewEngine cria engine com persistencia em SQLite
func NewEngine(dataDir string) (*Engine, error) {
	dbPath := filepath.Join(dataDir, "flags.db")
	db, err := store.NewDB(dbPath)
	if err != nil {
		return nil, fmt.Errorf("abrir db flags: %w", err)
	}
	if err := db.Migrate(`
		CREATE TABLE IF NOT EXISTS flags (
			key TEXT PRIMARY KEY,
			data TEXT,
			updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
		);
		CREATE TABLE IF NOT EXISTS flag_events (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			time DATETIME DEFAULT CURRENT_TIMESTAMP,
			flag TEXT,
			user TEXT,
			result TEXT,
			reason TEXT
		);
	`); err != nil {
		return nil, fmt.Errorf("migrate: %w", err)
	}
	return &Engine{
		flags:     map[string]*Flag{},
		db:        db,
		maxEvents: 10000,
	}, nil
}

func (e *Engine) Close() error { return e.db.Close() }

func (e *Engine) loadFlags() error {
	rows, err := e.db.Query("SELECT key, data FROM flags")
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var key, data string
		if err := rows.Scan(&key, &data); err != nil {
			return err
		}
		var f Flag
		if err := json.Unmarshal([]byte(data), &f); err != nil {
			continue
		}
		e.flags[key] = &f
	}
	return nil
}

// CreateFlag adiciona flag nova
func (e *Engine) CreateFlag(f Flag) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if _, ok := e.flags[f.Key]; ok {
		return fmt.Errorf("flag %s ja existe", f.Key)
	}
	f.CreatedAt = time.Now()
	f.UpdatedAt = f.CreatedAt
	e.flags[f.Key] = &f
	return e.save(f)
}

// UpdateFlag atualiza flag
func (e *Engine) UpdateFlag(f Flag) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if _, ok := e.flags[f.Key]; !ok {
		return fmt.Errorf("flag %s nao encontrada", f.Key)
	}
	f.UpdatedAt = time.Now()
	e.flags[f.Key] = &f
	return e.save(f)
}

func (e *Engine) save(f Flag) error {
	data, _ := json.Marshal(f)
	_, err := e.db.Exec(
		"INSERT OR REPLACE INTO flags (key, data, updated_at) VALUES (?, ?, ?)",
		f.Key, string(data), f.UpdatedAt)
	return err
}

// GetFlag retorna flag
func (e *Engine) GetFlag(key string) (*Flag, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	f, ok := e.flags[key]
	return f, ok
}

// ListFlags lista todas as flags
func (e *Engine) ListFlags() []*Flag {
	e.mu.RLock()
	defer e.mu.RUnlock()
	out := make([]*Flag, 0, len(e.flags))
	for _, f := range e.flags {
		out = append(out, f)
	}
	return out
}

// DeleteFlag remove flag
func (e *Engine) DeleteFlag(key string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if _, ok := e.flags[key]; !ok {
		return fmt.Errorf("flag nao encontrada")
	}
	delete(e.flags, key)
	_, err := e.db.Exec("DELETE FROM flags WHERE key = ?", key)
	return err
}

// Evaluate eh o ponto de entrada principal: dado um context, qual valor a flag retorna?
func (e *Engine) Evaluate(ctx Context, flagKey string) (interface{}, error) {
	f, ok := e.GetFlag(flagKey)
	if !ok {
		return nil, fmt.Errorf("flag %s nao encontrada", flagKey)
	}
	if !f.Enabled {
		return f.DefaultValue, nil
	}

	value, reason := e.evaluateInternal(f, ctx)
	e.logEvent(flagKey, ctx.UserID, fmt.Sprintf("%v", value), reason)
	return value, nil
}

func (e *Engine) evaluateInternal(f *Flag, ctx Context) (interface{}, string) {
	// 1. Regras com match targeting
	for _, rule := range f.Rules {
		if e.matchesRule(rule, ctx) {
			for _, v := range f.Variations {
				if v.Key == rule.Variation {
					return v.Value, "rule:" + rule.Name
				}
			}
		}
	}

	// 2. Rollouts percentuais
	if f.Targeting != nil && len(f.Targeting.Rollouts) > 0 {
		// Avalia cada rollout em ordem
		bucket := hashToBucket(ctx.UserID, f.Key)
		cumulative := 0.0
		for _, r := range f.Targeting.Rollouts {
			// Targeting de user especifico
			for _, u := range r.Users {
				if u == ctx.UserID {
					for _, v := range f.Variations {
						if v.Key == r.Variation {
							return v.Value, "user-targeted"
						}
					}
				}
			}
			cumulative += r.Percent
			if bucket < cumulative {
				for _, v := range f.Variations {
					if v.Key == r.Variation {
						return v.Value, fmt.Sprintf("rollout-%.0f%%", r.Percent)
					}
				}
			}
		}
	}

	// 3. Default
	for _, v := range f.Variations {
		if v.Key == "default" || len(v.Key) == 0 {
			return v.Value, "default"
		}
	}
	return f.DefaultValue, "fallback"
}

func (e *Engine) matchesRule(rule Rule, ctx Context) bool {
	for _, m := range rule.Match {
		val := e.getAttr(ctx, m.Attribute)
		if !e.matchValue(val, m.Op, m.Value) {
			return false
		}
	}
	return true
}

func (e *Engine) getAttr(ctx Context, attr string) string {
	switch attr {
	case "user_id":
		return ctx.UserID
	case "namespace":
		return ctx.Namespace
	case "service":
		return ctx.Service
	case "version":
		return ctx.Version
	}
	if ctx.Attrs != nil {
		return ctx.Attrs[attr]
	}
	return ""
}

func (e *Engine) matchValue(val, op string, expected interface{}) bool {
	switch op {
	case "eq":
		return val == fmt.Sprintf("%v", expected)
	case "neq":
		return val != fmt.Sprintf("%v", expected)
	case "in":
		if list, ok := expected.([]interface{}); ok {
			for _, e := range list {
				if val == fmt.Sprintf("%v", e) {
					return true
				}
			}
		}
	case "regex":
		// implementacao real regex
		return false
	}
	return false
}

// hashToBucket hash consistente de user_id+flag para bucket 0-99
func hashToBucket(userID, flagKey string) float64 {
	h := 0
	for _, c := range userID + flagKey {
		h = h*31 + int(c)
	}
	if h < 0 {
		h = -h
	}
	return float64(h%10000) / 100.0
}

func (e *Engine) logEvent(flag, user, result, reason string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.events = append(e.events, Event{
		Time:   time.Now(),
		Flag:   flag,
		User:   user,
		Result: result,
		Reason: reason,
	})
	if len(e.events) > e.maxEvents {
		e.events = e.events[len(e.events)-e.maxEvents:]
	}
	e.db.Exec(
		"INSERT INTO flag_events (flag, user, result, reason) VALUES (?, ?, ?, ?)",
		flag, user, result, reason)
}

// Events lista ultimos eventos
func (e *Engine) Events(limit int) []Event {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if limit <= 0 || limit > len(e.events) {
		limit = len(e.events)
	}
	out := make([]Event, limit)
	copy(out, e.events[len(e.events)-limit:])
	return out
}

// UpdateFlagBool atalho para toggle
func (e *Engine) UpdateFlagBool(key string, enabled bool) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	f, ok := e.flags[key]
	if !ok {
		return fmt.Errorf("flag nao encontrada")
	}
	f.Enabled = enabled
	f.UpdatedAt = time.Now()
	return e.save(*f)
}

// RolloutPercentage ajusta rollout percentile (para canary flag)
func (e *Engine) RolloutPercentage(key, variation string, percent float64) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	f, ok := e.flags[key]
	if !ok {
		return fmt.Errorf("flag nao encontrada")
	}
	if f.Targeting == nil {
		f.Targeting = &Targeting{}
	}
	for i, r := range f.Targeting.Rollouts {
		if r.Variation == variation {
			f.Targeting.Rollouts[i].Percent = percent
			f.UpdatedAt = time.Now()
			return e.save(*f)
		}
	}
	f.Targeting.Rollouts = append(f.Targeting.Rollouts, Rollout{
		Variation: variation,
		Percent:   percent,
	})
	f.UpdatedAt = time.Now()
	return e.save(*f)
}

// Para garantir que variavel os eh compilado
var _ = context.Background
var _ = os.Getenv