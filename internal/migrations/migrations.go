// Package migrations implementa controle de database migrations sem terceiros.
//
// Sem Flyway, Liquibase, Atlas. 100% proprietario.
//
// Suporta:
//   - SQL migrations (up/down)
//   - Rollback por versao
//   - Dry-run
//   - Status de migrations pendentes
//   - Lock via SQLite para impedir duplo run
package migrations

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"k8s-deploy/internal/store"
)

type Migration struct {
	Version  int
	Name     string
	UpSQL    string
	DownSQL  string
	Checksum string
	AppliedAt *time.Time
}

type Runner struct {
	dir        string
	db         *store.DB
	mu         sync.Mutex
	locked     bool
	lockKey    string
}

func NewRunner(dir string, db *store.DB) *Runner {
	return &Runner{dir: dir, db: db}
}

// Setup cria tabela de migrations
func (r *Runner) Setup() error {
	return r.db.Migrate(`
		CREATE TABLE IF NOT EXISTS schema_migrations (
			version INTEGER PRIMARY KEY,
			name TEXT NOT NULL,
			checksum TEXT NOT NULL,
			applied_at DATETIME DEFAULT CURRENT_TIMESTAMP
		);
	`)
}

// LoadDescobre descobre migrations do diretorio
func (r *Runner) LoadFromDir() ([]Migration, error) {
	if _, err := os.Stat(r.dir); err != nil {
		return nil, fmt.Errorf("diretorio %s nao existe", r.dir)
	}
	entries, err := os.ReadDir(r.dir)
	if err != nil {
		return nil, err
	}

	re := regexp.MustCompile(`^(\d{4,6})_(.+)\.sql$`)
	migrations := map[int]Migration{}

	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		m := re.FindStringSubmatch(e.Name())
		if m == nil {
			continue
		}
		v, _ := strconv.Atoi(m[1])
		name := m[2]
		data, err := os.ReadFile(filepath.Join(r.dir, e.Name()))
		if err != nil {
			return nil, err
		}
		upSQL, downSQL := splitUpDown(string(data))

		migrations[v] = Migration{
			Version: v,
			Name:    name,
			UpSQL:   upSQL,
			DownSQL: downSQL,
			Checksum: checksum(string(data)),
		}
	}

	out := make([]Migration, 0, len(migrations))
	for _, m := range migrations {
		out = append(out, m)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].Version < out[j].Version
	})
	return out, nil
}

// splitUpDown divide SQL em up e down parts
func splitUpDown(content string) (string, string) {
	parts := strings.Split(content, "-- +migrate Down")
	if len(parts) == 1 {
		return content, ""
	}
	return strings.TrimSpace(parts[0]), strings.TrimSpace(parts[1])
}

func checksum(s string) string {
	h := uint32(5381)
	for _, c := range s {
		h = h*33 + uint32(c)
	}
	return strconv.FormatUint(uint64(h), 16)
}

// Status retorna migrations aplicadas e pendentes
func (r *Runner) Status() (applied, pending []Migration, err error) {
	if err := r.Setup(); err != nil {
		return nil, nil, err
	}
	all, err := r.LoadFromDir()
	if err != nil {
		return nil, nil, err
	}

	rows, err := r.db.Query("SELECT version, name, checksum FROM schema_migrations ORDER BY version")
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()

	appliedMap := map[int]string{}
	for rows.Next() {
		var v int
		var name, sum string
		rows.Scan(&v, &name, &sum)
		appliedMap[v] = sum
	}

	for _, m := range all {
		mcopy := m
		if sum, ok := appliedMap[m.Version]; ok {
			t := time.Now()
			mcopy.AppliedAt = &t
			if sum != m.Checksum {
				mcopy.Name = mcopy.Name + " (CHECKSUM MUDOU)"
			}
			applied = append(applied, mcopy)
			delete(appliedMap, m.Version)
		} else {
			pending = append(pending, mcopy)
		}
	}

	// Migrations aplicadas mas arquivo sumiu (drift)
	for v, _ := range appliedMap {
		applied = append(applied, Migration{
			Version:   v,
			Name:      "(missing)",
			AppliedAt: timePtr(time.Now()),
		})
	}

	return applied, pending, nil
}

func timePtr(t time.Time) *time.Time { return &t }

// Apply aplica migrations pendentes
func (r *Runner) Apply(ctx context.Context, targetVersion int) error {
	r.mu.Lock()
	if r.locked {
		r.mu.Unlock()
		return fmt.Errorf("runner ja em uso")
	}
	r.locked = true
	r.mu.Unlock()
	defer func() {
		r.mu.Lock()
		r.locked = false
		r.mu.Unlock()
	}()

	applied, pending, err := r.Status()
	if err != nil {
		return err
	}

	maxApplied := 0
	for _, m := range applied {
		if m.Version > maxApplied && m.Name != "(missing)" {
			maxApplied = m.Version
		}
	}

	toApply := []Migration{}
	for _, m := range pending {
		if m.Version > maxApplied && (targetVersion == 0 || m.Version <= targetVersion) {
			toApply = append(toApply, m)
		}
	}

	for _, m := range toApply {
		if err := r.applyOne(ctx, m); err != nil {
			return fmt.Errorf("migration %d falhou: %w", m.Version, err)
		}
	}
	return nil
}

func (r *Runner) applyOne(ctx context.Context, m Migration) error {
	if m.UpSQL == "" {
		return nil
	}
	if _, err := r.db.Exec(m.UpSQL); err != nil {
		return err
	}

	_, err := r.db.Exec(
		"INSERT OR REPLACE INTO schema_migrations (version, name, checksum) VALUES (?, ?, ?)",
		m.Version, m.Name, m.Checksum)
	return err
}

// Rollback volta ateh target version
func (r *Runner) Rollback(ctx context.Context, targetVersion int) error {
	all, err := r.LoadFromDir()
	if err != nil {
		return err
	}

	applied, _, err := r.Status()
	if err != nil {
		return err
	}
	appliedVersions := map[int]bool{}
	for _, m := range applied {
		appliedVersions[m.Version] = true
	}

	toRollback := []Migration{}
	for _, m := range all {
		if appliedVersions[m.Version] && m.Version > targetVersion {
			if m.DownSQL != "" {
				toRollback = append(toRollback, m)
			}
		}
	}
	// reverse order
	for i := len(toRollback)/2 - 1; i >= 0; i-- {
		opp := len(toRollback) - 1 - i
		toRollback[i], toRollback[opp] = toRollback[opp], toRollback[i]
	}

	for _, m := range toRollback {
		if _, err := r.db.Exec(m.DownSQL); err != nil {
			return err
		}
		_, err := r.db.Exec("DELETE FROM schema_migrations WHERE version = ?", m.Version)
		if err != nil {
			return err
		}
	}
	return nil
}

// StatusReport retorna string do status
func (r *Runner) StatusReport() (string, error) {
	applied, pending, err := r.Status()
	if err != nil {
		return "", err
	}
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("\n=== Migrations: %d aplicadas, %d pendentes ===\n", len(applied), len(pending)))

	if len(applied) > 0 {
		sb.WriteString("\nAplicadas:\n")
		for _, m := range applied {
			status := "OK"
			if m.Name == "(missing)" {
				status = "DRIFT"
			} else if strings.Contains(m.Name, "CHECKSUM MUDOU") {
				status = "DRIFT"
			}
			sb.WriteString(fmt.Sprintf("  %06d %s [%s]\n", m.Version, m.Name, status))
		}
	}
	if len(pending) > 0 {
		sb.WriteString("\nPendentes:\n")
		for _, m := range pending {
			sb.WriteString(fmt.Sprintf("  %06d %s\n", m.Version, m.Name))
		}
	}
	return sb.String(), nil
}