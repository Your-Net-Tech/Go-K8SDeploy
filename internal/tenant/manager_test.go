package tenant

import (
	"context"
	"crypto/rand"
	"fmt"
	"path/filepath"
	"sync"
	"testing"

	"k8s-deploy/internal/store"
)

func setupTestManager(t *testing.T) *Manager {
	key := make([]byte, 32)
	rand.Read(key)
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")
	db, err := store.NewDB(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })

	m, err := NewManager(db, key)
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Setup(); err != nil {
		t.Fatal(err)
	}
	return m
}

func TestTenantCreateAndGet(t *testing.T) {
	m := setupTestManager(t)
	ctx := context.Background()

	tenant, err := m.Create(ctx, "Test Corp", "test-corp", PlanPro, "us-east-1")
	if err != nil {
		t.Fatal(err)
	}
	if tenant.Slug != "test-corp" {
		t.Fatalf("expected slug test-corp, got %s", tenant.Slug)
	}
	if tenant.Plan != PlanPro {
		t.Fatalf("expected plan PRO, got %s", tenant.Plan)
	}
	if tenant.Status != "active" {
		t.Fatalf("expected active, got %s", tenant.Status)
	}

	// Get
	got, ok := m.Get("test-corp")
	if !ok {
		t.Fatal("tenant not found")
	}
	if got.ID != tenant.ID {
		t.Fatalf("id mismatch")
	}
}

func TestTenantPlanUpdate(t *testing.T) {
	m := setupTestManager(t)
	ctx := context.Background()

	_, _ = m.Create(ctx, "Test", "test", PlanFree, "us-east-1")
	err := m.UpdatePlan("test", PlanEnterprise)
	if err != nil {
		t.Fatal(err)
	}
	got, _ := m.Get("test")
	if got.Plan != PlanEnterprise {
		t.Fatalf("plan not updated: %s", got.Plan)
	}
	if got.Quota.Clusters != 1000 {
		t.Fatalf("quota not updated: %d clusters", got.Quota.Clusters)
	}
}

func TestTenantSuspend(t *testing.T) {
	m := setupTestManager(t)
	ctx := context.Background()

	_, _ = m.Create(ctx, "Test", "test", PlanPro, "us-east-1")
	err := m.Suspend("test", "non-payment")
	if err != nil {
		t.Fatal(err)
	}
	got, _ := m.Get("test")
	if got.Status != "suspended" {
		t.Fatalf("status not suspended: %s", got.Status)
	}
}

func TestTenantSoftDelete(t *testing.T) {
	m := setupTestManager(t)
	ctx := context.Background()

	_, _ = m.Create(ctx, "Test", "test", PlanPro, "us-east-1")
	err := m.SoftDelete("test")
	if err != nil {
		t.Fatal(err)
	}
	got, _ := m.Get("test")
	if got.Status != "deleted" {
		t.Fatalf("not deleted")
	}
	if got.DeletedAt == nil {
		t.Fatal("DeletedAt not set")
	}
}

func TestTenantEncryption(t *testing.T) {
	m := setupTestManager(t)

	plaintext := []byte("secret-data-12345")
	ciphertext, err := m.Encrypt(plaintext)
	if err != nil {
		t.Fatal(err)
	}

	decrypted, err := m.Decrypt(ciphertext)
	if err != nil {
		t.Fatal(err)
	}
	if string(decrypted) != string(plaintext) {
		t.Fatal("decryption mismatch")
	}
}

func TestTenantEncryptionField(t *testing.T) {
	m := setupTestManager(t)

	field := "ssn-123-45-6789"
	encrypted, err := m.EncryptField(field)
	if err != nil {
		t.Fatal(err)
	}
	if encrypted == field {
		t.Fatal("field not encrypted")
	}

	decrypted, err := m.DecryptField(encrypted)
	if err != nil {
		t.Fatal(err)
	}
	if decrypted != field {
		t.Fatalf("decryption mismatch: %s", decrypted)
	}
}

func TestDefaultQuota(t *testing.T) {
	tests := []struct {
		plan     Plan
		clusters int
		deploys  int
		users    int
	}{
		{PlanFree, 1, 100, 1},
		{PlanStarter, 3, 1000, 5},
		{PlanPro, 20, 10000, 50},
		{PlanEnterprise, 1000, 1000000, 1000},
	}

	for _, tt := range tests {
		q := DefaultQuota(tt.plan)
		if q.Clusters != tt.clusters {
			t.Errorf("plan %s: expected %d clusters, got %d", tt.plan, tt.clusters, q.Clusters)
		}
		if q.DeploysPerMonth != tt.deploys {
			t.Errorf("plan %s: expected %d deploys, got %d", tt.plan, tt.deploys, q.DeploysPerMonth)
		}
		if q.Users != tt.users {
			t.Errorf("plan %s: expected %d users, got %d", tt.plan, tt.users, q.Users)
		}
	}
}

func TestConcurrentTenantAccess(t *testing.T) {
	m := setupTestManager(t)
	ctx := context.Background()
	for i := 0; i < 100; i++ {
		_, _ = m.Create(ctx, "Test", fmt.Sprintf("test-%d", i), PlanPro, "us-east-1")
	}

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			slug := fmt.Sprintf("test-%d", idx)
			got, ok := m.Get(slug)
			if !ok {
				t.Errorf("tenant %s not found", slug)
				return
			}
			if got.Slug != slug {
				t.Errorf("slug mismatch for %s", slug)
			}
		}(i)
	}
	wg.Wait()
}