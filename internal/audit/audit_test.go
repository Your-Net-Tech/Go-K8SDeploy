package audit

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"k8s-deploy/internal/store"
)

type MockSigner struct{}

func (m MockSigner) Sign(plaintext []byte) (string, error) {
	return "mock_signature_of_" + string(plaintext), nil
}

func TestAuditLogIntegrity(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")
	db, err := store.NewDB(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })

	logger, err := New(db, MockSigner{})
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()

	// Insere alguns logs de auditoria
	err = logger.Log(ctx, Event{
		TenantID:     "tenant-1",
		ActorID:      "user-1",
		ActorType:    "user",
		Action:       ActionUserLogin,
		Resource:     "login",
		ResourceType: "auth",
		Outcome:      OutcomeSuccess,
	})
	if err != nil {
		t.Fatal(err)
	}

	err = logger.Log(ctx, Event{
		TenantID:     "tenant-1",
		ActorID:      "user-1",
		ActorType:    "user",
		Action:       ActionDeployStart,
		Resource:     "deploy/web-app",
		ResourceType: "deploy",
		Outcome:      OutcomeSuccess,
	})
	if err != nil {
		t.Fatal(err)
	}

	// 1. A cadeia deve ser válida inicialmente
	ok, err := logger.Verify("tenant-1")
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("cadeia de hash íntegra reportou falha")
	}

	// 2. Simula adulteração (tampering) no DB de forma maliciosa
	_, err = db.Exec("UPDATE audit_events SET outcome = ? WHERE action = ?", OutcomeFailure, ActionUserLogin)
	if err != nil {
		t.Fatal(err)
	}

	// 3. A verificação deve detectar a quebra da integridade da cadeia de hashes
	ok, err = logger.Verify("tenant-1")
	if err == nil && ok {
		t.Fatal("falha de segurança: adulteração do banco não foi detectada pelo verificador de integridade")
	}
}

func TestAuditUniqueIDGeneration(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")
	db, err := store.NewDB(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })

	logger, _ := New(db, nil)
	ctx := context.Background()

	// Insere múltiplos eventos sem ID explícito para acionar a geração de IDs únicos aleatórios
	for i := 0; i < 5; i++ {
		err := logger.Log(ctx, Event{
			TenantID: "tenant-a",
			Action:   ActionConfigUpdate,
		})
		if err != nil {
			t.Fatalf("falha ao inserir evento de auditoria #%d: %v (provável colisão de ID/stub)", i, err)
		}
	}
}

func TestAuditRetentionCleanup(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")
	db, _ := store.NewDB(dbPath)
	t.Cleanup(func() { db.Close() })

	logger, _ := New(db, nil)
	ctx := context.Background()

	// Evento antigo
	oldTime := time.Now().Add(-400 * 24 * time.Hour)
	err := logger.Log(ctx, Event{
		Timestamp: oldTime,
		TenantID:  "tenant-a",
		Action:    ActionUserLogout,
	})
	if err != nil {
		t.Fatal(err)
	}

	// Evento recente
	err = logger.Log(ctx, Event{
		Timestamp: time.Now(),
		TenantID:  "tenant-a",
		Action:    ActionUserLogin,
	})
	if err != nil {
		t.Fatal(err)
	}

	// Limpa dados com retenção padrão de 365 dias
	err = logger.Cleanup(DefaultRetention())
	if err != nil {
		t.Fatal(err)
	}

	events, _ := logger.Query(Filter{TenantID: "tenant-a", Limit: 10})
	if len(events) != 1 {
		t.Fatalf("retenção falhou: esperado 1 evento, restaram %d", len(events))
	}
	if events[0].Action != ActionUserLogin {
		t.Fatal("evento errado foi mantido após limpeza de retenção")
	}
}
