package compliance

import (
	"bytes"
	"path/filepath"
	"testing"

	"k8s-deploy/internal/store"
)

func TestComplianceKMSKeySecurity(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")
	db, err := store.NewDB(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })

	masterKey := make([]byte, 32)
	for i := range masterKey {
		masterKey[i] = byte(i)
	}

	km, err := NewKeyManager(masterKey, db, nil)
	if err != nil {
		t.Fatal(err)
	}

	// Gera chaves para dois tenants diferentes
	key1, err := km.TenantKey("tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	key2, err := km.TenantKey("tenant-b")
	if err != nil {
		t.Fatal(err)
	}

	// 1. As chaves devem ter 32 bytes (AES-256)
	if len(key1) != 32 || len(key2) != 32 {
		t.Fatalf("tamanho incorreto da chave gerada: len=%d", len(key1))
	}

	// 2. Chaves de tenants diferentes não devem ser iguais (isolamento de clearance)
	if bytes.Equal(key1, key2) {
		t.Fatal("vulnerabilidade de segurança: chaves geradas para tenants diferentes são iguais")
	}

	// 3. Garantir que as chaves têm alta entropia (não sequenciais/previsíveis)
	zeroCount := 0
	for _, b := range key1 {
		if b == 0 {
			zeroCount++
		}
	}
	if zeroCount > 10 {
		t.Errorf("baixa entropia detectada na chave do KMS: zeroCount=%d", zeroCount)
	}
}

func TestComplianceKeyRotation(t *testing.T) {
	tmpDir := t.TempDir()
	db := setupTestDB(t, tmpDir)

	masterKey := make([]byte, 32)
	km, _ := NewKeyManager(masterKey, db, nil)

	keyBefore, _ := km.TenantKey("tenant-a")

	// Rotaciona chave
	err := km.RotateTenantKey("tenant-a")
	if err != nil {
		t.Fatal(err)
	}

	keyAfter, _ := km.TenantKey("tenant-a")

	if bytes.Equal(keyBefore, keyAfter) {
		t.Fatal("rotação de chave falhou: chave permaneceu a mesma")
	}
}

func setupTestDB(t *testing.T, dir string) *store.DB {
	dbPath := filepath.Join(dir, "test.db")
	db, err := store.NewDB(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}
