package webhook

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestWebhookSignatureVerificationAndBodyPreservation(t *testing.T) {
	secret := "super-secret-signature-key-1234"
	r := New("8080", secret, nil)

	payload := []byte(`{"repository":{"full_name":"test-repo"},"ref":"refs/heads/main","after":"abcdef123456","sender":{"login":"github-user"}}`)

	// Gera HMAC SHA256 correto
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(payload)
	signature := "sha256=" + hex.EncodeToString(mac.Sum(nil))

	req := httptest.NewRequest(http.MethodPost, "/webhook/github", bytes.NewReader(payload))
	req.Header.Set("X-Hub-Signature-256", signature)

	// 1. Assinatura correta deve passar
	if !r.verify(req, "X-Hub-Signature-256") {
		t.Fatal("assinatura válida foi rejeitada pelo verificador de webhook")
	}

	// 2. O corpo da requisição DEVE ser preservado para leitura posterior
	restoredPayload, err := io.ReadAll(req.Body)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(payload, restoredPayload) {
		t.Fatal("vulnerabilidade/bug: o corpo da requisição foi corrompido ou esgotado após a validação da assinatura")
	}

	// 3. Assinatura inválida deve falhar
	reqInvalid := httptest.NewRequest(http.MethodPost, "/webhook/github", bytes.NewReader(payload))
	reqInvalid.Header.Set("X-Hub-Signature-256", "sha256=invalidhashvalue")

	if r.verify(reqInvalid, "X-Hub-Signature-256") {
		t.Fatal("falha de segurança: assinatura inválida foi aceita pelo verificador de webhook")
	}
}
