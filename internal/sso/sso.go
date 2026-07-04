package sso

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"k8s-deploy/internal/notify"
	"k8s-deploy/internal/rbac"
	"gopkg.in/yaml.v3"
)

type Provider struct {
	Name         string   `yaml:"name"`
	Issuer       string   `yaml:"issuer"`
	ClientID     string   `yaml:"client_id"`
	ClientSecret string   `yaml:"client_secret"`
	RedirectURL  string   `yaml:"redirect_url"`
	Scopes       []string `yaml:"scopes"`
	Enabled      bool     `yaml:"enabled"`
}

type Session struct {
	Token     string    `json:"token"`
	Subject   string    `json:"subject"`
	Name      string    `json:"name"`
	Email     string    `json:"email"`
	Provider  string    `json:"provider"`
	Roles     []string  `json:"roles"`
	ExpiresAt time.Time `json:"expires_at"`
	CreatedAt time.Time `json:"created_at"`
}

type Server struct {
	mu          sync.RWMutex
	sessions    map[string]*Session
	pkceStates  map[string]string
	providers   map[string]*Provider
	rbac        *rbac.Engine
	notifier    *notify.Notifier
	dataDir     string
	sessionTTL  time.Duration
}

func New(dataDir string, r *rbac.Engine, n *notify.Notifier) *Server {
	return &Server{
		sessions:   map[string]*Session{},
		pkceStates: map[string]string{},
		providers:  map[string]*Provider{},
		rbac:       r,
		notifier:   n,
		dataDir:    dataDir,
		sessionTTL: 24 * time.Hour,
	}
}

func (s *Server) AddProvider(p Provider) error {
	if p.Issuer == "" || p.ClientID == "" || p.RedirectURL == "" {
		return fmt.Errorf("issuer, client_id e redirect_url sao obrigatorios")
	}
	if len(p.Scopes) == 0 {
		p.Scopes = []string{"openid", "profile", "email"}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.providers[p.Name] = &p
	return nil
}

func (s *Server) LoginURL(providerName, state string) (string, error) {
	s.mu.RLock()
	p, ok := s.providers[providerName]
	s.mu.RUnlock()
	if !ok {
		return "", fmt.Errorf("provider %s nao encontrado", providerName)
	}

	codeVerifier := generateCodeVerifier()
	s.pkceStates[state] = codeVerifier

	authURL := fmt.Sprintf("%s/protocol/openid-connect/auth", p.Issuer)
	redirectURL := fmt.Sprintf(
		"%s?response_type=code&client_id=%s&redirect_uri=%s&scope=%s&state=%s&code_challenge=%s&code_challenge_method=S256",
		authURL,
		p.ClientID,
		p.RedirectURL,
		joinScopes(p.Scopes),
		state,
		codeVerifier,
	)
	return redirectURL, nil
}

func (s *Server) HandleCallback(w http.ResponseWriter, r *http.Request, providerName string) error {
	code := r.URL.Query().Get("code")
	state := r.URL.Query().Get("state")
	if code == "" || state == "" {
		return fmt.Errorf("codigo ou state ausente")
	}

	s.mu.RLock()
	p := s.providers[providerName]
	codeVerifier, ok := s.pkceStates[state]
	s.mu.RUnlock()

	if !ok {
		return fmt.Errorf("state invalido")
	}
	if p == nil {
		return fmt.Errorf("provider nao encontrado")
	}

	token, err := s.exchangeCode(p, code, codeVerifier)
	if err != nil {
		return fmt.Errorf("falha no code exchange: %w", err)
	}

	idToken, ok := token["id_token"].(string)
	if !ok {
		return fmt.Errorf("id_token ausente")
	}

	claims, err := parseJWT(idToken)
	if err != nil {
		return fmt.Errorf("parse jwt: %w", err)
	}

	sessionToken := generateSessionToken()
	sess := &Session{
		Token:     sessionToken,
		Subject:   claims["sub"].(string),
		Name:      getString(claims, "name"),
		Email:     getString(claims, "email"),
		Provider:  providerName,
		Roles:     s.resolveRoles(claims),
		ExpiresAt: time.Now().Add(s.sessionTTL),
		CreatedAt: time.Now(),
	}

	s.mu.Lock()
	s.sessions[sessionToken] = sess
	delete(s.pkceStates, state)
	s.mu.Unlock()
	s.saveSessions()

	http.SetCookie(w, &http.Cookie{
		Name:     "k8d_session",
		Value:    sessionToken,
		Path:     "/",
		HttpOnly: true,
		Secure:   true,
		MaxAge:   int(s.sessionTTL.Seconds()),
	})

	s.notifier.Send("sso-login",
		fmt.Sprintf("%s (%s) via %s", sess.Name, sess.Email, providerName),
		"info")

	http.Redirect(w, r, "/", http.StatusFound)
	return nil
}

func (s *Server) Validate(token string) (*Session, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	sess, ok := s.sessions[token]
	if !ok {
		return nil, false
	}
	if time.Now().After(sess.ExpiresAt) {
		return nil, false
	}
	return sess, true
}

func (s *Server) Logout(token string) {
	s.mu.Lock()
	delete(s.sessions, token)
	s.mu.Unlock()
	s.saveSessions()
}

func (s *Server) ExchangeCodeForSession(code, state, providerName string) (*Session, error) {
	s.mu.RLock()
	p := s.providers[providerName]
	codeVerifier, ok := s.pkceStates[state]
	s.mu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("state invalido")
	}
	token, err := s.exchangeCode(p, code, codeVerifier)
	if err != nil {
		return nil, err
	}

	idToken, ok := token["id_token"].(string)
	if !ok {
		return nil, fmt.Errorf("id_token ausente")
	}
	claims, _ := parseJWT(idToken)

	sessionToken := generateSessionToken()
	sess := &Session{
		Token:     sessionToken,
		Subject:   claims["sub"].(string),
		Name:      getString(claims, "name"),
		Email:     getString(claims, "email"),
		Provider:  providerName,
		Roles:     s.resolveRoles(claims),
		ExpiresAt: time.Now().Add(s.sessionTTL),
		CreatedAt: time.Now(),
	}
	s.mu.Lock()
	s.sessions[sessionToken] = sess
	delete(s.pkceStates, state)
	s.mu.Unlock()
	s.saveSessions()
	return sess, nil
}

func (s *Server) exchangeCode(p *Provider, code, codeVerifier string) (map[string]interface{}, error) {
	tokenURL := fmt.Sprintf("%s/protocol/openid-connect/token", p.Issuer)

	body := fmt.Sprintf(
		"grant_type=authorization_code&code=%s&client_id=%s&client_secret=%s&redirect_uri=%s&code_verifier=%s",
		code, p.ClientID, p.ClientSecret, p.RedirectURL, codeVerifier)

	resp, err := http.Post(tokenURL, "application/x-www-form-urlencoded",
		strings.NewReader(body))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("token endpoint retornou %d", resp.StatusCode)
	}

	var token map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&token); err != nil {
		return nil, err
	}
	return token, nil
}

func (s *Server) resolveRoles(claims map[string]interface{}) []string {
	groups := []string{}
	if g, ok := claims["groups"].([]interface{}); ok {
		for _, v := range g {
			if str, ok := v.(string); ok {
				groups = append(groups, str)
			}
		}
	}
	return groups
}

func (s *Server) saveSessions() {
	path := filepath.Join(s.dataDir, "sessions.json")
	s.mu.RLock()
	list := make([]*Session, 0, len(s.sessions))
	for _, sess := range s.sessions {
		list = append(list, sess)
	}
	s.mu.RUnlock()
	data, _ := yaml.Marshal(list)
	os.WriteFile(path, data, 0644)
}

func generateCodeVerifier() string {
	b := make([]byte, 32)
	rand.Read(b)
	return base64URLEncode(b)
}

func generateSessionToken() string {
	b := make([]byte, 32)
	rand.Read(b)
	return hex.EncodeToString(b)
}

func base64URLEncode(b []byte) string {
	return hex.EncodeToString(b)[:43]
}

func joinScopes(scopes []string) string {
	out := ""
	for i, s := range scopes {
		if i > 0 {
			out += " "
		}
		out += s
	}
	return out
}

func getString(m map[string]interface{}, k string) string {
	if v, ok := m[k].(string); ok {
		return v
	}
	return ""
}