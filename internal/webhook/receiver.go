package webhook

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os/exec"
	"sync"
	"time"

	"github.com/spf13/cobra"

	"k8s-deploy/internal/notify"
)

type Receiver struct {
	port      string
	secret    string
	notifier  *notify.Notifier
	mu        sync.Mutex
	triggers  []Trigger
}

type Trigger struct {
	Project   string
	Path      string
	On        string // push, pull_request, tag, all
	Branches  []string
	LastFired time.Time
}

type Event struct {
	Provider  string
	Event     string
	Repo      string
	Branch    string
	Commit    string
	Author    string
	Project   string
	Timestamp time.Time
}

func New(port string, secret string, n *notify.Notifier) *Receiver {
	return &Receiver{
		port:     port,
		secret:   secret,
		notifier: n,
	}
}

func (r *Receiver) AddTrigger(t Trigger) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.triggers = append(r.triggers, t)
}

func (r *Receiver) Start(ctx context.Context) error {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", r.health)
	mux.HandleFunc("/webhook/github", r.handleGitHub)
	mux.HandleFunc("/webhook/gitlab", r.handleGitLab)
	mux.HandleFunc("/webhook/generic", r.handleGeneric)
	mux.HandleFunc("/triggers", r.listTriggers)
	mux.HandleFunc("/trigger/{project}", r.manualTrigger)

	r.notifier.Send("webhook-receiver", fmt.Sprintf("started on :%s", r.port), "info")

	srv := &http.Server{Addr: ":" + r.port, Handler: mux}
	go func() {
		<-ctx.Done()
		srv.Shutdown(ctx)
	}()
	return srv.ListenAndServe()
}

func (r *Receiver) health(w http.ResponseWriter, req *http.Request) {
	w.WriteHeader(200)
	w.Write([]byte(`{"status":"ok"}`))
}

func (r *Receiver) listTriggers(w http.ResponseWriter, req *http.Request) {
	r.mu.Lock()
	defer r.mu.Unlock()
	json.NewEncoder(w).Encode(r.triggers)
}

func (r *Receiver) manualTrigger(w http.ResponseWriter, req *http.Request) {
	project := req.PathValue("project")
	r.fire(project, "manual")
	w.WriteHeader(202)
	w.Write([]byte(fmt.Sprintf(`{"fired":true,"project":"%s"}`, project)))
}

func (r *Receiver) handleGitHub(w http.ResponseWriter, req *http.Request) {
	if !r.verify(req, "X-Hub-Signature-256") {
		http.Error(w, "invalid signature", 401)
		return
	}

	body, _ := io.ReadAll(req.Body)
	eventType := req.Header.Get("X-GitHub-Event")

	if eventType == "ping" {
		w.WriteHeader(200)
		w.Write([]byte(`{"ok":true}`))
		return
	}

	var payload struct {
		Repository struct {
			FullName string `json:"full_name"`
		} `json:"repository"`
		Ref     string `json:"ref"`
		After   string `json:"after"`
		Sender  struct {
			Login string `json:"login"`
		} `json:"sender"`
	}

	if err := json.Unmarshal(body, &payload); err != nil {
		http.Error(w, "invalid json", 400)
		return
	}

	branch := ""
	if len(payload.Ref) > 11 && payload.Ref[:11] == "refs/heads/" {
		branch = payload.Ref[11:]
	}

	ev := Event{
		Provider:  "github",
		Event:     eventType,
		Repo:      payload.Repository.FullName,
		Branch:    branch,
		Commit:    payload.After,
		Author:    payload.Sender.Login,
		Timestamp: time.Now(),
	}

	r.notifier.Send("github-webhook",
		fmt.Sprintf("%s/%s @ %s por %s", ev.Repo, ev.Branch, ev.Commit[:7], ev.Author),
		"info")

	if eventType == "push" {
		r.handlePush(ev, branch)
	}

	w.WriteHeader(200)
	w.Write([]byte(`{"received":true}`))
}

func (r *Receiver) handleGitLab(w http.ResponseWriter, req *http.Request) {
	body, _ := io.ReadAll(req.Body)
	eventType := req.Header.Get("X-Gitlab-Event")

	var payload struct {
		Project struct {
			PathWithNamespace string `json:"path_withnamespace"`
		} `json:"project"`
		Ref    string `json:"ref"`
		After  string `json:"after"`
		User   struct {
			Username string `json:"username"`
		} `json:"user_username"`
	}

	if err := json.Unmarshal(body, &payload); err != nil {
		http.Error(w, "invalid json", 400)
		return
	}

	branch := ""
	if len(payload.Ref) > 11 {
		branch = payload.Ref[11:]
	}

	ev := Event{
		Provider:  "gitlab",
		Event:     eventType,
		Repo:      payload.Project.PathWithNamespace,
		Branch:    branch,
		Commit:    payload.After,
		Author:    payload.User.Username,
		Timestamp: time.Now(),
	}

	r.notifier.Send("gitlab-webhook",
		fmt.Sprintf("%s @ %s por %s", ev.Repo, branch, ev.Author), "info")

	if eventType == "Push Hook" {
		r.handlePush(ev, branch)
	}

	w.WriteHeader(200)
	w.Write([]byte(`{"received":true}`))
}

func (r *Receiver) handleGeneric(w http.ResponseWriter, req *http.Request) {
	body, _ := io.ReadAll(req.Body)
	var ev Event
	if err := json.Unmarshal(body, &ev); err != nil {
		http.Error(w, "invalid json", 400)
		return
	}
	ev.Timestamp = time.Now()
	if ev.Project != "" {
		r.fire(ev.Project, "generic")
	}
	w.WriteHeader(200)
}

func (r *Receiver) verify(req *http.Request, sigHeader string) bool {
	if r.secret == "" {
		return true
	}
	sig := req.Header.Get(sigHeader)
	if sig == "" {
		return false
	}

	body, _ := io.ReadAll(req.Body)
	req.Body = io.NopCloser(bytes.NewReader(body))

	mac := hmac.New(sha256.New, []byte(r.secret))
	mac.Write(body)
	expected := "sha256=" + hex.EncodeToString(mac.Sum(nil))
	return hmac.Equal([]byte(sig), []byte(expected))
}

func (r *Receiver) handlePush(ev Event, branch string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	for i, t := range r.triggers {
		if !r.matchTrigger(t, branch, "push") {
			continue
		}
		if time.Since(r.triggers[i].LastFired) < 30*time.Second {
			continue
		}
		r.triggers[i].LastFired = time.Now()
		r.fire(t.Project, "webhook")
	}
}

func (r *Receiver) matchTrigger(t Trigger, branch, event string) bool {
	if t.On != "" && t.On != "all" && t.On != event {
		return false
	}
	if len(t.Branches) > 0 {
		found := false
		for _, b := range t.Branches {
			if b == branch {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

func (r *Receiver) fire(project, source string) {
	cmd := exec.Command("k8s-deploy", "apply", "-p", project)
	out, err := cmd.CombinedOutput()
	if err != nil {
		r.notifier.Send("deploy-failed", fmt.Sprintf("%s (%s): %s", project, source, err), "error")
		return
	}
	r.notifier.Send("deploy-success", fmt.Sprintf("%s via %s\n%s", project, source, string(out)), "success")
}

var WebhookCmd = &cobra.Command{
	Use:   "webhook",
	Short: "Inicia servidor webhook (GitOps mode)",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := cmd.Context()
		port, _ := cmd.Flags().GetString("port")
		secret, _ := cmd.Flags().GetString("secret")
		n := notify.New()
		r := New(port, secret, n)
		fmt.Printf("Webhook receiver na :%s\n", port)
		return r.Start(ctx)
	},
}