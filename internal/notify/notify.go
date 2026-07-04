package notify

import (
	"bytes"
	"fmt"
	"net/http"
	"os/exec"
	"sync"
	"time"

	"gopkg.in/yaml.v3"
)

type Channel struct {
	Name    string                 `yaml:"name"`
	Type    string                 `yaml:"type"` // telegram, slack, webhook, telegram (default)
	Enabled bool                   `yaml:"enabled"`
	Config  map[string]interface{} `yaml:"config"`
}

type Notifier struct {
	channels []Channel
	mu       sync.RWMutex
	history  []Event
}

type Event struct {
	Time    time.Time `yaml:"time"`
	Title   string    `yaml:"title"`
	Message string    `yaml:"message"`
	Level   string    `yaml:"level"` // info, warning, error, success
	Source  string    `yaml:"source"`
}

func New() *Notifier {
	return &Notifier{
		channels: []Channel{},
		history:  []Event{},
	}
}

func (n *Notifier) AddChannel(c Channel) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.channels = append(n.channels, c)
}

func (n *Notifier) LoadConfig(path string) error {
	data, err := exec.Command("cat", path).Output()
	if err != nil {
		return err
	}
	cfg := struct {
		Channels []Channel `yaml:"channels"`
	}{}
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return err
	}
	for _, c := range cfg.Channels {
		if c.Enabled {
			n.AddChannel(c)
		}
	}
	return nil
}

func (n *Notifier) Send(title, message, level string) {
	ev := Event{
		Time:    time.Now(),
		Title:   title,
		Message: message,
		Level:   level,
		Source:  "k8s-deploy",
	}

	n.mu.Lock()
	n.history = append(n.history, ev)
	n.mu.Unlock()

	n.mu.RLock()
	channels := append([]Channel{}, n.channels...)
	n.mu.RUnlock()

	for _, ch := range channels {
		go n.sendTo(ch, ev)
	}
}

func (n *Notifier) sendTo(ch Channel, ev Event) {
	switch ch.Type {
	case "telegram":
		n.sendTelegram(ch, ev)
	case "slack":
		n.sendSlack(ch, ev)
	case "webhook":
		n.sendWebhook(ch, ev)
	}
}

func (n *Notifier) sendTelegram(ch Channel, ev Event) {
	botToken, _ := ch.Config["bot_token"].(string)
	chatID, _ := ch.Config["chat_id"].(string)
	if botToken == "" || chatID == "" {
		return
	}

	text := fmt.Sprintf("🚨 *%s*\n\n%s\n\n_Time: %s_\n_Level: %s_",
		ev.Title, ev.Message, ev.Time.Format("15:04:05"), ev.Level)

	url := fmt.Sprintf("https://api.telegram.org/bot%s/sendMessage", botToken)
	data := fmt.Sprintf("chat_id=%s&text=%s&parse_mode=Markdown", chatID, urlEncode(text))

	resp, err := http.Post(url, "application/x-www-form-urlencoded", bytes.NewReader([]byte(data)))
	if err != nil {
		return
	}
	defer resp.Body.Close()
}

func (n *Notifier) sendSlack(ch Channel, ev Event) {
	url, _ := ch.Config["url"].(string)
	if url == "" {
		return
	}

	emoji := ":white_check_mark:"
	switch ev.Level {
	case "warning":
		emoji = ":warning:"
	case "error":
		emoji = ":x:"
	case "success":
		emoji = ":rocket:"
	}

	payload := fmt.Sprintf(`{
		"blocks": [
			{"type":"header","text":{"type":"plain_text","text":"%s %s"}},

			{"type":"section","text":{"type":"mrkdwn","text":"%s"}},
			{"type":"context","elements":[{"type":"mrkdwn","text":":clock9: %s | %s"}]}
		]
	}`, emoji, ev.Title, ev.Message, ev.Time.Format("15:04:05"), ev.Level)

	req, _ := http.NewRequest("POST", url, bytes.NewReader([]byte(payload)))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return
	}
	defer resp.Body.Close()
}

func (n *Notifier) sendWebhook(ch Channel, ev Event) {
	url, _ := ch.Config["url"].(string)
	if url == "" {
		return
	}
	payload, _ := yaml.Marshal(ev)
	req, _ := http.NewRequest("POST", url, bytes.NewReader(payload))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Deploy-Event", ev.Level)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return
	}
	defer resp.Body.Close()
}

func urlEncode(s string) string {
	// simple URL encoding for query params
	out := ""
	for _, r := range s {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') {
			out += string(r)
		} else {
			out += fmt.Sprintf("%%%02X", r)
		}
	}
	return out
}

func (n *Notifier) History() []Event {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return append([]Event{}, n.history...)
}