package daemon

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/fsnotify/fsnotify"

	"k8s-deploy/internal/notify"
)

type Watcher struct {
	root        string
	notifier    *notify.Notifier
	applyFunc   func() error
	mu          sync.Mutex
	lastHash    string
	lastApplied time.Time
	debounce    time.Duration
	autoSync    bool
}

func New(root string, n *notify.Notifier, apply func() error) *Watcher {
	return &Watcher{
		root:      root,
		notifier:  n,
		applyFunc: apply,
		debounce:  5 * time.Second,
		autoSync:  true,
	}
}

func (w *Watcher) SetAutoSync(v bool)         { w.autoSync = v }
func (w *Watcher) SetDebounce(d time.Duration) { w.debounce = d }

func (w *Watcher) Run(ctx context.Context) error {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return err
	}
	defer watcher.Close()

	if err := filepath.Walk(w.root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return watcher.Add(path)
		}
		return nil
	}); err != nil {
		return err
	}

	w.notifier.Send("daemon-started", fmt.Sprintf("watching %s", w.root), "info")

	var pending *time.Timer

	for {
		select {
		case <-ctx.Done():
			return nil

		case event, ok := <-watcher.Events:
			if !ok {
				return nil
			}
			if event.Op&(fsnotify.Write|fsnotify.Create|fsnotify.Remove) == 0 {
				continue
			}
			if filepath.Ext(event.Name) != ".yaml" &&
				filepath.Ext(event.Name) != ".yml" &&
				filepath.Base(event.Name) != "kustomization.yaml" &&
				filepath.Base(event.Name) != "kustomization.yml" {
				continue
			}

			if pending != nil {
				pending.Stop()
			}
			pending = time.AfterFunc(w.debounce, func() {
				w.sync("file-change")
			})

		case err, ok := <-watcher.Errors:
			if !ok {
				return nil
			}
			w.notifier.Send("watcher-error", err.Error(), "error")
		}
	}
}

func (w *Watcher) sync(source string) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if time.Since(w.lastApplied) < 10*time.Second {
		return nil
	}

	hash, err := w.computeHash()
	if err != nil {
		return err
	}

	if hash == w.lastHash {
		return nil
	}

	if !w.autoSync {
		w.notifier.Send("sync-needed", fmt.Sprintf("manifests alterados (hash %s)", hash[:8]), "info")
		return nil
	}

	w.notifier.Send("sync-running", fmt.Sprintf("auto-sync triggered (%s)", source), "info")

	if err := w.applyFunc(); err != nil {
		w.notifier.Send("sync-failed", err.Error(), "error")
		w.lastApplied = time.Now()
		return err
	}

	w.lastHash = hash
	w.lastApplied = time.Now()
	w.notifier.Send("sync-success", fmt.Sprintf("manifests aplicados (hash %s)", hash[:8]), "success")
	return nil
}

func (w *Watcher) computeHash() (string, error) {
	h := sha256.New()
	err := filepath.Walk(w.root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		ext := filepath.Ext(path)
		if ext != ".yaml" && ext != ".yml" {
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		h.Write(data)
		return nil
	})
	if err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}