package helm

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"gopkg.in/yaml.v3"
)

type Chart struct {
	Name        string                 `yaml:"name"`
	Repo        string                 `yaml:"repo"`        // e.g. https://charts.bitnami.com/bitnami
	Chart       string                 `yaml:"chart"`       // chart name: e.g. postgresql
	Version     string                 `yaml:"version"`     // chart version
	Namespace   string                 `yaml:"namespace"`
	ReleaseName string                 `yaml:"release_name"`
	Values      map[string]interface{} `yaml:"values,omitempty"`
	ValuesFiles []string               `yaml:"values_files,omitempty"`
	Wait        bool                   `yaml:"wait"`        // wait for ready
	Timeout     time.Duration          `yaml:"timeout"`
	Atomic      bool                   `yaml:"atomic"`      // rollback on failure
	CreateNS    bool                   `yaml:"create_namespace"`
	Repository  string                 `yaml:"-"`           // path to cache dir
}

type Repo struct {
	Name string `yaml:"name"`
	URL  string `yaml:"url"`
}

type Engine struct {
	cacheDir  string
	repos     map[string]string // name -> url
	reposLock sync.RWMutex
	client    *http.Client
}

func New(cacheDir string) *Engine {
	os.MkdirAll(cacheDir, 0755)
	return &Engine{
		cacheDir: cacheDir,
		repos:    map[string]string{},
		client:   &http.Client{Timeout: 30 * time.Second},
	}
}

func (e *Engine) AddRepo(name, url string) error {
	e.reposLock.Lock()
	defer e.reposLock.Unlock()
	e.repos[name] = url
	return nil
}

func (e *Engine) HelmCmd(args ...string) (string, error) {
	cmd := exec.Command("helm", args...)
	out, err := cmd.CombinedOutput()
	return string(out), err
}

func (e *Engine) UpdateRepos() error {
	if len(e.repos) == 0 {
		return nil
	}
	reposFile := filepath.Join(e.cacheDir, "repositories.yaml")
	if err := e.writeReposFile(reposFile); err != nil {
		return err
	}
	_, err := e.HelmCmd("repo", "update", "--repository-config", reposFile)
	return err
}

func (e *Engine) writeReposFile(path string) error {
	c := struct {
		APIVersion  string       `yaml:"apiVersion"`
		Generated   string       `yaml:"generated"`
		Repositories []RepoEntry `yaml:"repositories"`
	}{
		APIVersion: "v1",
		Generated:  time.Now().Format(time.RFC3339),
	}

	e.reposLock.RLock()
	for n, u := range e.repos {
		c.Repositories = append(c.Repositories, RepoEntry{Name: n, URL: u})
	}
	e.reposLock.RUnlock()

	data, err := yaml.Marshal(c)
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0644)
}

type RepoEntry struct {
	Name string `yaml:"name"`
	URL  string `yaml:"url"`
}

func (e *Engine) PullChart(chart string) (string, error) {
	cachedPath := filepath.Join(e.cacheDir, "charts")
	os.MkdirAll(cachedPath, 0755)
	reposFile := filepath.Join(e.cacheDir, "repositories.yaml")
	if err := e.writeReposFile(reposFile); err != nil {
		return "", err
	}

	args := []string{"pull", chart, "--untar", "--untardir", cachedPath, "--repository-config", reposFile}
	out, err := e.HelmCmd(args...)
	if err != nil {
		return "", fmt.Errorf("helm pull %s: %s", chart, out)
	}

	name := chart
	name = strings.TrimSuffix(name, ".tgz")
	name = strings.Split(name, "/")[0]

	return filepath.Join(cachedPath, name), nil
}

func (e *Engine) Template(chart Chart) (string, error) {
	chartPath, err := e.PullChart(chart.Repo + "/" + chart.Chart)
	if err != nil {
		return "", err
	}
	if chart.Version != "" {
		chartPath = filepath.Join(e.cacheDir, "charts", fmt.Sprintf("%s-%s", chart.Chart, chart.Version))
		if _, err := os.Stat(chartPath); err != nil {
			chartPath, err = e.PullChart(fmt.Sprintf("%s/%s --version %s", chart.Repo, chart.Chart, chart.Version))
			if err != nil {
				return "", err
			}
		}
	}

	valuesFile, err := e.writeValuesFile(chart)
	if err != nil {
		return "", err
	}

	args := []string{"template", chart.ReleaseName, chartPath,
		"--namespace", chart.Namespace}
	if valuesFile != "" {
		args = append(args, "-f", valuesFile)
	}
	for _, vf := range chart.ValuesFiles {
		args = append(args, "-f", vf)
	}

	out, err := e.HelmCmd(args...)
	return out, err
}

func (e *Engine) Install(chart Chart) error {
	args := []string{"upgrade", "--install", chart.ReleaseName,
		fmt.Sprintf("%s/%s", chart.Repo, chart.Chart)}
	if chart.Version != "" {
		args = append(args, "--version", chart.Version)
	}
	if chart.Namespace != "" {
		args = append(args, "--namespace", chart.Namespace)
		args = append(args, "--create-namespace")
	}
	valuesFile, err := e.writeValuesFile(chart)
	if err != nil {
		return err
	}
	if valuesFile != "" {
		args = append(args, "-f", valuesFile)
	}
	for _, vf := range chart.ValuesFiles {
		args = append(args, "-f", vf)
	}
	if chart.Wait {
		args = append(args, "--wait")
	}
	if chart.Atomic {
		args = append(args, "--atomic")
	}
	if chart.Timeout > 0 {
		args = append(args, "--timeout", fmt.Sprintf("%ds", int(chart.Timeout.Seconds())))
	}

	reposFile := filepath.Join(e.cacheDir, "repositories.yaml")
	if err := e.writeReposFile(reposFile); err != nil {
		return err
	}
	args = append(args, "--repository-config", reposFile)

	out, err := e.HelmCmd(args...)
	if err != nil {
		return fmt.Errorf("helm install: %s", out)
	}
	return nil
}

func (e *Engine) Uninstall(releaseName, namespace string) error {
	args := []string{"uninstall", releaseName}
	if namespace != "" {
		args = append(args, "--namespace", namespace)
	}
	out, err := e.HelmCmd(args...)
	if err != nil {
		return fmt.Errorf("helm uninstall: %s", out)
	}
	return nil
}

func (e *Engine) List(namespace string) (string, error) {
	args := []string{"list"}
	if namespace != "" {
		args = append(args, "--namespace", namespace)
	}
	return e.HelmCmd(args...)
}

func (e *Engine) Status(releaseName, namespace string) (string, error) {
	return e.HelmCmd("status", releaseName, "--namespace", namespace)
}

func (e *Engine) writeValuesFile(c Chart) (string, error) {
	if c.Values == nil || len(c.Values) == 0 {
		return "", nil
	}
	hash := sha256.Sum256(mustJSON(c.Values))
	path := filepath.Join(e.cacheDir, fmt.Sprintf("values-%x.yaml", hash[:8]))
	if _, err := os.Stat(path); err == nil {
		return path, nil
	}
	data, err := yamlMarshal(c.Values)
	if err != nil {
		return "", err
	}
	if err := os.WriteFile(path, data, 0644); err != nil {
		return "", err
	}
	return path, nil
}

func mustJSON(v interface{}) []byte {
	b, _ := json.Marshal(v)
	return b
}

func yamlMarshal(v interface{}) ([]byte, error) {
	return yaml.Marshal(v)
}