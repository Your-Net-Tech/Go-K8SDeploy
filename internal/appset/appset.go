package appset

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"text/template"
)

// AppSet define um template para gerar N applications
type AppSet struct {
	Name     string            `yaml:"name"`
	Template string            `yaml:"template"`
	Selector Selector          `yaml:"selector"`
	Vars     map[string]string `yaml:"vars,omitempty"`
}

type Selector struct {
	MatchNamespaces []string `yaml:"match_namespaces"`
	MatchNames      []string `yaml:"match_names"`
	MatchLabels     map[string]string `yaml:"match_labels"`
}

// App gerada a partir de AppSet + Selector
type App struct {
	Name      string
	Cluster   string
	Namespace string
	Path      string
}

type Generator struct {
	TemplatesDir string
}

func New(templatesDir string) *Generator {
	return &Generator{TemplatesDir: templatesDir}
}

// Generate gera as Apps com base nos AppSets e Selector
func (g *Generator) Generate(sets []AppSet, projectsDir string) ([]App, error) {
	var apps []App
	for _, set := range sets {
		if set.Template == "" {
			continue
		}

		tmplPath := filepath.Join(g.TemplatesDir, set.Template)
		if _, err := os.Stat(tmplPath); err != nil {
			return nil, fmt.Errorf("template %s nao encontrado: %w", tmplPath, err)
		}

		tmplData, err := os.ReadFile(tmplPath)
		if err != nil {
			return nil, err
		}

		tmpl, err := template.New(set.Name).Parse(string(tmplData))
		if err != nil {
			return nil, fmt.Errorf("parse %s: %w", set.Template, err)
		}

		matches, err := g.resolveSelector(set.Selector, projectsDir)
		if err != nil {
			return nil, err
		}

		for _, m := range matches {
			data := map[string]interface{}{
				"name":      m.Name,
				"cluster":   m.Cluster,
				"namespace": m.Namespace,
				"path":      m.Path,
			}
			for k, v := range set.Vars {
				data[k] = v
			}

			var buf strings.Builder
			if err := tmpl.Execute(&buf, data); err != nil {
				return nil, err
			}

			apps = append(apps, App{
				Name:      fmt.Sprintf("%s-%s", set.Name, m.Name),
				Cluster:   m.Cluster,
				Namespace: m.Namespace,
				Path:      buf.String(),
			})
		}
	}
	return apps, nil
}

func (g *Generator) resolveSelector(sel Selector, dir string) ([]App, error) {
	var apps []App

	if len(sel.MatchNamespaces) > 0 || len(sel.MatchNames) > 0 || len(sel.MatchLabels) > 0 {
		entries, err := os.ReadDir(dir)
		if err != nil {
			return apps, err
		}
		for _, e := range entries {
			if !e.IsDir() {
				continue
			}
			name := e.Name()

			if len(sel.MatchNames) > 0 {
				found := false
				for _, n := range sel.MatchNames {
					if n == name {
						found = true
						break
					}
				}
				if !found {
					continue
				}
			}

			projDir := filepath.Join(dir, name)
			labelFile := filepath.Join(projDir, ".labels")
			labels := map[string]string{}
			if data, err := os.ReadFile(labelFile); err == nil {
				for _, line := range strings.Split(string(data), "\n") {
					if parts := strings.SplitN(line, "=", 2); len(parts) == 2 {
						labels[parts[0]] = parts[1]
					}
				}
			}

			if len(sel.MatchLabels) > 0 {
				match := true
				for k, v := range sel.MatchLabels {
					if labels[k] != v {
						match = false
						break
					}
				}
				if !match {
					continue
				}
			}

			namespace := name
			if len(sel.MatchNamespaces) > 0 {
				nsFound := false
				for _, ns := range sel.MatchNamespaces {
					if ns == name {
						namespace = ns
						nsFound = true
						break
					}
				}
				if !nsFound && len(sel.MatchNamespaces) > 0 {
					continue
				}
			}

			apps = append(apps, App{
				Name:      name,
				Namespace: namespace,
				Path:      filepath.Join(projDir, "manifests"),
			})
		}
	}
	return apps, nil
}

func (a App) String() string {
	return fmt.Sprintf("App{name=%s, ns=%s, path=%s}", a.Name, a.Namespace, a.Path)
}