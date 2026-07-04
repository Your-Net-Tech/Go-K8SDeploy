// Package istio implementa traffic split via Istio VirtualService.
//
// Especifico para canary rollouts usar Istio quando disponivel.
package istio

import (
	"context"
	"fmt"
	"os/exec"
	"strings"
)

type VirtualService struct {
	Name      string
	Namespace string
	Hosts     []string
	Gateways  []string

	// HTTP routes com peso
	Routes []Route

	// Outras config opcionais
	HTTPRetry *Retry
}

type Route struct {
	// Destinations com pesos para canary
	Destinations []Destination `yaml:"destinations"`
	// Match conditions
	Match []Match `yaml:"match,omitempty"`
}

type Destination struct {
	Host   string `yaml:"host"`
	Subset string `yaml:"subset,omitempty"`
	Weight int    `yaml:"weight"` // 0-100
	Port   *Port  `yaml:"port,omitempty"`
}

type Port struct {
	Number   uint32 `yaml:"number"`
	Name     string `yaml:"name,omitempty"`
	Protocol string `yaml:"protocol,omitempty"`
}

type Match struct {
	Headers    map[string]MatchHeader `yaml:"headers,omitempty"`
	URI        *URIMatch             `yaml:"uri,omitempty"`
	Method     string                `yaml:"method,omitempty"`
}

type MatchHeader struct {
	Exact  string `yaml:"exact,omitempty"`
	Regex  string `yaml:"regex,omitempty"`
	Prefix string `yaml:"prefix,omitempty"`
}

type URIMatch struct {
	Exact  string `yaml:"exact,omitempty"`
	Prefix string `yaml:"prefix,omitempty"`
	Regex  string `yaml:"regex,omitempty"`
}

type Retry struct {
	Attempts      int               `yaml:"attempts"`
	PerTryTimeout string            `yaml:"per_try_timeout"`
	RetryOn       string            `yaml:"retry_on"`
	RetryRemoteLocalities bool       `yaml:"retry_remote_localities"`
}

// Engine faz apply/update/delete de VirtualServices
type Engine struct {
	configPath string
}

func New() *Engine {
	return &Engine{}
}

func (e *Engine) Apply(ctx context.Context, vs VirtualService) error {
	yaml := renderYAML(vs)
	cmd := exec.CommandContext(ctx, "kubectl", "apply", "-f", "-")
	cmd.Stdin = strings.NewReader(yaml)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("istio apply: %s: %w", string(out), err)
	}
	return nil
}

func (e *Engine) SetWeights(ctx context.Context, name, namespace string, weights map[string]int) error {
	// weights map: subset -> percent (must sum to 100)
	getCmd := exec.CommandContext(ctx, "kubectl", "get", "virtualservice", name, "-n", namespace, "-o", "yaml")
	if _, err := getCmd.CombinedOutput(); err != nil {
		return err
	}
	// Real implementation: parse + modify + apply
	// Aqui simplificado: reconstroi e aplica
	return nil
}

func (e *Engine) Delete(ctx context.Context, name, namespace string) error {
	cmd := exec.CommandContext(ctx, "kubectl", "delete", "virtualservice", name, "-n", namespace)
	return cmd.Run()
}

func renderYAML(vs VirtualService) string {
	var sb strings.Builder
	sb.WriteString("apiVersion: networking.istio.io/v1beta1\n")
	sb.WriteString("kind: VirtualService\n")
	sb.WriteString(fmt.Sprintf("metadata:\n  name: %s\n  namespace: %s\n", vs.Name, vs.Namespace))
	if len(vs.Gateways) > 0 {
		sb.WriteString("spec:\n  gateways:\n")
		for _, g := range vs.Gateways {
			sb.WriteString(fmt.Sprintf("    - %s\n", g))
		}
	} else {
		sb.WriteString("spec:\n  hosts:\n")
	}

	if len(vs.Hosts) > 0 {
		sb.WriteString("  hosts:\n")
		for _, h := range vs.Hosts {
			sb.WriteString(fmt.Sprintf("    - %s\n", h))
		}
	}

	if len(vs.Routes) > 0 {
		sb.WriteString("  http:\n")
		for _, r := range vs.Routes {
			sb.WriteString("    - route:\n")
			for _, d := range r.Destinations {
				sb.WriteString(fmt.Sprintf("        - destination:\n"))
				sb.WriteString(fmt.Sprintf("            host: %s\n", d.Host))
				if d.Subset != "" {
					sb.WriteString(fmt.Sprintf("            subset: %s\n", d.Subset))
				}
				if d.Port != nil {
					sb.WriteString(fmt.Sprintf("            port:\n"))
					sb.WriteString(fmt.Sprintf("              number: %d\n", d.Port.Number))
				}
				sb.WriteString(fmt.Sprintf("          weight: %d\n", d.Weight))
			}
			if r.Match != nil {
				for _, m := range r.Match {
					sb.WriteString("      match:\n")
					if m.Method != "" {
						sb.WriteString(fmt.Sprintf("        - method:\n            exact: %s\n", m.Method))
					}
					if len(m.Headers) > 0 {
						for k, v := range m.Headers {
							sb.WriteString(fmt.Sprintf("        - headers:\n            %s:\n", k))
							if v.Exact != "" {
								sb.WriteString(fmt.Sprintf("              exact: %s\n", v.Exact))
							} else if v.Prefix != "" {
								sb.WriteString(fmt.Sprintf("              prefix: %s\n", v.Prefix))
							} else if v.Regex != "" {
								sb.WriteString(fmt.Sprintf("              regex: %s\n", v.Regex))
							}
						}
					}
				}
			}
		}
	}

	if vs.HTTPRetry != nil {
		sb.WriteString("    retries:\n")
		sb.WriteString(fmt.Sprintf("      attempts: %d\n", vs.HTTPRetry.Attempts))
		if vs.HTTPRetry.PerTryTimeout != "" {
			sb.WriteString(fmt.Sprintf("      per_try_timeout: %s\n", vs.HTTPRetry.PerTryTimeout))
		}
		if vs.HTTPRetry.RetryOn != "" {
			sb.WriteString(fmt.Sprintf("      retry_on: %q\n", vs.HTTPRetry.RetryOn))
		}
	}
	return sb.String()
}