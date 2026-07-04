package build

import (
	"context"
	"fmt"
	"os/exec"
	"strings"
	"time"

	"k8s-deploy/internal/config"
)

type Engine struct {
	registry string
}

func New(reg string) *Engine {
	return &Engine{registry: reg}
}

type Result struct {
	Service string
	Image   string
	Ok      bool
	Err     string
	Logs    string
}

func (e *Engine) BuildOnePublic(ctx context.Context, cfg *config.Config, svc config.Service, srcDir string) (Result, error) {
	return e.buildOne(ctx, cfg, svc, srcDir)
}

func (e *Engine) config() *config.Config { return nil }

func (e *Engine) BuildAll(ctx context.Context, cfg *config.Config, srcDir string) ([]Result, error) {
	results := make([]Result, 0, len(cfg.Services))
	for _, svc := range cfg.Services {
		res, err := e.buildOne(ctx, cfg, svc, srcDir)
		if err != nil {
			res = Result{Service: svc.Name, Ok: false, Err: err.Error()}
		}
		results = append(results, res)
		if !res.Ok && cfg.Pipeline.RollbackOnFailure {
			return results, fmt.Errorf("build %s falhou: %s", svc.Name, res.Err)
		}
	}
	return results, nil
}

func (e *Engine) buildOne(ctx context.Context, cfg *config.Config, svc config.Service, srcDir string) (Result, error) {
	podName := fmt.Sprintf("kaniko-%s", strings.ToLower(svc.Name))
	image := fmt.Sprintf("%s/%s", e.registry, svc.Image)

	yaml := renderKanikoPod(podName, cfg, svc, image, srcDir, e.registry)

	// Aplica pod do Kaniko
	if err := kubectl(ctx, "apply", "-f", "-", yaml); err != nil {
		return Result{}, fmt.Errorf("aplicar kaniko pod: %w", err)
	}
	defer kubectl(ctx, "delete", "pod", podName, "-n", cfg.Cluster.Namespace, "--ignore-not-found=true", "--wait=false")

	// Espera o pod completar (com timeout)
	logs, err := waitForPod(ctx, podName, cfg.Cluster.Namespace, svc.Image, cfg.Pipeline.HealthCheckTimeout)
	if err != nil {
		return Result{Service: svc.Name, Image: image, Logs: logs}, err
	}

	// Verifica se a imagem foi pushada
	if err := verifyImage(ctx, image); err != nil {
		return Result{Service: svc.Name, Image: image, Logs: logs}, err
	}

	return Result{Service: svc.Name, Image: image, Ok: true, Logs: logs}, nil
}

func renderKanikoPod(name string, cfg *config.Config, svc config.Service, image, srcDir, registry string) string {
	args := []string{
		fmt.Sprintf("--dockerfile=%s", svc.Dockerfile),
		fmt.Sprintf("--context=dir:///workspace/%s", svc.Context),
		fmt.Sprintf("--destination=%s", image),
		fmt.Sprintf("--cache=true"),
		fmt.Sprintf("--cache-repo=%s/cache", registry),
	}
	for k, v := range svc.BuildArgs {
		args = append(args, fmt.Sprintf("--build-arg=%s=%s", k, v))
	}

	insecureFlag := ""
	if cfg.Registry.Insecure {
		insecureFlag = "    - name: registry-cert\n      value: |\n        -----BEGIN CERTIFICATE-----\n        [insecure-registry]\n        -----END CERTIFICATE-----\n"
	}

	return fmt.Sprintf(`apiVersion: v1
kind: Pod
metadata:
  name: %s
  namespace: %s
  labels:
    app: kaniko-build
    service: %s
spec:
  restartPolicy: Never
  containers:
  - name: kaniko
    image: gcr.io/kaniko-project/executor:v1.23.2
    args:
%s
    env:
    - name: GOOGLE_APPLICATION_CREDENTIALS
      value: /dev/null
%s
    volumeMounts:
    - name: source
      mountPath: /workspace
  volumes:
  - name: source
    hostPath:
      path: %s
      type: DirectoryOrCreate
`, name, cfg.Cluster.Namespace, svc.Name, indentArgs(args), insecureFlag, srcDir)
}

func indentArgs(args []string) string {
	out := ""
	for _, a := range args {
		out += fmt.Sprintf("    - %q\n", a)
	}
	return strings.TrimRight(out, "\n")
}

func kubectl(ctx context.Context, args ...interface{}) error {
	strs := make([]string, 0, len(args))
	for i, a := range args {
		if i == len(args)-1 {
			continue // ultimo eh string yaml
		}
		if s, ok := a.(string); ok {
			strs = append(strs, s)
		}
	}
	cmd := exec.CommandContext(ctx, "kubectl", strs...)
	if len(args) > 0 {
		if last, ok := args[len(args)-1].(string); ok {
			cmd.Stdin = strings.NewReader(last)
		}
	}
	return cmd.Run()
}

func waitForPod(ctx context.Context, name, ns, image string, timeout time.Duration) (string, error) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		out, err := exec.CommandContext(ctx, "kubectl", "get", "pod", name, "-n", ns,
			"-o", "jsonpath={.status.phase}").Output()
		if err == nil {
			phase := strings.TrimSpace(string(out))
			if phase == "Succeeded" {
				// pega logs
				logs, _ := exec.CommandContext(ctx, "kubectl", "logs", name, "-n", ns).CombinedOutput()
				return string(logs), nil
			}
			if phase == "Failed" {
				logs, _ := exec.CommandContext(ctx, "kubectl", "logs", name, "-n", ns).CombinedOutput()
				return string(logs), fmt.Errorf("kaniko pod falhou")
			}
		}
		time.Sleep(5 * time.Second)
	}
	return "", fmt.Errorf("timeout aguardando kaniko pod")
}

func verifyImage(ctx context.Context, image string) error {
	imageTag := image
	if !strings.Contains(image, ":") {
		imageTag = image + ":latest"
	}
	cmd := exec.CommandContext(ctx, "ctr", "-n", "k8s.io", "images", "ls")
	out, err := cmd.Output()
	if err != nil {
		return err
	}
	if !strings.Contains(string(out), imageTag) {
		return fmt.Errorf("imagem %s não encontrada após build", imageTag)
	}
	return nil
}