package k8s

import (
	"context"
	"fmt"
	"os/exec"
	"time"
)

type Client struct {
	kubeconfig string
}

func NewClient(kubeconfig string) *Client {
	return &Client{kubeconfig: kubeconfig}
}

func (c *Client) run(ctx context.Context, args ...string) error {
	cmd := exec.CommandContext(ctx, "kubectl", args...)
	cmd.Stdout = nil
	cmd.Stderr = nil
	return cmd.Run()
}

func (c *Client) runOutput(ctx context.Context, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, "kubectl", args...)
	out, err := cmd.CombinedOutput()
	return string(out), err
}

func (c *Client) DryRun(ctx context.Context, manifest string) error {
	return c.run(ctx, "apply", "--dry-run=client", "-f", manifest)
}

func (c *Client) ApplyFile(ctx context.Context, manifest string) error {
	return c.run(ctx, "apply", "-f", manifest)
}

func (c *Client) WaitForReady(ctx context.Context, project, namespace string, timeout time.Duration) error {
	args := []string{"wait", "--for=condition=Ready", "pod", "-l", fmt.Sprintf("app=%s", project), "-n", namespace, "--timeout", timeout.String()}
	return c.run(ctx, args...)
}

func (c *Client) GetPods(ctx context.Context, namespace string) (string, error) {
	return c.runOutput(ctx, "get", "pods", "-n", namespace, "-o", "wide")
}

func (c *Client) GetPodLogs(ctx context.Context, name, namespace string) (string, error) {
	return c.runOutput(ctx, "logs", name, "-n", namespace)
}

func (c *Client) DeleteResource(ctx context.Context, kind, name, namespace string) error {
	return c.run(ctx, "delete", kind, name, "-n", namespace)
}

func (c *Client) DescribePod(ctx context.Context, name, namespace string) (string, error) {
	return c.runOutput(ctx, "describe", "pod", name, "-n", namespace)
}

func (c *Client) ScaleDeployment(ctx context.Context, name, namespace string, replicas int) error {
	return c.run(ctx, "scale", fmt.Sprintf("deployment/%s", name), "-n", namespace, fmt.Sprintf("--replicas=%d", replicas))
}