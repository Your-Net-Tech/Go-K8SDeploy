package kustomize

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

type Build struct {
	Path       string
	Manifests  []string
	HasOverlay bool
	Resources  []string
}

func Detect(path string) (*Build, error) {
	kustFile := filepath.Join(path, "kustomization.yaml")
	if _, err := os.Stat(kustFile); err != nil {
		alt := filepath.Join(path, "kustomization.yml")
		if _, err := os.Stat(alt); err != nil {
			return nil, fmt.Errorf("no kustomization in %s", path)
		}
		kustFile = alt
	}

	out, err := exec.Command("kubectl", "kustomize", path).Output()
	if err != nil {
		return nil, err
	}

	b := &Build{
		Path:      path,
		Manifests: splitManifests(string(out)),
	}
	return b, nil
}

func splitManifests(content string) []string {
	out := []string{}
	current := ""
	for _, line := range strings.Split(content, "\n") {
		if strings.HasPrefix(line, "---") && current != "" {
			out = append(out, current)
			current = line + "\n"
		} else {
			current += line + "\n"
		}
	}
	if strings.TrimSpace(current) != "" {
		out = append(out, current)
	}
	return out
}

func Apply(path string) error {
	cmd := exec.Command("kubectl", "apply", "-k", path)
	cmd.Stdout = nil
	cmd.Stderr = nil
	return cmd.Run()
}

func Build_(path string, outputFile string) error {
	out, err := exec.Command("kubectl", "kustomize", path).Output()
	if err != nil {
		return err
	}
	return os.WriteFile(outputFile, out, 0644)
}