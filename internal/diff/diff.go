package diff

import (
	"bytes"
	"fmt"
	"os/exec"
	"strings"
)

type Diff struct {
	File     string
	Changes  []string
	Added    bool
	Modified bool
	Removed  bool
}

func Run(manifests []string) ([]Diff, error) {
	results := []Diff{}
	for _, m := range manifests {
		cmd := exec.Command("kubectl", "diff", "-f", m, "--field-manager=k8s-deploy")
		var stdout, stderr bytes.Buffer
		cmd.Stdout = &stdout
		cmd.Stderr = &stderr

		err := cmd.Run()
		if err != nil {
			// kubectl diff returns 0 on no diff, 1 on diff, 2 on error
			if exitErr, ok := err.(*exec.ExitError); ok {
				if exitErr.ExitCode() == 1 {
					// diff found
					d := Diff{File: m}
					for _, line := range strings.Split(stdout.String(), "\n") {
						if strings.HasPrefix(line, "+") || strings.HasPrefix(line, "-") {
							d.Changes = append(d.Changes, line)
							if strings.HasPrefix(line, "+") {
								d.Added = true
							}
							if strings.HasPrefix(line, "-") {
								d.Removed = true
							}
						}
					}
					d.Modified = d.Added || d.Removed
					results = append(results, d)
					continue
				}
			}
			return nil, fmt.Errorf("diff %s: %s", m, stderr.String())
		}
		// no diff
		results = append(results, Diff{File: m})
	}
	return results, nil
}

func Render(diffs []Diff) string {
	var sb strings.Builder
	sb.WriteString("=== Diff Preview ===\n\n")
	for _, d := range diffs {
		if len(d.Changes) == 0 {
			sb.WriteString(fmt.Sprintf("  [no change] %s\n", d.File))
			continue
		}
		sb.WriteString(fmt.Sprintf("  --- %s ---\n", d.File))
		for _, c := range d.Changes {
			if len(c) > 200 {
				c = c[:200] + "..."
			}
			sb.WriteString(fmt.Sprintf("    %s\n", c))
		}
	}
	return sb.String()
}