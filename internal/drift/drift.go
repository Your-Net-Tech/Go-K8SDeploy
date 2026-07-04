package drift

import (
	"bytes"
	"fmt"
	"os/exec"
	"strings"
)

type Drift struct {
	Resource string
	Kind     string
	Status   string // "modified", "missing", "extra"
	Field    string
	Expected string
	Actual   string
}

type Report struct {
	FilesChecked int
	Drifts       []Drift
	OK           bool
}

func Detect(manifests []string) (*Report, error) {
	r := &Report{OK: true}

	for _, m := range manifests {
		r.FilesChecked++

		// kubectl diff retorna 0 se nao ha diff, 1 se ha
		cmd := exec.Command("kubectl", "diff", "-f", m, "--field-manager=k8s-deploy")
		var stdout bytes.Buffer
		cmd.Stdout = &stdout
		cmd.Stderr = nil

		err := cmd.Run()
		if err == nil {
			continue
		}
		if exitErr, ok := err.(*exec.ExitError); ok && exitErr.ExitCode() == 1 {
			d := parseDiff(m, stdout.String())
			r.Drifts = append(r.Drifts, d...)
			r.OK = false
		}
	}
	return r, nil
}

func parseDiff(file, diffOutput string) []Drift {
	drifts := []Drift{}

	lines := strings.Split(diffOutput, "\n")
	current := Drift{Resource: file}

	for _, line := range lines {
		if strings.HasPrefix(line, "diff") || strings.HasPrefix(line, "+++") || strings.HasPrefix(line, "---") {
			continue
		}
		if strings.HasPrefix(line, "@@") {
			continue
		}
		if strings.HasPrefix(line, "kind:") {
			current.Kind = strings.TrimSpace(strings.TrimPrefix(line, "kind:"))
		}
		if strings.HasPrefix(line, "name:") || strings.HasPrefix(line, "  name:") {
			current.Resource = strings.TrimSpace(strings.TrimPrefix(strings.TrimPrefix(line, "  "), "name:"))
		}
		if strings.HasPrefix(line, "-") || strings.HasPrefix(line, "+") {
			current.Status = "modified"
			parts := strings.SplitN(line[1:], ":", 2)
			if len(parts) == 2 {
				current.Field = strings.TrimSpace(parts[0])
				if strings.HasPrefix(line, "-") {
					current.Expected = strings.TrimSpace(parts[1])
				} else {
					current.Actual = strings.TrimSpace(parts[1])
				}
			}
		}
		if current.Status == "modified" && current.Field != "" {
			drifts = append(drifts, current)
			current = Drift{Resource: file}
		}
	}
	return drifts
}

func (r *Report) String() string {
	if r.OK {
		return fmt.Sprintf("✓ Nenhum drift detectado (%d manifestos verificados)", r.FilesChecked)
	}

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("⚠ DRIFT DETECTADO em %d manifestos:\n\n", r.FilesChecked))
	for _, d := range r.Drifts {
		sb.WriteString(fmt.Sprintf("  [%s] %s/%s\n", d.Status, d.Kind, d.Resource))
		if d.Field != "" {
			sb.WriteString(fmt.Sprintf("      campo: %s\n", d.Field))
			if d.Expected != "" {
				sb.WriteString(fmt.Sprintf("      esperado: %s\n", d.Expected))
			}
			if d.Actual != "" {
				sb.WriteString(fmt.Sprintf("      atual:    %s\n", d.Actual))
			}
		}
	}
	return sb.String()
}