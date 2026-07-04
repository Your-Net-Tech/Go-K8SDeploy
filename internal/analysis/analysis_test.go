package analysis

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestCheckFailedRestarts(t *testing.T) {
	cases := []struct {
		name     string
		pods     []PodInfo
		max      int
		expected bool
	}{
		{
			name:     "no restarts",
			pods:     []PodInfo{{Name: "p1", RestartCount: 0}},
			max:      3,
			expected: true,
		},
		{
			name:     "restarts at limit",
			pods:     []PodInfo{{Name: "p1", RestartCount: 3}},
			max:      3,
			expected: true,
		},
		{
			name:     "restarts above limit",
			pods:     []PodInfo{{Name: "p1", RestartCount: 5}},
			max:      3,
			expected: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e := New(&MockK8sAPI{Pods: tc.pods})
			c := Condition{Type: "failed_restarts", Max: tc.max}
			r := e.checkFailedRestarts(context.Background(), "default", c)
			if r.Success != tc.expected {
				t.Errorf("expected Success=%v, got %v", tc.expected, r.Success)
			}
		})
	}
}

func TestCheckCrashed(t *testing.T) {
	tests := []struct {
		name     string
		pods     []PodInfo
		max      int
		expected bool
	}{
		{"all running", []PodInfo{{Name: "p1", Phase: "Running"}}, 0, true},
		{"one crashed but allowed", []PodInfo{{Name: "p1", Reason: "CrashLoopBackOff"}}, 1, true},
		{"one crashed exceeds limit", []PodInfo{{Name: "p1", Reason: "CrashLoopBackOff"}}, 0, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := New(&MockK8sAPI{Pods: tt.pods})
			c := Condition{Type: "crashed", PodCount: tt.max}
			r := e.checkCrashed(context.Background(), "default", c)
			if r.Success != tt.expected {
				t.Errorf("Expected %v, got %v (msg=%s)", tt.expected, r.Success, r.Message)
			}
		})
	}
}

func TestCheckOOMKilled(t *testing.T) {
	e := New(&MockK8sAPI{Pods: []PodInfo{{Name: "p1", Reason: "OOMKilled"}}})
	c := Condition{Type: "oom_killed", Max: 0}
	r := e.checkOOMKilled(context.Background(), "default", c)
	if r.Success {
		t.Error("expected failure for OOMKilled pod")
	}

	e2 := New(&MockK8sAPI{Pods: []PodInfo{{Name: "p1", Phase: "Running"}}})
	r2 := e2.checkOOMKilled(context.Background(), "default", c)
	if !r2.Success {
		t.Error("expected success for non-OOM pod")
	}
}

func TestCheckErrorLogs(t *testing.T) {
	e := New(&MockK8sAPI{
		Pods: []PodInfo{{Name: "p1", Phase: "Running"}},
		Logs: map[string]string{
			"p1": "starting server\nINFO: ready\nERROR: connection refused\n",
		},
	})
	c := Condition{Type: "error_log_matches", Pattern: "ERROR", Max: 5}
	r := e.checkErrorLogs(context.Background(), "default", c)
	if !r.Success {
		t.Errorf("expected success with 1 error (max 5), got failure: %s", r.Message)
	}
}

func TestCheckErrorLogsTooMany(t *testing.T) {
	logs := ""
	for i := 0; i < 10; i++ {
		logs += "ERROR: something broke\n"
	}
	e := New(&MockK8sAPI{
		Pods: []PodInfo{{Name: "p1", Phase: "Running"}},
		Logs: map[string]string{"p1": logs},
	})
	c := Condition{Type: "error_log_matches", Pattern: "ERROR", Max: 5}
	r := e.checkErrorLogs(context.Background(), "default", c)
	if r.Success {
		t.Error("expected failure with 10 errors (max 5)")
	}
}

func TestCheckInvalidRegex(t *testing.T) {
	e := New(&MockK8sAPI{})
	c := Condition{Type: "error_log_matches", Pattern: "[invalid(", Max: 1}
	r := e.checkErrorLogs(context.Background(), "default", c)
	if r.Success {
		t.Error("expected failure with invalid regex")
	}
}

func TestRunChecksParallel(t *testing.T) {
	e := New(&MockK8sAPI{
		Pods: []PodInfo{{Name: "p1", Phase: "Running", RestartCount: 0}},
	})
	a := Analysis{
		Conditions: []Condition{
			{Type: "failed_restarts", Max: 0},
			{Type: "crashed", PodCount: 0},
			{Type: "oom_killed", Max: 0},
		},
		FailureLimit: 0,
	}

	results, ok, err := e.RunChecks(context.Background(), "default", a)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Errorf("expected ok=true, got false")
	}
	if len(results) != 3 {
		t.Errorf("expected 3 results, got %d", len(results))
	}
}

func TestRunChecksFailureLimit(t *testing.T) {
	e := New(&MockK8sAPI{
		Pods: []PodInfo{{Name: "p1", Reason: "CrashLoopBackOff"}},
	})
	a := Analysis{
		Conditions: []Condition{
			{Type: "crashed", PodCount: 0},
			{Type: "failed_restarts", Max: 100},
		},
		FailureLimit: 0,
	}
	results, ok, err := e.RunChecks(context.Background(), "default", a)
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Error("expected ok=false: crashed failed and FailureLimit=0")
	}
	if results[0].Success {
		t.Error("crashed should fail")
	}
}

func TestRunChecksWithinFailureLimit(t *testing.T) {
	e := New(&MockK8sAPI{
		Pods: []PodInfo{{Name: "p1", Reason: "CrashLoopBackOff"}},
	})
	a := Analysis{
		Conditions: []Condition{
			{Type: "crashed", PodCount: 0},
			{Type: "failed_restarts", Max: 100},
		},
		FailureLimit: 1,
	}
	_, ok, err := e.RunChecks(context.Background(), "default", a)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Error("expected ok=true: 1 failure allowed by FailureLimit=1")
	}
}

func TestReport(t *testing.T) {
	results := []Result{
		{Type: "a", Success: true},
		{Type: "b", Success: false, Message: "test"},
	}
	out := Report(results)
	if out == "" {
		t.Error("expected non-empty report")
	}
	if !contains(out, "1 ok") || !contains(out, "1 fail") {
		t.Errorf("report should show counts: %s", out)
	}
	if !contains(out, "test") {
		t.Errorf("report should include failure message: %s", out)
	}
}

type MockK8sAPI struct {
	mu        sync.Mutex
	Pods      []PodInfo
	Logs      map[string]string
	getPodsCalls int
}

func (m *MockK8sAPI) GetPods(ctx context.Context, ns string) ([]PodInfo, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.getPodsCalls++
	return m.Pods, nil
}

func (m *MockK8sAPI) GetPodLogs(ctx context.Context, name, ns string, tail int) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if log, ok := m.Logs[name]; ok {
		return log, nil
	}
	return "", fmt.Errorf("no logs for %s", name)
}

func (m *MockK8sAPI) WatchConcurrent() {
	m.mu.Lock()
	defer m.mu.Unlock()
}

var _ = errors.New
var _ = time.Now

func contains(s, substr string) bool {
	if len(substr) == 0 {
		return true
	}
	for i := 0; i+len(substr) <= len(s); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}