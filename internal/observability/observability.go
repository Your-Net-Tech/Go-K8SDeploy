// Package observability unifica metrics + logs + tracing.
//
// Sem Prometheus, Grafana, ELK externos. 100% proprietario.
//
// - Metrics: histograms, counters, gauges
// - Logs estruturados (JSON)
// - Tracing distribuido (proprietary)
package observability

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

type Level string

const (
	LevelDebug   Level = "DEBUG"
	LevelInfo    Level = "INFO"
	LevelWarn    Level = "WARN"
	LevelError   Level = "ERROR"
	LevelFatal   Level = "FATAL"
)

type LogEntry struct {
	Time    time.Time              `json:"time"`
	Level   Level                  `json:"level"`
	Message string                 `json:"message"`
	Fields  map[string]interface{} `json:"fields,omitempty"`
	TraceID string                 `json:"trace_id,omitempty"`
	SpanID  string                 `json:"span_id,omitempty"`
}

// Logger com saida estruturada
type Logger struct {
	mu       sync.Mutex
	outputs  []LogOutput
	fields   map[string]interface{}
	hostname string
	process  string
}

type LogOutput interface {
	Write(entry LogEntry) error
}

type StdoutOutput struct{}

func (s *StdoutOutput) Write(e LogEntry) error {
	data, _ := json.Marshal(e)
	os.Stdout.Write(data)
	os.Stdout.Write([]byte("\n"))
	return nil
}

type FileOutput struct {
	file *os.File
}

func NewFileOutput(path string) (*FileOutput, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}
	return &FileOutput{file: f}, nil
}

func (f *FileOutput) Write(e LogEntry) error {
	data, _ := json.Marshal(e)
	f.file.Write(data)
	f.file.Write([]byte("\n"))
	return nil
}

func NewLogger() *Logger {
	h, _ := os.Hostname()
	return &Logger{
		outputs:  []LogOutput{&StdoutOutput{}},
		fields:   map[string]interface{}{"service": "k8s-deploy"},
		hostname: h,
		process:  fmt.Sprintf("%d", os.Getpid()),
	}
}

func (l *Logger) With(fields map[string]interface{}) *Logger {
	cp := &Logger{
		outputs:  l.outputs,
		fields:   map[string]interface{}{},
		hostname: l.hostname,
		process:  l.process,
	}
	for k, v := range l.fields {
		cp.fields[k] = v
	}
	for k, v := range fields {
		cp.fields[k] = v
	}
	return cp
}

func (l *Logger) log(level Level, msg string, fields map[string]interface{}) {
	entry := LogEntry{
		Time:    time.Now().UTC(),
		Level:   level,
		Message: msg,
		Fields:  l.fields,
	}
	for k, v := range fields {
		if entry.Fields == nil {
			entry.Fields = map[string]interface{}{}
		}
		entry.Fields[k] = v
	}

	if tracer := GetCurrentTracer(); tracer != nil {
		// current span lookup - simplified
		_ = tracer
	}

	l.mu.Lock()
	defer l.mu.Unlock()
	for _, out := range l.outputs {
		go out.Write(entry)
	}
}

func (l *Logger) Debug(msg string, fields ...map[string]interface{}) { l.log(LevelDebug, msg, mergeFields(fields)) }
func (l *Logger) Info(msg string, fields ...map[string]interface{})  { l.log(LevelInfo, msg, mergeFields(fields)) }
func (l *Logger) Warn(msg string, fields ...map[string]interface{})  { l.log(LevelWarn, msg, mergeFields(fields)) }
func (l *Logger) Error(msg string, fields ...map[string]interface{}) { l.log(LevelError, msg, mergeFields(fields)) }
func (l *Logger) Fatal(msg string, fields ...map[string]interface{}) { l.log(LevelFatal, msg, mergeFields(fields)) }

func mergeFields(f []map[string]interface{}) map[string]interface{} {
	if len(f) == 0 {
		return nil
	}
	out := map[string]interface{}{}
	for _, m := range f {
		for k, v := range m {
			out[k] = v
		}
	}
	return out
}

// Metrics
type Metrics struct {
	Counters   sync.Map // name -> *atomic.Uint64
	Histograms sync.Map // name -> *Histogram
	Gauges     sync.Map // name -> *atomic.Int64
}

type Histogram struct {
	mu      sync.Mutex
	buckets []float64
	counts  []uint64
	sum     float64
	count   uint64
}

func NewMetrics() *Metrics { return &Metrics{} }

func (m *Metrics) Counter(name string) *atomic.Uint64 {
	v, _ := m.Counters.LoadOrStore(name, &atomic.Uint64{})
	return v.(*atomic.Uint64)
}

func (m *Metrics) Gauge(name string) *atomic.Int64 {
	v, _ := m.Gauges.LoadOrStore(name, &atomic.Int64{})
	return v.(*atomic.Int64)
}

func (m *Metrics) Histogram(name string, buckets []float64) *Histogram {
	v, _ := m.Histograms.LoadOrStore(name, &Histogram{
		buckets: buckets,
		counts:  make([]uint64, len(buckets)+1),
	})
	return v.(*Histogram)
}

func (h *Histogram) Observe(v float64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.count++
	h.sum += v
	for i, b := range h.buckets {
		if v <= b {
			h.counts[i]++
		}
	}
	h.counts[len(h.buckets)]++
}

// Prometheus exposition
func (m *Metrics) Prometheus() string {
	var out []byte
	m.Counters.Range(func(k, v interface{}) bool {
		name := k.(string)
		c := v.(*atomic.Uint64)
		out = append(out, []byte("# TYPE "+name+" counter\n"+name+" "+itoa(c.Load())+"\n")...)
		return true
	})
	m.Gauges.Range(func(k, v interface{}) bool {
		name := k.(string)
		g := v.(*atomic.Int64)
		out = append(out, []byte("# TYPE "+name+" gauge\n"+name+" "+itoa(uint64(g.Load()))+"\n")...)
		return true
	})
	m.Histograms.Range(func(k, v interface{}) bool {
		name := k.(string)
		h := v.(*Histogram)
		h.mu.Lock()
		defer h.mu.Unlock()
		out = append(out, []byte("# TYPE "+name+" histogram\n")...)
		for i, b := range h.buckets {
			out = append(out, []byte(name+"_bucket{le=\""+ftoa(b)+"\"} "+itoa(h.counts[i])+"\n")...)
		}
		out = append(out, []byte(name+"_bucket{le=\"+Inf\"} "+itoa(h.counts[len(h.buckets)])+"\n")...)
		out = append(out, []byte(name+"_count "+itoa(h.count)+"\n")...)
		out = append(out, []byte(name+"_sum "+ftoa(h.sum)+"\n")...)
		return true
	})
	return string(out)
}

func (m *Metrics) Handler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	w.Write([]byte(m.Prometheus()))
}

// Tracing integration - delegate to tracing package
type Tracer struct {
	currentSpanFn func() *Span
	startSpanFn   func(name string) *Span
}

type Span = tracingSpan

type tracingSpan struct {
	TraceID   string
	SpanID    string
	ParentID  string
	Name      string
	StartTime time.Time
	EndTime   time.Time
	Duration  time.Duration
	Status    string
	Attributes map[string]string
	Events    []SpanEvent
}

type TraceID = string
type SpanID = string

type SpanStatus = string

const (
	StatusUnset SpanStatus = "unset"
	StatusOK    SpanStatus = "ok"
	StatusError SpanStatus = "error"
)

type SpanEvent struct {
	Time  time.Time
	Name  string
	Attrs map[string]string
}

type Sink interface {
	Write(span *Span) error
	Close() error
}

type Sampler interface {
	ShouldSample(traceID TraceID, name string) bool
}

var currentTracer atomic.Pointer[Tracer]

func SetCurrentTracer(t *Tracer)  { currentTracer.Store(t) }
func GetCurrentTracer() *Tracer    { return currentTracer.Load() }

// Otel interface for compatibility (sem dependência externa)
func WithSpan(ctx context.Context, name string, fn func(ctx context.Context) error) error {
	tracer := GetCurrentTracer()
	if tracer == nil {
		return fn(ctx)
	}
	span := tracer.startSpanFn(name)
	defer func() {
		if span != nil {
			span.EndTime = time.Now()
		}
	}()
	return fn(ctx)
}

func itoa(n uint64) string {
	if n == 0 { return "0" }
	out := []byte{}
	for n > 0 {
		out = append([]byte{byte('0' + n%10)}, out...)
		n /= 10
	}
	return string(out)
}

func ftoa(f float64) string {
	w := int64(f)
	frac := int64((f - float64(w)) * 1000)
	if frac < 0 { frac = -frac }
	return itoa(uint64(w)) + "." + itoa(uint64(frac))
}