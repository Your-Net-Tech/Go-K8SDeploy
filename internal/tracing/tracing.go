// Package tracing implementa distributed tracing estilo OpenTelemetry.
//
// PROPRIETÁRIO - nao depende do OTel collector.
// Formato do trace compativel com OTLP/JSON para integracao futura.
//
// Spans sao organizados em tree com parent-child.
// Cada trace tem ID, cada span tem ID, parent ID, duration.
package tracing

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

// TraceID identifica um trace
type TraceID string

// SpanID identifica um span dentro do trace
type SpanID string

// Span eh uma operacao individual dentro do trace
type Span struct {
	TraceID   TraceID              `json:"trace_id"`
	SpanID    SpanID               `json:"span_id"`
	ParentID  SpanID               `json:"parent_id,omitempty"`
	Name      string               `json:"name"`
	Kind      SpanKind             `json:"kind"`
	StartTime time.Time            `json:"start_time"`
	EndTime   time.Time            `json:"end_time"`
	Duration  time.Duration        `json:"duration_ns"`
	Status    SpanStatus           `json:"status"`
	Attributes map[string]string  `json:"attributes,omitempty"`
	Events    []SpanEvent          `json:"events,omitempty"`
}

type SpanKind string

const (
	SpanInternal SpanKind = "internal"
	SpanClient   SpanKind = "client"
	SpanServer   SpanKind = "server"
	SpanProducer SpanKind = "producer"
	SpanConsumer SpanKind = "consumer"
)

type SpanStatus string

const (
	StatusUnset SpanStatus = "unset"
	StatusOK    SpanStatus = "ok"
	StatusError SpanStatus = "error"
)

type SpanEvent struct {
	Time    time.Time         `json:"time"`
	Name    string            `json:"name"`
	Attrs   map[string]string `json:"attrs,omitempty"`
}

// SpanContext eh o estado do span
type SpanContext struct {
	mu        sync.Mutex
	span      *Span
	tracer    *Tracer
	finished  bool
}

// Tracer eh o criador de spans
type Tracer struct {
	mu        sync.RWMutex
	spans     map[SpanID]*Span
	traces    map[TraceID][]SpanID
	sink      Sink
	sampler   Sampler
	traceIDCounter atomic.Uint64
	spanIDCounter  atomic.Uint64
	maxSpans  int
}

type Sink interface {
	Write(span *Span) error
	Close() error
}

type Sampler interface {
	ShouldSample(traceID TraceID, name string) bool
}

// AlwaysSampler amostra tudo
type AlwaysSampler struct{}

func (a *AlwaysSampler) ShouldSample(_ TraceID, _ string) bool { return true }

// ProbabilitySampler amostra X% dos traces
type ProbabilitySampler struct {
	Rate float64
}

func (p *ProbabilitySampler) ShouldSample(_ TraceID, _ string) bool {
	return randFloat() < p.Rate
}

func randFloat() float64 {
	return float64(time.Now().UnixNano()%1000) / 1000.0
}

// MemorySink guarda spans em memoria
type MemorySink struct {
	mu    sync.RWMutex
	spans []*Span
	max   int
}

func NewMemorySink(max int) *MemorySink {
	return &MemorySink{max: max}
}

func (m *MemorySink) Write(s *Span) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.spans = append(m.spans, s)
	if len(m.spans) > m.max {
		m.spans = m.spans[len(m.spans)-m.max:]
	}
	return nil
}

func (m *MemorySink) Close() error { return nil }

func (m *MemorySink) Snapshot() []*Span {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]*Span, len(m.spans))
	copy(out, m.spans)
	return out
}

// FileSink escreve spans em arquivo (formato OTLP/JSON-like)
type FileSink struct {
	mu     sync.Mutex
	writer *json.Encoder
	file   *os.File
}

func NewFileSink(path string) (*FileSink, error) {
	os.MkdirAll(filepath.Dir(path), 0755)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}
	return &FileSink{
		writer: json.NewEncoder(f),
		file:   f,
	}, nil
}

func (f *FileSink) Write(s *Span) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.writer.Encode(s)
}

func (f *FileSink) Close() error { return f.file.Close() }

// NewTracer cria novo tracer
func NewTracer(sink Sink, sampler Sampler) *Tracer {
	return &Tracer{
		spans:   map[SpanID]*Span{},
		traces:  map[TraceID][]SpanID{},
		sink:    sink,
		sampler: sampler,
		maxSpans: 100000,
	}
}

// generateTraceID gera novo trace ID
func (t *Tracer) generateTraceID() TraceID {
	n := t.traceIDCounter.Add(1)
	return TraceID(fmt.Sprintf("trace-%d-%d", time.Now().UnixNano(), n))
}

// generateSpanID gera novo span ID
func (t *Tracer) generateSpanID() SpanID {
	n := t.spanIDCounter.Add(1)
	return SpanID(fmt.Sprintf("span-%d", n))
}

// StartSpan inicia um span novo
func (t *Tracer) StartSpan(name string, opts ...SpanOption) *SpanContext {
	if t.sampler != nil && !t.sampler.ShouldSample("", name) {
		return &SpanContext{tracer: t, span: nil}
	}

	cfg := &spanConfig{}
	for _, o := range opts {
		o(cfg)
	}

	traceID := cfg.traceID
	if traceID == "" {
		traceID = t.generateTraceID()
	}

	span := &Span{
		TraceID:   traceID,
		SpanID:    t.generateSpanID(),
		ParentID:  cfg.parentID,
		Name:      name,
		Kind:      cfg.kind,
		StartTime: time.Now(),
		Status:    StatusUnset,
		Attributes: map[string]string{},
	}
	for k, v := range cfg.attributes {
		span.Attributes[k] = v
	}

	t.mu.Lock()
	t.spans[span.SpanID] = span
	t.traces[traceID] = append(t.traces[traceID], span.SpanID)
	t.mu.Unlock()

	return &SpanContext{
		tracer: t,
		span:   span,
	}
}

type SpanOption func(*spanConfig)

type spanConfig struct {
	kind       SpanKind
	parentID   SpanID
	traceID    TraceID
	attributes map[string]string
}

func WithKind(k SpanKind) SpanOption {
	return func(c *spanConfig) { c.kind = k }
}

func WithParent(id SpanID) SpanOption {
	return func(c *spanConfig) { c.parentID = id }
}

func WithTrace(id TraceID) SpanOption {
	return func(c *spanConfig) { c.traceID = id }
}

func WithAttr(k, v string) SpanOption {
	return func(c *spanConfig) {
		if c.attributes == nil {
			c.attributes = map[string]string{}
		}
		c.attributes[k] = v
	}
}

// End finaliza o span
func (sc *SpanContext) End(opts ...EndOption) {
	if sc.span == nil || sc.finished {
		return
	}
	sc.finished = true

	cfg := &endConfig{}
	for _, o := range opts {
		o(cfg)
	}

	sc.mu.Lock()
	defer sc.mu.Unlock()
	sc.span.EndTime = time.Now()
	sc.span.Duration = sc.span.EndTime.Sub(sc.span.StartTime)

	if cfg.err != nil {
		sc.span.Status = StatusError
		sc.span.Attributes["error"] = cfg.err.Error()
	} else if sc.span.Status == StatusUnset {
		sc.span.Status = StatusOK
	}

	for k, v := range cfg.attributes {
		sc.span.Attributes[k] = v
	}

	for _, ev := range cfg.events {
		sc.span.Events = append(sc.span.Events, ev)
	}

	if sc.tracer.sink != nil {
		sc.tracer.sink.Write(sc.span)
	}
}

type EndOption func(*endConfig)

type endConfig struct {
	err        error
	attributes map[string]string
	events     []SpanEvent
}

func WithError(e error) EndOption {
	return func(c *endConfig) { c.err = e }
}

func WithEndAttr(k, v string) EndOption {
	return func(c *endConfig) {
		if c.attributes == nil {
			c.attributes = map[string]string{}
		}
		c.attributes[k] = v
	}
}

func WithEvent(name string, attrs map[string]string) EndOption {
	return func(c *endConfig) {
		c.events = append(c.events, SpanEvent{
			Time:  time.Now(),
			Name:  name,
			Attrs: attrs,
		})
	}
}

// AddEvent adiciona evento em span sem finalizar
func (sc *SpanContext) AddEvent(name string, attrs map[string]string) {
	if sc.span == nil {
		return
	}
	sc.mu.Lock()
	defer sc.mu.Unlock()
	sc.span.Events = append(sc.span.Events, SpanEvent{
		Time:  time.Now(),
		Name:  name,
		Attrs: attrs,
	})
}

// SetAttribute adiciona atributo
func (sc *SpanContext) SetAttribute(k, v string) {
	if sc.span == nil {
		return
	}
	sc.mu.Lock()
	defer sc.mu.Unlock()
	sc.span.Attributes[k] = v
}

// TraceIDFromContext retorna trace ID atual
func (sc *SpanContext) TraceID() TraceID {
	if sc.span == nil {
		return ""
	}
	return sc.span.TraceID
}

// SpanID retorna ID do span atual
func (sc *SpanContext) SpanID() SpanID {
	if sc.span == nil {
		return ""
	}
	return sc.span.SpanID
}

// GetTrace retorna todos os spans de um trace
func (t *Tracer) GetTrace(traceID TraceID) []*Span {
	t.mu.RLock()
	defer t.mu.RUnlock()
	ids, ok := t.traces[traceID]
	if !ok {
		return nil
	}
	out := make([]*Span, 0, len(ids))
	for _, id := range ids {
		if s, ok := t.spans[id]; ok {
			out = append(out, s)
		}
	}
	return out
}

// GetAllTraces retorna lista de todos os trace IDs
func (t *Tracer) GetAllTraces() []TraceID {
	t.mu.RLock()
	defer t.mu.RUnlock()
	out := make([]TraceID, 0, len(t.traces))
	for id := range t.traces {
		out = append(out, id)
	}
	return out
}

// SpanFromContext cria span a partir de context.Context
func SpanFromContext(ctx context.Context, tracer *Tracer) *SpanContext {
	if v := ctx.Value(spanKey{}); v != nil {
		if sc, ok := v.(*SpanContext); ok {
			return sc
		}
	}
	return tracer.StartSpan("default")
}

// ContextWithSpan injeta span no context
func ContextWithSpan(ctx context.Context, sc *SpanContext) context.Context {
	return context.WithValue(ctx, spanKey{}, sc)
}

type spanKey struct{}

// NoopSpan retorna um span vazio (quando sampling off)
func NoopSpan() *SpanContext { return &SpanContext{} }

// TopSpans agrupa spans por nome e calcula latencias
type SpanSummary struct {
	Name      string
	Count     int
	P50MS     float64
	P95MS     float64
	P99MS     float64
	ErrorRate float64
}

func (t *Tracer) Summary() []SpanSummary {
	t.mu.RLock()
	defer t.mu.RUnlock()

	byName := map[string][]*Span{}
	for _, s := range t.spans {
		byName[s.Name] = append(byName[s.Name], s)
	}

	out := make([]SpanSummary, 0, len(byName))
	for name, spans := range byName {
		sorted := make([]time.Duration, len(spans))
		errors := 0
		for i, s := range spans {
			sorted[i] = s.Duration
			if s.Status == StatusError {
				errors++
			}
		}
		for i := 0; i < len(sorted); i++ {
			for j := i + 1; j < len(sorted); j++ {
				if sorted[j] < sorted[i] {
					sorted[i], sorted[j] = sorted[j], sorted[i]
				}
			}
		}
		p50 := sorted[len(sorted)*50/100]
		p95 := sorted[len(sorted)*95/100]
		p99 := sorted[len(sorted)*99/100]
		if len(sorted) == 0 {
			continue
		}
		out = append(out, SpanSummary{
			Name: name,
			Count: len(spans),
			P50MS: float64(p50.Microseconds()) / 1000,
			P95MS: float64(p95.Microseconds()) / 1000,
			P99MS: float64(p99.Microseconds()) / 1000,
			ErrorRate: float64(errors) / float64(len(spans)),
		})
	}
	return out
}