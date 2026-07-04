package metrics

import (
	"encoding/json"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"k8s-deploy/internal/resilience"
)

type Server struct {
	mu      sync.RWMutex
	counters map[string]*atomic.Uint64
	gauges   map[string]*atomic.Int64
	histograms map[string]*histogramVec
	muM     *resilience.Metrics
}

type histogramVec struct {
	mu      sync.Mutex
	buckets []float64
	counts  []uint64
	count   uint64
	sum     float64
}

func NewServer() *Server {
	return &Server{
		counters:   map[string]*atomic.Uint64{},
		gauges:     map[string]*atomic.Int64{},
		histograms: map[string]*histogramVec{},
		muM:        resilience.NewMetrics(),
	}
}

func (s *Server) Counter(name string) *atomic.Uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.counters[name]; !ok {
		s.counters[name] = &atomic.Uint64{}
	}
	return s.counters[name]
}

func (s *Server) Gauge(name string) *atomic.Int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.gauges[name]; !ok {
		s.gauges[name] = &atomic.Int64{}
	}
	return s.gauges[name]
}

func (s *Server) Histogram(name string, buckets []float64) *histogramVec {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.histograms[name]; !ok {
		s.histograms[name] = &histogramVec{buckets: buckets, counts: make([]uint64, len(buckets)+1)}
	}
	return s.histograms[name]
}

func (h *histogramVec) Observe(v float64) {
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

func (s *Server) Prometheus() string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	out := ""
	for n, c := range s.counters {
		out += "# TYPE " + n + " counter\n"
		out += n + " " + formatInt(c.Load()) + "\n"
	}
	for n, g := range s.gauges {
		out += "# TYPE " + n + " gauge\n"
		out += n + " " + formatInt(uint64(g.Load())) + "\n"
	}
	for n, h := range s.histograms {
		h.mu.Lock()
		out += "# TYPE " + n + " histogram\n"
		for i, b := range h.buckets {
			bucketLabel := formatFloat(b)
			out += n + "_bucket{le=\"" + bucketLabel + "\"} " + formatInt(h.counts[i]) + "\n"
		}
		out += n + "_bucket{le=\"+Inf\"} " + formatInt(h.counts[len(h.buckets)]) + "\n"
		out += n + "_count " + formatInt(h.count) + "\n"
		out += n + "_sum " + formatFloat(h.sum) + "\n"
		h.mu.Unlock()
	}
	return out
}

func (s *Server) HandleHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	w.Write([]byte(s.Prometheus()))
}

func (s *Server) Time(name string, start time.Time) {
	dur := time.Since(start).Seconds()
	h := s.Histogram(name, []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10})
	h.Observe(dur)
}

func (s *Server) Snapshot() map[string]interface{} {
	s.mu.RLock()
	defer s.mu.RUnlock()
	snap := map[string]interface{}{
		"counters":   map[string]uint64{},
		"gauges":     map[string]int64{},
		"histograms": map[string]interface{}{},
	}
	for n, c := range s.counters {
		snap["counters"].(map[string]uint64)[n] = c.Load()
	}
	for n, g := range s.gauges {
		snap["gauges"].(map[string]int64)[n] = g.Load()
	}
	return snap
}

func (s *Server) JSON(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(s.Snapshot())
}

func formatInt(n uint64) string {
	if n == 0 {
		return "0"
	}
	out := ""
	for n > 0 {
		out = string(rune('0'+n%10)) + out
		n /= 10
	}
	return out
}

func formatFloat(f float64) string {
	out := ""
	whole := int64(f)
	out = formatInt(uint64(whole))
	out += "."
	frac := int64((f - float64(whole)) * 1000)
	if frac == 0 {
		out += "0"
	} else {
		out += formatInt(uint64(frac))
	}
	return out
}