package benchmark

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"go.temporal.io/sdk/client"
)

type benchmarkTimingRecorder struct {
	path string

	mu      sync.Mutex
	records map[string]*benchmarkTimingStat
}

type benchmarkTimingStat struct {
	count  int64
	total  time.Duration
	max    time.Duration
	values []time.Duration
}

func newBenchmarkTimingRecorderFromEnvironment(kind, taskQueue string) (*benchmarkTimingRecorder, error) {
	profileDir := os.Getenv(benchmarkProfileDirEnv)
	if profileDir == "" {
		return nil, nil
	}
	if err := os.MkdirAll(profileDir, 0o755); err != nil {
		return nil, fmt.Errorf("create timing dir %q: %w", profileDir, err)
	}
	path := filepath.Join(
		profileDir,
		fmt.Sprintf(
			"%s-%s-%d-timing.txt",
			sanitizeProfileName(kind),
			sanitizeProfileName(taskQueue),
			time.Now().UnixNano(),
		),
	)
	return &benchmarkTimingRecorder{
		path:    path,
		records: make(map[string]*benchmarkTimingStat),
	}, nil
}

func (r *benchmarkTimingRecorder) record(phase string, elapsed time.Duration) {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	stat := r.records[phase]
	if stat == nil {
		stat = &benchmarkTimingStat{}
		r.records[phase] = stat
	}
	stat.count++
	stat.total += elapsed
	stat.values = append(stat.values, elapsed)
	if elapsed > stat.max {
		stat.max = elapsed
	}
}

func (r *benchmarkTimingRecorder) writeSummary() (string, error) {
	if r == nil {
		return "", nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()

	file, err := os.Create(r.path)
	if err != nil {
		return "", fmt.Errorf("create timing summary %q: %w", r.path, err)
	}
	defer file.Close()

	keys := make([]string, 0, len(r.records))
	for key := range r.records {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	if _, err := fmt.Fprintln(file, "phase\tcount\ttotal_ms\tavg_us\tp50_ms\tp95_ms\tp99_ms\tmax_ms"); err != nil {
		return "", fmt.Errorf("write timing summary header: %w", err)
	}
	for _, key := range keys {
		stat := r.records[key]
		if stat.count == 0 {
			continue
		}
		avg := stat.total / time.Duration(stat.count)
		values := append([]time.Duration(nil), stat.values...)
		sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
		if _, err := fmt.Fprintf(
			file,
			"%s\t%d\t%.3f\t%.3f\t%.3f\t%.3f\t%.3f\t%.3f\n",
			key,
			stat.count,
			float64(stat.total)/float64(time.Millisecond),
			float64(avg)/float64(time.Microsecond),
			float64(timingQuantile(values, 0.50))/float64(time.Millisecond),
			float64(timingQuantile(values, 0.95))/float64(time.Millisecond),
			float64(timingQuantile(values, 0.99))/float64(time.Millisecond),
			float64(stat.max)/float64(time.Millisecond),
		); err != nil {
			return "", fmt.Errorf("write timing summary row for %q: %w", key, err)
		}
	}
	return r.path, nil
}

func timingQuantile(values []time.Duration, quantile float64) time.Duration {
	if len(values) == 0 {
		return 0
	}
	index := int(float64(len(values)-1)*quantile + 0.5)
	return values[index]
}

type benchmarkTimingHandler struct {
	recorder *benchmarkTimingRecorder
	scope    string
	tags     map[string]string
}

func newBenchmarkTimingHandler(scope string, recorder *benchmarkTimingRecorder) client.MetricsHandler {
	if recorder == nil {
		return client.MetricsNopHandler
	}
	return &benchmarkTimingHandler{
		recorder: recorder,
		scope:    scope,
		tags:     make(map[string]string),
	}
}

func (h *benchmarkTimingHandler) WithTags(tags map[string]string) client.MetricsHandler {
	child := &benchmarkTimingHandler{
		recorder: h.recorder,
		scope:    h.scope,
		tags:     make(map[string]string, len(h.tags)+len(tags)),
	}
	for key, value := range h.tags {
		child.tags[key] = value
	}
	for key, value := range tags {
		child.tags[key] = value
	}
	return child
}

func (h *benchmarkTimingHandler) Counter(name string) client.MetricsCounter {
	return benchmarkTimingCounter{}
}

func (h *benchmarkTimingHandler) Gauge(name string) client.MetricsGauge {
	return benchmarkTimingGauge{}
}

func (h *benchmarkTimingHandler) Timer(name string) client.MetricsTimer {
	return benchmarkTimingTimer{handler: h, name: name}
}

type benchmarkTimingTimer struct {
	handler *benchmarkTimingHandler
	name    string
}

func (t benchmarkTimingTimer) Record(elapsed time.Duration) {
	if t.handler == nil || t.handler.recorder == nil {
		return
	}
	if phase, ok := benchmarkTimingPhase(t.handler.scope, t.name, t.handler.tags); ok {
		t.handler.recorder.record(phase, elapsed)
	}
}

func benchmarkTimingPhase(scope, name string, tags map[string]string) (string, bool) {
	switch name {
	case "temporal_request_latency":
		return scope + ".request_latency." + benchmarkTimingOperation(tags), true
	case "temporal_long_request_latency":
		return scope + ".long_request_latency." + benchmarkTimingOperation(tags), true
	case "temporal_workflow_task_execution_latency":
		return scope + ".workflow_task_execution_latency", true
	case "temporal_workflow_task_schedule_to_start_latency":
		return scope + ".workflow_task_schedule_to_start_latency", true
	case "temporal_workflow_endtoend_latency":
		return scope + ".workflow_endtoend_latency", true
	default:
		return "", false
	}
}

func benchmarkTimingOperation(tags map[string]string) string {
	if tags == nil {
		return "unknown"
	}
	operation := tags["operation"]
	if operation == "" {
		return "unknown"
	}
	return sanitizeProfileName(strings.ToLower(operation))
}

type benchmarkTimingCounter struct{}

func (benchmarkTimingCounter) Inc(int64) {}

type benchmarkTimingGauge struct{}

func (benchmarkTimingGauge) Update(float64) {}
