package benchmark

import (
	"bufio"
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestRunFixedParallelismKeepsRequestedInflight(t *testing.T) {
	t.Helper()

	var runs atomic.Int64
	var current atomic.Int64
	var maxSeen atomic.Int64

	err := runFixedParallelism(context.Background(), 40, 4, func() error {
		runs.Add(1)
		inFlight := current.Add(1)
		for {
			seen := maxSeen.Load()
			if inFlight <= seen || maxSeen.CompareAndSwap(seen, inFlight) {
				break
			}
		}
		time.Sleep(2 * time.Millisecond)
		current.Add(-1)
		return nil
	})
	if err != nil {
		t.Fatalf("runFixedParallelism returned error: %v", err)
	}
	if got := runs.Load(); got != 40 {
		t.Fatalf("runFixedParallelism executed %d runs, want 40", got)
	}
	if got := maxSeen.Load(); got > 4 {
		t.Fatalf("runFixedParallelism reached %d in-flight runs, want <= 4", got)
	}
}

func TestTemporalClientOptionsIncludeCloudSettings(t *testing.T) {
	options := temporalClientOptions("example.tmprl.cloud:7233", "example.account", "secret")
	if options.HostPort != "example.tmprl.cloud:7233" || options.Namespace != "example.account" {
		t.Fatalf("client address/namespace = %q/%q", options.HostPort, options.Namespace)
	}
	if options.Credentials == nil {
		t.Fatal("client credentials are nil with an API key")
	}
	if withoutKey := temporalClientOptions("localhost:7233", "default", ""); withoutKey.Credentials != nil {
		t.Fatal("client credentials are non-nil without an API key")
	}
}

func TestRunFixedParallelismStopsOnFirstError(t *testing.T) {
	t.Helper()

	wantErr := errors.New("boom")
	var runs atomic.Int64
	err := runFixedParallelism(context.Background(), 100, 6, func() error {
		call := runs.Add(1)
		if call == 9 {
			return wantErr
		}
		time.Sleep(time.Millisecond)
		return nil
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("runFixedParallelism error = %v, want %v", err, wantErr)
	}
	if got := runs.Load(); got < 9 || got > 14 {
		t.Fatalf("runFixedParallelism executed %d runs after cancellation, want a small bounded overrun", got)
	}
}

func TestBenchmarkWorkerProfilerFinalizeWritesProfiles(t *testing.T) {
	t.Helper()

	controlReader, controlWriter, err := os.Pipe()
	if err != nil {
		t.Fatalf("create control pipe: %v", err)
	}
	ackReader, ackWriter, err := os.Pipe()
	if err != nil {
		t.Fatalf("create ack pipe: %v", err)
	}
	defer controlReader.Close()
	defer controlWriter.Close()
	defer ackReader.Close()
	defer ackWriter.Close()

	profileDir := t.TempDir()
	profiler, err := newBenchmarkWorkerProfiler(controlReader, ackWriter, profileDir, wasmWorkerKind, "queue/name")
	if err != nil {
		t.Fatalf("newBenchmarkWorkerProfiler returned error: %v", err)
	}
	defer profiler.close()
	profiler.eagerCount = &atomic.Int64{}
	profiler.eagerCount.Store(17)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		profiler.serveFinalizeRequests()
	}()

	if _, err := controlWriter.WriteString(profileFinalizeCommand + "\n"); err != nil {
		t.Fatalf("write finalize command: %v", err)
	}
	response, err := bufio.NewReader(ackReader).ReadString('\n')
	if err != nil {
		t.Fatalf("read finalize response: %v", err)
	}
	wg.Wait()

	fields := slices.DeleteFunc(splitFields(response), func(field string) bool { return field == "" })
	if len(fields) != 6 {
		t.Fatalf("finalize response %q has %d fields, want 6", response, len(fields))
	}
	if fields[0] != "ok" {
		t.Fatalf("finalize response %q did not acknowledge success", response)
	}
	if fields[1] != "eager_activity_dispatch=17" {
		t.Fatalf("finalize response eager count = %q, want eager_activity_dispatch=17", fields[1])
	}
	for _, path := range fields[2:] {
		info, err := os.Stat(path)
		if err != nil {
			t.Fatalf("profile %q missing: %v", path, err)
		}
		if info.IsDir() {
			t.Fatalf("profile %q is a directory", path)
		}
		if filepath.Dir(path) != profileDir {
			t.Fatalf("profile %q written outside %q", path, profileDir)
		}
	}
}

func TestBenchmarkTimingRecorderWritesQuantiles(t *testing.T) {
	recorder := &benchmarkTimingRecorder{
		path:    filepath.Join(t.TempDir(), "timing.txt"),
		records: make(map[string]*benchmarkTimingStat),
	}
	for _, elapsed := range []time.Duration{time.Millisecond, 2 * time.Millisecond, 10 * time.Millisecond} {
		recorder.record("rpc", elapsed)
	}
	path, err := recorder.writeSummary()
	if err != nil {
		t.Fatal(err)
	}
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	want := "rpc\t3\t13.000\t4333.333\t2.000\t10.000\t10.000\t10.000"
	if !strings.Contains(string(contents), want) {
		t.Fatalf("timing summary = %q, want row containing %q", contents, want)
	}
}

func splitFields(response string) []string {
	fields := make([]string, 0, 5)
	current := ""
	for _, r := range response {
		switch r {
		case '\n', '\t':
			fields = append(fields, current)
			current = ""
		default:
			current += string(r)
		}
	}
	if current != "" {
		fields = append(fields, current)
	}
	return fields
}
