package benchmark

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func runFixedParallelism(ctx context.Context, total, concurrency int, run func() error) error {
	if total <= 0 {
		return nil
	}
	if concurrency < 1 {
		concurrency = 1
	}
	if concurrency > total {
		concurrency = total
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var next atomic.Int64
	var firstErr atomic.Pointer[error]
	var wg sync.WaitGroup
	wg.Add(concurrency)

	for range concurrency {
		go func() {
			defer wg.Done()
			for {
				if ctx.Err() != nil {
					return
				}
				index := int(next.Add(1) - 1)
				if index >= total {
					return
				}
				if err := run(); err != nil {
					capturedErr := err
					if firstErr.CompareAndSwap(nil, &capturedErr) {
						cancel()
					}
					return
				}
			}
		}()
	}

	wg.Wait()
	if err := firstErr.Load(); err != nil {
		return *err
	}
	return nil
}

type benchmarkWorkerProcess struct {
	command    *exec.Cmd
	profiler   *benchmarkWorkerProfileController
	workerKind string
}

func (process *benchmarkWorkerProcess) close() {
	if process == nil || process.profiler == nil {
		return
	}
	process.profiler.close()
}

type benchmarkWorkerProfileController struct {
	kind        string
	commandPipe *os.File
	ackPipe     *os.File
	childRead   *os.File
	childWrite  *os.File
}

func configureWorkerProfiling(b *testing.B, command *exec.Cmd, kind string) *benchmarkWorkerProfileController {
	b.Helper()
	profileDir := os.Getenv(benchmarkProfileDirEnv)

	childControl, parentCommand, err := os.Pipe()
	if err != nil {
		b.Fatalf("create profile control pipe: %v", err)
	}
	parentAck, childAck, err := os.Pipe()
	if err != nil {
		_ = parentCommand.Close()
		_ = childControl.Close()
		b.Fatalf("create profile ack pipe: %v", err)
	}

	command.Env = append(command.Env,
		benchmarkProfileControlFD+"=3",
		benchmarkProfileAckFD+"=4",
	)
	if profileDir != "" {
		command.Env = append(command.Env, benchmarkProfileDirEnv+"="+profileDir)
	}
	command.ExtraFiles = append(command.ExtraFiles, childControl, childAck)
	return &benchmarkWorkerProfileController{
		kind:        kind,
		commandPipe: parentCommand,
		ackPipe:     parentAck,
		childRead:   childControl,
		childWrite:  childAck,
	}
}

func (controller *benchmarkWorkerProfileController) closeChildFiles() {
	if controller == nil {
		return
	}
	_ = controller.childRead.Close()
	_ = controller.childWrite.Close()
	controller.childRead = nil
	controller.childWrite = nil
}

func (controller *benchmarkWorkerProfileController) finalize() ([]string, int64, error) {
	if controller == nil {
		return nil, 0, nil
	}
	if _, err := io.WriteString(controller.commandPipe, profileFinalizeCommand+"\n"); err != nil {
		return nil, 0, fmt.Errorf("send finalize request: %w", err)
	}
	if err := controller.commandPipe.Close(); err != nil {
		return nil, 0, fmt.Errorf("close finalize request pipe: %w", err)
	}
	controller.commandPipe = nil

	response, err := bufio.NewReader(controller.ackPipe).ReadString('\n')
	if err != nil {
		return nil, 0, fmt.Errorf("read finalize response: %w", err)
	}
	response = strings.TrimSpace(response)
	fields := strings.Split(response, "\t")
	if len(fields) == 0 {
		return nil, 0, errors.New("empty finalize response")
	}
	if fields[0] != "ok" {
		if len(fields) > 1 {
			return nil, 0, errors.New(strings.Join(fields[1:], "\t"))
		}
		return nil, 0, fmt.Errorf("unexpected finalize response %q", response)
	}
	if len(fields) < 2 || !strings.HasPrefix(fields[1], "eager_activity_dispatch=") {
		return nil, 0, fmt.Errorf("finalize response is missing eager activity count: %q", response)
	}
	eagerCount, err := strconv.ParseInt(strings.TrimPrefix(fields[1], "eager_activity_dispatch="), 10, 64)
	if err != nil {
		return nil, 0, fmt.Errorf("parse eager activity count: %w", err)
	}
	return fields[2:], eagerCount, nil
}

func (controller *benchmarkWorkerProfileController) close() {
	if controller == nil {
		return
	}
	for _, file := range []*os.File{controller.commandPipe, controller.ackPipe, controller.childRead, controller.childWrite} {
		if file != nil {
			_ = file.Close()
		}
	}
}

type benchmarkWorkerProfiler struct {
	control        *os.File
	ack            *os.File
	cpu            *os.File
	heap           *os.File
	mutex          *os.File
	block          *os.File
	profilePaths   []string
	timingRecorder *benchmarkTimingRecorder
	eagerCount     *atomic.Int64
	mutexFraction  int
	finalizeSignal sync.Once
}

func newBenchmarkWorkerProfilerFromEnvironment(kind, taskQueue string) (*benchmarkWorkerProfiler, error) {
	profileDir := os.Getenv(benchmarkProfileDirEnv)
	controlFD, err := getenvInt(benchmarkProfileControlFD)
	if err != nil {
		return nil, err
	}
	ackFD, err := getenvInt(benchmarkProfileAckFD)
	if err != nil {
		return nil, err
	}
	return newBenchmarkWorkerProfiler(
		os.NewFile(uintptr(controlFD), "profile-control"),
		os.NewFile(uintptr(ackFD), "profile-ack"),
		profileDir,
		kind,
		taskQueue,
	)
}

func newBenchmarkWorkerProfiler(control, ack *os.File, profileDir, kind, taskQueue string) (*benchmarkWorkerProfiler, error) {
	profiler := &benchmarkWorkerProfiler{
		control: control,
		ack:     ack,
	}
	if profileDir == "" {
		return profiler, nil
	}
	if err := os.MkdirAll(profileDir, 0o755); err != nil {
		return nil, fmt.Errorf("create profile dir %q: %w", profileDir, err)
	}
	if err := profiler.openProfiles(profileDir, kind, taskQueue); err != nil {
		profiler.close()
		return nil, err
	}
	profiler.mutexFraction = runtime.SetMutexProfileFraction(1)
	runtime.SetBlockProfileRate(1)
	if err := pprof.StartCPUProfile(profiler.cpu); err != nil {
		profiler.close()
		return nil, fmt.Errorf("start cpu profile: %w", err)
	}
	return profiler, nil
}

func (profiler *benchmarkWorkerProfiler) openProfiles(profileDir, kind, taskQueue string) error {
	prefix := filepath.Join(
		profileDir,
		fmt.Sprintf("%s-%s-%d", sanitizeProfileName(kind), sanitizeProfileName(taskQueue), time.Now().UnixNano()),
	)
	specs := []struct {
		suffix string
		target **os.File
	}{
		{suffix: "-cpu.pprof", target: &profiler.cpu},
		{suffix: "-heap.pprof", target: &profiler.heap},
		{suffix: "-mutex.pprof", target: &profiler.mutex},
		{suffix: "-block.pprof", target: &profiler.block},
	}
	for _, spec := range specs {
		path := prefix + spec.suffix
		file, err := os.Create(path)
		if err != nil {
			return fmt.Errorf("create profile %q: %w", path, err)
		}
		*spec.target = file
		profiler.profilePaths = append(profiler.profilePaths, path)
	}
	return nil
}

func (profiler *benchmarkWorkerProfiler) serveFinalizeRequests() {
	if profiler == nil {
		return
	}
	reader := bufio.NewReader(profiler.control)
	command, err := reader.ReadString('\n')
	if err != nil {
		profiler.writeFinalizeResponse(fmt.Errorf("read finalize request: %w", err))
		return
	}
	if strings.TrimSpace(command) != profileFinalizeCommand {
		profiler.writeFinalizeResponse(fmt.Errorf("unexpected finalize request %q", strings.TrimSpace(command)))
		return
	}
	profiler.writeFinalizeResponse(profiler.finalizeProfiles())
}

func (profiler *benchmarkWorkerProfiler) writeFinalizeResponse(finalizeErr error) {
	if finalizeErr != nil {
		_, _ = fmt.Fprintf(profiler.ack, "error\t%s\n", strings.ReplaceAll(finalizeErr.Error(), "\n", " "))
		return
	}
	eagerCount := int64(0)
	if profiler.eagerCount != nil {
		eagerCount = profiler.eagerCount.Load()
	}
	fields := append([]string{fmt.Sprintf("eager_activity_dispatch=%d", eagerCount)}, profiler.profilePaths...)
	_, _ = fmt.Fprintf(profiler.ack, "ok\t%s\n", strings.Join(fields, "\t"))
}

func (profiler *benchmarkWorkerProfiler) finalizeProfiles() error {
	var finalizeErr error
	profiler.finalizeSignal.Do(func() {
		if profiler.cpu != nil {
			pprof.StopCPUProfile()
			runtime.SetBlockProfileRate(0)
			runtime.SetMutexProfileFraction(profiler.mutexFraction)
			runtime.GC()
			if err := pprof.WriteHeapProfile(profiler.heap); err != nil {
				finalizeErr = fmt.Errorf("write heap profile: %w", err)
				return
			}
			if err := writeNamedProfile("mutex", profiler.mutex); err != nil {
				finalizeErr = err
				return
			}
			if err := writeNamedProfile("block", profiler.block); err != nil {
				finalizeErr = err
				return
			}
		}
		if profiler.timingRecorder != nil {
			timingPath, err := profiler.timingRecorder.writeSummary()
			if err != nil {
				finalizeErr = err
				return
			}
			if timingPath != "" {
				profiler.profilePaths = append(profiler.profilePaths, timingPath)
			}
		}
		finalizeErr = profiler.closeProfiles()
	})
	return finalizeErr
}

func (profiler *benchmarkWorkerProfiler) closeProfiles() error {
	var closeErr error
	for _, file := range []*os.File{profiler.cpu, profiler.heap, profiler.mutex, profiler.block} {
		if file == nil {
			continue
		}
		if err := file.Close(); err != nil && closeErr == nil {
			closeErr = err
		}
	}
	return closeErr
}

func (profiler *benchmarkWorkerProfiler) close() {
	if profiler == nil {
		return
	}
	_ = profiler.closeProfiles()
	for _, file := range []*os.File{profiler.control, profiler.ack} {
		if file != nil {
			_ = file.Close()
		}
	}
}

func writeNamedProfile(name string, file *os.File) error {
	profile := pprof.Lookup(name)
	if profile == nil {
		return fmt.Errorf("profile %q is unavailable", name)
	}
	if err := profile.WriteTo(file, 0); err != nil {
		return fmt.Errorf("write %s profile: %w", name, err)
	}
	return nil
}

func getenvInt(name string) (int, error) {
	value := os.Getenv(name)
	if value == "" {
		return 0, fmt.Errorf("%s is missing", name)
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return 0, fmt.Errorf("parse %s=%q: %w", name, value, err)
	}
	return parsed, nil
}

func sanitizeProfileName(value string) string {
	var builder strings.Builder
	for _, r := range value {
		switch {
		case r >= 'a' && r <= 'z':
			builder.WriteRune(r)
		case r >= 'A' && r <= 'Z':
			builder.WriteRune(r)
		case r >= '0' && r <= '9':
			builder.WriteRune(r)
		default:
			builder.WriteByte('-')
		}
	}
	if builder.Len() == 0 {
		return "profile"
	}
	return builder.String()
}
