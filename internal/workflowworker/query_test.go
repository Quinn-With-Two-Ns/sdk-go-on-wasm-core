package workflowworker

import (
	"errors"
	"strings"
	"testing"

	workflowactivation "github.com/temporalio/sdk-go-on-wasm-core/internal/corepb/workflowactivation"
	workflowcommands "github.com/temporalio/sdk-go-on-wasm-core/internal/corepb/workflowcommands"
	workflowcompletion "github.com/temporalio/sdk-go-on-wasm-core/internal/corepb/workflowcompletion"
)

func TestQueryReadsLiveWorkflowState(t *testing.T) {
	worker := newTestWorker()
	worker.Register("queryable", func(ctx *Context) error {
		state := "waiting"
		if err := SetQueryHandler(ctx, "status", func() (string, error) {
			return state, nil
		}); err != nil {
			return err
		}
		for {
			var next string
			if err := GetSignalChannel(ctx, "state").Receive(ctx, &next); err != nil {
				return err
			}
			state = next
		}
	})

	initialized := worker.handleActivation(initializeActivation("run-1", "queryable", nil))
	if commands := successfulCommands(t, initialized); len(commands) != 0 {
		t.Fatalf("initial activation emitted %d commands", len(commands))
	}
	if got := queryValue[string](t, worker, worker.handleActivation(activation("run-1", queryJob(t, worker, "query-1", "status")))); got != "waiting" {
		t.Fatalf("initial query result = %q, want waiting", got)
	}

	signalled := worker.handleActivation(activation("run-1", signalJob(t, worker, "state", "ready")))
	if commands := successfulCommands(t, signalled); len(commands) != 0 {
		t.Fatalf("signal activation emitted %d commands", len(commands))
	}
	if got := queryValue[string](t, worker, worker.handleActivation(activation("run-1", queryJob(t, worker, "query-2", "status")))); got != "ready" {
		t.Fatalf("updated query result = %q, want ready", got)
	}
}

func TestQueryDecodesArgumentsAndEncodesResult(t *testing.T) {
	worker := newTestWorker()
	worker.Register("queryable", func(ctx *Context) error {
		if err := SetQueryHandler(ctx, "repeat", func(value string, count int) (string, error) {
			return strings.Repeat(value, count), nil
		}); err != nil {
			return err
		}
		return Sleep(ctx, 1)
	})
	worker.handleActivation(initializeActivation("run-1", "queryable", nil))

	completion := worker.handleActivation(activation("run-1", queryJob(t, worker, "query-1", "repeat", "go", 3)))
	if got := queryValue[string](t, worker, completion); got != "gogogo" {
		t.Fatalf("query result = %q, want gogogo", got)
	}
}

func TestMultipleQueriesProduceIndependentResponses(t *testing.T) {
	worker := newTestWorker()
	worker.Register("queryable", func(ctx *Context) error {
		if err := SetQueryHandler(ctx, "first", func() (string, error) { return "one", nil }); err != nil {
			return err
		}
		if err := SetQueryHandler(ctx, "second", func() (string, error) { return "two", nil }); err != nil {
			return err
		}
		return Sleep(ctx, 1)
	})
	worker.handleActivation(initializeActivation("run-1", "queryable", nil))

	completion := worker.handleActivation(activation(
		"run-1",
		queryJob(t, worker, "query-1", "first"),
		queryJob(t, worker, "query-2", "second"),
	))
	commands := successfulCommands(t, completion)
	if len(commands) != 2 {
		t.Fatalf("completion has %d commands, want 2 query responses", len(commands))
	}
	for i, want := range []string{"one", "two"} {
		result := commands[i].GetRespondToQuery()
		if result == nil || result.GetSucceeded() == nil {
			t.Fatalf("command %d is not a successful query response: %+v", i, commands[i])
		}
		var got string
		if err := worker.payloadConverter.FromPayload(result.GetSucceeded().Response, &got); err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Fatalf("query response %d = %q, want %q", i, got, want)
		}
	}
}

func TestQueryRegistrationReplacesExistingHandler(t *testing.T) {
	worker := newTestWorker()
	worker.Register("queryable", func(ctx *Context) error {
		if err := SetQueryHandler(ctx, "status", func() (string, error) { return "old", nil }); err != nil {
			return err
		}
		if err := SetQueryHandler(ctx, "status", func() (string, error) { return "new", nil }); err != nil {
			return err
		}
		return Sleep(ctx, 1)
	})
	worker.handleActivation(initializeActivation("run-1", "queryable", nil))

	completion := worker.handleActivation(activation("run-1", queryJob(t, worker, "query-1", "status")))
	if got := queryValue[string](t, worker, completion); got != "new" {
		t.Fatalf("replacement query result = %q, want new", got)
	}
}

func TestQueryDecodeFailureOnlyFailsQuery(t *testing.T) {
	worker := newTestWorker()
	worker.Register("queryable", func(ctx *Context) error {
		if err := SetQueryHandler(ctx, "double", func(value int) (int, error) { return value * 2, nil }); err != nil {
			return err
		}
		return Sleep(ctx, 1)
	})
	worker.handleActivation(initializeActivation("run-1", "queryable", nil))

	completion := worker.handleActivation(activation("run-1", queryJob(t, worker, "query-1", "double", "not-an-int")))
	failure := queryResult(t, completion).GetFailed()
	if failure == nil || !strings.Contains(failure.Message, `decode query "double" input`) {
		t.Fatalf("decode failure = %+v", failure)
	}
	if worker.getRun("run-1") == nil {
		t.Fatal("query decode failure evicted the workflow run")
	}
}

func TestQueryFailuresDoNotFailWorkflow(t *testing.T) {
	worker := newTestWorker()
	wantErr := errors.New("query failed")
	worker.Register("queryable", func(ctx *Context) error {
		for name, handler := range map[string]any{
			"error": func() (string, error) { return "", wantErr },
			"panic": func() (string, error) { panic("query panic") },
			"ok":    func() (string, error) { return "healthy", nil },
		} {
			if err := SetQueryHandler(ctx, name, handler); err != nil {
				return err
			}
		}
		return Sleep(ctx, 1)
	})
	worker.handleActivation(initializeActivation("run-1", "queryable", nil))

	for _, testCase := range []struct {
		name string
		want string
	}{
		{name: "missing", want: `query "missing" is not registered`},
		{name: "error", want: wantErr.Error()},
		{name: "panic", want: "query panic"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			completion := worker.handleActivation(activation("run-1", queryJob(t, worker, "query-"+testCase.name, testCase.name)))
			failure := queryResult(t, completion).GetFailed()
			if failure == nil || !strings.Contains(failure.Message, testCase.want) {
				t.Fatalf("query failure = %+v, want message containing %q", failure, testCase.want)
			}
			if worker.getRun("run-1") == nil {
				t.Fatal("query failure evicted the workflow run")
			}
		})
	}

	if got := queryValue[string](t, worker, worker.handleActivation(activation("run-1", queryJob(t, worker, "query-ok", "ok")))); got != "healthy" {
		t.Fatalf("healthy query after failures = %q", got)
	}
}

func TestQueryHandlerCannotCallWorkflowAPIs(t *testing.T) {
	worker := newTestWorker()
	worker.Register("queryable", func(ctx *Context) error {
		if err := SetQueryHandler(ctx, "side-effect", func() (string, error) {
			NewTimer(ctx, 1)
			return "unreachable", nil
		}); err != nil {
			return err
		}
		return Sleep(ctx, 1)
	})
	worker.handleActivation(initializeActivation("run-1", "queryable", nil))

	completion := worker.handleActivation(activation("run-1", queryJob(t, worker, "query-1", "side-effect")))
	commands := successfulCommands(t, completion)
	if len(commands) != 1 {
		t.Fatalf("query emitted %d commands, want only its response", len(commands))
	}
	result := commands[0].GetRespondToQuery()
	if failure := result.GetFailed(); failure == nil || !strings.Contains(failure.Message, "must run in the currently executing workflow coroutine") {
		t.Fatalf("side-effect query result = %+v", result)
	}
}

func TestQueryHandlerCannotConsumeBufferedSignals(t *testing.T) {
	worker := newTestWorker()
	worker.Register("queryable", func(ctx *Context) error {
		signals := GetSignalChannel(ctx, "buffered")
		if err := SetQueryHandler(ctx, "consume", func() (string, error) {
			var value string
			_, err := signals.ReceiveAsync(&value)
			return value, err
		}); err != nil {
			return err
		}
		return Sleep(ctx, 1)
	})
	worker.handleActivation(initializeActivation("run-1", "queryable", nil))
	worker.handleActivation(activation("run-1", signalJob(t, worker, "buffered", "keep-me")))

	completion := worker.handleActivation(activation("run-1", queryJob(t, worker, "query-1", "consume")))
	failure := queryResult(t, completion).GetFailed()
	if failure == nil || !strings.Contains(failure.Message, "must run in a workflow coroutine") {
		t.Fatalf("signal-consuming query result = %+v", queryResult(t, completion))
	}
	if buffered := worker.getRun("run-1").signalChannel("buffered").Len(); buffered != 1 {
		t.Fatalf("buffered signals after query = %d, want 1", buffered)
	}
}

func TestQueryObservesReplayedStateAfterCacheEviction(t *testing.T) {
	worker := newTestWorker()
	worker.Register("queryable", func(ctx *Context) error {
		state := "waiting"
		if err := SetQueryHandler(ctx, "status", func() (string, error) { return state, nil }); err != nil {
			return err
		}
		for {
			var next string
			if err := GetSignalChannel(ctx, "state").Receive(ctx, &next); err != nil {
				return err
			}
			state = next
		}
	})
	worker.handleActivation(initializeActivation("run-1", "queryable", nil))
	worker.handleActivation(activation("run-1", &workflowactivation.WorkflowActivationJob{
		Variant: &workflowactivation.WorkflowActivationJob_RemoveFromCache{
			RemoveFromCache: &workflowactivation.RemoveFromCache{Reason: workflowactivation.RemoveFromCache_LANG_REQUESTED},
		},
	}))

	replay := initializeActivation("run-1", "queryable", nil)
	replay.Jobs = append(
		replay.Jobs,
		signalJob(t, worker, "state", "replayed"),
		queryJob(t, worker, "query-1", "status"),
	)
	if got := queryValue[string](t, worker, worker.handleActivation(replay)); got != "replayed" {
		t.Fatalf("replayed query result = %q, want replayed", got)
	}
}

func TestQueryRegistrationValidation(t *testing.T) {
	tests := []struct {
		name      string
		queryType string
		handler   any
		want      string
	}{
		{name: "empty type", handler: func() (string, error) { return "", nil }, want: "query name is required"},
		{name: "reserved type", queryType: "__internal", handler: func() (string, error) { return "", nil }, want: "reserved"},
		{name: "nil handler", queryType: "status", want: "function is required"},
		{name: "workflow context", queryType: "status", handler: func(*Context) (string, error) { return "", nil }, want: "must not accept"},
		{name: "missing result", queryType: "status", handler: func() error { return nil }, want: "must return (result, error)"},
		{name: "missing error", queryType: "status", handler: func() string { return "" }, want: "must return (result, error)"},
		{name: "wrong final result", queryType: "status", handler: func() (string, string) { return "", "" }, want: "must return error"},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			_, err := newRegisteredQuery(testCase.queryType, testCase.handler)
			if err == nil || !strings.Contains(err.Error(), testCase.want) {
				t.Fatalf("newRegisteredQuery() error = %v, want %q", err, testCase.want)
			}
		})
	}
}

func queryJob(t *testing.T, worker *Worker, queryID, queryType string, arguments ...any) *workflowactivation.WorkflowActivationJob {
	t.Helper()
	encoded, err := worker.payloadConverter.ToPayloads(arguments...)
	if err != nil {
		t.Fatal(err)
	}
	return &workflowactivation.WorkflowActivationJob{
		Variant: &workflowactivation.WorkflowActivationJob_QueryWorkflow{
			QueryWorkflow: &workflowactivation.QueryWorkflow{
				QueryId:   queryID,
				QueryType: queryType,
				Arguments: encoded.GetPayloads(),
			},
		},
	}
}

func queryResult(t *testing.T, completion *workflowcompletion.WorkflowActivationCompletion) *workflowcommands.QueryResult {
	t.Helper()
	commands := successfulCommands(t, completion)
	for _, command := range commands {
		if result := command.GetRespondToQuery(); result != nil {
			return result
		}
	}
	t.Fatalf("completion has no query response among %d commands", len(commands))
	return nil
}

func queryValue[T any](t *testing.T, worker *Worker, completion *workflowcompletion.WorkflowActivationCompletion) T {
	t.Helper()
	result := queryResult(t, completion)
	succeeded := result.GetSucceeded()
	if succeeded == nil {
		t.Fatalf("query %q failed: %s", result.QueryId, failureMessage(result.GetFailed()))
	}
	var value T
	if err := worker.payloadConverter.FromPayload(succeeded.Response, &value); err != nil {
		t.Fatalf("decode query %q result: %v", result.QueryId, err)
	}
	return value
}
