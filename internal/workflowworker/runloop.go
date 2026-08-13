package workflowworker

import (
	"context"
	"errors"
	"fmt"

	"github.com/temporalio/sdk-go-on-wasm-core/internal/completionpipe"
	workflowactivation "github.com/temporalio/sdk-go-on-wasm-core/internal/corepb/workflowactivation"
)

type workflowWorkerState int

const (
	workflowWorkerStateRunning workflowWorkerState = iota
	workflowWorkerStateDraining
)

type workflowRunLoop struct {
	worker              *Worker
	ctx                 context.Context
	stopPolling         context.CancelFunc
	pollEventsCh        <-chan workflowPollEvent
	processorResultsCh  chan workflowProcessorResult
	completionPipeline  *completionpipe.Pipeline
	completionResultsCh <-chan *completionpipe.Result
	state               workflowWorkerState
	firstErr            error
	laterErr            error
	activeProcessors    int
	ctxDone             <-chan struct{}
}

func newWorkflowRunLoop(worker *Worker, ctx context.Context) *workflowRunLoop {
	pollCtx, stopPolling := context.WithCancel(ctx)
	pollEvents := make(chan workflowPollEvent)
	processorResults := make(chan workflowProcessorResult)
	completionPipeline := completionpipe.New(worker.workflowCompletionLimit())
	go worker.pollWorkflows(pollCtx, pollEvents)
	return &workflowRunLoop{
		worker:              worker,
		ctx:                 ctx,
		stopPolling:         stopPolling,
		pollEventsCh:        pollEvents,
		processorResultsCh:  processorResults,
		completionPipeline:  completionPipeline,
		completionResultsCh: completionPipeline.Results(),
		state:               workflowWorkerStateRunning,
		ctxDone:             ctx.Done(),
	}
}

func (loop *workflowRunLoop) run() error {
	for {
		if loop.shouldFinish() {
			return loop.finish()
		}

		select {
		case <-loop.ctxDone:
			loop.startDraining(nil)
		case event, ok := <-loop.pollEventsCh:
			loop.handlePollEvent(event, ok)
		case result := <-loop.processorResultsCh:
			loop.handleProcessorResult(result)
		case result := <-loop.completionResultsCh:
			loop.handleCompletionResult(result)
		}
	}
}

func (loop *workflowRunLoop) shouldFinish() bool {
	return loop.state != workflowWorkerStateRunning &&
		loop.pollEventsCh == nil &&
		loop.activeProcessors == 0 &&
		loop.completionPipeline.Outstanding() == 0
}

func (loop *workflowRunLoop) handlePollEvent(event workflowPollEvent, ok bool) {
	if !ok {
		loop.pollEventsCh = nil
		if loop.state == workflowWorkerStateRunning {
			if loop.ctx.Err() != nil {
				loop.startDraining(nil)
			} else {
				loop.startDraining(errors.New("workflow poll loop stopped unexpectedly"))
			}
		}
		return
	}
	if event.err != nil {
		if loop.ctx.Err() != nil && isWorkflowCancellationError(event.err) {
			loop.startDraining(nil)
		} else {
			loop.startDraining(event.err)
		}
		return
	}
	if event.activation == nil {
		loop.startDraining(errors.New("workflow poll loop returned an empty activation"))
		return
	}
	if event.payloadCodecErr != nil {
		if err := loop.worker.enqueueWorkflowCompletion(
			loop.completionPipeline,
			failedActivation(event.activation.RunId, event.payloadCodecErr),
			event.activation.RunId,
			nil,
		); err != nil {
			loop.startDraining(err)
		}
		return
	}
	loop.dispatchActivation(event.activation)
}

func (loop *workflowRunLoop) dispatchActivation(activation *workflowactivation.WorkflowActivation) {
	if loop.activeProcessors >= loop.worker.workflowCompletionLimit() {
		dispatchErr := fmt.Errorf(
			"core exceeded the configured maximum of %d concurrent workflow task executions",
			loop.worker.workflowCompletionLimit(),
		)
		loop.startDraining(dispatchErr)
		if err := loop.worker.enqueueWorkflowCompletion(
			loop.completionPipeline,
			failedActivation(activation.RunId, dispatchErr),
			activation.RunId,
			nil,
		); err != nil {
			loop.startDraining(err)
		}
		return
	}
	loop.activeProcessors++
	var runTurn *workflowRunTurn
	if activation.RunId != "" {
		runTurn = loop.worker.reserveRunTurn(activation.RunId)
	}
	go loop.worker.processActivation(activation, runTurn, loop.processorResultsCh)
}

func (loop *workflowRunLoop) handleProcessorResult(result workflowProcessorResult) {
	loop.activeProcessors--
	if err := loop.worker.enqueueWorkflowCompletion(
		loop.completionPipeline,
		result.completion,
		result.runID,
		result.runTurn,
	); err != nil {
		loop.startDraining(err)
	}
}

func (loop *workflowRunLoop) handleCompletionResult(result *completionpipe.Result) {
	result.Finalize()
	if result.Err != nil {
		loop.startDraining(result.Err)
	}
}

func (loop *workflowRunLoop) recordError(err error) {
	if err == nil {
		return
	}
	if loop.firstErr == nil {
		loop.firstErr = err
		return
	}
	loop.laterErr = errors.Join(loop.laterErr, err)
}

func (loop *workflowRunLoop) startDraining(err error) {
	loop.recordError(err)
	if loop.state == workflowWorkerStateRunning {
		loop.state = workflowWorkerStateDraining
		loop.stopPolling()
		loop.ctxDone = nil
	}
}

func (loop *workflowRunLoop) finish() error {
	loop.worker.stopAllRuns()
	return errors.Join(loop.firstErr, loop.laterErr, loop.worker.shutdownCore())
}
