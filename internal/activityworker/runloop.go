package activityworker

import (
	"context"
	"errors"

	"github.com/temporalio/sdk-go-on-wasm-core/internal/completionpipe"
	activitytask "github.com/temporalio/sdk-go-on-wasm-core/internal/corepb/activitytask"
)

type activityWorkerState int

const (
	activityWorkerStateRunning activityWorkerState = iota
	activityWorkerStateDraining
)

type activityRunLoop struct {
	worker              *Worker
	ctx                 context.Context
	stopPolling         context.CancelFunc
	pollEventsCh        <-chan pollEvent
	activityEventsCh    chan activityEvent
	completionPipeline  *completionpipe.Pipeline
	completionResultsCh <-chan *completionpipe.Result
	running             map[string]*runningActivity
	state               activityWorkerState
	firstErr            error
	laterErr            error
	activeExecutions    int
	ctxDone             <-chan struct{}
}

func newActivityRunLoop(
	ctx context.Context,
	worker *Worker,
	stopPolling context.CancelFunc,
	pollEvents <-chan pollEvent,
	activityEvents chan activityEvent,
	completionPipeline *completionpipe.Pipeline,
) *activityRunLoop {
	return &activityRunLoop{
		worker:              worker,
		ctx:                 ctx,
		stopPolling:         stopPolling,
		pollEventsCh:        pollEvents,
		activityEventsCh:    activityEvents,
		completionPipeline:  completionPipeline,
		completionResultsCh: completionPipeline.Results(),
		running:             make(map[string]*runningActivity),
		state:               activityWorkerStateRunning,
		ctxDone:             ctx.Done(),
	}
}

func (loop *activityRunLoop) run() error {
	for {
		if loop.shouldFinish() {
			return errors.Join(loop.firstErr, loop.laterErr)
		}

		select {
		case <-loop.ctxDone:
			loop.startDraining(nil)
		case event, ok := <-loop.pollEventsCh:
			loop.handlePollEvent(event, ok)
		case event := <-loop.activityEventsCh:
			loop.handleActivityEvent(event)
		case result := <-loop.completionResultsCh:
			loop.handleCompletionResult(result)
		}
	}
}

func (loop *activityRunLoop) shouldFinish() bool {
	return loop.state != activityWorkerStateRunning &&
		loop.pollEventsCh == nil &&
		loop.completionPipeline.Outstanding() == 0
}

func (loop *activityRunLoop) recordError(err error) {
	if err == nil {
		return
	}
	if loop.firstErr == nil {
		loop.firstErr = err
		return
	}
	loop.laterErr = errors.Join(loop.laterErr, err)
}

func (loop *activityRunLoop) startDraining(err error) {
	loop.recordError(err)
	if loop.state == activityWorkerStateRunning {
		loop.state = activityWorkerStateDraining
		loop.stopPolling()
		loop.ctxDone = nil
		loop.recordError(loop.worker.cancelRunningActivities(
			loop.running,
			&loop.activeExecutions,
			"activity worker is shutting down",
			loop.completionPipeline,
		))
	}
}

func (loop *activityRunLoop) handlePollEvent(event pollEvent, ok bool) {
	if !ok {
		loop.pollEventsCh = nil
		if loop.state == activityWorkerStateRunning {
			if loop.ctx.Err() != nil {
				loop.startDraining(nil)
			} else {
				loop.startDraining(errors.New("activity poll loop stopped unexpectedly"))
			}
		}
		return
	}
	if event.err != nil {
		if loop.ctx.Err() != nil && isActivityCancellationError(event.err) {
			loop.startDraining(nil)
		} else {
			loop.startDraining(event.err)
		}
		return
	}
	if event.task == nil {
		loop.startDraining(errors.New("activity poll loop returned an empty task"))
		return
	}
	if event.payloadCodecErr != nil {
		if err := loop.worker.enqueueCompletion(
			loop.completionPipeline,
			failedCompletion(event.task.TaskToken, event.payloadCodecErr),
			nil,
		); err != nil {
			loop.startDraining(err)
		}
		return
	}
	if err := loop.handlePolledTask(event.task); err != nil {
		loop.startDraining(err)
	}
}

func (loop *activityRunLoop) handlePolledTask(task *activitytask.ActivityTask) error {
	if start := task.GetStart(); start != nil {
		if loop.state == activityWorkerStateRunning {
			return loop.worker.startActivity(
				loop.ctx,
				loop.running,
				&loop.activeExecutions,
				task.TaskToken,
				start,
				loop.activityEventsCh,
			)
		}
		return loop.worker.enqueueCompletion(
			loop.completionPipeline,
			cancelledCompletion(task.TaskToken, "activity worker is shutting down"),
			nil,
		)
	}
	cancel := task.GetCancel()
	if cancel == nil {
		return errors.New("core delivered an activity task without a start or cancellation")
	}
	return loop.worker.cancelRunningActivity(
		loop.running,
		&loop.activeExecutions,
		task.TaskToken,
		cancel.Reason.String(),
		loop.completionPipeline,
	)
}

func (loop *activityRunLoop) handleActivityEvent(event activityEvent) {
	if loop.running[string(event.activity.taskToken)] != event.activity {
		return
	}
	if err := loop.worker.enqueueActivityCompletion(
		loop.running,
		&loop.activeExecutions,
		event.activity,
		event.completion,
		loop.completionPipeline,
	); err != nil {
		loop.startDraining(err)
	}
}

func (loop *activityRunLoop) handleCompletionResult(result *completionpipe.Result) {
	result.Finalize()
	if result.Err != nil {
		loop.startDraining(result.Err)
	}
}
