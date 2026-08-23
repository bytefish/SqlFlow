package sqlflow

import (
	"context"
	"encoding/json"
	"errors"
)

var ErrSuspendTask = errors.New("task suspended waiting for event")

type TaskContext struct {
	TaskID    string
	RunID     string
	Attempt   int
	QueueName string
	DB        Driver
	Ctx       context.Context
	RawParams []byte
}

func Step[T any](ctx *TaskContext, stepName string, action func() (T, error)) (T, error) {
	var zero T

	stateRow, err := ctx.DB.GetTaskCheckpointState(ctx.Ctx, ctx.QueueName, ctx.TaskID, stepName, 0)
	if err != nil {
		return zero, err
	}

	if stateRow != nil && len(stateRow.State) > 0 {
		var result T
		err := json.Unmarshal(stateRow.State, &result)
		return result, err
	}

	result, err := action()
	if err != nil {
		return zero, err
	}

	stateBytes, _ := json.Marshal(result)
    
	err = ctx.DB.SetTaskCheckpointState(ctx.Ctx, ctx.QueueName, ctx.TaskID, stepName, string(stateBytes), ctx.RunID, 0)
	
	return result, err
}

func AwaitEvent[T any](ctx *TaskContext, eventName string, stepName string, timeout *int) (T, error) {
	var zero T
	result, err := ctx.DB.AwaitEvent(ctx.Ctx, ctx.QueueName, ctx.TaskID, ctx.RunID, stepName, eventName, timeout)
	if err != nil {
		return zero, err
	}

	if result == nil {
		return zero, errors.New("unexpected empty result from await_event")
	}

	if result.ShouldSuspend {
		return zero, ErrSuspendTask 
	}

	var payload T

	if len(result.Payload) > 0 {
		if err := json.Unmarshal(result.Payload, &payload); err != nil {
			return zero, err
		}
	}

	return payload, nil
}