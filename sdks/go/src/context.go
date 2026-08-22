package sqlflow

import (
	"context"
	"encoding/json"
	"errors"
)

var ErrSuspendTask = errors.New("task suspended waiting for event")

type TaskContext struct {
	TaskID      string
	RunID       string
	Attempt     int
	QueueName   string
	DB          Driver
	Ctx         context.Context
    RawParams   []byte
}

func (c *TaskContext) Step(stepName string, action func() (any, error)) (any, error) {
	stateRow, err := c.DB.GetTaskCheckpointState(c.Ctx, c.QueueName, c.TaskID, stepName, 0)
	if err != nil {
		return nil, err
	}

	if stateRow != nil && len(stateRow.State) > 0 {
		var result any
		json.Unmarshal(stateRow.State, &result)
		return result, nil
	}

	result, err := action()
	if err != nil {
		return nil, err
	}

	stateBytes, _ := json.Marshal(result)
    
	err = c.DB.SetTaskCheckpointState(c.Ctx, c.QueueName, c.TaskID, stepName, string(stateBytes), c.RunID, 0)
	
	return result, err
}

func (c *TaskContext) AwaitEvent(eventName string, stepName string, timeout *int) (any, error) {
	result, err := c.DB.AwaitEvent(c.Ctx, c.QueueName, c.TaskID, c.RunID, stepName, eventName, timeout)
	if err != nil {
		return nil, err
	}

	if result == nil {
		return nil, errors.New("unexpected empty result from await_event")
	}

	if result.ShouldSuspend {
		return nil, ErrSuspendTask 
	}

	var payload any
	if len(result.Payload) > 0 {
		json.Unmarshal(result.Payload, &payload)
	}

	return payload, nil
}