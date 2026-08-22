package sqlflow

import (
	"context"
	"encoding/json"
)

type Client struct {
	db       Driver
	registry map[string]Handler
}

func NewClient(db Driver) *Client {
	return &Client{
		db:       db,
		registry: make(map[string]Handler),
	}
}

func (c *Client) CreateQueue(ctx context.Context, queueName string, storageMode string) error {
	if storageMode == "" {
		storageMode = "unpartitioned"
	}
	return c.db.CreateQueue(ctx, queueName, storageMode)
}

func (c *Client) Spawn(ctx context.Context, options SpawnOptions, taskName string, params any) (*SpawnResult, error) {
	var paramsBytes []byte
	var err error

	if params != nil {
		paramsBytes, err = json.Marshal(params)
		if err != nil {
			return nil, err
		}
	} else {
		paramsBytes = []byte("{}")
	}

	optionsBytes, err := json.Marshal(options)

	if err != nil {
		return nil, err
	}

	return c.db.SpawnTask(ctx, options.QueueName, taskName, paramsBytes, optionsBytes)
}

func (c *Client) EmitEvent(ctx context.Context, options EmitEventOptions, eventName string, payload any) error {
	var payloadBytes []byte
	var err error

	if payload != nil {
		payloadBytes, err = json.Marshal(payload)
		if err != nil {
			return err
		}
	} else {
		payloadBytes = []byte("{}")
	}

	return c.db.EmitEvent(ctx, options.QueueName, eventName, string(payloadBytes))
}

func (c *Client) CancelTask(ctx context.Context, options CancelTaskOptions, taskID string) error {
	return c.db.CancelTask(ctx, options.QueueName, taskID)
}

func (c *Client) RegisterTask(taskName string, handler Handler) {
	c.registry[taskName] = handler
}

func (c *Client) CreateWorker(opts WorkerOptions) *Worker {
	return NewWorker(opts, c.db, c.registry)
}

func RegisterWorkflow[T any](c *Client, taskName string, workflow func(ctx *TaskContext, params T) error) {
	handler := func(ctx *TaskContext) error {
		var params T
		if len(ctx.RawParams) > 0 {
			if err := json.Unmarshal(ctx.RawParams, &params); err != nil {
				return err
			}
		}
		return workflow(ctx, params)
	}
	
	c.registry[taskName] = handler
}