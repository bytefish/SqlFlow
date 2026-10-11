package sqlflow

import (
	"context"
	"time"
)

type Driver interface {
	CreateQueue(ctx context.Context, tenantID string, queueName string, storageMode string) error
	SpawnTask(ctx context.Context, tenantID string, queueName string, taskName string, params []byte, options []byte) (*SpawnResult, error)
	ClaimTask(ctx context.Context, tenantID string, queueName string, workerID string, claimTimeout int, qty int) ([]ClaimedTask, error)
	CompleteRun(ctx context.Context, tenantID string, queueName string, runID string, state string) error
	ScheduleRun(ctx context.Context, tenantID string, queueName string, runID string, wakeAt time.Time) error
	FailRun(ctx context.Context, tenantID string, queueName string, runID string, reason string, retryAt time.Time) error
	SetTaskCheckpointState(ctx context.Context, tenantID string, queueName string, taskID string, stepName string, state string, ownerRun string, extendClaimBy int) error
	GetTaskCheckpointState(ctx context.Context, tenantID string, queueName string, taskID string, stepName string, includePending int) (*CheckpointState, error)
	AwaitEvent(ctx context.Context, tenantID string, queueName string, taskID string, runID string, stepName string, eventName string, timeout *int) (*AwaitEventResult, error)
	EmitEvent(ctx context.Context, tenantID string, queueName string, eventName string, payload string) error
	CancelTask(ctx context.Context, tenantID string, queueName string, taskID string) error
	GetNextAvailableAt(ctx context.Context, tenantID string, queueName string) (*time.Time, error)
	CreateQueueSignalListener(ctx context.Context) (QueueSignalListener, error)
}