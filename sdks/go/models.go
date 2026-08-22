package sqlflow

import (
	"time"
)

type ClaimedTask struct {
	RunID         string  `db:"run_id"`
	TaskID        string  `db:"task_id"`
	Attempt       int     `db:"attempt"`
	TaskName      string  `db:"task_name"`
	Params        []byte  `db:"params"`
	RetryStrategy []byte  `db:"retry_strategy"`
	MaxAttempts   *int    `db:"max_attempts"`
	Headers       []byte  `db:"headers"`
	WakeEvent     *string `db:"wake_event"`
	EventPayload  []byte  `db:"event_payload"`
}

type CheckpointState struct {
	CheckpointName string    `db:"checkpoint_name"`
	State          []byte    `db:"state"`
	Status         string    `db:"status"`
	OwnerRunID     *string   `db:"owner_run_id"`
	UpdatedAt      time.Time `db:"updated_at"`
}

type AwaitEventResult struct {
	ShouldSuspend bool   `db:"should_suspend"`
	Payload       []byte `db:"payload"`
}

type SpawnOptions struct {
	QueueName      string         `json:"queue_name"`
	MaxAttempts    *int           `json:"max_attempts,omitempty"`
	Headers        map[string]any `json:"headers,omitempty"`
	RetryStrategy  map[string]any `json:"retry_strategy,omitempty"`
	Cancellation   map[string]any `json:"cancellation,omitempty"`
	IdempotencyKey string         `json:"idempotency_key,omitempty"`
}

type SpawnResult struct {
	TaskID  string `json:"task_id"`
	RunID   string `json:"run_id"`
	Attempt int    `json:"attempt"`
	Created bool   `json:"created"`
}

type EmitEventOptions struct {
	QueueName string `json:"queue_name"`
}

type CancelTaskOptions struct {
	QueueName string `json:"queue_name"`
}

type WorkerOptions struct {
	WorkerID     string
	QueueName    string
	PollInterval time.Duration
	Concurrency  int
}