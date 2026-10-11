package natsdriver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	sqlflow "github.com/bytefish/SqlFlow/sdks/go"
	natsgo "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const (
	defaultStreamName    = "SQLFLOW_SIGNALS"
	defaultSubjectPrefix = "sqlflow.work"
)

type DriverDecorator struct {
	sqlflow.Driver

	connection    *natsgo.Conn
	jetStream     jetstream.JetStream
	streamName    string
	subjectPrefix string

	mu       sync.Mutex
	listener sqlflow.QueueSignalListener
}

type Options struct {
	StreamName    string
	SubjectPrefix string
}

func NewDriverDecorator(
	ctx context.Context,
	inner sqlflow.Driver,
	connection *natsgo.Conn,
	options Options,
) (*DriverDecorator, error) {
	if inner == nil {
		return nil, errors.New("inner driver must not be nil")
	}

	if connection == nil {
		return nil, errors.New("NATS connection must not be nil")
	}

	streamName := options.StreamName
	if streamName == "" {
		streamName = defaultStreamName
	}

	subjectPrefix := options.SubjectPrefix
	if subjectPrefix == "" {
		subjectPrefix = defaultSubjectPrefix
	}

	js, err := jetstream.New(connection)
	if err != nil {
		return nil, fmt.Errorf(
			"create JetStream context: %w",
			err,
		)
	}

	_, err = js.CreateOrUpdateStream(
		ctx,
		jetstream.StreamConfig{
			Name:     streamName,
			Subjects: []string{subjectPrefix + ".>"},
			Storage:  jetstream.FileStorage,
		},
	)
	if err != nil {
		return nil, fmt.Errorf(
			"create SQLFlow signal stream: %w",
			err,
		)
	}

	return &DriverDecorator{
		Driver:        inner,
		connection:    connection,
		jetStream:     js,
		streamName:    streamName,
		subjectPrefix: subjectPrefix,
	}, nil
}

func (d *DriverDecorator) SpawnTask(
	ctx context.Context,
	tenantID string,
	queueName string,
	taskName string,
	params []byte,
	options []byte,
) (*sqlflow.SpawnResult, error) {
	result, err := d.Driver.SpawnTask(
		ctx,
		tenantID,
		queueName,
		taskName,
		params,
		options,
	)
	if err != nil {
		return nil, err
	}

	if result.Created {
		if err := d.publishSignal(
			ctx,
			tenantID,
			queueName,
			"spawn",
		); err != nil {
			// Do not report SpawnTask as failed.
		}
	}

	return result, nil
}

func (d *DriverDecorator) EmitEvent(
	ctx context.Context,
	tenantID string,
	queueName string,
	eventName string,
	payload string,
) error {
	err := d.Driver.EmitEvent(
		ctx,
		tenantID,
		queueName,
		eventName,
		payload,
	)
	if err != nil {
		return err
	}

	if err := d.publishSignal(
		ctx,
		tenantID,
		queueName,
		"event",
	); err != nil {
		// Event is durable. Ignore signal failure.
	}

	return nil
}

// Pass-through functions for the new interface signatures:
func (d *DriverDecorator) CreateQueue(ctx context.Context, tenantID string, queueName string, storageMode string) error {
	return d.Driver.CreateQueue(ctx, tenantID, queueName, storageMode)
}

func (d *DriverDecorator) ClaimTask(ctx context.Context, tenantID string, queueName string, workerID string, claimTimeout int, qty int) ([]sqlflow.ClaimedTask, error) {
	return d.Driver.ClaimTask(ctx, tenantID, queueName, workerID, claimTimeout, qty)
}

func (d *DriverDecorator) CompleteRun(ctx context.Context, tenantID string, queueName string, runID string, state string) error {
	return d.Driver.CompleteRun(ctx, tenantID, queueName, runID, state)
}

func (d *DriverDecorator) ScheduleRun(ctx context.Context, tenantID string, queueName string, runID string, wakeAt time.Time) error {
	return d.Driver.ScheduleRun(ctx, tenantID, queueName, runID, wakeAt)
}

func (d *DriverDecorator) FailRun(ctx context.Context, tenantID string, queueName string, runID string, reason string, retryAt time.Time) error {
	return d.Driver.FailRun(ctx, tenantID, queueName, runID, reason, retryAt)
}

func (d *DriverDecorator) SetTaskCheckpointState(ctx context.Context, tenantID string, queueName string, taskID string, stepName string, state string, ownerRun string, extendClaimBy int) error {
	return d.Driver.SetTaskCheckpointState(ctx, tenantID, queueName, taskID, stepName, state, ownerRun, extendClaimBy)
}

func (d *DriverDecorator) GetTaskCheckpointState(ctx context.Context, tenantID string, queueName string, taskID string, stepName string, includePending int) (*sqlflow.CheckpointState, error) {
	return d.Driver.GetTaskCheckpointState(ctx, tenantID, queueName, taskID, stepName, includePending)
}

func (d *DriverDecorator) AwaitEvent(ctx context.Context, tenantID string, queueName string, taskID string, runID string, stepName string, eventName string, timeout *int) (*sqlflow.AwaitEventResult, error) {
	return d.Driver.AwaitEvent(ctx, tenantID, queueName, taskID, runID, stepName, eventName, timeout)
}

func (d *DriverDecorator) CancelTask(ctx context.Context, tenantID string, queueName string, taskID string) error {
	return d.Driver.CancelTask(ctx, tenantID, queueName, taskID)
}

func (d *DriverDecorator) GetNextAvailableAt(ctx context.Context, tenantID string, queueName string) (*time.Time, error) {
	return d.Driver.GetNextAvailableAt(ctx, tenantID, queueName)
}

func (d *DriverDecorator) CreateQueueSignalListener(
	ctx context.Context,
) (sqlflow.QueueSignalListener, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.listener != nil {
		return d.listener, nil
	}

	listener := NewQueueSignalListener(
		d.jetStream,
		d.streamName,
		d.subjectPrefix,
	)

	d.listener = listener

	return listener, nil
}

func (d *DriverDecorator) publishSignal(
	ctx context.Context,
	tenantID string,
	queueName string,
	event string,
) error {
	payload, err := json.Marshal(
		signalMessage{
			TenantID: tenantID,
			Queue:    queueName,
			Event:    event,
		},
	)
	if err != nil {
		return fmt.Errorf(
			"serialize NATS signal: %w",
			err,
		)
	}

	subject := d.subjectForQueue(tenantID, queueName)

	_, err = d.jetStream.Publish(
		ctx,
		subject,
		payload,
	)
	if err != nil {
		return fmt.Errorf(
			"publish NATS signal to %q: %w",
			subject,
			err,
		)
	}

	return nil
}

func (d *DriverDecorator) subjectForQueue(
	tenantID string,
	queueName string,
) string {
	return d.subjectPrefix +
		"." +
		encodeQueueName(tenantID) +
		"." +
		encodeQueueName(queueName)
}

type signalMessage struct {
	TenantID string `json:"tenant_id"`
	Queue    string `json:"queue"`
	Event    string `json:"event"`
}