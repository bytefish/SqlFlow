package natsdriver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

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
	queueName string,
	taskName string,
	params []byte,
	options []byte,
) (*sqlflow.SpawnResult, error) {
	result, err := d.Driver.SpawnTask(
		ctx,
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
			queueName,
			"spawn",
		); err != nil {
			/*
			 * The database transaction already succeeded.
			 *
			 * Do not report SpawnTask as failed because retrying the
			 * operation could create another task unless idempotency
			 * is configured.
			 *
			 * The worker's reconciliation interval remains the
			 * fallback if signaling fails.
			 */
		}
	}

	return result, nil
}

func (d *DriverDecorator) EmitEvent(
	ctx context.Context,
	queueName string,
	eventName string,
	payload string,
) error {
	err := d.Driver.EmitEvent(
		ctx,
		queueName,
		eventName,
		payload,
	)
	if err != nil {
		return err
	}

	if err := d.publishSignal(
		ctx,
		queueName,
		"event",
	); err != nil {
		/*
		 * The event is already durable in PostgreSQL.
		 *
		 * Do not return an error that might cause callers to emit
		 * the same logical event again. Reconciliation will wake
		 * workers if the signal is lost.
		 */
	}

	return nil
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
	queueName string,
	event string,
) error {
	payload, err := json.Marshal(
		signalMessage{
			Queue: queueName,
			Event: event,
		},
	)
	if err != nil {
		return fmt.Errorf(
			"serialize NATS signal: %w",
			err,
		)
	}

	subject := d.subjectForQueue(queueName)

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
	queueName string,
) string {
	return d.subjectPrefix +
		"." +
		encodeQueueName(queueName)
}

type signalMessage struct {
	Queue string `json:"queue"`
	Event string `json:"event"`
}