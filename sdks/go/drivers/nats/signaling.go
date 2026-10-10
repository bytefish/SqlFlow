package natsdriver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	sqlflow "github.com/bytefish/SqlFlow/sdks/go"
	"github.com/nats-io/nats.go/jetstream"
)

type QueueSignalListener struct {
	jetStream     jetstream.JetStream
	streamName    string
	subjectPrefix string

	ctx    context.Context
	cancel context.CancelFunc

	mu      sync.RWMutex
	queues  map[string]*registeredQueue
	closed  bool
	closeMu sync.Once
}

type registeredQueue struct {
	signal chan struct{}

	consumer jetstream.Consumer
	consume  jetstream.ConsumeContext
}

func NewQueueSignalListener(
	js jetstream.JetStream,
	streamName string,
	subjectPrefix string,
) *QueueSignalListener {
	ctx, cancel := context.WithCancel(
		context.Background(),
	)

	return &QueueSignalListener{
		jetStream:     js,
		streamName:    streamName,
		subjectPrefix: subjectPrefix,
		ctx:           ctx,
		cancel:        cancel,
		queues:        make(map[string]*registeredQueue),
	}
}

func (l *QueueSignalListener) RegisterQueue(
	ctx context.Context,
	queueName string,
) error {
	if queueName == "" {
		return errors.New("queue name must not be empty")
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	if l.closed {
		return errors.New(
			"NATS queue signal listener is closed",
		)
	}

	if _, exists := l.queues[queueName]; exists {
		return nil
	}

	subject := l.subjectForQueue(queueName)
	durableName := durableNameForQueue(queueName)

	consumer, err := l.jetStream.CreateOrUpdateConsumer(
		ctx,
		l.streamName,
		jetstream.ConsumerConfig{
			Durable:       durableName,
			FilterSubject: subject,
			AckPolicy:     jetstream.AckExplicitPolicy,
		},
	)
	if err != nil {
		return fmt.Errorf(
			"create NATS consumer for queue %q: %w",
			queueName,
			err,
		)
	}

	queue := &registeredQueue{
		signal:   make(chan struct{}, 1),
		consumer: consumer,
	}

	/*
	 * Force one immediate database check after worker startup.
	 */
	queue.signal <- struct{}{}

	consumeContext, err := consumer.Consume(
		func(message jetstream.Msg) {
			var signal signalMessage

			if err := json.Unmarshal(
				message.Data(),
				&signal,
			); err != nil {
				/*
				 * Invalid signaling data should not block the
				 * consumer forever.
				 */
				_ = message.Term()
				return
			}

			if signal.Queue != queueName {
				_ = message.Term()
				return
			}

			select {
			case queue.signal <- struct{}{}:
			default:
				/*
				 * A wake-up is already pending.
				 *
				 * Coalescing is intentional because the database
				 * remains the source of truth.
				 */
			}

			_ = message.Ack()
		},
	)
	if err != nil {
		return fmt.Errorf(
			"start NATS consumer for queue %q: %w",
			queueName,
			err,
		)
	}

	queue.consume = consumeContext
	l.queues[queueName] = queue

	return nil
}

func (l *QueueSignalListener) WaitForSignal(
	ctx context.Context,
	queueName string,
	timeout time.Duration,
) (bool, error) {
	l.mu.RLock()
	queue := l.queues[queueName]
	closed := l.closed
	l.mu.RUnlock()

	if closed {
		return false, errors.New(
			"NATS queue signal listener is closed",
		)
	}

	if queue == nil {
		return false, fmt.Errorf(
			"queue %q is not registered",
			queueName,
		)
	}

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case <-queue.signal:
		return true, nil

	case <-timer.C:
		return false, nil

	case <-ctx.Done():
		return false, ctx.Err()

	case <-l.ctx.Done():
		return false, l.ctx.Err()
	}
}

func (l *QueueSignalListener) Close() error {
	l.closeMu.Do(func() {
		l.mu.Lock()
		l.closed = true

		for _, queue := range l.queues {
			if queue.consume != nil {
				queue.consume.Stop()
			}
		}

		l.mu.Unlock()
		l.cancel()
	})

	return nil
}

func (l *QueueSignalListener) subjectForQueue(
	queueName string,
) string {
	return l.subjectPrefix +
		"." +
		encodeQueueName(queueName)
}

func durableNameForQueue(queueName string) string {
	return "sqlflow-" + encodeQueueName(queueName)
}

var _ sqlflow.QueueSignalListener =
	(*QueueSignalListener)(nil)