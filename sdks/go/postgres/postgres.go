package postgres

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/bytefish/SqlFlow/sdks/go"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type PostgresDriver struct {
	pool       *pgxpool.Pool
	connString string
	listener   sqlflow.QueueSignalListener
	mu         sync.Mutex
}

func NewPostgresDriver(ctx context.Context, connString string) (*PostgresDriver, error) {
	pool, err := pgxpool.New(ctx, connString)
	if err != nil {
		return nil, err
	}
	return &PostgresDriver{
		pool:       pool,
		connString: connString,
	}, nil
}

func (p *PostgresDriver) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()

	if listener, ok := p.listener.(*PostgresQueueSignalListener); ok {
		listener.Close()
	}

	p.pool.Close()
}

func (p *PostgresDriver) CreateQueue(ctx context.Context, queueName string, storageMode string) error {
	_, err := p.pool.Exec(ctx, "CALL ssf.create_queue($1, $2)", queueName, storageMode)
	return err
}

func (p *PostgresDriver) SpawnTask(ctx context.Context, queueName string, taskName string, params []byte, options []byte) (*sqlflow.SpawnResult, error) {
	var res sqlflow.SpawnResult
	
	err := p.pool.QueryRow(ctx, 
		"SELECT task_id, run_id, attempt, created FROM ssf.spawn_task($1, $2, $3, $4)", 
		queueName, taskName, string(params), string(options),
	).Scan(&res.TaskID, &res.RunID, &res.Attempt, &res.Created)
	
	if err != nil {
		return nil, err
	}
	return &res, nil
}

func (p *PostgresDriver) CreateQueueSignalListener(
	ctx context.Context,
) (sqlflow.QueueSignalListener, error) {

	p.mu.Lock()
	defer p.mu.Unlock()

	if p.listener != nil {
		return p.listener, nil
	}

	listener := NewPostgresQueueSignalListener(p.connString)

	err := listener.Start(ctx)
	if err != nil {
		return nil, err
	}

	p.listener = listener

	return listener, nil
}

func (p *PostgresDriver) ClaimTask(ctx context.Context, queueName string, workerID string, claimTimeout int, qty int) ([]sqlflow.ClaimedTask, error) {
	rows, err := p.pool.Query(ctx, 
		"SELECT * FROM ssf.claim_task($1, $2, $3, $4)", 
		queueName, workerID, claimTimeout, qty,
	)
	if err != nil {
		return nil, err
	}
	
	return pgx.CollectRows(rows, pgx.RowToStructByName[sqlflow.ClaimedTask])
}

func (p *PostgresDriver) CompleteRun(ctx context.Context, queueName string, runID string, state string) error {
	_, err := p.pool.Exec(ctx, "CALL ssf.complete_run($1, $2, $3)", queueName, runID, state)
	return err
}

func (p *PostgresDriver) ScheduleRun(ctx context.Context, queueName string, runID string, wakeAt time.Time) error {
	_, err := p.pool.Exec(ctx, "CALL ssf.schedule_run($1, $2, $3)", queueName, runID, wakeAt)
	return err
}

func (p *PostgresDriver) FailRun(ctx context.Context, queueName string, runID string, reason string, retryAt time.Time) error {
	_, err := p.pool.Exec(ctx, "CALL ssf.fail_run($1, $2, $3, $4)", queueName, runID, reason, retryAt)
	return err
}

func (p *PostgresDriver) SetTaskCheckpointState(ctx context.Context, queueName string, taskID string, stepName string, state string, ownerRun string, extendClaimBy int) error {
	_, err := p.pool.Exec(ctx, 
		"CALL ssf.set_task_checkpoint_state($1, $2, $3, $4, $5, $6)", 
		queueName, taskID, stepName, state, ownerRun, extendClaimBy,
	)
	return err
}

func (p *PostgresDriver) GetTaskCheckpointState(ctx context.Context, queueName string, taskID string, stepName string, includePending int) (*sqlflow.CheckpointState, error) {
	rows, err := p.pool.Query(ctx, 
		"SELECT * FROM ssf.get_task_checkpoint_state($1, $2, $3, $4)", 
		queueName, taskID, stepName, includePending,
	)
	if err != nil {
		return nil, err
	}
	
	res, err := pgx.CollectOneRow(rows, pgx.RowToStructByName[sqlflow.CheckpointState])
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil 
		}
		return nil, err
	}
	return &res, nil
}

func (p *PostgresDriver) AwaitEvent(ctx context.Context, queueName string, taskID string, runID string, stepName string, eventName string, timeout *int) (*sqlflow.AwaitEventResult, error) {
	rows, err := p.pool.Query(ctx, 
		"SELECT * FROM ssf.await_event($1, $2, $3, $4, $5, $6)", 
		queueName, taskID, runID, stepName, eventName, timeout,
	)
	if err != nil {
		return nil, err
	}
	
	res, err := pgx.CollectOneRow(rows, pgx.RowToStructByName[sqlflow.AwaitEventResult])
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	return &res, nil
}

func (p *PostgresDriver) EmitEvent(ctx context.Context, queueName string, eventName string, payload string) error {
	_, err := p.pool.Exec(ctx, "CALL ssf.emit_event($1, $2, $3)", queueName, eventName, payload)
	return err
}

func (p *PostgresDriver) CancelTask(ctx context.Context, queueName string, taskID string) error {
	_, err := p.pool.Exec(ctx, "CALL ssf.cancel_task($1, $2)", queueName, taskID)
	return err
}

func (p *PostgresDriver) GetNextAvailableAt(ctx context.Context, queueName string) (*time.Time, error) {
	var nextAvailableAt *time.Time
	
	err := p.pool.QueryRow(
		ctx,
		"SELECT ssf.get_next_available_at($1)",
		queueName,
	).Scan(&nextAvailableAt)
	
	if err != nil {
		return nil, err
	}
	
	return nextAvailableAt, nil
}


type PostgresQueueSignalListener struct {
	connString string

	mu sync.RWMutex

	signals map[string]chan bool
	
	cancelCtx context.CancelFunc
}

func NewPostgresQueueSignalListener(
	connString string,
) *PostgresQueueSignalListener {

	return &PostgresQueueSignalListener{
		connString: connString,
		signals:    make(map[string]chan bool),
	}
}

func (p *PostgresQueueSignalListener) RegisterQueue(
	ctx context.Context,
	queueName string,
) error {

	p.mu.Lock()
	defer p.mu.Unlock()

	if _, ok := p.signals[queueName]; ok {
		return nil
	}

	ch :=
		make(
			chan bool,
			1,
		)

	ch <- true

	p.signals[queueName] =
		ch

	return nil
}

func (p *PostgresQueueSignalListener) WaitForSignal(
	ctx context.Context,
	queueName string,
	timeout time.Duration,
) (bool, error) {

	p.mu.RLock()

	ch :=
		p.signals[queueName]

	p.mu.RUnlock()

	if ch == nil {
		select {
		case <-time.After(timeout):
			return false, nil
		case <-ctx.Done():
			return false, ctx.Err()
		}
	}

	select {

	case <-ch:
		return true, nil

	case <-time.After(timeout):
		return false, nil

	case <-ctx.Done():
		return false, ctx.Err()
	}
}

func (p *PostgresQueueSignalListener) Start(
	ctx context.Context,
) error {

	listenerCtx, cancel := context.WithCancel(context.Background())
	p.cancelCtx = cancel

	conn, err :=
		pgx.Connect(
			listenerCtx,
			p.connString,
		)

	if err != nil {
		return err
	}

	_, err =
		conn.Exec(
			listenerCtx,
			"LISTEN ssf_work_available",
		)

	if err != nil {
		conn.Close(context.Background())
		return err
	}

	go func() {

		defer conn.Close(
			context.Background(),
		)

		for {

			notification, err :=
				conn.WaitForNotification(
					listenerCtx,
				)

			if err != nil {

				if listenerCtx.Err() != nil {
					return
				}

				time.Sleep(time.Second)
				continue
			}

			queueName :=
				notification.Payload

			p.signalQueue(
				queueName,
			)
		}
	}()

	return nil
}

func (p *PostgresQueueSignalListener) Close() {
	if p.cancelCtx != nil {
		p.cancelCtx()
	}
}

func (p *PostgresQueueSignalListener) signalQueue(queueName string) {

	p.mu.RLock()

	ch, exists :=
		p.signals[queueName]

	p.mu.RUnlock()

	if !exists {
		return
	}

	select {

	case ch <- true:

	default:
	}
}

var _ sqlflow.QueueSignalListener = (*PostgresQueueSignalListener)(nil)