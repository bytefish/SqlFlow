package sqlflow

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"time"
    "golang.org/x/time/rate"
)

type Handler func(ctx *TaskContext) error

type Worker struct {
	Options  WorkerOptions
	DB       Driver
	Registry map[string]Handler
	semaphore chan struct{}
	cancel    context.CancelFunc
	rateLimiter *rate.Limiter
	signals QueueSignalListener
}

type QueueSignalListener interface {
	RegisterQueue(
		ctx context.Context,
		queueName string,
	) error

	WaitForSignal(
		ctx context.Context,
		queueName string,
		timeout time.Duration,
	) (bool, error)
}

func NewWorker(
	opts WorkerOptions,
	db Driver,
	registry map[string]Handler,
	signals QueueSignalListener,
) *Worker {

	var limiter *rate.Limiter

	if opts.MaxTasksPerSecond > 0 {

		burst := opts.RateLimitBurstSize

		if burst <= 0 {
			burst = opts.MaxTasksPerSecond
		}

		limiter = rate.NewLimiter(
			rate.Limit(opts.MaxTasksPerSecond),
			burst,
		)
	}

	return &Worker{
		Options:     opts,
		DB:          db,
		Registry:    registry,
		signals:     signals,
		rateLimiter: limiter,
		semaphore: make(
			chan struct{},
			opts.Concurrency,
		),
	}
}

func (w *Worker) Start(ctx context.Context) {
	workerCtx, cancel := context.WithCancel(ctx)

	w.cancel = cancel

	err := w.signals.RegisterQueue(workerCtx, w.Options.QueueName)

	if err != nil {
		panic(err)
	}

	log.Printf(
		"Worker %s started on queue '%s'",
		w.Options.WorkerID,
		w.Options.QueueName,
	)

	go w.pollLoop(workerCtx)
}

func (w *Worker) Stop() {
	if w.cancel != nil {
		w.cancel()
	}
}

func (w *Worker) pollLoop(
	ctx context.Context,
) {
	queueMayContainWork := true

	reconciliationInterval :=
		time.Minute

	for {

		select {

		case <-ctx.Done():
			return

		default:
		}

		if !queueMayContainWork {

			_, err := w.signals.WaitForSignal(ctx, w.Options.QueueName, reconciliationInterval)

			if err != nil {

				if ctx.Err() != nil {
					return
				}

				log.Printf("[ERROR] Signal wait failed: %v", err)

				time.Sleep(time.Second)

				continue
			}

			queueMayContainWork = true
		}

		availableCapacity := cap(w.semaphore) - len(w.semaphore)

		if availableCapacity <= 0 {

			time.Sleep(
				10 * time.Millisecond,
			)

			continue
		}

		claimQty := availableCapacity

		if w.rateLimiter != nil {

			err := w.rateLimiter.WaitN(
				ctx,
				claimQty,
			)

			if err != nil {

				if ctx.Err() != nil {
					return
				}

				continue
			}
		}

		tasks, err :=
			w.DB.ClaimTask(
				ctx,
				w.Options.QueueName,
				w.Options.WorkerID,
				300,
				claimQty,
			)

		if err != nil {

			log.Printf(
				"[ERROR] Error claiming tasks: %v",
				err,
			)

			queueMayContainWork = false

			time.Sleep(time.Second)

			continue
		}

		if len(tasks) == 0 {

			queueMayContainWork = false

			continue
		}

		for _, task := range tasks {

			w.semaphore <- struct{}{}

			taskCopy := task

			go func() {

				defer func() {
					<- w.semaphore
				}()

				w.processTask(
					ctx,
					taskCopy,
				)

			}()
		}

		queueMayContainWork =
			len(tasks) ==
				claimQty
	}
}

func (w *Worker) processTask(ctx context.Context, task ClaimedTask) {
	defer func() { <-w.semaphore }() // Always release the semaphore slot at the end

	handler, exists := w.Registry[task.TaskName]
	if !exists {
		log.Printf("Task %s is not registered", task.TaskName)

		w.DB.FailRun(ctx, w.Options.QueueName, task.RunID, `{"error":"unregistered"}`, time.Now().Add(5*time.Minute))

		return
	}

	taskCtx := &TaskContext{
		TaskID:    task.TaskID,
		RunID:     task.RunID,
		Attempt:   task.Attempt,
		QueueName: w.Options.QueueName,
		DB:        w.DB,
		Ctx:       ctx,
		RawParams: task.Params,
	}

	err := handler(taskCtx)

	if errors.Is(err, ErrSuspendTask) {
		log.Printf("Task %s suspended", task.TaskID)
		return
	} else if err != nil {
		log.Printf("Task %s failed: %v", task.TaskID, err)
		retryAt := time.Now().Add(time.Duration(taskCtx.Attempt*taskCtx.Attempt) * time.Minute)
		errorDetails, _ := json.Marshal(map[string]string{"error": err.Error()})
		w.DB.FailRun(ctx, w.Options.QueueName, task.RunID, string(errorDetails), retryAt)
		return
	}

	w.DB.CompleteRun(ctx, w.Options.QueueName, task.RunID, "succeeded")
}