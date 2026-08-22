package sqlflow

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"time"
)

type Handler func(ctx *TaskContext) error

type Worker struct {
	Options   WorkerOptions
	DB        Driver
	Registry  map[string]Handler
	semaphore chan struct{}
	cancel    context.CancelFunc
}

func NewWorker(opts WorkerOptions, db Driver, registry map[string]Handler) *Worker {
	return &Worker{
		Options:   opts,
		DB:        db,
		Registry:  registry,
		semaphore: make(chan struct{}, opts.Concurrency),
	}
}

func (w *Worker) Start(ctx context.Context) {
	workerCtx, cancel := context.WithCancel(ctx)
	w.cancel = cancel

	log.Printf("Worker %s started on queue '%s'", w.Options.WorkerID, w.Options.QueueName)
	go w.pollLoop(workerCtx)
}

func (w *Worker) Stop() {
	if w.cancel != nil {
		w.cancel()
	}
}

func (w *Worker) pollLoop(ctx context.Context) {
	ticker := time.NewTicker(w.Options.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			w.semaphore <- struct{}{}

			tasks, err := w.DB.ClaimTask(ctx, w.Options.QueueName, w.Options.WorkerID, 300, 1)

			if err != nil {
				log.Printf("[ERROR] Error while claiming task: %v", err)
				<-w.semaphore
				continue
			}

			if len(tasks) == 0 {
				<-w.semaphore
				continue
			}

			go w.processTask(ctx, tasks[0])
		}
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