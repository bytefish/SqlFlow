package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/bytefish/SqlFlow/sdks/go"
	"github.com/bytefish/SqlFlow/sdks/go/postgres"
)

type MyWorkflowParams struct {
	Message string `json:"message"`
}

func myFirstWorkflow(ctx *sqlflow.TaskContext, params MyWorkflowParams) error {
	log.Printf("Workflow started. Task ID: %s, Parameters: %+v\n", ctx.TaskID, params)

	result1, err := sqlflow.Step(ctx, "fetch-data", func() (map[string]string, error) {
		log.Println("  -> Executing 'fetch-data' (this only runs once)...")
		time.Sleep(2 * time.Second) // Simulate work
		return map[string]string{"status": "Data loaded"}, nil
	})
	if err != nil {
		return err
	}

	log.Printf("Result of fetch-data: %v\n", result1)

	_, err = sqlflow.Step(ctx, "process-data", func() (string, error) {
		log.Println("  -> Executing 'process-data'...")
		return "Processing completed", nil
	})

	if err != nil {
		return err
	}

	log.Println("Workflow completed successfully!")
	return nil
}

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)

	defer cancel()

	connString := "postgres://postgres:password@localhost:5432/sqlflow_db"
    
	driver, err := postgres.NewPostgresDriver(ctx, connString)
    
	if err != nil {
		log.Fatalf("Error connecting to database: %v", err)
	}
	defer driver.Close()

	client := sqlflow.NewClient(driver)

	_ = driver.CreateQueue(ctx, "default-queue", "unpartitioned")

	sqlflow.RegisterWorkflow(client, "my-first-task", myFirstWorkflow)

	workerOpts := sqlflow.WorkerOptions{
		WorkerID:     "worker-go-1",
		QueueName:    "default-queue",
		PollInterval: 1 * time.Second,
		Concurrency:  5,
	}

	worker := client.CreateWorker(workerOpts)
	worker.Start(ctx)
	log.Println("Worker is running and listening on queue 'default-queue'. Press Ctrl+C to exit.")

	go func() {
		time.Sleep(1 * time.Second)

		log.Println("Spawning a new test task...")

		attempts := 3
        
		options := sqlflow.SpawnOptions{
			QueueName:   "default-queue",
			MaxAttempts: &attempts,
		}

		params := map[string]any{
			"message": "Hello from Go!",
		}

		res, err := client.Spawn(ctx, options, "my-first-task", params)

		if err != nil {
			log.Printf("Error spawning task: %v\n", err)
		} else {
			log.Printf("Task spawned! Run ID: %s\n", res.RunID)
		}
	}()

	<-ctx.Done()

	log.Println("Shutting down system gracefully...")
	worker.Stop()

	time.Sleep(500 * time.Millisecond)
	log.Println("Goodbye!")
}