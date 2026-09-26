# Getting Started with the Go SDK

Install the SqlFlow Go SDK using `go get`:

```bash
go get github.com/bytefish/SqlFlow/sdks/go
```

For PostgreSQL support, also import the postgres subpackage in your application:

```go
import (
	"github.com/bytefish/SqlFlow/sdks/go"
	"github.com/bytefish/SqlFlow/sdks/go/postgres"
)
```

## Building a Durable AI Agent

The classic examples for durable execution are usually e-commerce checkouts. But there's a rapidly growing use case developers are dealing with: Autonomous AI Agents. Building AI agents introduces severe state-management challenges:

1. **Slow API Calls:** LLM API calls are inherently slow, prone to timeouts, and expensive. If a server crashes while waiting 30 seconds for an AI generation, standard in-memory goroutine state is lost forever.
2. **Human-in-the-Loop:** You don't want an AI pushing code to production without human review. Agents need to pause execution, ask a human for permission, and resume only when approved (hours or days later).

Traditional approaches require you to build complex state machines, database polling loops, or heavy external infrastructure. With SqlFlow, we can write our agent as a standard, sequential Go function. The framework will automatically checkpoint the state to Postgres, sleep without blocking worker threads, and wake up exactly where it left off.

### 1. The Domain Models

We define our inputs and outputs as standard Go structs with JSON tags.

```go
type AgentTask struct {
	IssueID string `json:"issue_id"`
}

type Issue struct {
	StackTrace string `json:"stack_trace"`
}

type Solution struct {
	PatchedCode string `json:"patched_code"`
}

type HumanApproval struct {
	Approved bool   `json:"approved"`
	Reason   string `json:"reason,omitempty"`
}

type AgentResult struct {
	Success        bool   `json:"success"`
	PullRequestURL string `json:"pull_request_url,omitempty"`
}
```

### 2. The Autonomous Agent Workflow (The Core Concept)

The workflow is a normal Go function that takes a `*sqlflow.TaskContext`. 

The magic is in the **`sqlflow.Step`** function: every time a step completes, its result is automatically checkpointed to the Postgres database. If the process crashes or is restarted, the framework replays the job, skips the already completed steps, and loads their results directly from the database.

Instead of blocking a goroutine with an infinite loop or `time.Sleep`, we use **`sqlflow.AwaitEvent[T]`** to wait for human interaction. This instructs the engine to safely suspend the workflow state to the database and free up the worker completely until an external system fires the webhook event.

```go
var llmService = &LlmService{}       // Simulated external service
var gitHubService = &GitHubService{} // Simulated external service

func autonomousAgentWorkflow(ctx *sqlflow.TaskContext, task AgentTask) error {
	// 1. Fetch data safely (results are checkpointed)
	bugReport, err := sqlflow.Step(ctx, "fetch-issue-context", func() (Issue, error) {
		return gitHubService.GetIssueDetails(task.IssueID), nil
	})
	if err != nil { return err }

	isApproved := false
	attempt := 0
	lastFeedback := "Initial Attempt"

	for !isApproved && attempt < 3 {
		attempt++
		correlationID := fmt.Sprintf("%s-attempt-%d", ctx.TaskID, attempt)

		// 2. Call the slow, expensive LLM safely
		proposedFix, err := sqlflow.Step(ctx, fmt.Sprintf("generate-code-fix-%d", attempt), func() (Solution, error) {
			return llmService.GenerateFix(bugReport.StackTrace, lastFeedback), nil
		})
		if err != nil { return err }

		_, err = sqlflow.Step(ctx, fmt.Sprintf("notify-reviewer-%d", attempt), func() (bool, error) {
			gitHubService.RequestHumanReview(task.IssueID, proposedFix, correlationID)
			return true, nil
		})
		if err != nil { return err }

		// 3. Wait for a human decision without blocking a thread! State is saved to DB here.
		approval, err := sqlflow.AwaitEvent[HumanApproval](
			ctx,
			fmt.Sprintf("agent-approval:%s:%s", task.IssueID, correlationID),
			fmt.Sprintf("wait-for-human-review-%d", attempt),
			nil,
		)
		if err != nil { return err }

		isApproved = approval.Approved
		if approval.Reason != "" {
			lastFeedback = approval.Reason
		}
	}

	if isApproved {
		prURL, err := sqlflow.Step(ctx, "create-pull-request", func() (string, error) {
			return gitHubService.CreatePullRequest(task.IssueID, "apply-fix"), nil
		})
		if err != nil { return err }
		
		return nil // Success
	}

	return fmt.Errorf("agent failed to find a solution after 3 attempts")
}
```

### 3. Application Setup & API (`net/http`)

We wire up the standard Go `net/http` router, connect the PostgreSQL driver, register the workflow, and start the background worker pool.

```go
func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	connString := "postgres://postgres:password@localhost:5432/sqlflow_db"
	dbDriver, _ := postgres.NewPostgresDriver(ctx, connString)
	defer dbDriver.Close()

	sqlflowClient := sqlflow.NewClient(dbDriver)
	_ = sqlflowClient.CreateQueue(ctx, "ai-agent-queue", "unpartitioned")

	// Map the workflow function to the string identifier
	sqlflow.RegisterWorkflow(sqlflowClient, "solve-bug", autonomousAgentWorkflow)

	// Start the background worker
	worker := sqlflowClient.CreateWorker(sqlflow.WorkerOptions{
		WorkerID:     "agent-worker-1",
		QueueName:    "ai-agent-queue",
		Concurrency:  5,
	})
	worker.Start(ctx)

	mux := http.NewServeMux()

	// Endpoint 1: Spawn the Agent Task
	mux.HandleFunc("POST /agent/start", func(w http.ResponseWriter, r *http.Request) {
		var task AgentTask
		json.NewDecoder(r.Body).Decode(&task)

		res, _ := sqlflowClient.Spawn(r.Context(), sqlflow.SpawnOptions{QueueName: "ai-agent-queue"}, "solve-bug", task)
		
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{"run_id": res.RunID, "task_id": res.TaskID})
	})

	// Endpoint 2: Resume the Agent upon Human Review
	mux.HandleFunc("POST /agent/review/{issue_id}/{correlation_id}", func(w http.ResponseWriter, r *http.Request) {
		issueID := r.PathValue("issue_id")
		correlationID := r.PathValue("correlation_id")

		var approval HumanApproval
		json.NewDecoder(r.Body).Decode(&approval)

		eventName := fmt.Sprintf("agent-approval:%s:%s", issueID, correlationID)
		sqlflowClient.EmitEvent(r.Context(), sqlflow.EmitEventOptions{QueueName: "ai-agent-queue"}, eventName, approval)
		
		w.WriteHeader(http.StatusOK)
	})

	server := &http.Server{Addr: ":8000", Handler: mux}
	go server.ListenAndServe()

	<-ctx.Done() // Block until shutdown signal
	worker.Stop()
}
```