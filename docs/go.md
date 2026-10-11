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
	log.Printf("[Workflow] Agent starts investigation for ticket %s (Tenant: %s)", task.IssueID, ctx.TenantID)

	bugReport, err := sqlflow.Step(ctx, "fetch-issue-context", func() (Issue, error) {
		return gitHubService.GetIssueDetails(task.IssueID), nil
	})
	if err != nil {
		return err
	}

	isApproved := false
	attempt := 0
	lastFeedback := "Initial Attempt"

	for !isApproved && attempt < 3 {
		attempt++
		correlationID := fmt.Sprintf("%s-attempt-%d", ctx.TaskID, attempt)
		log.Printf("[Workflow] Attempt %d/3: Generating fix based on: '%s'", attempt, lastFeedback)

		proposedFix, err := sqlflow.Step(ctx, fmt.Sprintf("generate-code-fix-%d", attempt), func() (Solution, error) {
			return llmService.GenerateFix(bugReport.StackTrace, lastFeedback), nil
		})
		if err != nil {
			return err
		}

		_, err = sqlflow.Step(ctx, fmt.Sprintf("notify-reviewer-%d", attempt), func() (bool, error) {
			gitHubService.RequestHumanReview(task.IssueID, proposedFix, correlationID)
			notificationService.NotifyReviewer(task.IssueID, correlationID)
			return true, nil
		})

		if err != nil {
			return err
		}

		log.Printf("[Workflow] Review requested for %s. Agent goes to sleep and waits...", correlationID)

		approval, err := sqlflow.AwaitEvent[HumanApproval](
			ctx,
			fmt.Sprintf("agent-approval:%s:%s:%s", ctx.TenantID, task.IssueID, correlationID),
			fmt.Sprintf("wait-for-human-review-%d", attempt),
			nil,
		)
		if err != nil {
			return err
		}

		isApproved = approval.Approved
		if approval.Reason != "" {
			lastFeedback = approval.Reason
		} else {
			lastFeedback = "No feedback has been given"
		}

		if !isApproved {
			log.Printf("[Workflow] ❌ Attempt %d was rejected: %s", attempt, lastFeedback)
		}
	}

	if isApproved {
		log.Println("[Workflow] ✅ Fix approved! Creating pull request...")

		prURL, err := sqlflow.Step(ctx, "create-pull-request", func() (string, error) {
			return gitHubService.CreatePullRequest(task.IssueID, "apply-fix"), nil
		})
		if err != nil {
			return err
		}

		log.Printf("[Workflow] Mission accomplished! PR created: %s", prURL)
		return nil
	}

	log.Printf("[Workflow] 🚨 Maximum attempts reached. Escalating ticket %s to a human.", task.IssueID)
	_, err = sqlflow.Step(ctx, "notify-senior-developer", func() (bool, error) {
		gitHubService.EscalateToSenior(task.IssueID, "Agent could not find a solution after 3 attempts.")
		return true, nil
	})

	return err
}
```

### 3. Application Setup & API (`net/http`)

We wire up the standard Go `net/http` router, connect the PostgreSQL driver, register the workflow, and start the background worker pool.

```go
var sqlflowClient *sqlflow.Client

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	connString := "postgres://postgres:password@localhost:5432/sqlflow_db"
	dbDriver, err := postgres.NewPostgresDriver(ctx, connString)
	if err != nil {
		log.Fatalf("Error connecting to the database: %v", err)
	}
	defer dbDriver.Close()

	sqlflowClient = sqlflow.NewClient(dbDriver)
	_ = sqlflowClient.CreateQueue(ctx, "tenant-alpha", "ai-agent-queue", "unpartitioned")

	sqlflow.RegisterWorkflow(sqlflowClient, "solve-bug", autonomousAgentWorkflow)

	worker, err := sqlflowClient.CreateWorker(
		ctx,
		sqlflow.WorkerOptions{
			TenantID:    "tenant-alpha",
			QueueName:   "ai-agent-queue",
			WorkerID:    "worker-1",
			Concurrency: 5,
			BatchSize:   5,
		},
	)

	if err != nil {
		panic(err)
	}

	worker.Start(ctx)

	log.Println("Background worker started.")

	mux := http.NewServeMux()

	mux.HandleFunc("POST /agent/{tenant_id}/start", func(w http.ResponseWriter, r *http.Request) {
		tenantID := r.PathValue("tenant_id")

		var task AgentTask
		if err := json.NewDecoder(r.Body).Decode(&task); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		options := sqlflow.SpawnOptions{
			TenantID:  tenantID,
			QueueName: "ai-agent-queue",
		}
		res, err := sqlflowClient.Spawn(r.Context(), options, "solve-bug", task)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"run_id":  res.RunID,
			"task_id": res.TaskID,
			"status":  fmt.Sprintf("Agent dispatched to fix Issue #%s for tenant %s", task.IssueID, tenantID),
		})
	})

	mux.HandleFunc("POST /agent/{tenant_id}/review/{issue_id}/{correlation_id}", func(w http.ResponseWriter, r *http.Request) {
		tenantID := r.PathValue("tenant_id")
		issueID := r.PathValue("issue_id")
		correlationID := r.PathValue("correlation_id")

		var approval HumanApproval
		if err := json.NewDecoder(r.Body).Decode(&approval); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		eventName := fmt.Sprintf("agent-approval:%s:%s:%s", tenantID, issueID, correlationID)
		options := sqlflow.EmitEventOptions{
			TenantID:  tenantID,
			QueueName: "ai-agent-queue",
		}

		err := sqlflowClient.EmitEvent(r.Context(), options, eventName, approval)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		msg := fmt.Sprintf("Fix for %s rejected. Agent will try again using feedback: '%s'", correlationID, approval.Reason)
		if approval.Approved {
			msg = fmt.Sprintf("Fix for %s approved. Agent is completing the work.", correlationID)
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"message": msg})
	})

	server := &http.Server{
		Addr:    ":8000",
		Handler: mux,
	}

	go func() {
		log.Println("Web API is running at http://localhost:8000")
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("HTTP Server Fehler: %v", err)
		}
	}()

	<-ctx.Done()
	log.Println("Shutting down system...")

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()

	server.Shutdown(shutdownCtx)
	worker.Stop()
	time.Sleep(500 * time.Millisecond)

	log.Println("System shut down successfully.")
}
```