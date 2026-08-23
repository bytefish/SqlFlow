package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"local/sqlflow"
	"local/sqlflow/postgres"
)

// Models

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
	Reason         string `json:"reason,omitempty"`
}

// Services

type LlmService struct{}

func (s *LlmService) GenerateFix(stackTrace string, lastFeedback string) Solution {
	log.Printf("[LlmService] Agent is thinking: 'Learned from feedback: %s'", lastFeedback)
	time.Sleep(2500 * time.Millisecond)

	var code string
	if strings.Contains(strings.ToLower(lastFeedback), "error handling") {
		code = "// AI: Improved Logging & Error Handling added\nif(data == null) raise ValueError('Null data');"
	} else {
		code = "// AI: Simple Fix for the NullReferenceException\nif(data is None): return"
	}

	log.Printf("[LlmService] LLM generated a potential fix:\n%s", code)
	return Solution{PatchedCode: code}
}

type GitHubService struct{}

func (s *GitHubService) GetIssueDetails(issueID string) Issue {
	log.Printf("[GitHubService] Fetching details for ticket #%s from the repository...", issueID)
	time.Sleep(800 * time.Millisecond)
	return Issue{StackTrace: "NullReferenceException at PaymentGateway.cs:42"}
}

func (s *GitHubService) CreatePullRequest(issueID string, code string) string {
	log.Printf("[GitHubService] PR for issue #%s has been created...", issueID)
	time.Sleep(1200 * time.Millisecond)
	return fmt.Sprintf("https://github.com/company/repo/pull/%d", rand.Intn(9000)+1000)
}

func (s *GitHubService) EscalateToSenior(issueID string, reason string) {
	log.Printf("[GitHubService] 🚨 ESCALATION to Senior Developer: Issue #%s - Reason: %s", issueID, reason)
	time.Sleep(500 * time.Millisecond)
}

func (s *GitHubService) RequestHumanReview(issueID string, proposedFix Solution, correlationID string) {
	log.Printf("[GitHubService] ⏳ ACTION REQUIRED: A solution for issue #%s (Correlation ID %s) is available:\n%s",
		issueID, correlationID, proposedFix.PatchedCode)
	time.Sleep(1200 * time.Millisecond)
}

type LocalNotificationService struct{}

func (s *LocalNotificationService) NotifyReviewer(issueID string, correlationID string) {
	log.Printf("[LocalNotification] Ping! Please perform code review %s for issue %s.", correlationID, issueID)
}

// Workflow

var llmService = &LlmService{}
var gitHubService = &GitHubService{}
var notificationService = &LocalNotificationService{}

func autonomousAgentWorkflow(ctx *sqlflow.TaskContext, task AgentTask) error {
	log.Printf("[Workflow] Agent starts investigation for ticket %s", task.IssueID)

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
			fmt.Sprintf("agent-approval:%s:%s", task.IssueID, correlationID),
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
	_ = sqlflowClient.CreateQueue(ctx, "ai-agent-queue", "unpartitioned")

	sqlflow.RegisterWorkflow(sqlflowClient, "solve-bug", autonomousAgentWorkflow)

	workerOpts := sqlflow.WorkerOptions{
		WorkerID:     "agent-worker-1",
		QueueName:    "ai-agent-queue",
		PollInterval: 1 * time.Second,
		Concurrency:  5,
	}
	
	worker := sqlflowClient.CreateWorker(workerOpts)
	worker.Start(ctx)
	
	log.Println("Background worker started.")

	mux := http.NewServeMux()

	mux.HandleFunc("POST /agent/start", func(w http.ResponseWriter, r *http.Request) {
		var task AgentTask
		if err := json.NewDecoder(r.Body).Decode(&task); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		options := sqlflow.SpawnOptions{QueueName: "ai-agent-queue"}
		res, err := sqlflowClient.Spawn(r.Context(), options, "solve-bug", task)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"run_id":  res.RunID,
			"task_id": res.TaskID,
			"status":  fmt.Sprintf("Agent dispatched to fix Issue #%s", task.IssueID),
		})
	})

	mux.HandleFunc("POST /agent/review/{issue_id}/{correlation_id}", func(w http.ResponseWriter, r *http.Request) {
		issueID := r.PathValue("issue_id")
		correlationID := r.PathValue("correlation_id")

		var approval HumanApproval
		if err := json.NewDecoder(r.Body).Decode(&approval); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		eventName := fmt.Sprintf("agent-approval:%s:%s", issueID, correlationID)
		options := sqlflow.EmitEventOptions{QueueName: "ai-agent-queue"}
		
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