package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	sqlflow "github.com/bytefish/SqlFlow/sdks/go"
	natsdriver "github.com/bytefish/SqlFlow/sdks/go/drivers/nats"
	"github.com/bytefish/SqlFlow/sdks/go/drivers/postgres"
	natsgo "github.com/nats-io/nats.go"
)

const (
	queueName       = "ai-agent-queue"
	httpAddress     = ":8000"
	defaultPostgres = "postgres://postgres:password@127.0.0.1:5432/sqlflow_db"
	defaultNATS     = "nats://127.0.0.1:4222"
	shutdownTimeout = 5 * time.Second
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

type StartAgentResponse struct {
	RunID  string `json:"run_id"`
	TaskID string `json:"task_id"`
	Status string `json:"status"`
}

type MessageResponse struct {
	Message string `json:"message"`
}

// Services

type LlmService struct{}

func (s *LlmService) GenerateFix(
	stackTrace string,
	lastFeedback string,
) Solution {
	log.Printf(
		"[LlmService] Agent is thinking: learned from feedback %q",
		lastFeedback,
	)

	time.Sleep(2500 * time.Millisecond)

	var code string
	if strings.Contains(
		strings.ToLower(lastFeedback),
		"error handling",
	) {
		code = "// AI: Improved logging and error handling added\n" +
			"if data == nil { return errors.New(\"data must not be nil\") }"
	} else {
		code = "// AI: Simple fix for the NullReferenceException\n" +
			"if data == nil { return nil }"
	}

	log.Printf(
		"[LlmService] LLM generated a potential fix:\n%s",
		code,
	)

	return Solution{PatchedCode: code}
}

type GitHubService struct{}

func (s *GitHubService) GetIssueDetails(issueID string) Issue {
	log.Printf(
		"[GitHubService] Fetching details for ticket #%s from the repository...",
		issueID,
	)

	time.Sleep(800 * time.Millisecond)

	return Issue{
		StackTrace: "NullReferenceException at PaymentGateway.cs:42",
	}
}

func (s *GitHubService) CreatePullRequest(
	issueID string,
	code string,
) string {
	log.Printf(
		"[GitHubService] Creating PR for issue #%s with code:\n%s",
		issueID,
		code,
	)

	time.Sleep(1200 * time.Millisecond)

	return fmt.Sprintf(
		"https://github.com/company/repo/pull/%d",
		rand.Intn(9000)+1000,
	)
}

func (s *GitHubService) EscalateToSenior(
	issueID string,
	reason string,
) {
	log.Printf(
		"[GitHubService] ESCALATION to Senior Developer: Issue #%s - Reason: %s",
		issueID,
		reason,
	)

	time.Sleep(500 * time.Millisecond)
}

func (s *GitHubService) RequestHumanReview(
	issueID string,
	proposedFix Solution,
	correlationID string,
) {
	log.Printf(
		"[GitHubService] ACTION REQUIRED: Solution for issue #%s (Correlation ID %s):\n%s",
		issueID,
		correlationID,
		proposedFix.PatchedCode,
	)

	time.Sleep(1200 * time.Millisecond)
}

type LocalNotificationService struct{}

func (s *LocalNotificationService) NotifyReviewer(
	issueID string,
	correlationID string,
) {
	log.Printf(
		"[LocalNotification] Please review %s for issue %s.",
		correlationID,
		issueID,
	)
}

// Workflow

var (
	llmService          = &LlmService{}
	gitHubService       = &GitHubService{}
	notificationService = &LocalNotificationService{}
)

func autonomousAgentWorkflow(
	ctx *sqlflow.TaskContext,
	task AgentTask,
) error {
	log.Printf(
		"[Workflow] Agent starts investigation for ticket %s (Tenant: %s)",
		task.IssueID,
		ctx.TenantID,
	)

	bugReport, err := sqlflow.Step(
		ctx,
		"fetch-issue-context",
		func() (Issue, error) {
			return gitHubService.GetIssueDetails(task.IssueID), nil
		},
	)
	if err != nil {
		return err
	}

	isApproved := false
	attempt := 0
	lastFeedback := "Initial Attempt"

	for !isApproved && attempt < 3 {
		attempt++

		correlationID := fmt.Sprintf(
			"%s-attempt-%d",
			ctx.TaskID,
			attempt,
		)

		log.Printf(
			"[Workflow] Attempt %d/3: generating fix based on %q",
			attempt,
			lastFeedback,
		)

		proposedFix, stepErr := sqlflow.Step(
			ctx,
			fmt.Sprintf("generate-code-fix-%d", attempt),
			func() (Solution, error) {
				return llmService.GenerateFix(
					bugReport.StackTrace,
					lastFeedback,
				), nil
			},
		)
		if stepErr != nil {
			return stepErr
		}

		_, stepErr = sqlflow.Step(
			ctx,
			fmt.Sprintf("notify-reviewer-%d", attempt),
			func() (bool, error) {
				gitHubService.RequestHumanReview(
					task.IssueID,
					proposedFix,
					correlationID,
				)

				notificationService.NotifyReviewer(
					task.IssueID,
					correlationID,
				)

				return true, nil
			},
		)
		if stepErr != nil {
			return stepErr
		}

		log.Printf(
			"[Workflow] Review requested for %s. Agent waits for approval...",
			correlationID,
		)

		approval, awaitErr := sqlflow.AwaitEvent[HumanApproval](
			ctx,
			fmt.Sprintf(
				"agent-approval:%s:%s:%s",
				ctx.TenantID,
				task.IssueID,
				correlationID,
			),
			fmt.Sprintf("wait-for-human-review-%d", attempt),
			nil,
		)
		if awaitErr != nil {
			return awaitErr
		}

		isApproved = approval.Approved

		if approval.Reason != "" {
			lastFeedback = approval.Reason
		} else {
			lastFeedback = "No feedback has been given"
		}

		if !isApproved {
			log.Printf(
				"[Workflow] Attempt %d was rejected: %s",
				attempt,
				lastFeedback,
			)
		}
	}

	if isApproved {
		log.Println("[Workflow] Fix approved. Creating pull request...")

		prURL, stepErr := sqlflow.Step(
			ctx,
			"create-pull-request",
			func() (string, error) {
				return gitHubService.CreatePullRequest(
					task.IssueID,
					"apply-fix",
				), nil
			},
		)
		if stepErr != nil {
			return stepErr
		}

		result := AgentResult{
			Success:        true,
			PullRequestURL: prURL,
		}

		log.Printf(
			"[Workflow] Mission accomplished: %+v",
			result,
		)

		return nil
	}

	log.Printf(
		"[Workflow] Maximum attempts reached. Escalating ticket %s.",
		task.IssueID,
	)

	_, err = sqlflow.Step(
		ctx,
		"notify-senior-developer",
		func() (bool, error) {
			gitHubService.EscalateToSenior(
				task.IssueID,
				"Agent could not find a solution after 3 attempts.",
			)

			return true, nil
		},
	)

	return err
}

func main() {
	ctx, cancel := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer cancel()

	postgresURL := envOrDefault(
		"SQLFLOW_POSTGRES_URL",
		defaultPostgres,
	)

	natsURL := envOrDefault(
		"SQLFLOW_NATS_URL",
		defaultNATS,
	)

	postgresDriver, err := postgres.NewPostgresDriver(
		ctx,
		postgresURL,
	)
	if err != nil {
		log.Fatalf("connect to PostgreSQL: %v", err)
	}
	defer postgresDriver.Close()

	natsConnection, err := natsgo.Connect(
		natsURL,
		natsgo.Name("sqlflow-go-ai-agent"),
		natsgo.RetryOnFailedConnect(true),
		natsgo.MaxReconnects(-1),
		natsgo.ReconnectWait(time.Second),
	)
	if err != nil {
		log.Fatalf("connect to NATS: %v", err)
	}
	defer natsConnection.Close()

	decoratedDriver, err := natsdriver.NewDriverDecorator(
		ctx,
		postgresDriver,
		natsConnection,
		natsdriver.Options{},
	)
	if err != nil {
		log.Fatalf("initialize NATS signaling: %v", err)
	}

	sqlflowClient := sqlflow.NewClient(decoratedDriver)

	// In diesem Beispiel registrieren wir eine Queue für einen bestimmten Mandanten
	// In echten Anwendungen würde dies vermutlich dynamisch oder außerhalb stattfinden.
	if err := sqlflowClient.CreateQueue(
		ctx,
		"tenant-alpha",
		queueName,
		"unpartitioned",
	); err != nil {
		log.Fatalf("create queue %q: %v", queueName, err)
	}

	sqlflow.RegisterWorkflow(
		sqlflowClient,
		"solve-bug",
		autonomousAgentWorkflow,
	)

	// Worker für diesen spezifischen Tenant starten
	worker, err := sqlflowClient.CreateWorker(
		ctx,
		sqlflow.WorkerOptions{
			TenantID:    "tenant-alpha",
			QueueName:   queueName,
			WorkerID:    "agent-worker-1",
			Concurrency: 5,
			BatchSize:   5,
		},
	)
	if err != nil {
		log.Fatalf("create worker: %v", err)
	}

	worker.Start(ctx)
	log.Println("Background worker started with NATS JetStream signaling.")

	server := &http.Server{
		Addr:              httpAddress,
		Handler:           createHTTPHandler(sqlflowClient),
		ReadHeaderTimeout: 5 * time.Second,
	}

	serverErrors := make(chan error, 1)

	go func() {
		log.Printf("Web API is running at http://localhost%s", httpAddress)

		err := server.ListenAndServe()
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErrors <- err
		}
	}()

	select {
	case <-ctx.Done():
		log.Println("Shutdown signal received.")

	case serverErr := <-serverErrors:
		log.Printf("HTTP server failed: %v", serverErr)
		cancel()
	}

	shutdownCtx, shutdownCancel := context.WithTimeout(
		context.Background(),
		shutdownTimeout,
	)
	defer shutdownCancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Printf("HTTP server shutdown failed: %v", err)
	}

	worker.Stop()

	if err := natsConnection.Drain(); err != nil {
		log.Printf("NATS connection drain failed: %v", err)
	}

	log.Println("System shut down successfully.")
}

func createHTTPHandler(
	client *sqlflow.Client,
) http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc(
		"POST /agent/{tenant_id}/start",
		func(w http.ResponseWriter, r *http.Request) {
			tenantID := r.PathValue("tenant_id")

			var task AgentTask
			if err := decodeJSON(r, &task); err != nil {
				writeError(w, http.StatusBadRequest, err)
				return
			}

			if strings.TrimSpace(task.IssueID) == "" {
				writeError(
					w,
					http.StatusBadRequest,
					errors.New("issue_id must not be empty"),
				)
				return
			}

			result, err := client.Spawn(
				r.Context(),
				sqlflow.SpawnOptions{
					TenantID:  tenantID,
					QueueName: queueName,
				},
				"solve-bug",
				task,
			)
			if err != nil {
				writeError(w, http.StatusInternalServerError, err)
				return
			}

			writeJSON(
				w,
				http.StatusOK,
				StartAgentResponse{
					RunID:  result.RunID,
					TaskID: result.TaskID,
					Status: fmt.Sprintf(
						"Agent dispatched to fix Issue #%s for tenant %s",
						task.IssueID,
						tenantID,
					),
				},
			)
		},
	)

	mux.HandleFunc(
		"POST /agent/{tenant_id}/review/{issue_id}/{correlation_id}",
		func(w http.ResponseWriter, r *http.Request) {
			tenantID := r.PathValue("tenant_id")
			issueID := r.PathValue("issue_id")
			correlationID := r.PathValue("correlation_id")

			var approval HumanApproval
			if err := decodeJSON(r, &approval); err != nil {
				writeError(w, http.StatusBadRequest, err)
				return
			}

			eventName := fmt.Sprintf(
				"agent-approval:%s:%s:%s",
				tenantID,
				issueID,
				correlationID,
			)

			err := client.EmitEvent(
				r.Context(),
				sqlflow.EmitEventOptions{
					TenantID:  tenantID,
					QueueName: queueName,
				},
				eventName,
				approval,
			)
			if err != nil {
				writeError(w, http.StatusInternalServerError, err)
				return
			}

			message := fmt.Sprintf(
				"Fix for %s rejected. Agent will retry using feedback: %q",
				correlationID,
				approval.Reason,
			)

			if approval.Approved {
				message = fmt.Sprintf(
					"Fix for %s approved. Agent is completing the work.",
					correlationID,
				)
			}

			writeJSON(
				w,
				http.StatusOK,
				MessageResponse{Message: message},
			)
		},
	)

	return mux
}

func decodeJSON(
	r *http.Request,
	destination any,
) error {
	defer r.Body.Close()

	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()

	if err := decoder.Decode(destination); err != nil {
		return fmt.Errorf("decode JSON request: %w", err)
	}

	return nil
}

func writeJSON(
	w http.ResponseWriter,
	status int,
	value any,
) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)

	if err := json.NewEncoder(w).Encode(value); err != nil {
		log.Printf("encode JSON response: %v", err)
	}
}

func writeError(
	w http.ResponseWriter,
	status int,
	err error,
) {
	writeJSON(
		w,
		status,
		map[string]string{"error": err.Error()},
	)
}

func envOrDefault(
	name string,
	fallback string,
) string {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return fallback
	}

	return value
}