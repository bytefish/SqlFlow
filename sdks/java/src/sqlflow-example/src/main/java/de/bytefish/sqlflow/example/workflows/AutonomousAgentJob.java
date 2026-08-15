package de.bytefish.sqlflow.example.workflows;

import com.fasterxml.jackson.databind.JsonNode;
import de.bytefish.sqlflow.core.infrastructure.*;
import de.bytefish.sqlflow.example.models.*;
import de.bytefish.sqlflow.example.services.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.Optional;


    /**
     * Der autonome Agent, implementiert als typsicherer Job.
     * Durch @Component wird er automatisch von Spring Boot verwaltet und kann
     * Services injiziert bekommen.
     */
    @Component
    public class AutonomousAgentJob implements Job<AgentTask, AgentResult> {

        private static final Logger logger = LoggerFactory.getLogger(AutonomousAgentJob.class);

        private final LlmService llmService;
        private final GitHubService gitHubService;
        private final LocalNotificationService localNotificationService;

        // Constructor Injection durch Spring Boot
        public AutonomousAgentJob(LlmService llmService, GitHubService gitHubService, LocalNotificationService localNotificationService) {
            this.llmService = llmService;
            this.gitHubService = gitHubService;
            this.localNotificationService = localNotificationService;
        }

        @Override
        public AgentResult execute(TaskContext ctx, AgentTask task) throws Exception {
            logger.info("Agent starts researching ticket {}", task.issueId());

            // Step 1: Load Issue Context
            Issue bugReport = ctx.step("fetch-issue-context", Issue.class, () ->
                    gitHubService.getIssueDetails(task.issueId())
            );

            boolean isApproved = false;
            int attempt = 0;
            String lastFeedback = "Initial Attempt";

            while (!isApproved && attempt < 3) {
                attempt++;
                String correlationId = ctx.getTaskId() + "-attempt-" + attempt;

                logger.info("Attempt {}/3: Generating a fix based on: {}", attempt, lastFeedback);

                final String currentFeedback = lastFeedback; // Für Lambda effectively final machen

                // Step 2: LLM generiert den Code
                Solution proposedFix = ctx.step("generate-code-fix-" + attempt, Solution.class, () ->
                        llmService.generateFix(bugReport.stackTrace(), currentFeedback)
                );

                // Step 3: Reviewer benachrichtigen
                ctx.step("notify-reviewer-" + attempt, () -> {
                    gitHubService.requestHumanReview(task.issueId(), proposedFix, correlationId);

                    localNotificationService.notifyReviewer(task.issueId(), correlationId);
                });

                logger.info("Review for {} has been requested. Agent goes idle and waits for the code review...", correlationId);

                // Step 4: Suspendierung (Warten auf menschliche Eingabe)
                // Das Framework wirft hier die SuspendTaskException und gibt den Virtual Thread frei!
                Optional<JsonNode> reviewOpt = ctx.awaitEvent(
                        "agent-approval:" + task.issueId() + ":" + correlationId,
                        "wait-for-human-review-" + attempt,
                        null,
                        JsonNode.class
                );

                if (reviewOpt.isPresent()) {
                    JsonNode review = reviewOpt.get();
                    isApproved = review.has("approved") && review.get("approved").asBoolean();
                    lastFeedback = review.has("reason") ? review.get("reason").asText() : "No feedback has been given";

                    if (!isApproved) {
                        logger.warn("Attempt {} has been rejected: {}", attempt, lastFeedback);
                    }
                }
            }

            // Step 5: Abschluss
            if (isApproved) {
                logger.info("Fix approved. Creating Pull Request...");

                String prUrl = ctx.step("create-pull-request", String.class, () ->
                        gitHubService.createPullRequest(task.issueId(), "apply-fix")
                );

                logger.info("Mission accomplished, the PR has been created: {}", prUrl);
                return new AgentResult(true, prUrl, null);

            } else {
                logger.error("Maximum number of attempts reached. Escalates ticket {} to a human.", task.issueId());

                ctx.step("notify-senior-developer", () -> {
                    gitHubService.escalateToSenior(task.issueId(), "Agent didn't find a solution after 3 attempts.");
                });

                return new AgentResult(false, null, "Escalated to human supervisor after 3 failures.");
            }
        }
    }