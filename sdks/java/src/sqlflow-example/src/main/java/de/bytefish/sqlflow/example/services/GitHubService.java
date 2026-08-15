package de.bytefish.sqlflow.example.services;


import de.bytefish.sqlflow.example.models.Issue;
import de.bytefish.sqlflow.example.models.Solution;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

    // --- GitHub Service ---
    public interface GitHubService {
        Issue getIssueDetails(String id) throws InterruptedException;

        String createPullRequest(String id, String code) throws InterruptedException;

        void requestHumanReview(String issueId, Solution proposedFix, String correlationId) throws InterruptedException;

        void escalateToSenior(String id, String reason) throws InterruptedException;
    }

    @Service
    public static class DefaultGitHubService implements GitHubService {
        private static final Logger logger = LoggerFactory.getLogger(DefaultGitHubService.class);

        @Override
        public Issue getIssueDetails(String issueId) throws InterruptedException {
            logger.info("GitHub: Gets Ticket #{} details from the Repository...", issueId);
            Thread.sleep(800);
            return new Issue("NullReferenceException at PaymentGateway.java:42");
        }

        @Override
        public String createPullRequest(String issueId, String code) throws InterruptedException {
            logger.info("GitHub: PR for Issue #{} has been created...", issueId);
            Thread.sleep(1200);
            return "https://github.com/company/repo/pull/" + (int)(Math.random() * 9000 + 1000);
        }

        @Override
        public void escalateToSenior(String id, String reason) throws InterruptedException {
            logger.error("ESCALATION to Senior Developer: Issue #{} - Reason: {}", id, reason);
            Thread.sleep(500);
        }

        @Override
        public void requestHumanReview(String issueId, Solution proposedFix, String correlationId) throws InterruptedException {
            logger.info("ACTION REQUIRED: Solution for Issue #{} with Correlation-ID {} has been created: {}...",
                    issueId, correlationId, proposedFix.patchedCode());
            Thread.sleep(1200);
        }
    }

    // --- LLM Service ---
    public interface LlmService {
        Solution generateFix(String log, String lastFeedback) throws InterruptedException;
    }

    @Service
    public static class DefaultLlmService implements LlmService {
        private static final Logger logger = LoggerFactory.getLogger(DefaultLlmService.class);

        @Override
        public Solution generateFix(String log, String lastFeedback) throws InterruptedException {
            logger.info("Agent is thinking: 'Learned from feedback: {}'", lastFeedback);

            // Simulate expensive LLM call
            Thread.sleep(2500);

            String code = lastFeedback.contains("error handling")
                    ? "// AI: Improved Logging & Error-Handling added\nif(data == null) throw new IllegalArgumentException();"
                    : "// AI: Simple Fix for the NullReferenceException\nif(data == null) return;";

            logger.info("LLM has generated a potential fix: \n{}", code);
            return new Solution(code);
        }
    }

    // --- Notification Service ---
    public interface LocalNotificationService {
        void notifyReviewer(String issueId, String correlationId);
    }

    @Service
    public static class DefaultLocalNotificationService implements LocalNotificationService {
        private static final Logger logger = LoggerFactory.getLogger(DefaultLocalNotificationService.class);

        @Override
        public void notifyReviewer(String issueId, String correlationId) {
            String notification = """
                    
                    ======================================================
                    SqlServerFlow LLM AGENT PAUSED...
                    
                    The issue '%s' requires human approval.
                    To approve the code, execute the following request:
                    
                    POST http://localhost:8080/agent/review/%s/%s
                    Content-Type: application/json
                    
                    {
                      "approved": true,
                      "reason": "LGTM!"
                    }
                    ======================================================
                    """.formatted(issueId, issueId, correlationId);

            logger.info(notification);
        }
    }