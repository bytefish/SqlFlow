package de.bytefish.sqlflow.example.services;

import de.bytefish.sqlflow.example.models.Issue;
import de.bytefish.sqlflow.example.models.Solution;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
    public class DefaultGitHubService implements GitHubService {
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
