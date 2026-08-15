package de.bytefish.sqlflow.example.services;

import de.bytefish.sqlflow.example.models.Issue;
import de.bytefish.sqlflow.example.models.Solution;

public interface GitHubService {
    Issue getIssueDetails(String id);

    String createPullRequest(String id, String code);

    void requestHumanReview(String issueId, Solution proposedFix, String correlationId);

    void escalateToSenior(String id, String reason);
}

