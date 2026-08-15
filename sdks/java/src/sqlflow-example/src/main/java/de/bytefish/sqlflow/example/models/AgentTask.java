package de.bytefish.sqlflow.example.models;

import com.fasterxml.jackson.annotation.JsonProperty;

    public record AgentTask(
            @JsonProperty("issue_id") String issueId
    ) {}

    public record AgentResult(
            @JsonProperty("success") boolean success,
            @JsonProperty("pull_request_url") String pullRequestUrl,
            @JsonProperty("reason") String reason
    ) {}

    public record HumanApproval(
            @JsonProperty("approved") boolean approved,
            @JsonProperty("reason") String reason
    ) {}

    public record Issue(
            @JsonProperty("stack_trace") String stackTrace
    ) {}

    public record Solution(
            @JsonProperty("patched_code") String patchedCode
    ) {}