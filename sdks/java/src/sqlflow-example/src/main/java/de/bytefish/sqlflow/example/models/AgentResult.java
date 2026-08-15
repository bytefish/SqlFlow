package de.bytefish.sqlflow.example.models;

import com.fasterxml.jackson.annotation.JsonProperty;

public record AgentResult(
            @JsonProperty("success") boolean success,
            @JsonProperty("pull_request_url") String pullRequestUrl,
            @JsonProperty("reason") String reason
    ) {}
