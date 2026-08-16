package de.bytefish.sqlflow.example.models;

import com.fasterxml.jackson.annotation.JsonProperty;

public record AgentTask(
        @JsonProperty("issue_id") String issueId
) {}