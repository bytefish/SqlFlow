package de.bytefish.sqlflow.core.models;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public record ClaimedTask(
            String runId,
            String taskId,
            String taskName,
            int attempt,
            JsonNode params,
            JsonNode retryStrategy,
            Integer maxAttempts,
            ObjectNode headers,
            String wakeEvent,
            JsonNode eventPayload
    ) {}
