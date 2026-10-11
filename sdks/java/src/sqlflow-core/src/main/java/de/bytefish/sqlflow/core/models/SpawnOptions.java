package de.bytefish.sqlflow.core.models;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public record SpawnOptions(
        String tenantId,
        String queue,
        Integer maxAttempts,
        JsonNode retryStrategy,
        ObjectNode headers
) {}