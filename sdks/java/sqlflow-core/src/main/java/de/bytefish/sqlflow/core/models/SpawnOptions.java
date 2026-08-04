package de.bytefish.sqlflow.core.models;

import com.fasterxml.jackson.databind.node.ObjectNode;

public record SpawnOptions(
            String queue,
            int maxAttempts,
            Object retryStrategy,
            ObjectNode headers,
            Object cancellation
    ) {
        public SpawnOptions(String queue) {
            this(queue, 5, null, null, null);
        }
    }
