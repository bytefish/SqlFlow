package de.bytefish.sqlflow.core.models;

import com.fasterxml.jackson.databind.JsonNode;

import java.time.Instant;

public record CheckpointRow(
        String checkpointName,
        JsonNode state,
        String status,
        String ownerRunId,
        Instant updatedAt
    ) {}
