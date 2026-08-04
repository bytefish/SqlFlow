package de.bytefish.sqlflow.core.models;

public record SpawnResult(
            String taskId,
            String runId,
            int attempt
    ) {}
