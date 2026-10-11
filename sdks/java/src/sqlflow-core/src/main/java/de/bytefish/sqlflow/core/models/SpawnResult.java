package de.bytefish.sqlflow.core.models;

public record SpawnResult(String tenantId, String taskId, String runId, int attempt) {}