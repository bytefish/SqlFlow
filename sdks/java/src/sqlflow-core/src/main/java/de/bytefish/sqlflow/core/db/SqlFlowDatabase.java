package de.bytefish.sqlflow.core.db;

import com.fasterxml.jackson.databind.JsonNode;
import de.bytefish.sqlflow.core.models.CheckpointRow;
import de.bytefish.sqlflow.core.models.ClaimedTask;
import de.bytefish.sqlflow.core.models.SpawnResult;

import java.sql.Connection;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.List;

public interface SqlFlowDatabase {
    void createQueue(String tenantId, String queueName);

    void dropQueue(String tenantId, String queueName);

    List<String> listQueues(String tenantId);

    SpawnResult spawnTask(String tenantId, String queue, String taskName, String paramsJson, String optionsJson);

    void cancelTask(String tenantId, String queue, String taskId);

    void emitEvent(String tenantId, String queue, String eventName, String payloadJson);

    List<ClaimedTask> claimTasks(String tenantId, String queue, String workerId, int timeout, int count);

    void completeRun(String tenantId, String queue, String runId, String resultJson);

    void failRun(String tenantId, String queue, String runId, String errorJson);

    List<CheckpointRow> getCheckpointStates(String tenantId, String queue, String taskId, String runId);

    JsonNode getSingleCheckpoint(String tenantId, String queue, String taskId, String checkpointName);

    void persistCheckpoint(String tenantId, String queue, String taskId, String runId, String checkpointName, String stateJson, int timeout);

    void scheduleRun(String tenantId, String queue, String runId, Instant wakeAt);

    void heartbeat(String tenantId, String queue, String runId, int seconds);

    EventResult awaitEvent(String tenantId, String queue, String taskId, String runId, String checkpointName, String eventName, Integer timeout);

    void releaseWorkerClaims(String tenantId, String queue, String workerId);

    Integer cleanupTasks(String tenantId, String queue, int ttlSeconds, int limit);

    Integer cleanupEvents(String tenantId, String queue, int ttlSeconds, int limit);

    OffsetDateTime getNextAvailableAt(String tenantId, String queue);

    record EventResult(boolean shouldSuspend, JsonNode payload) {}
}