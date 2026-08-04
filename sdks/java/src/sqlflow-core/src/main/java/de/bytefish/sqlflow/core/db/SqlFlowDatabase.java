package de.bytefish.sqlflow.core.db;


import com.fasterxml.jackson.databind.JsonNode;
import de.bytefish.sqlflow.core.models.CheckpointRow;
import de.bytefish.sqlflow.core.models.ClaimedTask;
import de.bytefish.sqlflow.core.models.SpawnResult;

import java.sql.Connection;
import java.time.Instant;
import java.util.List;

public interface SqlFlowDatabase {
    void createQueue(String queueName);
    void dropQueue(String queueName);
    List<String> listQueues();

    SpawnResult spawnTask(String queue, String taskName, String paramsJson, String optionsJson);
    void cancelTask(String queue, String taskId);
    void emitEvent(String queue, String eventName, String payloadJson);

    List<ClaimedTask> claimTasks(String queue, String workerId, int timeout, int count);
    void completeRun(String queue, String runId, String resultJson);
    void failRun(String queue, String runId, String errorJson);

    List<CheckpointRow> getCheckpointStates(String queue, String taskId, String runId);
    JsonNode getSingleCheckpoint(String queue, String taskId, String checkpointName);
    void persistCheckpoint(String queue, String taskId, String runId, String checkpointName, String stateJson, int timeout);

    void scheduleRun(String queue, String runId, Instant wakeAt);
    void heartbeat(String queue, String runId, int seconds);
    EventResult awaitEvent(String queue, String taskId, String runId, String checkpointName, String eventName, Integer timeout);

    void releaseWorkerClaims(String queue, String workerId);
    Integer cleanupTasks(String queue, int ttlSeconds, int limit);
    Integer cleanupEvents(String queue, int ttlSeconds, int limit);

    record EventResult(boolean shouldSuspend, JsonNode payload) {}
}