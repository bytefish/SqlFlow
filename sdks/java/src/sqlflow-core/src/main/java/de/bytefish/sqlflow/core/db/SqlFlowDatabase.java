package de.bytefish.sqlflow.core.db;


import com.fasterxml.jackson.databind.JsonNode;
import de.bytefish.sqlflow.core.models.CheckpointRow;
import de.bytefish.sqlflow.core.models.ClaimedTask;
import de.bytefish.sqlflow.core.models.SpawnResult;

import java.sql.Connection;
import java.time.Instant;
import java.util.List;

public interface SqlFlowDatabase {

    void createQueue(Connection conn, String queueName);

    void dropQueue(Connection conn, String queueName);

    List<String> listQueues(Connection conn);

    SpawnResult spawnTask(Connection conn, String queue, String taskName, String paramsJson, String optionsJson);

    void cancelTask(Connection conn, String queue, String taskId);

    void emitEvent(Connection conn, String queue, String eventName, String payloadJson);

    List<ClaimedTask> claimTasks(Connection conn, String queue, String workerId, int timeout, int count);

    void completeRun(Connection conn, String queue, String runId, String resultJson);

    void failRun(Connection conn, String queue, String runId, String errorJson);

    List<CheckpointRow> getCheckpointStates(Connection conn, String queue, String taskId, String runId);

    JsonNode getSingleCheckpoint(Connection conn, String queue, String taskId, String checkpointName);

    void persistCheckpoint(Connection conn, String queue, String taskId, String runId, String checkpointName, String stateJson, int timeout);

    void scheduleRun(Connection conn, String queue, String runId, Instant wakeAt);

    void heartbeat(Connection conn, String queue, String runId, int seconds);

    EventResult awaitEvent(Connection conn, String queue, String taskId, String runId, String checkpointName, String eventName, Integer timeout);

    void releaseWorkerClaims(Connection conn, String queue, String workerId);

    Integer cleanupTasks(Connection conn, String queue, int ttlSeconds, int limit);

    Integer cleanupEvents(Connection conn, String queue, int ttlSeconds, int limit);

    record EventResult(boolean shouldSuspend, JsonNode payload) {}
}