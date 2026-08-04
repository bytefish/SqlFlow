package de.bytefish.sqlflow.core.infrastructure;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import de.bytefish.sqlflow.core.db.SqlFlowDatabase;
import de.bytefish.sqlflow.core.exceptions.SuspendTaskException;
import de.bytefish.sqlflow.core.exceptions.TimeoutErrorException;
import de.bytefish.sqlflow.core.models.CheckpointRow;
import de.bytefish.sqlflow.core.models.ClaimedTask;

import java.sql.Connection;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.logging.Logger;

public class TaskContext {

    private static final ObjectMapper mapper = new ObjectMapper().registerModule(new JavaTimeModule());

    private final Map<String, Integer> stepNameCounter = new HashMap<>();
    private final Logger logger;

    private final String taskId;
    private final Connection connection;
    private final SqlFlowDatabase db;
    private final String queueName;
    private ClaimedTask task; // No longer final, as we re-assign new records on state changes
    private final Map<String, JsonNode> checkpointCache;
    private final int claimTimeout;

    private TaskContext(Logger logger, String taskId, Connection con, SqlFlowDatabase db,
                        String queueName, ClaimedTask task, Map<String, JsonNode> checkpointCache,
                        int claimTimeout) {
        this.logger = logger;
        this.taskId = taskId;
        this.connection = con;
        this.db = db;
        this.queueName = queueName;
        this.task = task;
        this.checkpointCache = checkpointCache;
        this.claimTimeout = claimTimeout;
    }

    public static TaskContext create(Logger logger, String taskId, Connection con,
                                     SqlFlowDatabase db, String queueName, ClaimedTask task,
                                     int claimTimeout) {

        List<CheckpointRow> checkpoints = db.getCheckpointStates(con, queueName, task.taskId(), task.runId());

        Map<String, JsonNode> cache = new HashMap<>();

        for (CheckpointRow cp : checkpoints) {
            cache.put(cp.checkpointName(), cp.state());
        }

        return new TaskContext(logger, taskId, con, db, queueName, task, cache, claimTimeout);
    }

    public String getTaskId() {
        return taskId;
    }

    public <T> T step(String name, Class<T> returnType, Callable<T> fn) throws Exception {
        String checkpointName = getCheckpointName(name);
        JsonNode state = lookupCheckpoint(checkpointName);

        if (state != null && !state.isNull()) {
            return mapper.treeToValue(state, returnType);
        }

        T rv = fn.call();

        JsonNode rvJson = mapper.valueToTree(rv);
        String rvString = rvJson != null ? rvJson.toString() : "null";

        db.persistCheckpoint(connection, queueName, task.taskId(), task.runId(), checkpointName, rvString, claimTimeout);
        checkpointCache.put(checkpointName, rvJson);

        return rv;
    }

    public void step(String name, Runnable fn) throws Exception {
        step(name, Boolean.class, () -> {
            fn.run();
            return true;
        });
    }

    public void sleepFor(String stepName, double durationSeconds) {
        Instant wakeAt = Instant.now().plus((long)(durationSeconds * 1000), ChronoUnit.MILLIS);
        sleepUntil(stepName, wakeAt);
    }

    public void sleepUntil(String stepName, Instant wakeAt) {
        String checkpointName = getCheckpointName(stepName);
        JsonNode state = lookupCheckpoint(checkpointName);

        Instant actualWakeAt = wakeAt;

        if (state != null && state.isTextual()) {
            actualWakeAt = Instant.parse(state.asText());
        } else if (state == null) {
            try {
                String wakeString = mapper.writeValueAsString(wakeAt);
                db.persistCheckpoint(connection, queueName, task.taskId(), task.runId(), checkpointName, wakeString, claimTimeout);
                checkpointCache.put(checkpointName, mapper.valueToTree(wakeAt));
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        }

        if (Instant.now().isBefore(actualWakeAt)) {
            db.scheduleRun(connection, queueName, task.runId(), actualWakeAt);

            throw new SuspendTaskException();
        }
    }

    private String getCheckpointName(String name) {
        int count = stepNameCounter.getOrDefault(name, 0) + 1;
        stepNameCounter.put(name, count);
        return count == 1 ? name : name + "#" + count;
    }

    private JsonNode lookupCheckpoint(String checkpointName) {
        if (checkpointCache.containsKey(checkpointName)) {
            return checkpointCache.get(checkpointName);
        }

        JsonNode state = db.getSingleCheckpoint(connection, queueName, task.taskId(), checkpointName);
        if (state != null) {
            checkpointCache.put(checkpointName, state);
        }
        return state;
    }

    public JsonNode awaitEvent(String eventName, String stepName, Double timeoutSeconds) {
        String finalStepName = stepName != null ? stepName : "$awaitEvent:" + eventName;
        Integer timeout = timeoutSeconds != null ? (int) Math.floor(timeoutSeconds) : null;
        String checkpointName = getCheckpointName(finalStepName);

        JsonNode cached = lookupCheckpoint(checkpointName);
        if (cached != null) return cached;

        if (eventName.equals(task.wakeEvent()) && task.eventPayload() == null) {
            // Null out wake events immutably by replacing the record
            task = new ClaimedTask(task.runId(), task.taskId(), task.taskName(), task.attempt(), task.params(),
                    task.retryStrategy(), task.maxAttempts(), task.headers(), null, null);
            throw new TimeoutErrorException("Timed out waiting for event \"" + eventName + "\"");
        }

        SqlFlowDatabase.EventResult result = db.awaitEvent(connection, queueName, task.taskId(), task.runId(), checkpointName, eventName, timeout);

        if (!result.shouldSuspend()) {
            checkpointCache.put(checkpointName, result.payload());
            // Null out event payload immutably by replacing the record
            task = new ClaimedTask(task.runId(), task.taskId(), task.taskName(), task.attempt(), task.params(),
                    task.retryStrategy(), task.maxAttempts(), task.headers(), task.wakeEvent(), null);
            return result.payload() != null ? result.payload() : JsonNodeFactory.instance.objectNode();
        }

        throw new SuspendTaskException();
    }

    public void heartbeat(Integer seconds) {
        db.heartbeat(connection, queueName, task.runId(), seconds != null ? seconds : claimTimeout);
    }

    public void emitEvent(String eventName, JsonNode payload) {
        if (eventName == null || eventName.isEmpty()) {
            throw new IllegalArgumentException("eventName must be a non-empty string");
        }
        String payloadJson = payload != null ? payload.toString() : "null";
        db.emitEvent(connection, queueName, eventName, payloadJson);
    }
}