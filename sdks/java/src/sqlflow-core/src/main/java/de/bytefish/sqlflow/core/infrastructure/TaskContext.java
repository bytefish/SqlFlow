package de.bytefish.sqlflow.core.infrastructure;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import de.bytefish.sqlflow.core.db.SqlFlowDatabase;
import de.bytefish.sqlflow.core.exceptions.SuspendTaskException;
import de.bytefish.sqlflow.core.exceptions.TimeoutErrorException;
import de.bytefish.sqlflow.core.models.CheckpointRow;
import de.bytefish.sqlflow.core.models.ClaimedTask;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;
import org.slf4j.Logger;

public class TaskContext {
    private static final ObjectMapper MAPPER = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    private final Map<String, Integer> stepNameCounter = new HashMap<>();
    private final Logger logger;

    private final String taskId;
    private final SqlFlowDatabase db;
    private final String queueName;
    private ClaimedTask task;
    private final Map<String, JsonNode> checkpointCache;
    private final int claimTimeout;

    private TaskContext(Logger logger, String taskId, SqlFlowDatabase db,
                        String queueName, ClaimedTask task, Map<String, JsonNode> checkpointCache,
                        int claimTimeout) {
        this.logger = logger;
        this.taskId = taskId;
        this.db = db;
        this.queueName = queueName;
        this.task = task;
        this.checkpointCache = checkpointCache;
        this.claimTimeout = claimTimeout;
    }

    public String getTaskId() {
        return taskId;
    }

    public String getRunId() {
        return task.runId();
    }

    public int getAttempt() {
        return task.attempt();
    }

    public static TaskContext create(Logger logger, String taskId, SqlFlowDatabase db,
                                     String queueName, ClaimedTask task, int claimTimeout) {
        List<CheckpointRow> checkpoints = db.getCheckpointStates(queueName, task.taskId(), task.runId());
        Map<String, JsonNode> cache = new HashMap<>();
        for (CheckpointRow cp : checkpoints) {
            cache.put(cp.checkpointName(), cp.state());
        }
        return new TaskContext(logger, taskId, db, queueName, task, cache, claimTimeout);
    }

    public <T> T step(String name, Class<T> returnType, Callable<T> fn) throws Exception {
        String checkpointName = getCheckpointName(name);
        Optional<JsonNode> state = lookupCheckpoint(checkpointName);

        if (state.isPresent() && !state.get().isNull()) {
            return MAPPER.treeToValue(state.get(), returnType);
        }

        logger.debug("Executing step: {}", checkpointName);
        T result = fn.call();

        JsonNode resultNode = MAPPER.valueToTree(result);
        db.persistCheckpoint(queueName, task.taskId(), task.runId(),
                checkpointName, resultNode.toString(), claimTimeout);
        checkpointCache.put(checkpointName, resultNode);

        return result;
    }

    public void step(String name, Runnable fn) throws Exception {
        step(name, Boolean.class, () -> {
            fn.run();
            return true;
        });
    }

    public <T> Optional<T> awaitEvent(String eventName, String stepName, Double timeoutSeconds, Class<T> payloadType) throws Exception {
        String finalStepName = stepName != null ? stepName : "$awaitEvent:" + eventName;
        Integer timeout = timeoutSeconds != null ? (int) Math.floor(timeoutSeconds) : null;
        String checkpointName = getCheckpointName(finalStepName);

        Optional<JsonNode> cached = lookupCheckpoint(checkpointName);
        if (cached.isPresent()) {
            return Optional.ofNullable(MAPPER.treeToValue(cached.get(), payloadType));
        }

        if (eventName.equals(task.wakeEvent()) && task.eventPayload() == null) {
            task = task.clearWakeEvent();
            throw new TimeoutErrorException("Timed out waiting for event: " + eventName);
        }

        SqlFlowDatabase.EventResult result = db.awaitEvent(queueName, task.taskId(), task.runId(), checkpointName, eventName, timeout);

        if (!result.shouldSuspend()) {
            checkpointCache.put(checkpointName, result.payload());
            task = task.clearWakeEvent();

            if (result.payload() == null || result.payload().isNull()) return Optional.empty();
            return Optional.ofNullable(MAPPER.treeToValue(result.payload(), payloadType));
        }

        throw new SuspendTaskException();
    }

    private Optional<JsonNode> lookupCheckpoint(String checkpointName) {
        if (checkpointCache.containsKey(checkpointName)) {
            return Optional.ofNullable(checkpointCache.get(checkpointName));
        }
        JsonNode state = db.getSingleCheckpoint(queueName, task.taskId(), checkpointName);
        if (state != null) {
            checkpointCache.put(checkpointName, state);
        }
        return Optional.ofNullable(state);
    }

    private String getCheckpointName(String name) {
        int count = stepNameCounter.getOrDefault(name, 0) + 1;
        stepNameCounter.put(name, count);
        return count == 1 ? name : name + "#" + count;
    }
}