package de.bytefish.sqlflow.core;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import de.bytefish.sqlflow.core.db.SqlFlowDatabase;
import de.bytefish.sqlflow.core.exceptions.CancelledTaskException;
import de.bytefish.sqlflow.core.exceptions.SuspendTaskException;
import de.bytefish.sqlflow.core.infrastructure.TaskContext;
import de.bytefish.sqlflow.core.infrastructure.TaskHandler;
import de.bytefish.sqlflow.core.models.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * The core implementation of the ISqlFlow interface.
 * Operates purely on the functional TaskHandler interface, matching the C# design perfectly.
 */
public class SqlFlow implements ISqlFlow {

    private static final Logger logger = LoggerFactory.getLogger(SqlFlow.class);

    private final SqlFlowDatabase db;
    private final ObjectMapper mapper;

    // Internal registry holding the functional execution wrappers
    private final Map<String, TaskHandler> taskHandlers = new ConcurrentHashMap<>();

    public SqlFlow(SqlFlowDatabase db, ObjectMapper mapper) {
        this.db = db;
        this.mapper = mapper;
    }

    @Override
    public void createQueue(String queueName) {
        db.createQueue(queueName);
    }

    @Override
    public void dropQueue(String queueName) {
        db.dropQueue(queueName);
    }

    @Override
    public List<String> listQueues() {
        return db.listQueues();
    }

    @Override
    public void registerTask(TaskRegistrationOptions options, TaskHandler handler) {
        if (options.name() == null || options.name().isEmpty()) {
            throw new IllegalArgumentException("Task registration requires a name");
        }
        taskHandlers.put(options.name(), handler);
    }

    @Override
    public <TRequest> SpawnResult spawn(SpawnOptions options, String taskName, TRequest request) {
        try {
            String paramsJson = mapper.writeValueAsString(request);
            String optionsJson = mapper.writeValueAsString(options);
            return db.spawnTask(options.queue(), taskName, paramsJson, optionsJson);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to serialize spawn request", e);
        }
    }

    @Override
    public void emitEvent(EmitEventOptions options, String eventName, Object payload) {
        if (eventName == null || eventName.isEmpty()) {
            throw new IllegalArgumentException("eventName required");
        }
        try {
            String payloadJson = mapper.writeValueAsString(payload);
            db.emitEvent(options.queue(), eventName, payloadJson);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to serialize event payload", e);
        }
    }

    @Override
    public void cancelTask(CancelTaskOptions options, String taskId) {
        db.cancelTask(options.queue(), taskId);
    }

    @Override
    public List<ClaimedTask> claimTasks(String queue, String workerId, int claimTimeout, int batchSize) {
        if (queue == null || queue.isEmpty()) {
            throw new IllegalArgumentException("Queue must be specified for claiming tasks");
        }
        return db.claimTasks(queue, workerId, claimTimeout, batchSize);
    }

    @Override
    public void workBatch(String queue, String workerId, int claimTimeout, int batchSize) {
        List<ClaimedTask> tasks = claimTasks(queue, workerId, claimTimeout, batchSize);
        for (ClaimedTask task : tasks) {
            executeTask(task, queue, claimTimeout, true);
        }
    }

    @Override
    public void executeTask(ClaimedTask task, String queue, int claimTimeout, boolean fatalOnLeaseTimeout) {
        TaskHandler handler = taskHandlers.get(task.taskName());

        if (handler == null) {
            logger.error("No handler registered for task: {}", task.taskName());
            try {
                ObjectNode errNode = mapper.createObjectNode();
                errNode.put("error", "Unknown task: " + task.taskName());
                db.failRun(queue, task.runId(), mapper.writeValueAsString(errNode));
            } catch (Exception ex) {
                // Ignore
            }
            return;
        }

        TaskContext ctx = TaskContext.create(logger, task.taskId(), db, queue, task, claimTimeout);

        try {
            // Emulating C#'s Task.WhenAny using CompletableFuture for lease timeouts
            CompletableFuture<Object> handlerFuture = CompletableFuture.supplyAsync(() -> {
                try {
                    return handler.handle(ctx, task.params());
                } catch (RuntimeException e) {
                    throw e; // Propagate directly
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });

            Object result;
            if (fatalOnLeaseTimeout) {
                // If it takes longer than 2x claimTimeout, we force a fatal cancellation.
                result = handlerFuture.get(claimTimeout * 2L, TimeUnit.SECONDS);
            } else {
                result = handlerFuture.join();
            }

            String resultJson = mapper.writeValueAsString(result);
            db.completeRun(queue, task.runId(), resultJson);

        } catch (Exception ex) {
            Throwable cause = ex;
            if (ex instanceof java.util.concurrent.ExecutionException && ex.getCause() != null) {
                cause = ex.getCause();
            }

            if (cause instanceof SuspendTaskException || cause instanceof CancelledTaskException) {
                return; // Expected control-flow exception (suspending thread)
            }

            if (cause instanceof TimeoutException) {
                logger.error("Task {} ({}) exceeded claim timeout by 2x.", task.taskName(), task.taskId());
                throw new RuntimeException("FatalLeaseTimeoutException", cause);
            }

            logger.error("[ssf] task execution failed: {}", cause.getMessage());
            try {
                ObjectNode errorNode = mapper.createObjectNode();
                errorNode.put("name", cause.getClass().getSimpleName());
                errorNode.put("message", cause.getMessage());

                StringBuilder stack = new StringBuilder();
                for(StackTraceElement el : cause.getStackTrace()) {
                    stack.append(el.toString()).append("\n");
                }
                errorNode.put("stack", stack.toString());

                db.failRun(queue, task.runId(), mapper.writeValueAsString(errorNode));
            } catch (Exception failErr) {
                logger.error("Failed to mark run as failed: {}", failErr.getMessage());
            }
        }
    }
}