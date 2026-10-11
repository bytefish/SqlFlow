package de.bytefish.sqlflow.core;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import de.bytefish.sqlflow.core.db.SqlFlowDatabase;
import de.bytefish.sqlflow.core.infrastructure.*;
import de.bytefish.sqlflow.core.exceptions.CancelledTaskException;
import de.bytefish.sqlflow.core.exceptions.SuspendTaskException;
import de.bytefish.sqlflow.core.models.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

/**
 * The core implementation of the ISqlFlow interface.
 * Operates purely on the functional TaskHandler interface, matching the C# design perfectly.
 */
public class SqlFlow implements ISqlFlow {

    private static final Logger logger = LoggerFactory.getLogger(SqlFlow.class);

    private final SqlFlowDatabase db;
    private final ObjectMapper mapper;

    private final IEventPublisher eventPublisher;
    private final IJobPublisher jobPublisher;

    // Internal registry holding the functional execution wrappers
    private final Map<String, TaskHandler> taskHandlers = new ConcurrentHashMap<>();

    // Standard constructor using default database publishers
    public SqlFlow(SqlFlowDatabase db, ObjectMapper mapper) {
        this.db = db;
        this.mapper = mapper;
        this.eventPublisher = new EventPublisher(db, mapper);
        this.jobPublisher = new JobPublisher(db, mapper);
    }

    // DI-friendly constructor allowing decorated publishers (like NatsEventPublisher)
    public SqlFlow(SqlFlowDatabase db, ObjectMapper mapper, IEventPublisher eventPublisher, IJobPublisher jobPublisher) {
        this.db = db;
        this.mapper = mapper;
        this.eventPublisher = eventPublisher;
        this.jobPublisher = jobPublisher;
    }

    @Override
    public void createQueue(String tenantId, String queueName) {
        db.createQueue(tenantId, queueName);
    }

    @Override
    public void dropQueue(String tenantId, String queueName) {
        db.dropQueue(tenantId, queueName);
    }

    @Override
    public List<String> listQueues(String tenantId) {
        return db.listQueues(tenantId);
    }

    @Override
    public void registerTask(TaskRegistrationOptions options, TaskHandler handler) {
        if (options.name() == null || options.name().isEmpty()) {
            throw new IllegalArgumentException("Task registration requires a name");
        }
        // Schlüssel im Handler-Registry berücksichtigt nun tenantId und Taskname
        String key = options.tenantId() + ":" + options.name();
        taskHandlers.put(key, handler);
    }

    @Override
    public <TRequest> SpawnResult spawn(SpawnOptions options, String taskName, TRequest request) {
        return jobPublisher.spawn(options, taskName, request);
    }

    @Override
    public void emitEvent(EmitEventOptions options, String eventName, Object payload) {
        eventPublisher.emitEvent(options.tenantId(), options.queue(), eventName, payload);
    }

    @Override
    public void cancelTask(CancelTaskOptions options, String taskId) {
        db.cancelTask(options.tenantId(), options.queue(), taskId);
    }

    @Override
    public List<ClaimedTask> claimTasks(String tenantId, String queue, String workerId, int claimTimeout, int batchSize) {
        if (queue == null || queue.isEmpty()) {
            throw new IllegalArgumentException("Queue must be specified for claiming tasks");
        }
        return db.claimTasks(tenantId, queue, workerId, claimTimeout, batchSize);
    }

    @Override
    public void workBatch(String tenantId, String queue, String workerId, int claimTimeout, int batchSize) {
        List<ClaimedTask> tasks = claimTasks(tenantId, queue, workerId, claimTimeout, batchSize);
        for (ClaimedTask task : tasks) {
            executeTask(task, tenantId, queue, claimTimeout, true);
        }
    }

    @Override
    public void executeTask(
            ClaimedTask task,
            String tenantId,
            String queue,
            int claimTimeout,
            boolean fatalOnLeaseTimeout) {

        String handlerKey = tenantId + ":" + task.taskName();
        TaskHandler handler = taskHandlers.get(handlerKey);

        if (handler == null) {
            logger.error(
                    "No handler registered for task: {} and tenant: {}",
                    task.taskName(),
                    tenantId
            );

            try {
                ObjectNode errNode = mapper.createObjectNode();
                errNode.put(
                        "error",
                        "Unknown task: " + task.taskName()
                );

                db.failRun(
                        tenantId,
                        queue,
                        task.runId(),
                        mapper.writeValueAsString(errNode)
                );
            } catch (Exception ex) {
                // Ignore
            }

            return;
        }

        TaskContext ctx = TaskContext.create(
                logger,
                tenantId,
                task.taskId(),
                db,
                queue,
                task,
                claimTimeout
        );

        try {
            CompletableFuture<Object> handlerFuture =
                    CompletableFuture.supplyAsync(() -> {
                        try {
                            return handler.handle(
                                    ctx,
                                    task.params()
                            );
                        } catch (RuntimeException ex) {
                            throw ex;
                        } catch (Exception ex) {
                            throw new RuntimeException(ex);
                        }
                    });

            Object result;

            if (fatalOnLeaseTimeout) {
                result = handlerFuture.get(
                        claimTimeout * 2L,
                        TimeUnit.SECONDS
                );
            } else {
                result = handlerFuture.join();
            }

            String resultJson =
                    mapper.writeValueAsString(result);

            db.completeRun(
                    tenantId,
                    queue,
                    task.runId(),
                    resultJson
            );
        } catch (Exception ex) {
            Throwable current = ex;
            Throwable controlFlowCause = null;

            while (current != null) {
                if (current instanceof SuspendTaskException
                        || current instanceof CancelledTaskException) {
                    controlFlowCause = current;
                    break;
                }

                current = current.getCause();
            }

            if (controlFlowCause != null) {
                logger.debug(
                        "Task control flow detected: tenantId={}, taskId={}, runId={}, exception={}",
                        tenantId,
                        task.taskId(),
                        task.runId(),
                        controlFlowCause.getClass().getName()
                );
                return;
            }

            Throwable cause = ex;

            if ((ex instanceof ExecutionException || ex instanceof CompletionException)
                    && ex.getCause() != null) {
                cause = ex.getCause();
            }

            if (cause instanceof TimeoutException) {
                logger.error(
                        "Task {} ({}) for tenant {} exceeded claim timeout by 2x.",
                        task.taskName(),
                        task.taskId(),
                        tenantId
                );

                throw new RuntimeException(
                        "FatalLeaseTimeoutException",
                        cause
                );
            }

            logger.error(
                    "[ssf] task execution failed for tenant {}: {}",
                    tenantId,
                    cause.getMessage()
            );

            try {
                ObjectNode errorNode = mapper.createObjectNode();

                errorNode.put(
                        "name",
                        cause.getClass().getSimpleName()
                );

                errorNode.put(
                        "message",
                        cause.getMessage()
                );

                StringBuilder stack = new StringBuilder();

                for (StackTraceElement element : cause.getStackTrace()) {
                    stack.append(element)
                            .append("\n");
                }

                errorNode.put(
                        "stack",
                        stack.toString()
                );

                db.failRun(
                        tenantId,
                        queue,
                        task.runId(),
                        mapper.writeValueAsString(errorNode)
                );
            } catch (Exception failError) {
                logger.error(
                        "Failed to mark run as failed: {}",
                        failError.getMessage()
                );
            }
        }
    }

    @Override
    public OffsetDateTime getNextAvailableAt(String tenantId, String queue) {
        if (queue == null || queue.trim().isEmpty()) {
            throw new IllegalArgumentException("Queue must be specified");
        }

        return db.getNextAvailableAt(tenantId, queue);
    }
}