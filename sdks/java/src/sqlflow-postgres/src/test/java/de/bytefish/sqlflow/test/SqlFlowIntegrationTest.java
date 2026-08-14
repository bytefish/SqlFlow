package de.bytefish.sqlflow.test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import de.bytefish.sqlflow.core.ISqlFlow;
import de.bytefish.sqlflow.core.SqlFlow;
import de.bytefish.sqlflow.core.infrastructure.Job;
import de.bytefish.sqlflow.core.infrastructure.JobFactory;
import de.bytefish.sqlflow.core.infrastructure.TaskContext;
import de.bytefish.sqlflow.core.models.*;
import de.bytefish.sqlflow.postgres.PostgresFlowDatabase;
import org.junit.Test;
import org.junit.jupiter.api.*;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

@Testcontainers
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class SqlFlowIntegrationTest {

    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:15-alpine")
            .withInitScript("init.sql");

    private static HikariDataSource dataSource;
    private ISqlFlow sqlFlow;
    private PostgresFlowDatabase db;
    private final ObjectMapper mapper = new ObjectMapper().registerModule(new JavaTimeModule());

    private static final String QUEUE = "test-queue";
    private static final String WORKER_ID = "test-worker-1";

    // --- Dummy Data Models ---
    public record MathParams(int a, int b) {}
    public record OrderParams(String orderId) {}
    public record PaymentEvent(boolean success, String ref) {}

    // --- Dummy Job Implementations ---
    public static class MathJob implements Job<MathParams, Integer> {
        @Override
        public Integer execute(TaskContext ctx, MathParams params) {
            return params.a() + params.b();
        }
    }

    public static class CheckpointJob implements Job<OrderParams, String> {
        static int step1ExecutionCount = 0;
        static boolean shouldCrash = true;

        @Override
        public String execute(TaskContext ctx, OrderParams params) throws Exception {
            // Step 1: Save. 2. Attempt this shouldn't be hit anymore.
            ctx.step("charge-creditcard", () -> {
                step1ExecutionCount++;
            });

            if (shouldCrash) {
                shouldCrash = false;
                throw new RuntimeException("Simulated Crash in Step 1!");
            }

            return "ORDER_COMPLETED";
        }
    }

    public static class AwaitEventJob implements Job<OrderParams, String> {
        @Override
        public String execute(TaskContext ctx, OrderParams params) throws Exception {
            Optional<PaymentEvent> payment = ctx.awaitEvent(
                    "payment-" + params.orderId(), "wait-for-payment", null, PaymentEvent.class
            );

            if (payment.isPresent() && payment.get().success()) {
                return "PAID_" + payment.get().ref();
            }
            return "FAILED";
        }
    }

    // --- Simple JobFactory for testing ---
    private static class TestJobFactory implements JobFactory {
        private final Map<Class<?>, Object> instances = new HashMap<>();
        public <T> void register(Class<T> clazz, T instance) { instances.put(clazz, instance); }

        @Override
        @SuppressWarnings("unchecked")
        public <T> T getJob(Class<T> jobType) {
            return (T) instances.get(jobType);
        }
    }

    @BeforeAll
    static void setupDataSource() {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(postgres.getJdbcUrl());
        config.setUsername(postgres.getUsername());
        config.setPassword(postgres.getPassword());
        config.setMaximumPoolSize(5);
        dataSource = new HikariDataSource(config);
    }

    @AfterAll
    static void teardownDataSource() {
        if (dataSource != null) dataSource.close();
    }

    @BeforeEach
    public void setup() {
        db = new PostgresFlowDatabase(dataSource, mapper);

        TestJobFactory factory = new TestJobFactory();
        factory.register(MathJob.class, new MathJob());
        factory.register(CheckpointJob.class, new CheckpointJob());
        factory.register(AwaitEventJob.class, new AwaitEventJob());

        sqlFlow = new SqlFlow(db, factory, mapper);

        // Initial setup
        sqlFlow.createQueue(QUEUE);
        sqlFlow.registerTask(new TaskRegistrationOptions("math-task", 3), MathJob.class);
        sqlFlow.registerTask(new TaskRegistrationOptions("checkpoint-task", 3), CheckpointJob.class);
        sqlFlow.registerTask(new TaskRegistrationOptions("event-task", 3), AwaitEventJob.class);
    }

    @Test
    @Order(1)
    public void testHappyPath_ExecutionCompletes() throws Exception {
        // 1. Spawn a new Task
        SpawnOptions options = new SpawnOptions(QUEUE, 3, null, null);
        SpawnResult spawnResult = sqlFlow.spawn(options, "math-task", new MathParams(10, 20));

        assertNotNull(spawnResult.taskId());

        // 2. Simulate a Worker process claiming the task
        sqlFlow.workBatch(QUEUE, WORKER_ID, 60, 5);

        // 3. Check if Status is Completed
        String status = getTaskStatus(spawnResult.taskId());
        assertEquals("completed", status, "Task sollte nach erfolgreicher Ausführung 'completed' sein.");
    }

    @Test
    @Order(2)
    public void testCheckpointing_RecoversFromCrash() throws Exception {
        CheckpointJob.step1ExecutionCount = 0;
        CheckpointJob.shouldCrash = true;

        SpawnOptions options = new SpawnOptions(QUEUE, 3, null, null);
        SpawnResult spawnResult = sqlFlow.spawn(options, "checkpoint-task", new OrderParams("ORD-123"));

        // Lauf 1: Task starts and crashes
        sqlFlow.workBatch(QUEUE, WORKER_ID, 60, 5);

        // State should be failed
        assertEquals("failed", getTaskStatus(spawnResult.taskId()));
        assertEquals(1, CheckpointJob.step1ExecutionCount, "Step 1 sollte beim ersten Mal ausgeführt worden sein.");

        // Now reset it to pending
        resetTaskStatusToPending(spawnResult.taskId());

        // Lauf 2: Start it again
        sqlFlow.workBatch(QUEUE, WORKER_ID, 60, 5);

        // Status sollte nun 'completed' sein.
        assertEquals("completed", getTaskStatus(spawnResult.taskId()));

        // And check if we skipped step 1
        assertEquals(1, CheckpointJob.step1ExecutionCount, "Step 1 durfte beim Retry wegen des Checkpoints nicht nochmal ausgeführt werden.");
    }

    @Test
    @Order(3)
    public void testEventSuspension_ResumesWhenEventEmitted() throws Exception {
        SpawnOptions options = new SpawnOptions(QUEUE, 3, null, null);
        SpawnResult spawnResult = sqlFlow.spawn(options, "event-task", new OrderParams("999"));

        // 1. Worker processes Task -> Task detects, that no Event exists -> throws SuspendTaskException
        // claimTimeout set to 1 second, so the lease expires and we can test it
        sqlFlow.workBatch(QUEUE, WORKER_ID, 1, 5);

        // Task throws a SuspendedTaskException, but isn't failed, but claimed until the lease times out.
        assertEquals("claimed", getTaskStatus(spawnResult.taskId()));

        // 2. Emit the Event
        EmitEventOptions eventOptions = new EmitEventOptions(QUEUE);

        sqlFlow.emitEvent(eventOptions, "payment-999", new PaymentEvent(true, "TX-ABC"));

        // 3. Wait until Lease has been timed out (claimTimeout was set to 1 Second)
        Thread.sleep(1100);

        // 4. Worker processes the task again, this time we've got an event.
        sqlFlow.workBatch(QUEUE, WORKER_ID, 60, 5);

        // Task should have parsed the event and finished successfully
        assertEquals("completed", getTaskStatus(spawnResult.taskId()));
    }

    @Test
    @Order(4)
    public void testCancelTask() throws Exception {
        SpawnOptions options = new SpawnOptions(QUEUE, 3, null, null);
        SpawnResult spawnResult = sqlFlow.spawn(options, "math-task", new MathParams(5, 5));

        // Cancel Task, BEFORE the Worker processed it
        CancelTaskOptions cancelOptions = new CancelTaskOptions(QUEUE);
        sqlFlow.cancelTask(cancelOptions, spawnResult.taskId());

        assertEquals("cancelled", getTaskStatus(spawnResult.taskId()));

        // The worker should now ignore the task
        sqlFlow.workBatch(QUEUE, WORKER_ID, 60, 5);

        // State may not have changed
        assertEquals("cancelled", getTaskStatus(spawnResult.taskId()));
    }

    private String getTaskStatus(String taskId) throws SQLException {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement("SELECT status FROM ssf.tasks WHERE task_id = ?")) {
            stmt.setObject(1, java.util.UUID.fromString(taskId));
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) return rs.getString(1);
            }
        }
        throw new IllegalStateException("Task was not found");
    }

    private void resetTaskStatusToPending(String taskId) throws SQLException {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(
                     "UPDATE ssf.tasks SET status = 'pending', locked_until = NULL, worker_id = NULL WHERE task_id = ?")) {
            stmt.setObject(1, java.util.UUID.fromString(taskId));
            stmt.executeUpdate();
        }
    }
}