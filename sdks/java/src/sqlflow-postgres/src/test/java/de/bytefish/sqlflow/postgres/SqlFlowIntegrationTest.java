package de.bytefish.sqlflow.postgres;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import de.bytefish.sqlflow.core.ISqlFlow;
import de.bytefish.sqlflow.core.SqlFlow;
import de.bytefish.sqlflow.core.models.*;
import de.bytefish.sqlflow.core.workers.SqlFlowWorker;
import de.bytefish.sqlflow.core.workers.WorkerOptions;
import org.junit.jupiter.api.*;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

@Testcontainers
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class SqlFlowIntegrationTest {

    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:15-alpine")
            .withInitScript("ssf-postgres.sql");

    private static HikariDataSource dataSource;
    private ISqlFlow sqlFlow;
    private PostgresFlowDatabase db;
    private final ObjectMapper mapper = new ObjectMapper().registerModule(new JavaTimeModule());

    private static final String QUEUE = "test-queue";
    private static final String WORKER_ID = "test-worker-1";

    // --- Dummy Models ---
    public record MathParams(int a, int b) {}
    public record OrderParams(String orderId) {}
    public record PaymentEvent(boolean success, String ref) {}

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
        sqlFlow = new SqlFlow(db, mapper);
        sqlFlow.createQueue(QUEUE);
    }

    @Test
    @Order(1)
    public void testBasicTaskExecution_Flow() throws Exception {
        // ARRANGE
        CompletableFuture<Integer> completionSource = new CompletableFuture<>();

        sqlFlow.registerTask(new TaskRegistrationOptions("add-numbers", 3), (ctx, parameters) -> {
            if (parameters == null || parameters.isNull()) {
                throw new IllegalStateException("Expected JSON parameters");
            }
            int a = parameters.get("a").asInt();
            int b = parameters.get("b").asInt();
            int sum = a + b;
            completionSource.complete(sum);
            return sum;
        });

        // ACT
        SpawnOptions options = new SpawnOptions(QUEUE, 3, null, null);
        sqlFlow.spawn(options, "add-numbers", new MathParams(10, 20));

        SqlFlowWorker worker = new SqlFlowWorker(WorkerOptions.builder()
                .workerId(WORKER_ID)
                .queue(QUEUE)
                .pollInterval(0.1) // Fast polling for tests
                .concurrency(1)
                .build(), sqlFlow);

        Thread workerThread = Thread.ofVirtual().start(worker);

        // Wait for task completion (or timeout)
        Integer result = completionSource.get(5, TimeUnit.SECONDS);
        worker.close();
        workerThread.join();

        // ASSERT
        assertEquals(30, result, "The worker should have summed 10 + 20 to get 30.");
    }

    @Test
    @Order(2)
    public void testCheckpointing_RecoversFromCrash() throws Exception {
        // ARRANGE
        AtomicInteger step1Count = new AtomicInteger(0);
        AtomicBoolean shouldCrash = new AtomicBoolean(true);

        sqlFlow.registerTask(new TaskRegistrationOptions("checkpoint-task", 3), (ctx, parameters) -> {
            ctx.step("charge-card", () -> {
                step1Count.incrementAndGet();
            });

            if (shouldCrash.getAndSet(false)) {
                throw new RuntimeException("Simulated crash right after Step 1!");
            }
            return "ORDER_COMPLETED";
        });

        SpawnOptions options = new SpawnOptions(QUEUE, 3, null, null);
        SpawnResult spawnResult = sqlFlow.spawn(options, "checkpoint-task", new OrderParams("ORD-123"));

        // ACT: Run worker until it processes the task (which will crash once, then retry)
        SqlFlowWorker worker = new SqlFlowWorker(WorkerOptions.builder()
                .workerId(WORKER_ID)
                .queue(QUEUE)
                .pollInterval(0.2)
                .concurrency(1)
                .build(), sqlFlow);

        Thread workerThread = Thread.ofVirtual().start(worker);

        // Wait until task reaches 'completed' state in DB (polling DB state)
        boolean isCompleted = false;
        for (int i = 0; i < 20; i++) {
            if ("completed".equals(getTaskState(spawnResult.taskId()))) {
                isCompleted = true;
                break;
            }
            Thread.sleep(500);
        }

        worker.close();
        workerThread.join();

        // ASSERT
        assertTrue(isCompleted, "Task should ultimately complete successfully.");
        // The core check: Step 1 must have run exactly ONCE because of the checkpoint!
        assertEquals(1, step1Count.get(), "Step 1 should not execute again on the retry due to checkpoint.");
    }

    @Test
    @Order(3)
    public void testEventSuspension_ResumesWhenEventEmitted() throws Exception {
        // ARRANGE
        CompletableFuture<String> paymentRef = new CompletableFuture<>();

        sqlFlow.registerTask(new TaskRegistrationOptions("event-task", 3), (ctx, parameters) -> {
            String orderId = parameters.get("orderId").asText();

            // This throws SuspendTaskException internally if event is missing!
            Optional<PaymentEvent> payment = ctx.awaitEvent(
                    "payment-" + orderId, "wait-for-payment", null, PaymentEvent.class
            );

            if (payment.isPresent() && payment.get().success()) {
                paymentRef.complete(payment.get().ref());
                return "PAID_" + payment.get().ref();
            }
            return "FAILED";
        });

        SpawnOptions options = new SpawnOptions(QUEUE, 3, null, null);

        SpawnResult spawnResult = sqlFlow.spawn(options, "event-task", new OrderParams("999"));

        SqlFlowWorker worker = new SqlFlowWorker(WorkerOptions.builder()
                .workerId(WORKER_ID)
                .queue(QUEUE)
                .pollInterval(0.2)
                .concurrency(1)
                .build(), sqlFlow);

        Thread workerThread = Thread.ofVirtual().start(worker);

        // Wait for the task to be suspended (state = 'sleeping' inside ssf.runs due to await_event logic)
        for (int i = 0; i < 10; i++) {
            if ("sleeping".equals(getTaskState(spawnResult.taskId()))) {
                break;
            }
            Thread.sleep(300);
        }

        assertEquals("sleeping", getTaskState(spawnResult.taskId()), "Task should be sleeping waiting for event.");

        // ACT: Emit the event to wake it up
        sqlFlow.emitEvent(new EmitEventOptions(QUEUE), "payment-999", new PaymentEvent(true, "TX-12345"));

        // Wait for completion signaled by the CompletableFuture inside the lambda
        String ref = paymentRef.get(5, TimeUnit.SECONDS);

        worker.close();
        workerThread.join();

        // ASSERT
        assertEquals("TX-12345", ref);
        assertEquals("completed", getTaskState(spawnResult.taskId()));
    }

    @Test
    @Order(4)
    public void testCancelTask() throws Exception {
        SpawnOptions options = new SpawnOptions(QUEUE, 3, null, null);
        SpawnResult spawnResult = sqlFlow.spawn(options, "dummy-task", new MathParams(5, 5));

        // ACT: Cancel task before it is picked up
        sqlFlow.cancelTask(new CancelTaskOptions(QUEUE), spawnResult.taskId());

        // ASSERT
        assertEquals("cancelled", getTaskState(spawnResult.taskId()));
    }

    // --- Helper to read DB state directly ---

    private String getTaskState(String taskId) throws SQLException {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement("SELECT state FROM ssf.tasks WHERE task_id = ?")) {
            stmt.setObject(1, java.util.UUID.fromString(taskId));
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) return rs.getString(1);
            }
        }
        throw new IllegalStateException("Task not found");
    }
}