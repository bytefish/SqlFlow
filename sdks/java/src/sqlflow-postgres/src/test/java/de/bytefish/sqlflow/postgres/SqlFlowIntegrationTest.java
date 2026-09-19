package de.bytefish.sqlflow.postgres;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import de.bytefish.sqlflow.core.ISqlFlow;
import de.bytefish.sqlflow.core.SqlFlow;
import de.bytefish.sqlflow.core.infrastructure.QueueSignalOptions;
import de.bytefish.sqlflow.core.models.*;
import de.bytefish.sqlflow.core.workers.DefaultSqlFlowDispatcher;
import de.bytefish.sqlflow.core.workers.SqlFlowDispatcher;
import de.bytefish.sqlflow.core.workers.WorkerInstance;
import de.bytefish.sqlflow.core.workers.WorkerOptions;
import org.junit.jupiter.api.*;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

@Testcontainers
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class SqlFlowIntegrationTest {

    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:18-alpine")
            .withInitScript("ssf-postgres.sql");

    private static HikariDataSource dataSource;
    private ISqlFlow sqlFlow;
    private PostgresFlowDatabase db;
    private final ObjectMapper mapper = new ObjectMapper().registerModule(new JavaTimeModule());

    private String queueName;
    private String workerId;

    public record MathParams(int a, int b) {}
    public record OrderParams(String orderId) {}
    public record PaymentEvent(boolean success, String ref) {}

    private SqlFlowDispatcher dispatcher;
    private PostgresQueueSignalListener signals;

    @BeforeAll
    static void setupDataSource() {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(postgres.getJdbcUrl());
        config.setUsername(postgres.getUsername());
        config.setPassword(postgres.getPassword());
        config.setMaximumPoolSize(10);
        dataSource = new HikariDataSource(config);
    }

    @AfterAll
    static void teardownDataSource() {
        if (dataSource != null) dataSource.close();
    }

    @BeforeEach
    void setup() {
        // Jeder Test bekommt eine komplett eigene Queue!
        queueName = "test-queue-" + UUID.randomUUID().toString();
        workerId = "worker-" + UUID.randomUUID().toString();

        db = new PostgresFlowDatabase(dataSource, mapper);
        sqlFlow = new SqlFlow(db, mapper);

        sqlFlow.createQueue(queueName);

        signals = new PostgresQueueSignalListener(dataSource);
        dispatcher = new DefaultSqlFlowDispatcher(sqlFlow, signals, new QueueSignalOptions(Duration.ofSeconds(30)));
    }

    @AfterEach
    void cleanup() throws Exception {
        if (signals != null) {
            signals.close();
        }
    }


    @Test
    @Order(1)
    public void testBasicTaskExecution_Flow() throws Exception
    {
        CompletableFuture<Integer> completionSource = new CompletableFuture<>();

        sqlFlow.registerTask(
                new TaskRegistrationOptions("add-numbers", 3),
                (ctx, parameters) -> {
                    int a = parameters.get("a").asInt();
                    int b = parameters.get("b").asInt();
                    int sum = a + b;
                    completionSource.complete(sum);
                    return sum;
                });

        SpawnResult spawnResult = sqlFlow.spawn(
                new SpawnOptions(queueName, 3, null, null),
                "add-numbers",
                new MathParams(10, 20));

        try (WorkerInstance worker = createWorker())
        {
            worker.start();

            Integer result = completionSource.get(5, TimeUnit.SECONDS);
            assertEquals(30, result);

            assertTrue(waitForTaskState(spawnResult.taskId(), "completed"), "Task should complete in DB");
        }
    }

    @Test
    @Order(2)
    public void testCheckpointing_RecoversFromCrash() throws Exception
    {
        AtomicInteger step1Count = new AtomicInteger();
        AtomicBoolean shouldCrash = new AtomicBoolean(true);
        CompletableFuture<Void> completed = new CompletableFuture<>();

        sqlFlow.registerTask(
                new TaskRegistrationOptions("checkpoint-task", 3),
                (ctx, parameters) -> {
                    ctx.step("charge-card", () -> step1Count.incrementAndGet());

                    if (shouldCrash.getAndSet(false)) {
                        throw new RuntimeException("Simulated crash after checkpoint.");
                    }

                    completed.complete(null);
                    return "ORDER_COMPLETED";
                });

        SpawnResult spawnResult = sqlFlow.spawn(
                new SpawnOptions(queueName, 3, null, null),
                "checkpoint-task",
                new OrderParams("ORD-123"));

        try (WorkerInstance worker = createWorker())
        {
            worker.start();

            completed.get(10, TimeUnit.SECONDS);

            assertEquals(1, step1Count.get(), "Step 1 should execute only once because it was checkpointed.");

            assertTrue(waitForTaskState(spawnResult.taskId(), "completed"), "Task should reach 'completed' state in the database.");
            assertEquals("completed", getTaskState(spawnResult.taskId()));
        }
    }

    @Test
    @Order(3)
    public void testEventSuspension_ResumesWhenEventEmitted() throws Exception
    {
        CompletableFuture<String> paymentRef = new CompletableFuture<>();

        sqlFlow.registerTask(
                new TaskRegistrationOptions("event-task", 3),
                (ctx, parameters) -> {
                    String orderId = parameters.get("orderId").asText();

                    Optional<PaymentEvent> payment = ctx.awaitEvent(
                            "payment-" + orderId,
                            "wait-for-payment",
                            null,
                            PaymentEvent.class);

                    if (payment.isPresent() && payment.get().success()) {
                        paymentRef.complete(payment.get().ref());
                        return "PAID_" + payment.get().ref();
                    }
                    return "FAILED";
                });

        SpawnResult spawnResult = sqlFlow.spawn(
                new SpawnOptions(queueName, 3, null, null),
                "event-task",
                new OrderParams("999"));

        try (WorkerInstance worker = createWorker())
        {
            worker.start();

            assertTrue(waitForTaskState(spawnResult.taskId(), "sleeping"), "Task should be sleeping waiting for event.");

            sqlFlow.emitEvent(
                    new EmitEventOptions(queueName),
                    "payment-999",
                    new PaymentEvent(true, "TX-12345"));

            String ref = paymentRef.get(10, TimeUnit.SECONDS);
            assertEquals("TX-12345", ref);

            assertTrue(waitForTaskState(spawnResult.taskId(), "completed"), "Task should resume and complete after event emission.");
        }
    }

    @Test
    @Order(4)
    public void testCancelTask() throws Exception {
        SpawnOptions options = new SpawnOptions(queueName, 3, null, null);
        SpawnResult spawnResult = sqlFlow.spawn(options, "dummy-task", new MathParams(5, 5));

        sqlFlow.cancelTask(new CancelTaskOptions(queueName), spawnResult.taskId());

        assertEquals("cancelled", getTaskState(spawnResult.taskId()));
    }

    private WorkerInstance createWorker()
    {
        return new WorkerInstance(
                WorkerOptions.builder()
                        .workerId(workerId)
                        .queue(queueName)
                        .claimTimeout(120)
                        .batchSize(1)
                        .concurrency(1)
                        .build(),
                dispatcher);
    }

    private boolean waitForTaskState(String taskId, String targetState) throws Exception {
        for (int i = 0; i < 20; i++) {
            if (targetState.equals(getTaskState(taskId))) {
                return true;
            }
            Thread.sleep(250);
        }
        return false;
    }

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