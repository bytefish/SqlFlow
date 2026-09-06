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

    private static final String QUEUE = "test-queue";
    private static final String WORKER_ID = "test-worker-1";

    // --- Dummy Models ---
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
        config.setMaximumPoolSize(5);
        dataSource = new HikariDataSource(config);
    }

    @AfterAll
    static void teardownDataSource() {
        if (dataSource != null) dataSource.close();
    }

    @BeforeEach
    void setup() {
        db = new PostgresFlowDatabase(dataSource, mapper);

        sqlFlow = new SqlFlow(db, mapper);

        sqlFlow.createQueue(QUEUE);

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
        // ARRANGE

        CompletableFuture<Integer> completionSource =
                new CompletableFuture<>();

        sqlFlow.registerTask(
                new TaskRegistrationOptions(
                        "add-numbers",
                        3),
                (ctx, parameters) ->
                {
                    int a =
                            parameters.get("a").asInt();

                    int b =
                            parameters.get("b").asInt();

                    int sum =
                            a + b;

                    completionSource.complete(
                            sum);

                    return sum;
                });

        SpawnResult spawnResult =
                sqlFlow.spawn(
                        new SpawnOptions(
                                QUEUE,
                                3,
                                null,
                                null),
                        "add-numbers",
                        new MathParams(
                                10,
                                20));

        try (
                PostgresQueueSignalListener signals =
                        new PostgresQueueSignalListener(
                                dataSource)
        )
        {
            DefaultSqlFlowDispatcher dispatcher =
                    new DefaultSqlFlowDispatcher(
                            sqlFlow,
                            signals,
                            new QueueSignalOptions(Duration.ofSeconds(30)));

            WorkerInstance worker =
                    new WorkerInstance(
                            WorkerOptions.builder()
                                    .workerId(
                                            WORKER_ID)
                                    .queue(
                                            QUEUE)
                                    .concurrency(
                                            1)
                                    .batchSize(
                                            1)
                                    .claimTimeout(
                                            30)
                                    .build(),
                            dispatcher);

            worker.start();

            try
            {
                Integer result =
                        completionSource.get(
                                5,
                                TimeUnit.SECONDS);

                assertEquals(
                        30,
                        result);
            }
            finally
            {
                worker.close();
            }
        }
    }

    @Test
    @Order(2)
    public void testCheckpointing_RecoversFromCrash()
            throws Exception
    {
        // ARRANGE

        AtomicInteger step1Count =
                new AtomicInteger();

        AtomicBoolean shouldCrash =
                new AtomicBoolean(true);

        CompletableFuture<Void> completed =
                new CompletableFuture<>();

        sqlFlow.registerTask(
                new TaskRegistrationOptions(
                        "checkpoint-task",
                        3),
                (ctx, parameters) ->
                {
                    ctx.step("charge-card", () -> step1Count.incrementAndGet());

                    /*
                     * Simulate a crash immediately after the step was
                     * checkpointed.
                     */
                    if (shouldCrash.getAndSet(false))
                    {
                        throw new RuntimeException("Simulated crash after checkpoint.");
                    }

                    completed.complete(null);

                    return "ORDER_COMPLETED";
                });

        SpawnResult spawnResult =
                sqlFlow.spawn(
                        new SpawnOptions(
                                QUEUE,
                                3,
                                null,
                                null),
                        "checkpoint-task",
                        new OrderParams(
                                "ORD-123"));

        try (
                PostgresQueueSignalListener signals =
                        new PostgresQueueSignalListener(
                                dataSource)
        )
        {
            DefaultSqlFlowDispatcher dispatcher =
                    new DefaultSqlFlowDispatcher(
                            sqlFlow,
                            signals,
                            new QueueSignalOptions(Duration.ofSeconds(30)));

            try (
                    WorkerInstance worker =
                            new WorkerInstance(
                                    WorkerOptions.builder()
                                            .workerId(WORKER_ID)
                                            .queue(QUEUE)
                                            .concurrency(1)
                                            .batchSize(1)
                                            .claimTimeout(30)
                                            .build(),
                                    dispatcher)
            )
            {
                worker.start();

                // Wait until the retry succeeds.
                completed.get(10, TimeUnit.SECONDS);

                // ASSERT

                assertEquals(
                        1,
                        step1Count.get(),
                        "Step 1 should execute only once because it was checkpointed.");

                assertEquals(
                        "completed",
                        getTaskState(
                                spawnResult.taskId()));
            }
        }
    }

    @Test
    @Order(3)
    public void testEventSuspension_ResumesWhenEventEmitted()
            throws Exception
    {
        CompletableFuture<String> paymentRef =
                new CompletableFuture<>();

        sqlFlow.registerTask(
                new TaskRegistrationOptions(
                        "event-task",
                        3),
                (ctx, parameters) ->
                {
                    String orderId =
                            parameters.get("orderId")
                                    .asText();

                    Optional<PaymentEvent> payment =
                            ctx.awaitEvent(
                                    "payment-" + orderId,
                                    "wait-for-payment",
                                    null,
                                    PaymentEvent.class);

                    if (payment.isPresent()
                            && payment.get().success())
                    {
                        paymentRef.complete(
                                payment.get().ref());

                        return "PAID_"
                                + payment.get().ref();
                    }

                    return "FAILED";
                });

        SpawnResult spawnResult =
                sqlFlow.spawn(
                        new SpawnOptions(
                                QUEUE,
                                3,
                                null,
                                null),
                        "event-task",
                        new OrderParams("999"));

        WorkerInstance worker = createWorker();

        try
        {
            //
            // Wait until the workflow goes to sleep.
            //
            for (int i = 0; i < 10; i++)
            {
                if ("sleeping".equals(
                        getTaskState(
                                spawnResult.taskId())))
                {
                    break;
                }

                Thread.sleep(300);
            }

            assertEquals(
                    "sleeping",
                    getTaskState(
                            spawnResult.taskId()),
                    "Task should be sleeping waiting for event.");

            //
            // Wake up workflow.
            //
            sqlFlow.emitEvent(
                    new EmitEventOptions(
                            QUEUE),
                    "payment-999",
                    new PaymentEvent(
                            true,
                            "TX-12345"));

            String ref =
                    paymentRef.get(
                            10,
                            TimeUnit.SECONDS);

            assertEquals(
                    "TX-12345",
                    ref);

            //
            // Wait until task has completed.
            //
            boolean completed = false;

            for (int i = 0; i < 20; i++)
            {
                if ("completed".equals(
                        getTaskState(
                                spawnResult.taskId())))
                {
                    completed = true;
                    break;
                }

                Thread.sleep(250);
            }

            assertTrue(
                    completed,
                    "Task should resume after event emission.");

            assertEquals(
                    "completed",
                    getTaskState(
                            spawnResult.taskId()));
        }
        finally
        {
            worker.close();
        }
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


    private WorkerInstance createWorker()
    {
        WorkerInstance worker =
                new WorkerInstance(
                        WorkerOptions.builder()
                                .workerId(WORKER_ID)
                                .queue(QUEUE)
                                .claimTimeout(120)
                                .batchSize(1)
                                .concurrency(1)
                                .build(),
                        dispatcher);

        worker.start();

        return worker;
    }
}