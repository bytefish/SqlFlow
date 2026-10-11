package de.bytefish.sqlflow.sqlserver;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import de.bytefish.sqlflow.core.ISqlFlow;
import de.bytefish.sqlflow.core.SqlFlow;
import de.bytefish.sqlflow.core.infrastructure.*;
import de.bytefish.sqlflow.core.models.*;
import de.bytefish.sqlflow.core.workers.DefaultSqlFlowDispatcher;
import de.bytefish.sqlflow.core.workers.SqlFlowDispatcher;
import de.bytefish.sqlflow.core.workers.WorkerInstance;
import de.bytefish.sqlflow.core.workers.WorkerOptions;
import de.bytefish.sqlflow.nats.NatsEventPublisher;
import de.bytefish.sqlflow.nats.NatsJobPublisher;
import de.bytefish.sqlflow.nats.NatsQueueSignalListener;
import io.nats.client.Connection;
import io.nats.client.Nats;
import org.junit.jupiter.api.*;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import de.bytefish.sqlflow.postgres.PostgresFlowDatabase;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

@Testcontainers
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class SqlFlowIntegrationTest {

    private NatsQueueSignalListener signals;
    private SqlFlowDispatcher dispatcher;

    // 1. Database Container
    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>(
            "postgres:16-alpine"
    )
            .withDatabaseName("SqlFlow")
            .withUsername("postgres")
            .withPassword("SuperStrongPasswort@!")
            .waitingFor(
                    Wait.forLogMessage(".*database system is ready to accept connections.*\\s", 2)
            )
            .withStartupTimeout(Duration.ofMinutes(2));

    // 2. NATS JetStream Container
    @Container
    static GenericContainer<?> natsContainer = new GenericContainer<>("nats:latest")
            .withExposedPorts(4222)
            .withCommand("-js"); // Crucial: Enable JetStream for durable queues

    private PostgresFlowDatabase db;
    private static HikariDataSource dataSource;
    private static Connection natsConnection;

    private ISqlFlow sqlFlow;
    private final ObjectMapper mapper = new ObjectMapper().registerModule(new JavaTimeModule());

    private static final String TENANT_ID = "tenant-test";
    private static final String QUEUE = "test-queue";
    private static final String WORKER_ID = "test-worker-1";

    // --- Dummy Models ---
    public record MathParams(int a, int b) {
    }

    public record OrderParams(String orderId) {
    }

    public record PaymentEvent(boolean success, String ref) {
    }

    @BeforeAll
    static void setupBeforeAll() throws Exception {
        initializeDatabase();
        initializeDataSource();
        initializeNats();
    }

    static void initializeDatabase() throws Exception {
        Path sqlFilePath = Paths.get("../../../../sql/ssf-postgres-minimal.sql").normalize().toAbsolutePath();

        if (!Files.exists(sqlFilePath)) {
            sqlFilePath = Paths.get("sql/ssf-postgres-minimal.sql").normalize().toAbsolutePath();
        }

        if (!Files.exists(sqlFilePath)) {
            throw new IllegalStateException("SQL script not found at: " + sqlFilePath);
        }

        postgres.copyFileToContainer(
                org.testcontainers.utility.MountableFile.forHostPath(sqlFilePath),
                "/tmp/ssf-postgres-minimal.sql"
        );

        org.testcontainers.containers.Container.ExecResult result = postgres.execInContainer(
                "psql",
                "-U", postgres.getUsername(),
                "-d", postgres.getDatabaseName(),
                "-f", "/tmp/ssf-postgres-minimal.sql"
        );

        if (result.getExitCode() != 0) {
            throw new IllegalStateException(
                    "SQL initialization failed:\n"
                            + result.getStdout()
                            + "\n"
                            + result.getStderr()
            );
        }
    }

    static void initializeDataSource() {
        HikariConfig config = new HikariConfig();

        config.setDriverClassName("org.postgresql.Driver");
        config.setJdbcUrl(postgres.getJdbcUrl());
        config.setUsername(postgres.getUsername());
        config.setPassword(postgres.getPassword());
        config.setMaximumPoolSize(5);

        dataSource = new HikariDataSource(config);
    }

    static void initializeNats() throws Exception {
        String natsUrl = "nats://" + natsContainer.getHost() + ":" + natsContainer.getMappedPort(4222);
        natsConnection = Nats.connect(natsUrl);
    }

    @AfterAll
    static void teardownAll() throws Exception {
        if (dataSource != null) dataSource.close();
        if (natsConnection != null) natsConnection.close();
    }

    @BeforeEach
    void setup() {
        db = new PostgresFlowDatabase(dataSource, mapper);

        // 1. Initialize the Base Database Publishers
        IEventPublisher dbEventPublisher = new EventPublisher(db, mapper);
        IJobPublisher dbJobPublisher = new JobPublisher(db, mapper);

        // 2. Wrap them with NATS JetStream Decorators
        IEventPublisher natsEventPublisher = new NatsEventPublisher(dbEventPublisher, natsConnection);
        IJobPublisher natsJobPublisher = new NatsJobPublisher(dbJobPublisher, natsConnection);

        // 3. Inject into the SqlFlow Client
        sqlFlow = new SqlFlow(db, mapper, natsEventPublisher, natsJobPublisher);

        sqlFlow.createQueue(TENANT_ID, QUEUE);

        // 4. Set up the NATS Signal Listener for the Dispatcher
        signals = new NatsQueueSignalListener(natsConnection);

        dispatcher = new DefaultSqlFlowDispatcher(sqlFlow, signals, new QueueSignalOptions(Duration.ofSeconds(30)));
    }

    @AfterEach
    void cleanup() throws Exception {
        if (signals != null) {
            signals.close();
        }
    }

    private WorkerInstance createWorker() {
        WorkerInstance worker = new WorkerInstance(
                WorkerOptions.builder()
                        .tenantId(TENANT_ID)
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

    @Test
    @Order(1)
    public void testBasicTaskExecution_Flow() throws Exception {
        // ARRANGE
        CompletableFuture<Integer> completionSource = new CompletableFuture<>();

        sqlFlow.registerTask(new TaskRegistrationOptions(TENANT_ID, "add-numbers", 3), (ctx, parameters) -> {
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
        SpawnOptions options = new SpawnOptions(TENANT_ID, QUEUE, 3, null, null);
        sqlFlow.spawn(options, "add-numbers", new MathParams(10, 20));

        WorkerInstance worker = createWorker();

        try {
            Integer result = completionSource.get(5, TimeUnit.SECONDS);

            assertEquals(30, result, "The worker should have summed 10 + 20 to get 30.");
        } finally {
            worker.close();
        }
    }

    @Test
    @Order(2)
    public void testCheckpointing_RecoversFromCrash() throws Exception {
        // ARRANGE
        AtomicInteger step1Count = new AtomicInteger(0);
        AtomicBoolean shouldCrash = new AtomicBoolean(true);

        sqlFlow.registerTask(new TaskRegistrationOptions(TENANT_ID, "checkpoint-task", 3), (ctx, parameters) -> {
            ctx.step("charge-card", () -> {
                step1Count.incrementAndGet();
            });

            if (shouldCrash.getAndSet(false)) {
                throw new RuntimeException("Simulated crash right after Step 1!");
            }
            return "ORDER_COMPLETED";
        });

        SpawnOptions options = new SpawnOptions(TENANT_ID, QUEUE, 3, null, null);
        SpawnResult spawnResult = sqlFlow.spawn(options, "checkpoint-task", new OrderParams("ORD-123"));

        // ACT: Run worker until it processes the task (which will crash once, then retry)
        WorkerInstance worker = createWorker();

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

        // ASSERT
        assertTrue(isCompleted, "Task should ultimately complete successfully.");
        assertEquals(1, step1Count.get(), "Step 1 should not execute again on the retry due to checkpoint.");
    }

    @Test
    @Order(3)
    public void testEventSuspension_ResumesWhenEventEmitted() throws Exception {
        // ARRANGE
        CompletableFuture<String> paymentRef = new CompletableFuture<>();

        sqlFlow.registerTask(new TaskRegistrationOptions(TENANT_ID, "event-task", 3), (ctx, parameters) -> {
            String orderId = parameters.get("orderId").asText();

            Optional<PaymentEvent> payment = ctx.awaitEvent(
                    "payment-" + orderId, "wait-for-payment", null, PaymentEvent.class
            );

            if (payment.isPresent() && payment.get().success()) {
                paymentRef.complete(payment.get().ref());
                return "PAID_" + payment.get().ref();
            }
            return "FAILED";
        });

        SpawnOptions options = new SpawnOptions(TENANT_ID, QUEUE, 3, null, null);

        SpawnResult spawnResult = sqlFlow.spawn(options, "event-task", new OrderParams("999"));

        WorkerInstance worker = createWorker();

        // Wait for the task to be suspended (state = 'sleeping')
        for (int i = 0; i < 10; i++) {
            if ("sleeping".equals(getTaskState(spawnResult.taskId()))) {
                break;
            }
            Thread.sleep(300);
        }

        assertEquals("sleeping", getTaskState(spawnResult.taskId()), "Task should be sleeping waiting for event.");

        // ACT: Emit the event to wake it up. This routes through NatsEventPublisher.
        sqlFlow.emitEvent(new EmitEventOptions(TENANT_ID, QUEUE), "payment-999", new PaymentEvent(true, "TX-12345"));

        // Wait for completion signaled by the CompletableFuture inside the lambda
        String ref = paymentRef.get(5, TimeUnit.SECONDS);

        boolean completed = false;

        for (int i = 0; i < 20; i++) {
            if ("completed".equals(getTaskState(spawnResult.taskId()))) {
                completed = true;
                break;
            }
            Thread.sleep(250);
        }

        assertEquals("TX-12345", ref);
        assertTrue(completed, "Task should eventually complete.");

        worker.close();
    }

    @Test
    @Order(4)
    public void testCancelTask() throws Exception {
        SpawnOptions options = new SpawnOptions(TENANT_ID, QUEUE, 3, null, null);
        SpawnResult spawnResult = sqlFlow.spawn(options, "dummy-task", new MathParams(5, 5));

        // ACT: Cancel task before it is picked up
        sqlFlow.cancelTask(new CancelTaskOptions(TENANT_ID, QUEUE), spawnResult.taskId());

        // ASSERT
        assertEquals("cancelled", getTaskState(spawnResult.taskId()));
    }

    // --- Helper to read DB state directly ---

    private String getTaskState(String taskId) throws SQLException {
        try (java.sql.Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(
                     "SELECT state FROM ssf.tasks WHERE tenant_id = ? AND task_id = ?::uuid"
             )) {

            stmt.setString(1, TENANT_ID);
            stmt.setString(2, taskId);

            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return rs.getString(1);
                }
            }
        }

        throw new IllegalStateException("Task not found");
    }
}