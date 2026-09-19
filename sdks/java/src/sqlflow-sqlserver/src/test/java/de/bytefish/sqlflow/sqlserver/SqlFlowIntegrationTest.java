package de.bytefish.sqlflow.sqlserver;

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
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.containers.MSSQLServerContainer;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.MountableFile;

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

    private SqlServerQueueSignalListener signals;

    private SqlFlowDispatcher dispatcher;

    @Container
    static MSSQLServerContainer sqlServer = (MSSQLServerContainer) new MSSQLServerContainer(
            "mcr.microsoft.com/mssql/server:2022-latest"
    ).acceptLicense()
            .withPassword("SuperStrongPasswort@!")
            .waitingFor(
                    Wait.forLogMessage(
                            ".*SQL Server is now ready for client connections.*\\s",
                            1
                    )
            )
            .withStartupTimeout(Duration.ofMinutes(2));

    private SqlServerFlowDatabase db;
    private static HikariDataSource dataSource;
    private ISqlFlow sqlFlow;
    private final ObjectMapper mapper = new ObjectMapper().registerModule(new JavaTimeModule());

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
    }

    static void initializeDatabase() throws Exception {
        sqlServer.copyFileToContainer(
                MountableFile.forClasspathResource("ssf-sqlserver.sql"),
                "/tmp/ssf-sqlserver.sql"
        );

        org.testcontainers.containers.Container.ExecResult result = sqlServer.execInContainer(
                "/bin/bash",
                "-c",
                """
                        SQLCMD=$(find /opt/mssql-tools*/bin/sqlcmd -type f -print -quit)
                        
                        "$SQLCMD" \
                          -S localhost \
                          -U "$1" \
                          -P "$2" \
                          -C \
                          -b \
                          -i /tmp/ssf-sqlserver.sql
                        """,
                "sql-init",
                sqlServer.getUsername(),
                sqlServer.getPassword()
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

        config.setDriverClassName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
        config.setJdbcUrl(sqlServer.getJdbcUrl() + ";databaseName=SqlFlow;encrypt=false;trustServerCertificate=true");
        config.setUsername(sqlServer.getUsername());
        config.setPassword(sqlServer.getPassword());
        config.setMaximumPoolSize(5);

        dataSource = new HikariDataSource(config);
    }

    @AfterAll
    static void teardownDataSource() {
        if (dataSource != null) dataSource.close();
    }

    @BeforeEach
    void setup() {
        db = new SqlServerFlowDatabase(dataSource, mapper);

        sqlFlow = new SqlFlow(db, mapper);

        sqlFlow.createQueue(QUEUE);

        signals = new SqlServerQueueSignalListener(dataSource);

        dispatcher = new DefaultSqlFlowDispatcher(sqlFlow, signals, new QueueSignalOptions(Duration.ofSeconds(30)));
    }

    @AfterEach
    void cleanup() throws Exception {
        if (signals != null) {
            signals.close();
        }
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

        WorkerInstance worker =
                createWorker();

        try
        {
            Integer result =
                    completionSource.get(
                            5,
                            TimeUnit.SECONDS);

            assertEquals(
                    30,
                    result,
                    "The worker should have summed 10 + 20 to get 30.");
        }
        finally
        {
            worker.close();
        }
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

        WorkerInstance worker = createWorker();

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

        boolean completed = false;

        for (int i = 0; i < 20; i++)
        {
            if ("completed".equals(getTaskState(spawnResult.taskId())))
            {
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
             PreparedStatement stmt = conn.prepareStatement(
                     "SELECT state FROM ssf.tasks WHERE task_id = ?"
             )) {

            stmt.setString(1, taskId);

            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return rs.getString(1);
                }
            }
        }

        throw new IllegalStateException("Task not found");
    }
}