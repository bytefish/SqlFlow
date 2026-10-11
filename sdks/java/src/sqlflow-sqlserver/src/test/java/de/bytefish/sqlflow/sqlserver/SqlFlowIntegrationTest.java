package de.bytefish.sqlflow.sqlserver;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import de.bytefish.sqlflow.core.ISqlFlow;
import de.bytefish.sqlflow.core.SqlFlow;
import de.bytefish.sqlflow.core.infrastructure.QueueSignalOptions;
import de.bytefish.sqlflow.core.models.CancelTaskOptions;
import de.bytefish.sqlflow.core.models.EmitEventOptions;
import de.bytefish.sqlflow.core.models.SpawnOptions;
import de.bytefish.sqlflow.core.models.SpawnResult;
import de.bytefish.sqlflow.core.models.TaskRegistrationOptions;
import de.bytefish.sqlflow.core.workers.DefaultSqlFlowDispatcher;
import de.bytefish.sqlflow.core.workers.QueueSignalListener;
import de.bytefish.sqlflow.core.workers.SqlFlowDispatcher;
import de.bytefish.sqlflow.core.workers.WorkerInstance;
import de.bytefish.sqlflow.core.workers.WorkerOptions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.MSSQLServerContainer;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.MountableFile;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class SqlFlowIntegrationTest {

    private static final String DATABASE_NAME = "SqlFlow";
    private static final String SA_PASSWORD = "SuperStrongPasswort@!";
    private static final String TENANT_ID = "tenant-test";
    private static final String QUEUE = "test-queue";
    private static final String WORKER_ID = "test-worker-1";

    @org.testcontainers.junit.jupiter.Container
    static MSSQLServerContainer<?> sqlServer = new MSSQLServerContainer<>(
            "mcr.microsoft.com/mssql/server:2022-latest"
    )
            .acceptLicense()
            .withPassword(SA_PASSWORD)
            .withStartupTimeout(Duration.ofMinutes(2));

    private static HikariDataSource dataSource;

    private final ObjectMapper mapper = new ObjectMapper()
            .registerModule(new JavaTimeModule());

    private SqlServerFlowDatabase db;
    private ISqlFlow sqlFlow;
    private QueueSignalListener signals;
    private SqlFlowDispatcher dispatcher;

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

    @AfterAll
    static void teardownAll() {
        if (dataSource != null) {
            dataSource.close();
        }
    }

    @BeforeEach
    void setup() {
        db = new SqlServerFlowDatabase(dataSource, mapper);
        sqlFlow = new SqlFlow(db, mapper);
        sqlFlow.createQueue(TENANT_ID, QUEUE);

        signals = new SqlServerQueueSignalListener(dataSource);
        dispatcher = new DefaultSqlFlowDispatcher(
                sqlFlow,
                signals,
                new QueueSignalOptions(Duration.ofSeconds(30))
        );
    }

    @AfterEach
    void cleanup() throws Exception {
        if (signals instanceof AutoCloseable closeable) {
            closeable.close();
        }
    }

    private static Path resolveSqlScript(String fileName) {
        Path sqlFilePath = Paths.get(
                "../../../../sql",
                fileName
        ).normalize().toAbsolutePath();

        if (!Files.exists(sqlFilePath)) {
            sqlFilePath = Paths.get(
                    "sql",
                    fileName
            ).normalize().toAbsolutePath();
        }

        if (!Files.exists(sqlFilePath)) {
            throw new IllegalStateException(
                    "SQL script not found: "
                            + fileName
                            + " at resolved path: "
                            + sqlFilePath
            );
        }

        return sqlFilePath;
    }

    static void initializeDatabase() throws Exception {
        Path minimalScript = resolveSqlScript("ssf-sqlserver-minimal.sql");
        Path signalingScript = resolveSqlScript("ssf-sqlserver-signaling.sql");

        sqlServer.copyFileToContainer(
                MountableFile.forHostPath(minimalScript),
                "/tmp/ssf-sqlserver-minimal.sql"
        );

        sqlServer.copyFileToContainer(
                MountableFile.forHostPath(signalingScript),
                "/tmp/ssf-sqlserver-signaling.sql"
        );

        executeSqlCommand(
                "master",
                "IF DB_ID(N'" + DATABASE_NAME + "') IS NULL CREATE DATABASE [" + DATABASE_NAME + "]",
                "Database creation"
        );

        executeSqlScript(
                "/tmp/ssf-sqlserver-minimal.sql",
                "Minimal SQL initialization"
        );

        executeSqlScript(
                "/tmp/ssf-sqlserver-signaling.sql",
                "Signaling SQL initialization"
        );
    }

    private static void executeSqlScript(
            String containerPath,
            String description) throws Exception {

        String command = buildSqlCmdPrefix(DATABASE_NAME)
                + " -b -i "
                + shellQuote(containerPath);

        executeContainerCommand(command, description);
    }

    private static void executeSqlCommand(
            String database,
            String sql,
            String description) throws Exception {

        String command = buildSqlCmdPrefix(database)
                + " -b -Q "
                + shellQuote(sql);

        executeContainerCommand(command, description);
    }

    private static String buildSqlCmdPrefix(String database) {
        return "SQLCMD=/opt/mssql-tools18/bin/sqlcmd; "
                + "if [ ! -x \"$SQLCMD\" ]; then SQLCMD=/opt/mssql-tools/bin/sqlcmd; fi; "
                + "if [ ! -x \"$SQLCMD\" ]; then "
                + "echo 'sqlcmd was not found in the SQL Server container' >&2; exit 127; fi; "
                + "\"$SQLCMD\""
                + " -S localhost"
                + " -U sa"
                + " -P " + shellQuote(SA_PASSWORD)
                + " -d " + shellQuote(database)
                + " -C";
    }

    private static void executeContainerCommand(
            String command,
            String description) throws Exception {

        Container.ExecResult result = sqlServer.execInContainer(
                "bash",
                "-c",
                command
        );

        if (result.getExitCode() != 0) {
            throw new IllegalStateException(
                    description
                            + " failed with exit code "
                            + result.getExitCode()
                            + ":\nSTDOUT:\n"
                            + result.getStdout()
                            + "\nSTDERR:\n"
                            + result.getStderr()
            );
        }
    }

    private static String shellQuote(String value) {
        return "'" + value.replace("'", "'\"'\"'") + "'";
    }

    static void initializeDataSource() {
        HikariConfig config = new HikariConfig();
        config.setDriverClassName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
        config.setJdbcUrl(sqlServer.getJdbcUrl() + ";databaseName=" + DATABASE_NAME);
        config.setUsername(sqlServer.getUsername());
        config.setPassword(sqlServer.getPassword());
        config.setMaximumPoolSize(5);

        dataSource = new HikariDataSource(config);
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
                dispatcher
        );

        worker.start();
        return worker;
    }

    @Test
    @Order(1)
    void testBasicTaskExecution_Flow() throws Exception {
        CompletableFuture<Integer> completionSource = new CompletableFuture<>();

        sqlFlow.registerTask(
                new TaskRegistrationOptions(TENANT_ID, "add-numbers", 3),
                (ctx, parameters) -> {
                    if (parameters == null || parameters.isNull()) {
                        throw new IllegalStateException("Expected JSON parameters");
                    }

                    int a = parameters.get("a").asInt();
                    int b = parameters.get("b").asInt();
                    int sum = a + b;
                    completionSource.complete(sum);
                    return sum;
                }
        );

        SpawnOptions options = new SpawnOptions(TENANT_ID, QUEUE, 3, null, null);
        sqlFlow.spawn(options, "add-numbers", new MathParams(10, 20));

        WorkerInstance worker = createWorker();

        try {
            Integer result = completionSource.get(5, TimeUnit.SECONDS);
            assertEquals(
                    30,
                    result,
                    "The worker should have summed 10 + 20 to get 30."
            );
        } finally {
            worker.close();
        }
    }

    @Test
    @Order(2)
    void testCheckpointing_RecoversFromCrash() throws Exception {
        AtomicInteger step1Count = new AtomicInteger(0);
        AtomicBoolean shouldCrash = new AtomicBoolean(true);

        sqlFlow.registerTask(
                new TaskRegistrationOptions(TENANT_ID, "checkpoint-task", 3),
                (ctx, parameters) -> {
                    ctx.step("charge-card", step1Count::incrementAndGet);

                    if (shouldCrash.getAndSet(false)) {
                        throw new RuntimeException(
                                "Simulated crash right after Step 1!"
                        );
                    }

                    return "ORDER_COMPLETED";
                }
        );

        SpawnOptions options = new SpawnOptions(TENANT_ID, QUEUE, 3, null, null);
        SpawnResult spawnResult = sqlFlow.spawn(
                options,
                "checkpoint-task",
                new OrderParams("ORD-123")
        );

        WorkerInstance worker = createWorker();

        try {
            boolean completed = waitForTaskState(
                    spawnResult.taskId(),
                    "completed",
                    20,
                    500
            );

            assertTrue(
                    completed,
                    "Task should ultimately complete successfully."
            );

            assertEquals(
                    1,
                    step1Count.get(),
                    "Step 1 should not execute again on retry due to checkpoint."
            );
        } finally {
            worker.close();
        }
    }

    @Test
    @Order(3)
    void testEventSuspension_ResumesWhenEventEmitted() throws Exception {
        CompletableFuture<String> paymentRef = new CompletableFuture<>();

        sqlFlow.registerTask(
                new TaskRegistrationOptions(TENANT_ID, "event-task", 3),
                (ctx, parameters) -> {
                    String orderId = parameters.get("orderId").asText();

                    Optional<PaymentEvent> payment = ctx.awaitEvent(
                            "payment-" + orderId,
                            "wait-for-payment",
                            null,
                            PaymentEvent.class
                    );

                    if (payment.isPresent() && payment.get().success()) {
                        paymentRef.complete(payment.get().ref());
                        return "PAID_" + payment.get().ref();
                    }

                    return "FAILED";
                }
        );

        SpawnOptions options = new SpawnOptions(TENANT_ID, QUEUE, 3, null, null);
        SpawnResult spawnResult = sqlFlow.spawn(
                options,
                "event-task",
                new OrderParams("999")
        );

        WorkerInstance worker = createWorker();

        try {
            assertTrue(
                    waitForTaskState(
                            spawnResult.taskId(),
                            "sleeping",
                            20,
                            500
                    ),
                    "Task should be sleeping while waiting for the event."
            );

            sqlFlow.emitEvent(
                    new EmitEventOptions(TENANT_ID, QUEUE),
                    "payment-999",
                    new PaymentEvent(true, "TX-12345")
            );

            String ref = paymentRef.get(10, TimeUnit.SECONDS);

            assertEquals("TX-12345", ref);
            assertTrue(
                    waitForTaskState(
                            spawnResult.taskId(),
                            "completed",
                            20,
                            500
                    ),
                    "Task should eventually complete."
            );
        } finally {
            worker.close();
        }
    }

    @Test
    @Order(4)
    void testCancelTask() throws Exception {
        SpawnOptions options = new SpawnOptions(TENANT_ID, QUEUE, 3, null, null);
        SpawnResult spawnResult = sqlFlow.spawn(
                options,
                "dummy-task",
                new MathParams(5, 5)
        );

        sqlFlow.cancelTask(
                new CancelTaskOptions(TENANT_ID, QUEUE),
                spawnResult.taskId()
        );

        assertEquals(
                "cancelled",
                getTaskState(spawnResult.taskId())
        );
    }

    private boolean waitForTaskState(
            String taskId,
            String expectedState,
            int attempts,
            long delayMillis) throws Exception {

        for (int i = 0; i < attempts; i++) {
            if (expectedState.equals(getTaskState(taskId))) {
                return true;
            }

            Thread.sleep(delayMillis);
        }

        return false;
    }

    private String getTaskState(String taskId) throws SQLException {
        try (java.sql.Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(
                     "SELECT state FROM ssf.tasks WHERE tenant_id = ? AND task_id = CAST(? AS UNIQUEIDENTIFIER)"
             )) {

            statement.setString(1, TENANT_ID);
            statement.setString(2, taskId);

            try (ResultSet resultSet = statement.executeQuery()) {
                if (resultSet.next()) {
                    return resultSet.getString(1);
                }
            }
        }

        throw new IllegalStateException("Task not found: " + taskId);
    }
}