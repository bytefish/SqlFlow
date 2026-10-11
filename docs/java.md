# Getting Started with the Java SDK

The SqlFlow SDK for Java is available in the Maven Central repository.

```xml
<dependencies>
    <!-- SqlFlow Core Module -->
    <dependency>
        <groupId>de.bytefish.sqlflow</groupId>
        <artifactId>sqlflow-core</artifactId>
        <version>1.5.0</version>
    </dependency>

    <!-- SqlFlow PostgreSQL Module -->
    <dependency>
        <groupId>de.bytefish.sqlflow</groupId>
        <artifactId>sqlflow-postgres</artifactId>
        <version>1.5.0</version>
    </dependency>
    
    <!-- SqlFlow SQL Server Module -->
    <dependency>
        <groupId>de.bytefish.sqlflow</groupId>
        <artifactId>sqlflow-sqlserver</artifactId>
        <version>1.5.0</version>
    </dependency>
        
    <!-- Optional: SqlFlow Nats Module -->
    <dependency>
        <groupId>de.bytefish.sqlflow</groupId>
        <artifactId>sqlflow-nats</artifactId>
        <version>1.5.0</version>
    </dependency>
</dependencies>
```
*Note: Only use the module for the database system you want!

## Building a Durable AI Agent

The classic examples for durable execution are usually e-commerce checkouts. But there's a rapidly growing use case developers are dealing with: Autonomous AI Agents. Building AI agents introduces severe state-management challenges:

1. **Slow API Calls:** LLM API calls are inherently slow, prone to timeouts, and expensive. If a server crashes while waiting 30 seconds for an AI generation, standard in-memory state is lost forever.
2. **Human-in-the-Loop:** You don't want an AI pushing code to production without human review. Agents need to pause execution, ask a human for permission, and resume only when approved (hours or days later).

Traditional approaches require you to build complex state machines, database polling loops, or heavy external infrastructure. With SqlFlow, we can write our agent as standard, sequential Java code. The framework will automatically checkpoint the state, sleep without blocking server threads, and wake up exactly where it left off.

### 1. The Domain Models

Let's define the data models representing our inputs, states, and final output using standard Java Records.

```java
public record AgentTask(
    @JsonProperty("issue_id") String issueId
) {}

public record Issue(
    @JsonProperty("stack_trace") String stackTrace
) {}

public record Solution(
    @JsonProperty("patched_code") String patchedCode
) {}

public record HumanApproval(
    @JsonProperty("approved") boolean approved,
    @JsonProperty("reason") String reason
) {}

public record AgentResult(
    @JsonProperty("success") boolean success,
    @JsonProperty("pull_request_url") String pullRequestUrl
) {}
```

### 2. The Autonomous Agent Job (The Core Concept)

We define our logic inside a `Job<T>`. The magic is in the **`ctx.step`** method: every time a step completes, its result is automatically checkpointed to the database. If the process crashes, the framework replays the job, skips the completed steps, and loads their results directly from the database.

Furthermore, we use **`ctx.awaitEvent`** to wait for human interaction. This instructs the engine to safely suspend the workflow state to the database and free up the worker thread entirely until an external system fires the webhook event.

```java
@Component
public class AutonomousAgentJob implements Job<AgentTask, AgentResult> {

    private static final Logger logger = LoggerFactory.getLogger(AutonomousAgentJob.class);

    private final LlmService llmService;
    private final GitHubService gitHubService;
    private final LocalNotificationService localNotificationService;

    public AutonomousAgentJob(LlmService llmService, GitHubService gitHubService, LocalNotificationService localNotificationService) {
        this.llmService = llmService;
        this.gitHubService = gitHubService;
        this.localNotificationService = localNotificationService;
    }

    @Override
    public AgentResult execute(TaskContext ctx, AgentTask task) throws Exception {
        logger.info("Agent starts researching ticket {} for tenant {}", task.issueId(), ctx.getTenantId());

        Issue bugReport = ctx.step("fetch-issue-context", Issue.class, () ->
                gitHubService.getIssueDetails(task.issueId())
        );

        boolean isApproved = false;
        int attempt = 0;
        String lastFeedback = "Initial Attempt";

        while (!isApproved && attempt < 3) {
            attempt++;
            String correlationId = ctx.getTaskId() + "-attempt-" + attempt;

            logger.info("Attempt {}/3: Generating a fix based on: {}", attempt, lastFeedback);

            final String currentFeedback = lastFeedback;

            Solution proposedFix = ctx.step("generate-code-fix-" + attempt, Solution.class, () ->
                    llmService.generateFix(bugReport.stackTrace(), currentFeedback)
            );

            ctx.step("notify-reviewer-" + attempt, () -> {
                gitHubService.requestHumanReview(task.issueId(), proposedFix, correlationId);

                localNotificationService.notifyReviewer(task.issueId(), correlationId);
            });

            logger.info("Review for {} has been requested. Agent goes idle and waits for the code review...", correlationId);

            Optional<JsonNode> reviewOpt = ctx.awaitEvent(
                    "agent-approval:" + ctx.getTenantId() + ":" + task.issueId() + ":" + correlationId,
                    "wait-for-human-review-" + attempt,
                    null,
                    JsonNode.class
            );

            if (reviewOpt.isPresent()) {
                JsonNode review = reviewOpt.get();
                isApproved = review.has("approved") && review.get("approved").asBoolean();
                lastFeedback = review.has("reason") ? review.get("reason").asText() : "No feedback has been given";

                if (!isApproved) {
                    logger.warn("Attempt {} has been rejected: {}", attempt, lastFeedback);
                }
            }
        }

        if (isApproved) {
            logger.info("Fix approved. Creating Pull Request...");

            String prUrl = ctx.step("create-pull-request", String.class, () ->
                    gitHubService.createPullRequest(task.issueId(), "apply-fix")
            );

            logger.info("Mission accomplished, the PR has been created: {}", prUrl);
            return new AgentResult(true, prUrl, null);

        } else {
            logger.error("Maximum number of attempts reached. Escalates ticket {} to a human.", task.issueId());

            ctx.step("notify-senior-developer", () -> {
                gitHubService.escalateToSenior(task.issueId(), "Agent didn't find a solution after 3 attempts.");
            });

            return new AgentResult(false, null, "Escalated to human supervisor after 3 failures.");
        }
    }
}
```

### 3. Dependency Injection & Background Workers (Spring Boot)

We use a Spring Boot `@Configuration` class to wire up our data source, the SqlFlow client, and map our Job to a background worker thread.

```java

@Configuration
public class SqlFlowConfiguration {

    @Bean(destroyMethod = "stop")
    public PostgreSQLContainer<?> postgresContainer() throws Exception {
        PostgreSQLContainer<?> postgres =
                new PostgreSQLContainer<>("postgres:18");

        postgres.start();

        initializeDatabase(postgres, "../../sql/ssf-postgres-minimal.sql");
        initializeDatabase(postgres, "../../sql/ssf-postgres-signaling.sql");

        return postgres;
    }

    private static void initializeDatabase(PostgreSQLContainer<?> postgres, String scriptPath) throws Exception {
        Path sqlFilePath = Paths.get(scriptPath).normalize().toAbsolutePath();

        if (!Files.exists(sqlFilePath)) {
            throw new IllegalStateException("SQL script not found at: " + sqlFilePath);
        }

        postgres.copyFileToContainer(
                org.testcontainers.utility.MountableFile.forHostPath(sqlFilePath),
                "/tmp/" + scriptPath
        );

        org.testcontainers.containers.Container.ExecResult result = postgres.execInContainer(
                "psql",
                "-U", postgres.getUsername(),
                "-d", postgres.getDatabaseName(),
                "-f", "/tmp/" + scriptPath
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

    @Bean
    public DataSource dataSource(
            PostgreSQLContainer<?> postgresContainer) {

        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(postgresContainer.getJdbcUrl());
        config.setUsername(postgresContainer.getUsername());
        config.setPassword(postgresContainer.getPassword());
        config.setMaximumPoolSize(10);

        return new HikariDataSource(config);
    }

    @Bean
    public ObjectMapper objectMapper() {
        return new ObjectMapper()
                .registerModule(new JavaTimeModule());
    }

    @Bean
    public JobFactory springJobFactory(ApplicationContext context) {
        return new JobFactory() {
            @Override
            public <T> T getJob(String tenantId, Class<T> jobType) {
                return context.getBean(jobType);
            }
        };
    }

    @Bean
    public ISqlFlow sqlFlow(
            DataSource dataSource,
            ObjectMapper mapper,
            JobFactory jobFactory) {

        PostgresFlowDatabase db = new PostgresFlowDatabase(dataSource, mapper);

        ISqlFlow client = new SqlFlow(db, mapper);

        String tenantId = "tenant-alpha";

        client.createQueue(tenantId, "ai-agent-queue");
        client.useJob(
                jobFactory,
                mapper,
                tenantId,
                "solve-bug",
                3,
                AutonomousAgentJob.class,
                AgentTask.class);

        return client;
    }

    @Bean
    public QueueSignalOptions queueSignalOptions() {
        return new QueueSignalOptions(Duration.ofSeconds(60));
    }

    @Bean
    public SqlFlowDispatcher sqlFlowDispatcher(
            ISqlFlow client,
            QueueSignalListener listener,
            QueueSignalOptions options) {
        return new DefaultSqlFlowDispatcher(client, listener, options);
    }

    @Bean
    public QueueSignalListener queueSignalListener(DataSource dataSource) {
        return new PostgresQueueSignalListener(dataSource);
    }

    @Bean
    public WorkerInstance aiWorkflow(SqlFlowDispatcher dispatcher) {
        WorkerOptions options =
                WorkerOptions.builder()
                        .tenantId("tenant-alpha")
                        .workerId("spring-worker-1")
                        .queue("ai-agent-queue")
                        .concurrency(10)
                        .claimTimeout(120)
                        .batchSize(10)
                        .maxTasksPerSecond(50)
                        .rateLimitBurstSize(100)
                        .build();

        return new WorkerInstance(
                options,
                dispatcher);
    }

    @Bean
    public SqlFlowWorkerLifecycle sqlFlowWorkerLifecycle(@Qualifier("aiWorkflow") WorkerInstance worker) {
        return new SqlFlowWorkerLifecycle(worker);
    }
}
```

### 4. Interacting with the Workflow (Controllers)

We expose standard Spring RestControllers to kick off workflows and resume suspended ones.

```java
@RestController
@RequestMapping("/agent")
public class AgentController {

    private final ISqlFlow sqlFlow;

    public AgentController(ISqlFlow sqlFlow) {
        this.sqlFlow = sqlFlow;
    }

    @PostMapping("/{tenantId}/start")
    public Map<String, String> startAgent(
            @PathVariable("tenantId") String tenantId,
            @RequestBody AgentTask task) {

        SpawnResult result = sqlFlow.spawn(new SpawnOptions(tenantId, "ai-agent-queue", null, null, null), "solve-bug", task);

        return Map.of(
                "tenantId", tenantId,
                "runId", result.runId(),
                "taskId", result.taskId(),
                "status", "Agent dispatched to fix Issue #" + task.issueId() + " for tenant " + tenantId);
    }

    @PostMapping("/{tenantId}/review/{issueId}/{correlationId}")
    public Map<String, String> review(
            @PathVariable("tenantId") String tenantId,
            @PathVariable("issueId") String issueId,
            @PathVariable("correlationId") String correlationId,
            @RequestBody HumanApproval approval) {

        String eventName = "agent-approval:" + tenantId + ":" + issueId + ":" + correlationId;

        sqlFlow.emitEvent(
                new EmitEventOptions(tenantId, "ai-agent-queue"),
                eventName,
                approval);

        String message = approval.approved()
                ? "Fix for " + correlationId + " approved. Agent is now completing its work."
                : "Fix for " + correlationId + " rejected. Agent tries again with feedback: '" + approval.reason() + "'";

        return Map.of("message", message);
    }
}
```