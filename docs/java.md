# Getting Started with the Java SDK

The SqlFlow SDK for Java is available in the Maven Central repository.

```xml
<dependencies>
    <!-- SqlFlow Core Module -->
    <dependency>
        <groupId>de.bytefish.sqlflow</groupId>
        <artifactId>sqlflow-core</artifactId>
        <version>1.0.1</version>
    </dependency>

    <!-- SqlFlow PostgreSQL Module -->
    <dependency>
        <groupId>de.bytefish.sqlflow</groupId>
        <artifactId>sqlflow-postgres</artifactId>
        <version>1.0.1</version>
    </dependency>
</dependencies>
```
*Note: Use `sqlflow-sqlserver` if you prefer Microsoft SQL Server.*


## Building a Durable AI Agent

The classic examples for durable execution are usually e-commerce checkouts. But there's a rapidly growing use case developers are dealing with: Autonomous AI Agents. Building AI agents introduces severe state-management challenges:

1. **Slow API Calls:** LLM API calls are inherently slow, prone to timeouts, and expensive. If a server crashes while waiting 30 seconds for an AI generation, standard in-memory state is lost forever.
2. **Human-in-the-Loop:** You don't want an AI pushing code to production without human review. Agents need to pause execution, ask a human for permission, and resume only when approved (hours or days later).

Traditional approaches require you to build complex state machines, database polling loops, or heavy external infrastructure. With SqlFlow, we can write our agent as standard, sequential Java code. The framework will automatically checkpoint the state, sleep without blocking server threads, and wake up exactly where it left off.

### 1. The Domain Models

Let's define the data models representing our inputs, states, and final output using standard Java Records.

```java
public record AgentTask(@JsonProperty("issue_id") String issueId) {}
public record Issue(@JsonProperty("stack_trace") String stackTrace) {}
public record Solution(@JsonProperty("patched_code") String patchedCode) {}
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

    private final LlmService llmService; // Simulated HTTP service
    private final GitHubService gitHubService; // Simulated HTTP service

    public AutonomousAgentJob(LlmService llmService, GitHubService gitHubService) {
        this.llmService = llmService;
        this.gitHubService = gitHubService;
    }

    @Override
    public AgentResult execute(TaskContext ctx, AgentTask task) throws Exception {
        Issue bugReport = ctx.step("fetch-issue-context", Issue.class, () ->
                gitHubService.getIssueDetails(task.issueId())
        );

        boolean isApproved = false;
        int attempt = 0;
        String lastFeedback = "Initial Attempt";

        while (!isApproved && attempt < 3) {
            attempt++;
            String correlationId = ctx.getTaskId() + "-attempt-" + attempt;
            final String currentFeedback = lastFeedback; 

            Solution proposedFix = ctx.step("generate-code-fix-" + attempt, Solution.class, () ->
                    llmService.generateFix(bugReport.stackTrace(), currentFeedback)
            );

            ctx.step("notify-reviewer-" + attempt, () -> {
                gitHubService.requestHumanReview(task.issueId(), proposedFix, correlationId);
            });

            // Wait for a human decision without blocking a thread! State is saved to DB here.
            Optional<JsonNode> reviewOpt = ctx.awaitEvent(
                    "agent-approval:" + task.issueId() + ":" + correlationId,
                    "wait-for-human-review-" + attempt,
                    null,
                    JsonNode.class
            );

            if (reviewOpt.isPresent()) {
                JsonNode review = reviewOpt.get();
                isApproved = review.has("approved") && review.get("approved").asBoolean();
                lastFeedback = review.has("reason") ? review.get("reason").asText() : "No feedback";
            }
        }

        if (isApproved) {
            String prUrl = ctx.step("create-pull-request", String.class, () ->
                    gitHubService.createPullRequest(task.issueId(), "apply-fix")
            );
            return new AgentResult(true, prUrl);
        } 
        return new AgentResult(false, null);
    }
}
```

### 3. Dependency Injection & Background Workers (Spring Boot)

We use a Spring Boot `@Configuration` class to wire up our data source, the SqlFlow client, and map our Job to a background worker thread.

```java
@Configuration
public class SqlFlowConfiguration {

    @Bean
    public ISqlFlow sqlFlow(DataSource dataSource, ObjectMapper mapper, JobFactory jobFactory) {
        PostgresFlowDatabase db = new PostgresFlowDatabase(dataSource, mapper);
        ISqlFlow client = new SqlFlow(db, mapper);

        client.createQueue("ai-agent-queue");
        client.useJob(jobFactory, mapper, "solve-bug", 3, AutonomousAgentJob.class, AgentTask.class);

        return client;
    }

    @Bean
    public SqlFlowWorker sqlFlowWorker(ISqlFlow client) {
        SqlFlowWorker worker = new SqlFlowWorker(
                WorkerOptions.builder()
                        .workerId("spring-worker-1")
                        .queue("ai-agent-queue")
                        .pollInterval(1.0)
                        .concurrency(5)
                        .build(),
                client);

        // Run the worker loop in the background using Java 21 Virtual Threads
        Thread.ofVirtual().start(worker);
        return worker;
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

    // 1. Kick off the agent asynchronously
    @PostMapping("/start")
    public Map<String, String> startAgent(@RequestBody AgentTask task) {
        SpawnResult result = sqlFlow.spawn(new SpawnOptions("ai-agent-queue", null, null, null), "solve-bug", task);
        return Map.of("runId", result.runId(), "taskId", result.taskId());
    }

    // 2. Webhook for a human to approve/reject the agent's code
    @PostMapping("/review/{issueId}/{correlationId}")
    public void review(@PathVariable String issueId, @PathVariable String correlationId, @RequestBody HumanApproval approval) {
        String eventName = "agent-approval:" + issueId + ":" + correlationId;
        
        // This wakes the sleeping AutonomousAgentJob right where it left off
        sqlFlow.emitEvent(new EmitEventOptions("ai-agent-queue"), eventName, approval);
    }
}
```