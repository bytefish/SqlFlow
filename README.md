# SqlFlow #

SqlFlow is a simple durable execution workflow system, for PostgreSQL and SQL Server. It handles scheduling 
and retries, without needing any other services to run in addition to PostgreSQL or SQL Server.

The SQL Script for creating the SqlFlow Database Schema is available here:

* [https://github.com/bytefish/SqlFlow/blob/main/sql/](https://github.com/bytefish/SqlFlow/blob/main/sql)

It took a large deal of inspiration from Absurd, but it features a much simpler database model. 

SqlFlow comes with a Management API and a Control Panel to understand you systems health and quickly find out 
how many tasks are being processed, which tasks are slow, which tasks currently await events, why tasks are 
failing and search for specific tasks:

<a href="https://raw.githubusercontent.com/bytefish/SqlFlow/main/docs/control-panel-event-blockades.jpg">
    <img src="https://raw.githubusercontent.com/bytefish/SqlFlow/main/docs/control-panel-event-blockades.jpg" alt="Screenshot of Event Blockades within the SqlFlow System" width="100%" />
</a>

There are two complete application examples for using the SDKs in Java and .NET:

* [Java SDK: Building a Durable AI Agent](#java-sdk-building-a-durable-ai-agent)
* [.NET SDK: Building a Durable AI Agent](#net-sdk-building-a-durable-ai-agent)
* [Python SDK: Building a Durable AI Agent](#python-sdk-building-a-durable-ai-agent)
* [Go SDK: Building a Durable AI Agent](#go-sdk-building-a-durable-ai-agent)


# Getting Started with the Database #

You'll start by creating the `ssf` database schema and tables:

* PostgreSQL: `sql/ssf-postgres.sql`
* SQL Server: `sql/ssf-sqlserver.sql`

# Getting Started with the .NET SDK #

To include SqlFlowSdk in your project, install the NuGet package using the .NET CLI:

```
dotnet add package SqlFlowSdk
```

Also add the SDK Implementation for the Database Management System to use:

```
dotnet add package SqlFlowSdk.Postgres
dotnet add package SqlFlowSdk.SqlServer
```

## Control Panel Extensions API in .NET ##

As of now the Control Panel Endpoints are only available in .NET, they might be ported to other SDKs.

If you want to add the Management Endpoints for the Control Panel, you need to add:

```
dotnet add package SqlFlowSdk.Management
```

Also add the Management API Implementation for the Database Management System to use:

```
dotnet add package SqlFlowSdk.Management.Postgres
dotnet add package SqlFlowSdk.Management.SqlServer
```

# Getting Started with the Java SDK #

The SqlFlow SDK for Java is available in Maven Central repository:

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

    <!-- SqlFlow SQL Server Module -->
    <dependency>
        <groupId>de.bytefish.sqlflow</groupId>
        <artifactId>sqlflow-sqlserver</artifactId>
        <version>1.0.1</version>
    </dependency>
    
</dependencies>
```

Use either the `sqlflow-postgres` or `sqlflow-sqlserver` module.

# Getting Started with the Go SDK #

Install the SqlFlow Go SDK using go get:

```bash
go get github.com/bytefish/SqlFlow/sdks/go
```

Then import the SDK and your preferred database driver in your application.

For PostgreSQL support, also import the postgres subpackage:

```go
import (
	"github.com/bytefish/SqlFlow/sdks/go"
	"github.com/bytefish/SqlFlow/sdks/go/postgres"
)
```

# Getting Started with the Python SDK #

Install the SqlFlow SDK from PyPI:

```bash
pip install sqlflow-sdk
```

If you want to use PostgreSQL support, install the PostgreSQL extra:

```bash
pip install "sqlflow-sdk[postgres]"
```

If you want to use SQL Server support, install the SQL Server extra:

```bash
pip install "sqlflow-sdk[sqlserver]"
```

To install all supported database drivers:

```bash
pip install "sqlflow-sdk[all]"
```

Then import the SDK in your application:

```python
from sqlflow import SqlFlow
from sqlflow.drivers.postgres import PostgresDriver
```

Or for SQL Server:

```python
from sqlflow import SqlFlow
from sqlflow.drivers.sqlserver import SqlServerDriver
```

Use either the PostgreSQL or SQL Server driver depending on your database backend.

# Java SDK: Building a Durable AI Agent #

## What we are going to build ##

The classic examples for durable execution are usually e-commerce checkouts or payment processing scenarios. But there's another rapidly 
growing use case developers are dealing with: Autonomous AI Agents. Building AI agents that interact with external APIs, write code, 
or execute complex workflows introduces challenges.

1. LLM API calls are inherently slow, prone to timeouts or rate limits. And they are also quite expensive, right? If a server crashes 
or restarts while waiting for a 30-second AI generation, standard async and await state is lost forever. 
2. You don't want an AI to push code to production or execute financial transactions without a human looking at it. Agents need to pause 
their execution, ask a human for permission and resume only when approved. This is sometimes hours or days later.

Traditional approaches require you to build complex state machines, database polling loops, or heavy external infrastructure. With SqlFlow, 
we can write our agent as standard, sequential C# code. The framework will automatically checkpoint the state to Postgres, sleep without 
blocking server threads, and wake up exactly where it left off.

## Building an Agent Job ##

To demonstrate how durable execution with SqlFlow works, we are going to build an autonomous AI agent that 
fixes bugs. The workflow is quickly laid out as: 

1. The agent receives a GitHub issue ID and fetches the stack trace.  
2. It generates a potential code fix using a Large Language Model (LLM).  
3. It pauses and asks a human for approval.  
4. If the human rejects the fix and provides feedback, the agent tries again (up to 3 times).  
5. If approved, it creates a Pull Request. If it fails 3 times, it escalates to a senior developer.

So first, let's define the data models that represent our inputs, states and final output:

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
        @JsonProperty("pull_request_url") String pullRequestUrl,
        @JsonProperty("reason") String reason
) {}
```

## The LLM Service ##

Next, we need a service to handle the AI code generation. In the real world, calling an LLM is a slow (and expensive) and the HTTP 
requests might fail or time out. We are wrapping these expensive calls with SqlFlow, so we don't lose all our state, if the 
server crashes.

For this demonstration, we are simulating ab LLM API call with some delay and return a hardcoded "code fixes" based on a reviewer's feedback:

```java
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

public interface LlmService {
    Solution generateFix(String log, String lastFeedback);
}

@Service
public class DefaultLlmService implements LlmService {
    private static final Logger logger = LoggerFactory.getLogger(DefaultLlmService.class);

    @Override
    public Solution generateFix(String log, String lastFeedback) {
        logger.info("Agent is thinking: 'Learned from feedback: {}'", lastFeedback);

        // Simulate expensive LLM call
        ServiceUtils.simulateDelay(2500);

        String code = lastFeedback.contains("error handling")
                ? "// AI: Improved Logging & Error-Handling added\nif(data == null) throw new IllegalArgumentException();"
                : "// AI: Simple Fix for the NullReferenceException\nif(data == null) return;";

        logger.info("LLM has generated a potential fix: \n{}", code);

        return new Solution(code);
    }
}
```

The agent needs to interact with the outside world. The GitHub service handles fetching the initial issue details and creating the final 
Pull Request. Whenever the LLM has generated has generated a solution, a human review is requested. If the LLM has been using more than 
a maximum amounts, the issue is escalated to a lead developer.

```java
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

public interface GitHubService {
    Issue getIssueDetails(String id);

    String createPullRequest(String id, String code);

    void requestHumanReview(String issueId, Solution proposedFix, String correlationId);

    void escalateToSenior(String id, String reason);
}

public class DefaultGitHubService implements GitHubService {
    private static final Logger logger = LoggerFactory.getLogger(DefaultGitHubService.class);

    @Override
    public Issue getIssueDetails(String issueId) {
        logger.info("GitHub: Gets Ticket #{} details from the Repository...", issueId);
        ServiceUtils.simulateDelay(800);
        return new Issue("NullReferenceException at PaymentGateway.java:42");
    }

    @Override
    public String createPullRequest(String issueId, String code) {
        logger.info("GitHub: PR for Issue #{} has been created...", issueId);
        ServiceUtils.simulateDelay(1200);
        return "https://github.com/company/repo/pull/" + (int) (Math.random() * 9000 + 1000);
    }

    @Override
    public void escalateToSenior(String id, String reason) {
        logger.error("ESCALATION to Senior Developer: Issue #{} - Reason: {}", id, reason);
        ServiceUtils.simulateDelay(500);
    }

    @Override
    public void requestHumanReview(String issueId, Solution proposedFix, String correlationId) {
        logger.info("ACTION REQUIRED: Solution for Issue #{} with Correlation-ID {} has been created: {}...",
                issueId, correlationId, proposedFix.patchedCode());
        ServiceUtils.simulateDelay(1200);
    }
}
```

## The Autonomous Agent Job ##

We define our logic inside an `IJob`. The magic is in the `ctx.Step` method: every time a step completes, its result is automatically checkpointed to the Postgres 
database. If the process crashes or is restarted, the framework replays the job. It skips the already completed steps and loads their results directly from 
the database.

And then instead of blocking a thread with `Task.Delay` or an infinite polling loop, we use `ctx.AwaitEvent` to wait for human interaction. This instructs the 
engine to safely suspend the workflow state to the database and free up the worker until an external system fires the specific event being awaited.

```java
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

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
        logger.info("Agent starts researching ticket {}", task.issueId());

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

            final String currentFeedback = lastFeedback; // Für Lambda effectively final machen

            Solution proposedFix = ctx.step("generate-code-fix-" + attempt, Solution.class, () ->
                    llmService.generateFix(bugReport.stackTrace(), currentFeedback)
            );

            ctx.step("notify-reviewer-" + attempt, () -> {
                gitHubService.requestHumanReview(task.issueId(), proposedFix, correlationId);

                localNotificationService.notifyReviewer(task.issueId(), correlationId);
            });

            logger.info("Review for {} has been requested. Agent goes idle and waits for the code review...", correlationId);

            Optional<JsonNode> reviewOpt = ctx.awaitEvent(
                    "agent-approval:" + task.issueId() + ":" + correlationId,
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

## Interacting with the System: Providing a Controller with HTTP Endpoints ##

Now there are two HTTP endpoints for interacting with the Job: 

* The `/agent/start` endpoint kicks off the process asynchronously and returns immediately.
* The `/agent/review/...` endpoint acts as our callback webhook. 
    * When the human reviewer approves or rejects a fix, this endpoint emits the event back into the queue, which then wakes up the sleeping job right where it left off.

Let's define an `AgentController` for it.

```java
@RestController
@RequestMapping("/agent")
public class AgentController {

    private final ISqlFlow sqlFlow;

    public AgentController(ISqlFlow sqlFlow) {
        this.sqlFlow = sqlFlow;
    }

    @PostMapping("/start")
    public Map<String, String> startAgent(
            @RequestBody AgentTask task) {

        SpawnResult result = sqlFlow.spawn(new SpawnOptions("ai-agent-queue", null, null, null), "solve-bug", task);

        return Map.of(
                "runId", result.runId(),
                "taskId", result.taskId(),
                "status", "Agent dispatched to fix Issue #" + task.issueId());
    }

    @PostMapping("/review/{issueId}/{correlationId}")
    public Map<String, String> review(
            @PathVariable("issueId") String issueId,
            @PathVariable("correlationId") String correlationId,
            @RequestBody HumanApproval approval) {

        String eventName = "agent-approval:" + issueId + ":" + correlationId;

        sqlFlow.emitEvent(
                new EmitEventOptions("ai-agent-queue"),
                eventName,
                approval);

        String message = approval.approved()
                ? "Fix for " + correlationId + " approved. Agent is now completing its work."
                : "Fix for " + correlationId + " rejected. Agent tries again with feedback: '" + approval.reason() + "'";

        return Map.of("message", message);
    }
}
```

## Putting It All Together: Dependency Injection ##

We are using Spring Boot, so we are using a class annotated with `@Configuration` to configure our dependencies. We 
are using TestContainers to spin up a new Postgres instance.

```java
@Configuration
public class SqlFlowConfiguration {

    @Bean(destroyMethod = "stop")
    public PostgreSQLContainer<?> postgresContainer() {
        PostgreSQLContainer<?> postgres =
                new PostgreSQLContainer<>("postgres:18")
                        .withInitScript("ssf-postgres.sql");

        postgres.start();
        return postgres;
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
        return context::getBean;
    }

    @Bean
    public ISqlFlow sqlFlow(
            DataSource dataSource,
            ObjectMapper mapper,
            JobFactory jobFactory) {

        PostgresFlowDatabase db = new PostgresFlowDatabase(dataSource, mapper);

        ISqlFlow client = new SqlFlow(db, mapper);

        client.createQueue("ai-agent-queue");
        client.useJob(
                jobFactory,
                mapper,
                "solve-bug",
                3,
                AutonomousAgentJob.class,
                AgentTask.class);

        return client;
    }

    @Bean
    public SqlFlowWorker sqlFlowWorker(ISqlFlow client) {
        SqlFlowWorker worker = new SqlFlowWorker(
                WorkerOptions.builder()
                        .workerId("spring-worker-1")
                        .queue("ai-agent-queue")
                        .pollInterval(1.0)
                        .concurrency(1)
                        .build(),
                client);

        Thread.ofVirtual().start(worker);
        return worker;
    }
}
```

Spring Boot now auto-wires everything, and what's left is to define the application entry point.

```java
@SpringBootApplication
public class AiSampleApplication {

    public static void main(String[] args) {
        SpringApplication.run(AiSampleApplication.class, args);
    }
}
```

## An Example Session with the AI Agent Job ##

### Getting the Tooling right ###

It's not stone age. I want to use tooling to fire my HTTP requests. There's somewhat of a standard 
established for tooling, which is the `http` format for HTTP requests.

And while it's easy to use `*.http` files with Visual Studio, IntelliJ doesn't come with a UI 
in it's Community Edition. But do not fear, you don't have to fight `curl`. JetBrains offers a 
CLI called `ijhttp` we can use.

We start by downloading it off the JetBrains pages:

```powershell
curl.exe -f -L -o ijhttp.zip "https://jb.gg/ijhttp/latest"
```

And extract it to a folder `Tools` in the User Profile:

```
Expand-Archive .\ijhttp.zip -DestinationPath "$env:USERPROFILE\Tools\ijhttp"
```

We can then add `ijhttp` to the search `Path` in Windows:

```powershell
$folder = "$env:USERPROFILE\Tools\ijhttp\ijhttp"
$userPath = [Environment]::GetEnvironmentVariable("Path", "User")

[Environment]::SetEnvironmentVariable("Path", "$userPath;$folder", "User")
```

### The *.http File with the Requests ###


```java
@baseUrl = https://localhost:5000
@issueId = 12345
@delayMs = 30000

### Start the Agent Job
# @name startAgent
POST {{baseUrl}}/agent/start
Content-Type: application/json

{
  "issue_id": "{{issueId}}"
}

> {%
    client.test("Agent was started", function () {
        client.assert(response.status === 200, "Expected HTTP 200");
        client.assert(response.body.taskId, "Response does not contain taskId");
    });

    client.global.set("taskId", response.body.taskId);
    client.log("Stored taskId: " + response.body.taskId);
%}

### Reject the first attempt after a delay
< {%
    await sleep(Number(request.variables.get("delayMs")));
%}
POST {{baseUrl}}/agent/review/{{issueId}}/{{taskId}}-attempt-1
Content-Type: application/json

{
  "approved": false,
  "reason": "This is way too simple, add a better error handling strategy!"
}

> {%
    client.test("First review was submitted", function () {
        client.assert(response.status === 200, "Expected HTTP 200");
    });
%}

### Approve the second attempt after another delay
< {%
    await sleep(Number(request.variables.get("delayMs")));
%}
POST {{baseUrl}}/agent/review/{{issueId}}/{{taskId}}-attempt-2
Content-Type: application/json

{
  "approved": true,
  "reason": "Now, this looks good!"
}

> {%
    client.test("Second review was submitted", function () {
        client.assert(response.status === 200, "Expected HTTP 200");
    });
%}
```

### Analyzing the Log Output ###

After starting the Backend we can see the Postgres container being booted:

```
2026-08-16T10:37:08.743+02:00  INFO 24396 --- [           main] tc.postgres:18                           : Creating container for image: postgres:18
2026-08-16T10:37:08.807+02:00  INFO 24396 --- [           main] tc.postgres:18                           : Container postgres:18 is starting: b98a7b3788ed1e6cf7400ea0cc0d446c03f9aa36b61de449f4fd5e9dd7e8e82d
2026-08-16T10:37:09.877+02:00  INFO 24396 --- [           main] tc.postgres:18                           : Container postgres:18 started in PT1.1334854S
2026-08-16T10:37:09.878+02:00  INFO 24396 --- [           main] tc.postgres:18                           : Container is started (JDBC URL: jdbc:postgresql://localhost:44757/test?loggerLevel=OFF)
2026-08-16T10:37:09.884+02:00  INFO 24396 --- [           main] org.testcontainers.ext.ScriptUtils       : Executing database script from ssf-postgres.sql
2026-08-16T10:37:10.001+02:00  INFO 24396 --- [           main] org.testcontainers.ext.ScriptUtils       : Executed database script from ssf-postgres.sql in 116 ms.
```

And the Worker for the `ai-agent-queue` being created:

```
2026-08-16T10:37:10.141+02:00  INFO 24396 --- [    virtual-103] d.b.sqlflow.core.workers.SqlFlowWorker   : SqlFlow Worker [spring-worker-1] started for queue 'ai-agent-queue'
```

The Backend is ready to perform. So let's give it something to eat.

We'll switch to the IntelliJ Terminal:

* `File → Settings → Tools → Terminal`

We'll then run out `*.http` script using `ijhttp -L VERBOSE agent-requests.http`. 

The first request for fixing an issue `12345` is sent:

```
PS sqlflow-example\requests> ijhttp -L VERBOSE agent-requests.http
┌─────────────────────────────────────────────────────────────────────────────┐
│                      Running IntelliJ HTTP Client with                      │
├────────────────────────┬────────────────────────────────────────────────────┤
│         Files          │ agent-requests.http                                │
├────────────────────────┼────────────────────────────────────────────────────┤
│   Public Environment   │                                                    │
├────────────────────────┼────────────────────────────────────────────────────┤
│  Private Environment   │                                                    │
└────────────────────────┴────────────────────────────────────────────────────┘
Request 'startAgent' POST http://localhost:5000/agent/start
= request =>
POST http://localhost:5000/agent/start
Content-Type: application/json
Content-Length: 25
User-Agent: IntelliJ HTTP Client/CLI 2026.1
Accept-Encoding: br, deflate, gzip, x-gzip
Accept: */*

{
  "issue_id": "12345"
}

###

<= response =
HTTP/1.1 200
Content-Type: application/json
Content-Length: 144
Date: Sun, 16 Aug 2026 08:44:33 GMT

{"taskId":"d51e000b-b1cc-4ac2-a30a-5521ae3f1c15","runId":"6b565cc1-872c-4cd1-b4b9-a84c5ba2e4b5","status":"Agent dispatched to fix Issue #12345"}

Response code: 200; Time: 448ms (448 ms); Content length: 144 bytes (144 B)
```

In the Backend we can see our fictional agent doing its fictional work:

```
2026-08-16T10:45:36.259+02:00  INFO 24396 --- [onPool-worker-3] d.b.s.e.workflows.AutonomousAgentJob     : Agent starts researching ticket 12345
2026-08-16T10:45:36.259+02:00  INFO 24396 --- [onPool-worker-3] d.b.s.e.workflows.AutonomousAgentJob     : Attempt 1/3: Generating a fix based on: Initial Attempt
2026-08-16T10:45:36.259+02:00  INFO 24396 --- [onPool-worker-3] d.b.s.e.workflows.AutonomousAgentJob     : Review for d51e000b-b1cc-4ac2-a30a-5521ae3f1c15-attempt-1 has been requested. Agent goes idle and waits for the code review...
```

We can see it goes idle and requests a human review. But the ficional fix looks way too simple, so we'll reject it:

```
Request 'Reject the first attempt after a delay' POST http://localhost:5000/agent/review/12345/d51e000b-b1cc-4ac2-a30a-5521ae3f1c15-attempt-1
= request =>
POST http://localhost:5000/agent/review/12345/d51e000b-b1cc-4ac2-a30a-5521ae3f1c15-attempt-1
Content-Type: application/json
Content-Length: 100
User-Agent: IntelliJ HTTP Client/CLI 2026.1
Accept-Encoding: br, deflate, gzip, x-gzip
Accept: */*

{
  "approved": false,
  "reason": "This is way too simple, add a better error handling strategy!"
}

###

<= response =
HTTP/1.1 200
Content-Type: application/json
Content-Length: 175
Date: Sun, 16 Aug 2026 08:45:05 GMT

{"message":"Fix for d51e000b-b1cc-4ac2-a30a-5521ae3f1c15-attempt-1 rejected. Agent tries again with feedback: 'This is way too simple, add a better error handling strategy!'"}

Response code: 200; Time: 26ms (26 ms); Content length: 175 bytes (175 B)
```

We can see the Backend receiving the request and the agent is generating another fix, based on our feedback:

```
2026-08-16T10:45:36.259+02:00  WARN 24396 --- [onPool-worker-3] d.b.s.e.workflows.AutonomousAgentJob     : Attempt 1 has been rejected: This is way too simple, add a better error handling strategy!
2026-08-16T10:45:36.259+02:00  INFO 24396 --- [onPool-worker-3] d.b.s.e.workflows.AutonomousAgentJob     : Attempt 2/3: Generating a fix based on: This is way too simple, add a better error handling strategy!
2026-08-16T10:45:36.259+02:00  INFO 24396 --- [onPool-worker-3] d.b.s.e.workflows.AutonomousAgentJob     : Review for d51e000b-b1cc-4ac2-a30a-5521ae3f1c15-attempt-2 has been requested. Agent goes idle and waits for the code review...
```

Let's not spend too many fictional tokens on this and accept the fix:

```
Request 'Approve the second attempt after another delay' POST http://localhost:5000/agent/review/12345/d51e000b-b1cc-4ac2-a30a-5521ae3f1c15-attempt-2
= request =>
POST http://localhost:5000/agent/review/12345/d51e000b-b1cc-4ac2-a30a-5521ae3f1c15-attempt-2
Content-Type: application/json
Content-Length: 59
User-Agent: IntelliJ HTTP Client/CLI 2026.1
Accept-Encoding: br, deflate, gzip, x-gzip
Accept: */*

{
  "approved": true,
  "reason": "Now, this looks good!"
}

###

<= response =
HTTP/1.1 200
Content-Type: application/json
Content-Length: 112
Date: Sun, 16 Aug 2026 08:45:35 GMT

{"message":"Fix for d51e000b-b1cc-4ac2-a30a-5521ae3f1c15-attempt-2 approved. Agent is now completing its work."}

Response code: 200; Time: 19ms (19 ms); Content length: 112 bytes (112 B)
```

In the logs we can see a happy agent completing the mission and creating a PR:

```
2026-08-16T10:45:36.259+02:00  INFO 24396 --- [onPool-worker-3] d.b.s.e.workflows.AutonomousAgentJob     : Fix approved. Creating Pull Request...
2026-08-16T10:45:36.261+02:00  INFO 24396 --- [onPool-worker-3] d.b.s.e.s.impl.DefaultGitHubService      : GitHub: PR for Issue #12345 has been created...
2026-08-16T10:45:37.467+02:00  INFO 24396 --- [onPool-worker-3] d.b.s.e.workflows.AutonomousAgentJob     : Mission accomplished, the PR has been created: https://github.com/company/repo/pull/7421
```


# .NET SDK: Building a Durable AI Agent #

## What we are going to build ##

The classic examples for durable execution are usually e-commerce checkouts or payment processing scenarios. But there's another rapidly 
growing use case developers are dealing with: Autonomous AI Agents. Building AI agents that interact with external APIs, write code, 
or execute complex workflows introduces challenges.

1. LLM API calls are inherently slow, prone to timeouts or rate limits. And they are also quite expensive, right? If a server crashes 
or restarts while waiting for a 30-second AI generation, standard async and await state is lost forever. 
2. You don't want an AI to push code to production or execute financial transactions without a human looking at it. Agents need to pause 
their execution, ask a human for permission and resume only when approved. This is sometimes hours or days later.

Traditional approaches require you to build complex state machines, database polling loops, or heavy external infrastructure. With SqlFlow, 
we can write our agent as standard, sequential C# code. The framework will automatically checkpoint the state to Postgres, sleep without 
blocking server threads, and wake up exactly where it left off.

## Building an Agent Job ##

To demonstrate how durable execution with SqlFlow works, we are going to build an autonomous AI agent that 
fixes bugs. The workflow is quickly laid out as: 

1. The agent receives a GitHub issue ID and fetches the stack trace.  
2. It generates a potential code fix using a Large Language Model (LLM).  
3. It pauses and asks a human for approval.  
4. If the human rejects the fix and provides feedback, the agent tries again (up to 3 times).  
5. If approved, it creates a Pull Request. If it fails 3 times, it escalates to a senior developer.

So first, let's define the data models that represent our inputs, states and final output:

```csharp
public class AgentTask 
{
    [JsonPropertyName("issue_id")]
    public string IssueId { get; set; } = ""; 
}

public class Issue 
{
    [JsonPropertyName("stack_trace")]
    public string StackTrace { get; set; } = ""; 
}

public class Solution 
{
    [JsonPropertyName("patched_code")]
    public string PatchedCode { get; set; } = ""; 
}

public class HumanApproval 
{
    [JsonPropertyName("approved")]
    public bool Approved { get; set; }

    [JsonPropertyName("reason")]
    public string? Reason { get; set; } 
}

public class AgentResult
{
    [JsonPropertyName("success")]
    public bool Success { get; set; }

    [JsonPropertyName("pull_request_url")]
    public string? PullRequestUrl { get; set; }

    [JsonPropertyName("reason")]
    public string? Reason { get; set; }
}
```

## The LLM Service ##

Next, we need a service to handle the AI code generation. In the real world, calling an LLM is a slow (and expensive) and the HTTP 
requests might fail or time out. We are wrapping these expensive calls with SqlFlow, so we don't lose all our state, if the 
server crashes.

For this demonstration, we are simulating ab LLM API call with some delay and return a hardcoded "code fixes" based on a reviewer's feedback:

```csharp
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using SqlFlowSdk.AiSample.Models;

namespace SqlFlowSdk.AiSample.Services;

public interface ILlmService
{
    Task<Solution> GenerateFixAsync(string log, string lastFeedback, CancellationToken ct);
}

public class LlmService : ILlmService
{
    private readonly ILogger<LlmService> _logger;
    public LlmService(ILogger<LlmService> logger) => _logger = logger;

    public async Task<Solution> GenerateFixAsync(string log, string lastFeedback, CancellationToken ct)
    {
        _logger.LogInformation("Agent is thinking: 'Learned from feedback: {feedback}'", lastFeedback);

        // Simulate a very expensive LLM call with a delay
        await Task.Delay(2500, ct);

        // Change Code based on human feedback
        string code = lastFeedback.Contains("error handling")
            ? "// AI: Improved Logging & Error-Handling added\nif(data == null) throw new ArgumentNullException();" 
            : "// AI: Simple Fix for the NullReferenceException\nif(data == null) return;";

        _logger.LogInformation("LLM has generated a potential fix: {PatchedCode}", code);

        return new Solution { PatchedCode = code };
    }
}
```

The agent needs to interact with the outside world. The GitHub service handles fetching the initial issue details and creating the final 
Pull Request. Whenever the LLM has generated has generated a solution, a human review is requested. If the LLM has been using more than 
a maximum amounts, the issue is escalated to a lead developer.

```csharp
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using SqlFlowSdk.AiSample.Models;

namespace SqlFlowSdk.AiSample.Services;

public interface IGitHubService
{
    Task<Issue> GetIssueDetailsAsync(string id, CancellationToken ct);

    Task<string> CreatePullRequestAsync(string id, string code, CancellationToken ct);

    Task RequestHumanReviewAsync(string issueId, Solution proposedFix, string correlationId, CancellationToken ct);

    Task EscalateToSeniorAsync(string id, string reason, CancellationToken ct);
}

public class GitHubService : IGitHubService
{
    private readonly ILogger<GitHubService> _logger;

    public GitHubService(ILogger<GitHubService> logger)
    {
        _logger = logger;
    }

    public async Task<Issue> GetIssueDetailsAsync(string issueId, CancellationToken ct)
    {
        _logger.LogInformation("GitHub: Gets Ticket #{id} details from the Repository...", issueId);

        await Task.Delay(800, ct);

        return new Issue { StackTrace = "NullReferenceException at PaymentGateway.cs:42" };
    }

    public async Task<string> CreatePullRequestAsync(string issueId, string code, CancellationToken ct)
    {
        _logger.LogInformation("GitHub: PR for Issue #{id} has been created...", issueId);

        await Task.Delay(1200, ct);

        return $"https://github.com/company/repo/pull/{new Random().Next(1000, 9999)}";
    }

    public async Task EscalateToSeniorAsync(string id, string reason, CancellationToken ct)
    {
        _logger.LogCritical("ESCALATION to Senior Developer: Issue #{id} - Grund: {reason}", id, reason);

        await Task.Delay(500, ct);
    }

    public async Task RequestHumanReviewAsync(string issueId, Solution proposedFix, string correlationId, CancellationToken ct)
    {
        _logger.LogInformation("ACTION REQUIRED: Solution for Issue #{id} with Correlation-ID {CorrelationId} has been created: {ProposedFix}...", issueId, correlationId, proposedFix.PatchedCode);

        await Task.Delay(1200, ct);
    }
}
```

## The Autonomous Agent Job ##

We define our logic inside an `IJob`. The magic is in the `ctx.Step` method: every time a step completes, its result is automatically checkpointed to the Postgres 
database. If the process crashes or is restarted, the framework replays the job. It skips the already completed steps and loads their results directly from 
the database.

And then instead of blocking a thread with `Task.Delay` or an infinite polling loop, we use `ctx.AwaitEvent` to wait for human interaction. This instructs the 
engine to safely suspend the workflow state to the database and free up the worker until an external system fires the specific event being awaited.

```csharp
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using SqlFlowSdk.AiSample.Models;
using SqlFlowSdk.AiSample.Services;
using SqlFlowSdk.Core;
using System.Text.Json.Nodes;

namespace SqlFlowSdk.AiSample;

public class AutonomousAgentJob : IJob<AgentTask, AgentResult>
{
    private readonly ILogger<AutonomousAgentJob> _logger;

    private readonly ILlmService _llmService;
    private readonly IGitHubService _gitHubService;
    private readonly ILocalNotificationService _localNotificationService;

    public AutonomousAgentJob(ILogger<AutonomousAgentJob> logger, ILlmService llmService, IGitHubService gitHubService, ILocalNotificationService localNotificationService)
    {
        _logger = logger;
        _llmService = llmService;
        _gitHubService = gitHubService;
        _localNotificationService = localNotificationService;
    }

    public async Task<AgentResult> ExecuteAsync(TaskContext ctx, AgentTask task)
    {
        _logger.LogInformation("Agent starts researching ticket {IssueId}", task.IssueId);

        // Load the Issue Context first, so the LLM has all relevant information
        var bugReport = await ctx.Step("fetch-issue-context", async () =>
            await _gitHubService.GetIssueDetailsAsync(task.IssueId, ctx.CancellationToken));

        bool isApproved = false;
        int attempt = 0;

        string lastFeedback = "Initial Attempt";

        while (!isApproved && attempt < 3)
        {
            attempt++;

            // Generate CorrelationID for the Event
            string correlationId = $"attempt-{attempt}";

            _logger.LogInformation("Attempt {attempt}/3: Generating a fix based on: {feedback}", attempt, lastFeedback);

            Solution proposedFix = await ctx.Step($"generate-code-fix-{attempt}", async () =>
                await _llmService.GenerateFixAsync(bugReport.StackTrace, lastFeedback, ctx.CancellationToken));

            await ctx.Step($"notify-reviewer-{attempt}", async () => {
                // Notify the reviewer via GitHub service, which could post a link to a GitHub issue or PR for review
                await _gitHubService.RequestHumanReviewAsync(task.IssueId, proposedFix, correlationId, ctx.CancellationToken);
                // Notify the reviewer via local notification service
                await _localNotificationService.NotifyReviewerAsync(task.IssueId, correlationId, ctx.CancellationToken);
            });

            _logger.LogInformation("Review for {CorrelationId} has been requested. Agent goes idle and waits for the code review...", correlationId);

            // Wair for a human decision without blocking a thread
            JsonNode? review = await ctx.AwaitEvent(
                eventName: $"agent-approval:{task.IssueId}:{correlationId}",
                stepName: $"wait-for-human-review-{attempt}"
            );
            
            isApproved = review["approved"]?.GetValue<bool>() ?? false;
            lastFeedback = review["reason"]?.GetValue<string>() ?? "No feedback has been given";

            if (!isApproved)
            {
                _logger.LogWarning("Attempt {attempt} has been rejected: {reason}", attempt, lastFeedback);
            }
        }

        if (isApproved)
        {
            _logger.LogInformation("Fix approved. Creating Pull Request...");

            string prUrl = await ctx.Step("create-pull-request", async () =>
            {
                return await _gitHubService.CreatePullRequestAsync(task.IssueId, "apply-fix", ctx.CancellationToken);
            });

            _logger.LogInformation("Mission accomplished, the PR has been created: {Url}", prUrl);

            return new AgentResult { Success = true, PullRequestUrl = prUrl };
        }
        else
        {
            _logger.LogError("Maximum number of attempts reached. Escalates ticket {IssueId} to a human.", task.IssueId);

            await ctx.Step("notify-senior-developer", async () =>
            {
                await _gitHubService.EscalateToSeniorAsync(task.IssueId, "Agent didn't find a solution after 3 attempts.", ctx.CancellationToken);
            });

            return new AgentResult { Success = false, Reason = "Escalated to human supervisor after 3 failures." };
        }
    }
}
```

## Putting It All Together: Dependency Injection ##

What's left is registering all dependencies.

In the example I have used TestContainers to spin up a Postgres instance. The Connection String for this instance is then passed to the 
`IServiceCollection#AddSqlFlowSdk(string connectionString)` Extension method, that registers and wires up all dependencies for interacting 
with the Postgres database.

We then configure a background worker that polls a queue for tasks and maps them to our `AutonomousAgentJob`. 

There are two HTTP endpoints for interacting with the Job: 

* The `/agent/start` endpoint kicks off the process asynchronously and returns immediately.
* The `/agent/review/...` endpoint acts as our callback webhook. 
    * When the human reviewer approves or rejects a fix, this endpoint emits the event back into the queue, which then wakes up the sleeping job right where it left off.

It looks like this.

```csharp
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

// ...

var builder = WebApplication.CreateBuilder(args);

// Start Docker Containers for dependencies
await DockerContainers.StartAllContainersAsync();

string connectionString = $"Host=127.0.0.1;Port=5432;Database=abdurd_db;Username=postgres;Password=password;";

// Add Logging
builder.Services.AddLogging(loggingBuilder => loggingBuilder.AddConsole());

builder.Services.AddSingleton<ILlmService, LlmService>();
builder.Services.AddSingleton<IGitHubService, GitHubService>();
builder.Services.AddSingleton<ILocalNotificationService, LocalNotificationService>();

// Register the SqlFlow SDK
builder.Services.AddSqlFlowSdk(connectionString);

// Configure Workers and Jobs. In this example, we have a queue for AI agents that process tasks related to bug fixing. The
// worker is configured to handle one task at a time and poll for new tasks every second. The job "solve-bug" is defined
// with a maximum of 3 attempts for each task.
builder.Services.AddSqlFlowWorker("ai-agent-queue", worker =>
{
    worker
        .SetConcurrency(1)
        .SetPollInterval(1);

    worker.AddJob<AutonomousAgentJob, AgentTask, AgentResult>("solve-bug", options =>
    {
        options.WithMaxAttempts(3);
    });
});

var app = builder.Build();

// A Webhook triggers the Agent, such as a new JIRA ticket or GitHub issue
app.MapPost("/agent/start", async (ISqlFlow client, [FromBody] AgentTask task, CancellationToken ct) =>
{
    var result = await client.SpawnAsync(new SpawnOptions
    {
        Queue = "ai-agent-queue"
    }, "solve-bug", task, ct);

    return Results.Ok(new { RunId = result.RunId, Status = $"Agent dispatched to fix Isse #{task.IssueId}" });
});

// A Lead-Developer clicks on "Approve" or "Reject", with Feeedback
app.MapPost("/agent/review/{issueId}/{correlationId}", async (
    IEventPublisher publisher,
    string issueId,
    string correlationId,
    [FromBody] HumanApproval approval,
    CancellationToken ct) =>
{
    // Wake up the agent, that is working on the ticket
    await publisher.EmitEventAsync(queue: "ai-agent-queue", eventName: $"agent-approval:{issueId}:{correlationId}", payload: approval, ct);

    string message = approval.Approved
        ? $"Fix for {correlationId} approved. Agent is now completing its work."
        : $"Fix for {correlationId} rejected. Agent tries again with feedback: '{approval.Reason}'";

    return Results.Ok(new { Message = message });
});

app.Run();
```

## An Example Session with the AI Agent Job ##

After starting the Backend we can see the Postgres container being booted and the SqlFlow Postgres queue being created:

```
[testcontainers.org 00:00:02.01] Wait for Docker container 5235c226fe2d to complete readiness checks
[testcontainers.org 00:00:03.05] Docker container 5235c226fe2d ready
info: SqlFlowSdk.Workers.SqlFlowGenericWorker[0]
      Create Queue if not exists: 'ai-agent-queue'
```

In the `SqlFlowSdk.AiSample.http` we are now executing a Request for fixing an Issue (say `12345`):

```
### Start the Agent Job for an AI fix for the Issue.
# @name startAgent
POST https://localhost:5000/agent/start
Content-Type: application/json
{
    "issue_id": "12345"
}
```

It returns the following content:

```
{
  "runId": "019f2db5-d7be-7d89-8fcb-d649e70e698a",
  "taskId": "3ff3cd1c-69ce-4bdb-9a14-6566955cb7cd",
  "status": "Agent dispatched to fix issue #12345"
}
```

We'll then extract the TaskID, because it's needed to build the CorrelationID for Event CorrelationIDs:

```
### Extract the Task ID and write it to a variable for later use.
@taskId = {{startAgent.response.body.$.taskId}}
```

In the Console of the Service, you can see the Agent working and requesting feedback. 

```
info: SqlFlowSdk.AiSample.AutonomousAgentJob[0]
      Agent starts researching ticket 12345
info: SqlFlowSdk.AiSample.Services.GitHubService[0]
      GitHub: Gets Ticket #12345 details from the Repository...
info: SqlFlowSdk.AiSample.AutonomousAgentJob[0]
      Attempt 1/3: Generating a fix based on: Initial Attempt
info: SqlFlowSdk.AiSample.Services.LlmService[0]
      Agent is thinking: 'Learned from feedback: Initial Attempt'
info: SqlFlowSdk.AiSample.Services.LlmService[0]
      LLM has generated a potential fix: // AI: Simple Fix for the NullReferenceException
if(data == null) return;
info: SqlFlowSdk.AiSample.Services.GitHubService[0]
      ACTION REQUIRED: Solution for Issue #12345 with Correlation-ID 3ff3cd1c-69ce-4bdb-9a14-6566955cb7cd-attempt-1 has been created: // AI: Simple Fix for the NullReferenceException
if(data == null) return;...
info: SqlFlowSdk.AiSample.AutonomousAgentJob[0]
      Review for attempt-1 has been requested. Agent goes idle and waits for the code review...
```

But let's say we don't like the fix and we want it to be rewritten. 

We will reject the code and tell it to make it simpler:

```
### We cannot approve such a simple fix, reject and tell it to improve.
POST https://localhost:5000/agent/review/12345/{{taskId}}-attempt-1
Content-Type: application/json
{
    "approved": false,
    "reason": "This is way too simple, add a better error handling strategy!"
}
```

We can then see the Agent doing its work:

```
warn: SqlFlowSdk.AiSample.AutonomousAgentJob[0]
      Attempt 1 has been rejected: This is way too simple, add a better error handling strategy!
info: SqlFlowSdk.AiSample.AutonomousAgentJob[0]
      Attempt 2/3: Generating a fix based on: This is way too simple, add a better error handling strategy!
info: SqlFlowSdk.AiSample.Services.LlmService[0]
      Agent is thinking: 'Learned from feedback: This is way too simple, add a better error handling strategy!'
info: SqlFlowSdk.AiSample.Services.LlmService[0]
      LLM has generated a potential fix: // AI: Improved Logging & Error-Handling added
if(data == null) throw new ArgumentNullException();
info: SqlFlowSdk.AiSample.Services.GitHubService[0]
      ACTION REQUIRED: Solution for Issue #12345 with Correlation-ID 3ff3cd1c-69ce-4bdb-9a14-6566955cb7cd-attempt-2 has been created: // AI: Improved Logging & Error-Handling added
if(data == null) throw new ArgumentNullException();...
info: SqlFlowSdk.AiSample.AutonomousAgentJob[0]
      Review for attempt-2 has been requested. Agent goes idle and waits for the code review...
```

This looks ok, so let's approve it:

```
### Send Human Feedback to the Agent
POST https://localhost:5000/agent/review/12345/{{taskId}}-attempt-2
Content-Type: application/json
{
    "approved": true,
    "reason": "Now, this looks good!"
}
```

And in the Console, we can see the PR finally being created:

```
info: SqlFlowSdk.AiSample.AutonomousAgentJob[0]
      Fix approved. Creating Pull Request...
info: SqlFlowSdk.AiSample.Services.GitHubService[0]
      GitHub: PR for Issue #12345 has been created...
info: SqlFlowSdk.AiSample.AutonomousAgentJob[0]
      Mission accomplished, the PR has been created: https://github.com/company/repo/pull/4232
```

And we can now simulate the Agent Loop like this:

```
### Start the Agent Job for an AI fix for the Issue.
# @name startAgent
POST https://localhost:5000/agent/start
Content-Type: application/json
{
    "issue_id": "12345"
}


### Extract the Task ID and write it to a variable for later use.
@taskId = {{startAgent.response.body.$.taskId}}


### We cannot approve such a simple fix, reject and tell it to improve.
POST https://localhost:5000/agent/review/12345/{{taskId}}-attempt-1
Content-Type: application/json
{
    "approved": false,
    "reason": "This is way too simple, add a better error handling strategy!"
}

### Send Human Feedback to the Agent
POST https://localhost:5000/agent/review/12345/{{taskId}}-attempt-2
Content-Type: application/json
{
    "approved": true,
    "reason": "Now, this looks good!"
}
```


# Python SDK: Building a Durable AI Agent #

## What we are going to build ##

The classic examples for durable execution are usually e-commerce checkouts or payment processing scenarios. But there's another rapidly 
growing use case developers are dealing with: Autonomous AI Agents. Building AI agents that interact with external APIs, write code, 
or execute complex workflows introduces challenges.

1. LLM API calls are inherently slow, prone to timeouts or rate limits. And they are also quite expensive, right? If a server crashes 
or restarts while waiting for a 30-second AI generation, standard async and await state is lost forever. 
2. You don't want an AI to push code to production or execute financial transactions without a human looking at it. Agents need to pause 
their execution, ask a human for permission and resume only when approved. This is sometimes hours or days later.

Traditional approaches require you to build complex state machines, database polling loops, or heavy external infrastructure. With SqlFlow, 
we can write our agent as standard, sequential C# code. The framework will automatically checkpoint the state to Postgres, sleep without 
blocking server threads, and wake up exactly where it left off.

## Building an Agent Job ##

To demonstrate how durable execution with SqlFlow works, we are going to build an autonomous AI agent that 
fixes bugs. The workflow is quickly laid out as: 

1. The agent receives a GitHub issue ID and fetches the stack trace.  
2. It generates a potential code fix using a Large Language Model (LLM).  
3. It pauses and asks a human for approval.  
4. If the human rejects the fix and provides feedback, the agent tries again (up to 3 times).  
5. If approved, it creates a Pull Request. If it fails 3 times, it escalates to a senior developer.

So first, let's define the data models that represent our inputs, states and final output:

```python
class AgentTask(BaseModel):
    issue_id: str

class Issue(BaseModel):
    stack_trace: str

class Solution(BaseModel):
    patched_code: str

class HumanApproval(BaseModel):
    approved: bool
    reason: Optional[str] = None

class AgentResult(BaseModel):
    success: bool
    pull_request_url: Optional[str] = None
    reason: Optional[str] = None
```

## The LLM Service ##

Next, we need a service to handle the AI code generation. In the real world, calling an LLM is a slow (and expensive) and the HTTP 
requests might fail or time out. We are wrapping these expensive calls with SqlFlow, so we don't lose all our state, if the 
server crashes.

For this demonstration, we are simulating ab LLM API call with some delay and return a hardcoded "code fixes" based on a reviewer's feedback:

```python
class LlmService:
    def __init__(self):
        self.logger = logging.getLogger("LlmService")

    async def generate_fix(self, log: str, last_feedback: str) -> dict:
        self.logger.info(f"Agent is thinking: 'Learned from feedback: {last_feedback}'")
        
        # Simulate a very expensive LLM call with a delay
        await asyncio.sleep(2.5)
        
        # Change Code based on human feedback
        if "error handling" in last_feedback.lower():
            code = "// AI: Improved Logging & Error-Handling added\nif(data == null) raise ValueError('Null data');"
        else:
            code = "// AI: Simple Fix for the NullReferenceException\nif(data is None): return"
            
        self.logger.info(f"LLM has generated a potential fix: {code}")
        
        # We return a dict to make it easily serializable by SqlFlow
        return Solution(patched_code=code).model_dump()
```

The agent needs to interact with the outside world. The GitHub service handles fetching the initial issue details and creating the final 
Pull Request. Whenever the LLM has generated has generated a solution, a human review is requested. If the LLM has been using more than 
a maximum amounts, the issue is escalated to a lead developer.

```python
class GitHubService:
    def __init__(self):
        self.logger = logging.getLogger("GitHubService")

    async def get_issue_details(self, issue_id: str) -> dict:
        self.logger.info(f"GitHub: Gets Ticket #{issue_id} details from the Repository...")
        await asyncio.sleep(0.8)
        return Issue(stack_trace="NullReferenceException at PaymentGateway.cs:42").model_dump()

    async def create_pull_request(self, issue_id: str, code: str) -> str:
        self.logger.info(f"GitHub: PR for Issue #{issue_id} has been created...")
        await asyncio.sleep(1.2)
        return f"https://github.com/company/repo/pull/{random.randint(1000, 9999)}"

    async def escalate_to_senior(self, issue_id: str, reason: str) -> None:
        self.logger.critical(f"ESCALATION to Senior Developer: Issue #{issue_id} - Reason: {reason}")
        await asyncio.sleep(0.5)

    async def request_human_review(self, issue_id: str, proposed_fix: dict, correlation_id: str) -> None:
        patched_code = proposed_fix.get("patched_code", "")
        self.logger.info(f"ACTION REQUIRED: Solution for Issue #{issue_id} with Correlation-ID {correlation_id} has been created: {patched_code}...")
        await asyncio.sleep(1.2)
```

## The Autonomous Agent Job ##

The workflow is just a normal Python method, that takes a `TaskContext` and parameters. The magic is in the `ctx.step` method: every time a step completes, 
its result is automatically checkpointed to the Postgres database. If the process crashes or is restarted, the framework replays the job. It skips the already 
completed steps and loads their results directly from the database.

And then instead of blocking a thread with `Task.Delay` or an infinite polling loop, we use `ctx.await_event` to wait for human interaction. This 
instructs the engine to safely suspend the workflow state to the database and free up the worker until an external system fires the specific event 
being awaited.

```python
async def autonomous_agent_workflow(ctx: TaskContext, params: dict) -> dict:
    logger = logging.getLogger("AutonomousAgentJob")
    task = AgentTask(**params)
    
    logger.info(f"Agent starts researching ticket {task.issue_id}")

    # Helper async functions for steps (since lambda doesn't work well with await in ctx.step)
    async def fetch_issue():
        return await github_service.get_issue_details(task.issue_id)
        
    # Load the Issue Context first, so the LLM has all relevant information
    bug_report_dict = await ctx.step("fetch-issue-context", fetch_issue)
    bug_report = Issue(**bug_report_dict)

    is_approved = False
    attempt = 0
    last_feedback = "Initial Attempt"

    while not is_approved and attempt < 3:
        attempt += 1
        correlation_id = f"{ctx.task_id}-attempt-{attempt}"
        
        logger.info(f"Attempt {attempt}/3: Generating a fix based on: {last_feedback}")

        async def generate_code():
            return await llm_service.generate_fix(bug_report.stack_trace, last_feedback)
            
        proposed_fix_dict = await ctx.step(f"generate-code-fix-{attempt}", generate_code)
        proposed_fix = Solution(**proposed_fix_dict)

        async def notify():
            await github_service.request_human_review(task.issue_id, proposed_fix.model_dump(), correlation_id)
            await notification_service.notify_reviewer(task.issue_id, correlation_id)
            return True # Step needs to return something serializable
            
        await ctx.step(f"notify-reviewer-{attempt}", notify)

        logger.info(f"Review for {correlation_id} has been requested. Agent goes idle and waits for the code review...")

        # Wait for a human decision without blocking a thread
        # This will throw SuspendTaskException if the event 
        # hasn't happened yet!
        review_data = await ctx.await_event(
            event_name=f"agent-approval:{task.issue_id}:{correlation_id}",
            step_name=f"wait-for-human-review-{attempt}"
        )

        approval = HumanApproval(**review_data)
        is_approved = approval.approved
        last_feedback = approval.reason or "No feedback has been given"

        if not is_approved:
            logger.warning(f"Attempt {attempt} has been rejected: {last_feedback}")

    if is_approved:
        logger.info("Fix approved. Creating Pull Request...")

        async def create_pr():
            return await github_service.create_pull_request(task.issue_id, "apply-fix")
            
        pr_url = await ctx.step("create-pull-request", create_pr)
        logger.info(f"Mission accomplished, the PR has been created: {pr_url}")
        
        return AgentResult(success=True, pull_request_url=pr_url).model_dump()
    else:
        logger.error(f"Maximum number of attempts reached. Escalates ticket {task.issue_id} to a human.")

        async def escalate():
            await github_service.escalate_to_senior(task.issue_id, "Agent didn't find a solution after 3 attempts.")
            return True
            
        await ctx.step("notify-senior-developer", escalate)
        
        return AgentResult(success=False, reason="Escalated to human supervisor after 3 failures.").model_dump()
```

We'll use 

The FastAPI application serves as the host for our SqlFlow runtime.

During application startup, we:

* Create a PostgreSQL driver and establish the database connection.
* Create a SqlFlow client instance.
* Create or verify the workflow queue.
* Register the workflow implementation under a task name.
* Start a worker that continuously polls the queue and executes tasks.

The worker runs in the background alongside the FastAPI application. It continuously claims available tasks 
from the queue, executes workflow steps, persists checkpoints, and resumes suspended workflows when events 
arrive.

```python
# Web API (FastAPI) and Application Setup

logging.basicConfig(level=logging.INFO)

# Global variables to hold our DB driver, Client and Worker

db_driver = None
sqlflow_client = None
worker = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifecycle manager for the FastAPI application."""
    global db_driver, sqlflow_client, worker
    
    connection_string = "postgresql://postgres:password@127.0.0.1:5432/sqlflow_db"
    
    db_driver = PostgresDriver(connection_string)

    await db_driver.connect()
    
    sqlflow_client = SqlFlow(db=db_driver)
    
    await sqlflow_client.create_queue("ai-agent-queue")
    
    # Register Workflow
    sqlflow_client.register_task("solve-bug", autonomous_agent_workflow, max_attempts=3)
    
    # Start Worker
    worker = sqlflow_client.create_worker(WorkerOptions(
        worker_id="agent-worker-1",
        queue_name="ai-agent-queue",
        poll_interval=1.0,
        concurrency=1
    ))
    
    await worker.start()
    
    yield # App runs here
    
    # Cleanup on shutdown
    if worker:
        await worker.stop()
    if db_driver:
        await db_driver.disconnect()

app = FastAPI(lifespan=lifespan)
```

And to interact with the application, we'll add a set of HTTP endpoints for starting and interacting 
with the system. Again it uses the `sqlflow_client` abstraction to simplify working with SqlFlow. 

```python
@app.post("/agent/start")
async def start_agent(task: AgentTask):
    """A Webhook triggers the Agent, such as a new JIRA ticket or GitHub issue."""
    options = SpawnOptions(queue_name="ai-agent-queue")
    
    result = await sqlflow_client.spawn(
        options=options, 
        task_name="solve-bug", 
        params=task.model_dump()
    )
    
    return {
        "run_id": result.run_id, 
        "task_id": result.task_id,
        "status": f"Agent dispatched to fix Issue #{task.issue_id}"
    }

@app.post("/agent/review/{issue_id}/{correlation_id}")
async def review_agent(issue_id: str, correlation_id: str, approval: HumanApproval):
    """A Lead-Developer clicks on 'Approve' or 'Reject', with Feedback."""
    
    # Wake up the agent that is working on the ticket
    event_name = f"agent-approval:{issue_id}:{correlation_id}"
    
    options = EmitEventOptions(queue_name="ai-agent-queue")
    await sqlflow_client.emit_event(
        options=options, 
        event_name=event_name, 
        payload=approval.model_dump()
    )
    
    message = (
        f"Fix for {correlation_id} approved. Agent is now completing its work."
        if approval.approved else
        f"Fix for {correlation_id} rejected. Agent tries again with feedback: '{approval.reason}'"
    )
    
    return {"message": message}
```

## An Example Session with the AI Agent Job ##

### Getting the Tooling right ###

It's not stone age. I want to use tooling to fire my HTTP requests. There's somewhat of a 
standard established for firing HTTP Requests, which is the `*.http` format. 

I am not an expert with Python, so I'll use a CLI provided by JetBrains called `ijhttp`, 
that makes it super easy to work with HTTP Requests.

We start by downloading it off the JetBrains pages:

```powershell
curl.exe -f -L -o ijhttp.zip "https://jb.gg/ijhttp/latest"
```

And extract it to a folder `Tools` in the User Profile:

```
Expand-Archive .\ijhttp.zip -DestinationPath "$env:USERPROFILE\Tools\ijhttp"
```

We can then add `ijhttp` to the search `Path` in Windows:

```powershell
$folder = "$env:USERPROFILE\Tools\ijhttp\ijhttp"
$userPath = [Environment]::GetEnvironmentVariable("Path", "User")

[Environment]::SetEnvironmentVariable("Path", "$userPath;$folder", "User")
```

### The *.http File with the Requests ###

```java
@baseUrl = http://localhost:8000
@issueId = 12345
@delayMs = 15000

### Start the Agent Job
# @name startAgent
POST {{baseUrl}}/agent/start
Content-Type: application/json

{
  "issue_id": "{{issueId}}"
}

> {%
    let body = response.body;
    if (typeof body === "string") {
        body = JSON.parse(body);
    }

    client.test("Agent was started", function () {
        client.assert(response.status === 200, "Expected HTTP 200");
        client.assert(body.task_id, "Response does not contain task_id");
    });

    client.global.set("task_id", body.task_id);
    client.log("Stored task_id: " + body.task_id);
%}

### Reject the first attempt after a delay
< {%
    // Directly use the evaluated template variable or fallback to 15000
    await sleep(parseInt("{{delayMs}}") || 15000);
%}
POST {{baseUrl}}/agent/review/{{issueId}}/{{task_id}}-attempt-1
Content-Type: application/json

{
  "approved": false,
  "reason": "This is way too simple, add a better error handling strategy!"
}

> {%
    client.test("First review was submitted", function () {
        client.assert(response.status === 200, "Expected HTTP 200");
    });
%}

### Approve the second attempt after another delay
< {%
    await sleep(parseInt("{{delayMs}}") || 15000);
%}
POST {{baseUrl}}/agent/review/{{issueId}}/{{task_id}}-attempt-2
Content-Type: application/json

{
  "approved": true,
  "reason": "Now, this looks good!"
}

> {%
    client.test("Second review was submitted", function () {
        client.assert(response.status === 200, "Expected HTTP 200");
    });
%}
```

### Analyzing the Session ###

We'll start the Backend by running:

```bash
poetry run uvicorn examples.ai_agent_api:app --reload
```

After starting the Backend we can see the Postgres container being booted, a Postgres connection check and the worker queue 
being created:

```
INFO:     Will watch for changes in these directories: ['C:\\Users\\philipp\\source\\repos\\bytefish\\SqlFlowCore\\sdks\\python']
INFO:     Uvicorn running on http://127.0.0.1:8000 (Press CTRL+C to quit)
INFO:     Started reloader process [24964] using StatReload
INFO:     Started server process [13256]
INFO:     Waiting for application startup.
INFO:sqlflow.postgres:PostgreSQL connection pool created.
INFO:sqlflow:Worker agent-worker-1 started on queue 'ai-agent-queue'.
INFO:     Application startup complete.
```

The Backend is ready to perform. So let's give it something to eat.

We'll then run out `*.http` script using `ijhttp -L VERBOSE agent-requests.http`. 

The first request for fixing an issue `12345` is sent:

```
PS sqlflow-example\requests> ijhttp -L VERBOSE agent-requests.http
┌─────────────────────────────────────────────────────────────────────────────┐
│                      Running IntelliJ HTTP Client with                      │
├────────────────────────┬────────────────────────────────────────────────────┤
│         Files          │ agent-requests.http                                │
├────────────────────────┼────────────────────────────────────────────────────┤
│   Public Environment   │                                                    │
├────────────────────────┼────────────────────────────────────────────────────┤
│  Private Environment   │                                                    │
└────────────────────────┴────────────────────────────────────────────────────┘
Request 'startAgent' POST http://localhost:8000/agent/start
= request =>
POST http://localhost:8000/agent/start
Content-Type: application/json
Content-Length: 25
User-Agent: IntelliJ HTTP Client/CLI 2026.1
Accept-Encoding: br, deflate, gzip, x-gzip
Accept: */*

{
  "issue_id": "12345"
}

###

<= response =
HTTP/1.1 200 OK
date: Fri, 21 Aug 2026 20:56:51 GMT
server: uvicorn
content-length: 146
content-type: application/json

{"run_id":"15fa7c33-93c6-4179-92ed-2c7bd7001627","task_id":"010f6fee-2fa7-42e1-8c91-389ce54c68f2","status":"Agent dispatched to fix Issue #12345"}

Response code: 200 (OK); Time: 454ms (454 ms); Content length: 146 bytes (146 B)

```

In the Backend we can see our fictional agent doing its fictional work:

```
INFO:     127.0.0.1:57179 - "POST /agent/start HTTP/1.1" 200 OK
INFO:AutonomousAgentJob:Agent starts researching ticket 12345
INFO:GitHubService:GitHub: Gets Ticket #12345 details from the Repository...
INFO:AutonomousAgentJob:Attempt 1/3: Generating a fix based on: Initial Attempt
INFO:LlmService:Agent is thinking: 'Learned from feedback: Initial Attempt'
INFO:LlmService:LLM has generated a potential fix: // AI: Simple Fix for the NullReferenceException
if(data is None): return
INFO:GitHubService:ACTION REQUIRED: Solution for Issue #12345 with Correlation-ID 010f6fee-2fa7-42e1-8c91-389ce54c68f2-attempt-1 has been created: // AI: Simple Fix for the NullReferenceException
if(data is None): return...
INFO:LocalNotification:Ping! Please review 010f6fee-2fa7-42e1-8c91-389ce54c68f2-attempt-1 for issue 12345.
INFO:AutonomousAgentJob:Review for 010f6fee-2fa7-42e1-8c91-389ce54c68f2-attempt-1 has been requested. Agent goes idle and waits for the code review...
INFO:sqlflow:Task 010f6fee-2fa7-42e1-8c91-389ce54c68f2 suspended: Task suspended waiting for event: agent-approval:12345:010f6fee-2fa7-42e1-8c91-389ce54c68f2-attempt-1
```

We can see it goes idle and requests a human review. But the ficional fix looks way too simple, so we'll reject it:

```

Request 'Reject the first attempt after a delay' POST http://localhost:8000/agent/review/12345/010f6fee-2fa7-42e1-8c91-389ce54c68f2-attempt-1
= request =>
POST http://localhost:8000/agent/review/12345/010f6fee-2fa7-42e1-8c91-389ce54c68f2-attempt-1
Content-Type: application/json
Content-Length: 100
User-Agent: IntelliJ HTTP Client/CLI 2026.1
Accept-Encoding: br, deflate, gzip, x-gzip
Accept: */*

{
  "approved": false,
  "reason": "This is way too simple, add a better error handling strategy!"
}

###

<= response =
HTTP/1.1 200 OK
date: Fri, 21 Aug 2026 20:57:22 GMT
server: uvicorn
content-length: 175
content-type: application/json

{"message":"Fix for 010f6fee-2fa7-42e1-8c91-389ce54c68f2-attempt-1 rejected. Agent tries again with feedback: 'This is way too simple, add a better error handling strategy!'"}

Response code: 200 (OK); Time: 19ms (19 ms); Content length: 175 bytes (175 B)
```

We can see the Backend receiving the request and the agent is generating another fix, based on our feedback:

```
INFO:     127.0.0.1:57182 - "POST /agent/review/12345/010f6fee-2fa7-42e1-8c91-389ce54c68f2-attempt-1 HTTP/1.1" 200 OK
INFO:AutonomousAgentJob:Agent starts researching ticket 12345
INFO:AutonomousAgentJob:Attempt 1/3: Generating a fix based on: Initial Attempt
INFO:AutonomousAgentJob:Review for 010f6fee-2fa7-42e1-8c91-389ce54c68f2-attempt-1 has been requested. Agent goes idle and waits for the code review...
WARNING:AutonomousAgentJob:Attempt 1 has been rejected: This is way too simple, add a better error handling strategy!
INFO:AutonomousAgentJob:Attempt 2/3: Generating a fix based on: This is way too simple, add a better error handling strategy!
INFO:LlmService:Agent is thinking: 'Learned from feedback: This is way too simple, add a better error handling strategy!'
INFO:LlmService:LLM has generated a potential fix: // AI: Improved Logging & Error-Handling added
if(data == null) raise ValueError('Null data');
INFO:GitHubService:ACTION REQUIRED: Solution for Issue #12345 with Correlation-ID 010f6fee-2fa7-42e1-8c91-389ce54c68f2-attempt-2 has been created: // AI: Improved Logging & Error-Handling added
if(data == null) raise ValueError('Null data');...
INFO:LocalNotification:Ping! Please review 010f6fee-2fa7-42e1-8c91-389ce54c68f2-attempt-2 for issue 12345.
INFO:AutonomousAgentJob:Review for 010f6fee-2fa7-42e1-8c91-389ce54c68f2-attempt-2 has been requested. Agent goes idle and waits for the code review...
INFO:sqlflow:Task 010f6fee-2fa7-42e1-8c91-389ce54c68f2 suspended: Task suspended waiting for event: agent-approval:12345:010f6fee-2fa7-42e1-8c91-389ce54c68f2-attempt-2
```

Let's not spend too many fictional tokens on this and accept the fix:

```

Request 'Approve the second attempt after another delay' POST http://localhost:8000/agent/review/12345/010f6fee-2fa7-42e1-8c91-389ce54c68f2-attempt-2
= request =>
POST http://localhost:8000/agent/review/12345/010f6fee-2fa7-42e1-8c91-389ce54c68f2-attempt-2
Content-Type: application/json
Content-Length: 59
User-Agent: IntelliJ HTTP Client/CLI 2026.1
Accept-Encoding: br, deflate, gzip, x-gzip
Accept: */*

{
  "approved": true,
  "reason": "Now, this looks good!"
}

###

<= response =
HTTP/1.1 200 OK
date: Fri, 21 Aug 2026 20:57:53 GMT
server: uvicorn
content-length: 112
content-type: application/json

{"message":"Fix for 010f6fee-2fa7-42e1-8c91-389ce54c68f2-attempt-2 approved. Agent is now completing its work."}

Response code: 200 (OK); Time: 21ms (21 ms); Content length: 112 bytes (112 B)
```

In the logs we can see a happy agent completing the mission and creating a PR:

```
WARNING:AutonomousAgentJob:Attempt 1 has been rejected: This is way too simple, add a better error handling strategy!
INFO:AutonomousAgentJob:Attempt 2/3: Generating a fix based on: This is way too simple, add a better error handling strategy!
INFO:AutonomousAgentJob:Review for 010f6fee-2fa7-42e1-8c91-389ce54c68f2-attempt-2 has been requested. Agent goes idle and waits for the code review...
INFO:AutonomousAgentJob:Fix approved. Creating Pull Request...
INFO:GitHubService:GitHub: PR for Issue #12345 has been created...
INFO:AutonomousAgentJob:Mission accomplished, the PR has been created: https://github.com/company/repo/pull/7272
INFO:sqlflow:Task 010f6fee-2fa7-42e1-8c91-389ce54c68f2 completed successfully.
```

# Go SDK: Building a Durable AI Agent #

## What we are going to build ##

The classic examples for durable execution are usually e-commerce checkouts or payment processing scenarios. But there's another rapidly 
growing use case developers are dealing with: Autonomous AI Agents. Building AI agents that interact with external APIs, write code, 
or execute complex workflows introduces challenges.

1. LLM API calls are inherently slow, prone to timeouts or rate limits. And they are also quite expensive, right? If a server crashes 
or restarts while waiting for a 30-second AI generation, standard async and await state is lost forever. 
2. You don't want an AI to push code to production or execute financial transactions without a human looking at it. Agents need to pause 
their execution, ask a human for permission and resume only when approved. This is sometimes hours or days later.

Traditional approaches require you to build complex state machines, database polling loops, or heavy external infrastructure. With SqlFlow, 
we can write our agent as standard, sequential Go code. The framework will automatically checkpoint the state to Postgres, sleep without blocking 
server threads, and wake up exactly where it left off.

## Building an Agent Job ##

To demonstrate how durable execution with SqlFlow works, we are going to build an autonomous AI agent that 
fixes bugs. The workflow is quickly laid out as: 

1. The agent receives a GitHub issue ID and fetches the stack trace.  
2. It generates a potential code fix using a Large Language Model (LLM).  
3. It pauses and asks a human for approval.  
4. If the human rejects the fix and provides feedback, the agent tries again (up to 3 times).  
5. If approved, it creates a Pull Request. If it fails 3 times, it escalates to a senior developer.

So first, let's define the data models that represent our inputs, states and final output:

```go
type AgentTask struct {
	IssueID string `json:"issue_id"`
}

type Issue struct {
	StackTrace string `json:"stack_trace"`
}

type Solution struct {
	PatchedCode string `json:"patched_code"`
}

type HumanApproval struct {
	Approved bool   `json:"approved"`
	Reason   string `json:"reason,omitempty"`
}

type AgentResult struct {
	Success        bool   `json:"success"`
	PullRequestURL string `json:"pull_request_url,omitempty"`
	Reason         string `json:"reason,omitempty"`
}
```

## The LLM Service ##

Next, we need a service to handle the AI code generation. In the real world, calling an LLM is a slow (and expensive) and the HTTP 
requests might fail or time out. We are wrapping these expensive calls with SqlFlow, so we don't lose all our state, if the 
server crashes.

For this demonstration, we are simulating an LLM API call with `time.Sleep` and return hardcoded "code fixes" based on a reviewer's 
feedback. Because of Go's goroutines, `time.Sleep` only blocks the current worker thread.

```go

type LlmService struct{}

func (s *LlmService) GenerateFix(stackTrace string, lastFeedback string) Solution {
	log.Printf("[LlmService] Agent is thinking: 'Learned from feedback: %s'", lastFeedback)
	time.Sleep(2500 * time.Millisecond)

	var code string
	if strings.Contains(strings.ToLower(lastFeedback), "error handling") {
		code = "// AI: Improved Logging & Error Handling added\nif(data == null) raise ValueError('Null data');"
	} else {
		code = "// AI: Simple Fix for the NullReferenceException\nif(data is None): return"
	}

	log.Printf("[LlmService] LLM generated a potential fix:\n%s", code)
    
	return Solution{PatchedCode: code}
}
```

The agent needs to interact with the outside world. The GitHub service handles fetching the initial issue details and creating the final 
Pull Request. Whenever the LLM has generated has generated a solution, a human review is requested. If the LLM has been using more than 
a maximum amounts, the issue is escalated to a lead developer.

```go
type GitHubService struct{}

func (s *GitHubService) GetIssueDetails(issueID string) Issue {
	log.Printf("[GitHubService] Fetching details for ticket #%s from the repository...", issueID)
	time.Sleep(800 * time.Millisecond)
	return Issue{StackTrace: "NullReferenceException at PaymentGateway.cs:42"}
}

func (s *GitHubService) CreatePullRequest(issueID string, code string) string {
	log.Printf("[GitHubService] PR for issue #%s has been created...", issueID)
	time.Sleep(1200 * time.Millisecond)
	return fmt.Sprintf("https://github.com/company/repo/pull/%d", rand.Intn(9000)+1000)
}

func (s *GitHubService) EscalateToSenior(issueID string, reason string) {
	log.Printf("[GitHubService] 🚨 ESCALATION to Senior Developer: Issue #%s - Reason: %s", issueID, reason)
	time.Sleep(500 * time.Millisecond)
}

func (s *GitHubService) RequestHumanReview(issueID string, proposedFix Solution, correlationID string) {
	log.Printf("[GitHubService] ⏳ ACTION REQUIRED: A solution for issue #%s (Correlation ID %s) is available:\n%s",
		issueID, correlationID, proposedFix.PatchedCode)
	time.Sleep(1200 * time.Millisecond)
}
```

## The Autonomous Agent Job ##

The workflow is just a normal Go function that takes a `sqlflow.TaskContext` and your parameters. The magic is in the `sqlflow.Step`function: 
every time a step completes, its result is automatically checkpointed to the Postgres database. If the process crashes or is restarted, the 
framework replays the job. It skips the already completed steps and loads their results directly from the database.

Instead of blocking a goroutine with an infinite polling loop, we use `sqlflow.AwaitEvent[T]` to wait for human interaction. This instructs the 
engine to safely suspend the workflow state to the database and free up the worker completely until an external system fires the specific 
event being awaited.

```go

var llmService = &LlmService{}
var gitHubService = &GitHubService{}
var notificationService = &LocalNotificationService{}

func autonomousAgentWorkflow(ctx *sqlflow.TaskContext, task AgentTask) error {
	log.Printf("[Workflow] Agent starts investigation for ticket %s", task.IssueID)

	bugReport, err := sqlflow.Step(ctx, "fetch-issue-context", func() (Issue, error) {
		return gitHubService.GetIssueDetails(task.IssueID), nil
	})
	if err != nil {
		return err
	}

	isApproved := false
	attempt := 0
	lastFeedback := "Initial Attempt"

	for !isApproved && attempt < 3 {
		attempt++
		correlationID := fmt.Sprintf("%s-attempt-%d", ctx.TaskID, attempt)
		log.Printf("[Workflow] Attempt %d/3: Generating fix based on: '%s'", attempt, lastFeedback)

		proposedFix, err := sqlflow.Step(ctx, fmt.Sprintf("generate-code-fix-%d", attempt), func() (Solution, error) {
			return llmService.GenerateFix(bugReport.StackTrace, lastFeedback), nil
		})
		if err != nil {
			return err
		}

		_, err = sqlflow.Step(ctx, fmt.Sprintf("notify-reviewer-%d", attempt), func() (bool, error) {
			gitHubService.RequestHumanReview(task.IssueID, proposedFix, correlationID)
			notificationService.NotifyReviewer(task.IssueID, correlationID)
			return true, nil
		})

		if err != nil {
			return err
		}

		log.Printf("[Workflow] Review requested for %s. Agent goes to sleep and waits...", correlationID)

		approval, err := sqlflow.AwaitEvent[HumanApproval](
			ctx,
			fmt.Sprintf("agent-approval:%s:%s", task.IssueID, correlationID),
			fmt.Sprintf("wait-for-human-review-%d", attempt),
			nil,
		)
		if err != nil {
			return err
		}

		isApproved = approval.Approved
		if approval.Reason != "" {
			lastFeedback = approval.Reason
		} else {
			lastFeedback = "No feedback has been given"
		}

		if !isApproved {
			log.Printf("[Workflow] ❌ Attempt %d was rejected: %s", attempt, lastFeedback)
		}
	}

	if isApproved {
		log.Println("[Workflow] ✅ Fix approved! Creating pull request...")

		prURL, err := sqlflow.Step(ctx, "create-pull-request", func() (string, error) {
			return gitHubService.CreatePullRequest(task.IssueID, "apply-fix"), nil
		})
		if err != nil {
			return err
		}

		log.Printf("[Workflow] Mission accomplished! PR created: %s", prURL)
		return nil
	}

	log.Printf("[Workflow] 🚨 Maximum attempts reached. Escalating ticket %s to a human.", task.IssueID)
	_, err = sqlflow.Step(ctx, "notify-senior-developer", func() (bool, error) {
		gitHubService.EscalateToSenior(task.IssueID, "Agent could not find a solution after 3 attempts.")
		return true, nil
	})

	return err
}
```

## Interacting with the System: Providing HTTP Endpoints ##

Now there are two HTTP endpoints for interacting with the Workflow: 

* The `/agent/start` endpoint kicks off the process asynchronously and returns immediately.
* The `/agent/review/...` endpoint acts as our callback webhook. 
    * When the human reviewer approves or rejects a fix, this endpoint emits the event back into the queue, which then wakes up the sleeping job right where it left off.

The standard Go `net/http` application serves as the host for our SqlFlow runtime.

During application startup, we:

* Create a PostgreSQL driver and establish the database connection.
* Create a SqlFlow client instance.
* Create or verify the workflow queue.
* Register the workflow implementation under a task name.
Ü Start a worker that continuously polls the queue and executes tasks.

The worker runs in the background using goroutines alongside the HTTP server. It continuously claims available tasks from the 
queue, executes workflow steps, persists checkpoints, and resumes suspended workflows when events arrive.

```go
var sqlflowClient *sqlflow.Client

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	connString := "postgres://postgres:password@localhost:5432/sqlflow_db"
	dbDriver, err := postgres.NewPostgresDriver(ctx, connString)
	if err != nil {
		log.Fatalf("Error connecting to the database: %v", err)
	}
	defer dbDriver.Close()

	sqlflowClient = sqlflow.NewClient(dbDriver)
	_ = sqlflowClient.CreateQueue(ctx, "ai-agent-queue", "unpartitioned")

	sqlflow.RegisterWorkflow(sqlflowClient, "solve-bug", autonomousAgentWorkflow)

	workerOpts := sqlflow.WorkerOptions{
		WorkerID:     "agent-worker-1",
		QueueName:    "ai-agent-queue",
		PollInterval: 1 * time.Second,
		Concurrency:  5,
	}
	
	worker := sqlflowClient.CreateWorker(workerOpts)
	worker.Start(ctx)
	
	log.Println("Background worker started.")

	mux := http.NewServeMux()

	mux.HandleFunc("POST /agent/start", func(w http.ResponseWriter, r *http.Request) {
		var task AgentTask
		if err := json.NewDecoder(r.Body).Decode(&task); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		options := sqlflow.SpawnOptions{QueueName: "ai-agent-queue"}
		res, err := sqlflowClient.Spawn(r.Context(), options, "solve-bug", task)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"run_id":  res.RunID,
			"task_id": res.TaskID,
			"status":  fmt.Sprintf("Agent dispatched to fix Issue #%s", task.IssueID),
		})
	})

	mux.HandleFunc("POST /agent/review/{issue_id}/{correlation_id}", func(w http.ResponseWriter, r *http.Request) {
		issueID := r.PathValue("issue_id")
		correlationID := r.PathValue("correlation_id")

		var approval HumanApproval
		if err := json.NewDecoder(r.Body).Decode(&approval); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		eventName := fmt.Sprintf("agent-approval:%s:%s", issueID, correlationID)
		options := sqlflow.EmitEventOptions{QueueName: "ai-agent-queue"}
		
		err := sqlflowClient.EmitEvent(r.Context(), options, eventName, approval)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		msg := fmt.Sprintf("Fix for %s rejected. Agent will try again using feedback: '%s'", correlationID, approval.Reason)
		if approval.Approved {
			msg = fmt.Sprintf("Fix for %s approved. Agent is completing the work.", correlationID)
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"message": msg})
	})

	server := &http.Server{
		Addr:    ":8000",
		Handler: mux,
	}

	go func() {
		log.Println("Web API is running at http://localhost:8000")
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("HTTP Server Fehler: %v", err)
		}
	}()

	<-ctx.Done()
	log.Println("Shutting down system...")

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	
	server.Shutdown(shutdownCtx)
	worker.Stop()
	time.Sleep(500 * time.Millisecond)
	
	log.Println("System shut down successfully.")
}
```

## An Example Session with the AI Agent Job ##

### Getting the Tooling right ###

It's not stone age. I want to use tooling to fire my HTTP requests. There's somewhat of a standard 
established for tooling, which is the `http` format for HTTP requests.

And while it's easy to use `*.http` files with Visual Studio, IntelliJ doesn't come with a UI 
in it's Community Edition. But do not fear, you don't have to fight `curl`. JetBrains offers a 
CLI called `ijhttp` we can use.

We start by downloading it off the JetBrains pages:

```powershell
curl.exe -f -L -o ijhttp.zip "https://jb.gg/ijhttp/latest"
```

And extract it to a folder `Tools` in the User Profile:

```
Expand-Archive .\ijhttp.zip -DestinationPath "$env:USERPROFILE\Tools\ijhttp"
```

We can then add `ijhttp` to the search `Path` in Windows:

```powershell
$folder = "$env:USERPROFILE\Tools\ijhttp\ijhttp"
$userPath = [Environment]::GetEnvironmentVariable("Path", "User")

[Environment]::SetEnvironmentVariable("Path", "$userPath;$folder", "User")
```

### The *.http File with the Requests ###

```java
@baseUrl = https://localhost:5000
@issueId = 12345
@delayMs = 30000

### Start the Agent Job
# @name startAgent
POST {{baseUrl}}/agent/start
Content-Type: application/json

{
  "issue_id": "{{issueId}}"
}

> {%
    client.test("Agent was started", function () {
        client.assert(response.status === 200, "Expected HTTP 200");
        client.assert(response.body.taskId, "Response does not contain taskId");
    });

    client.global.set("taskId", response.body.taskId);
    client.log("Stored taskId: " + response.body.taskId);
%}

### Reject the first attempt after a delay
< {%
    await sleep(Number(request.variables.get("delayMs")));
%}
POST {{baseUrl}}/agent/review/{{issueId}}/{{taskId}}-attempt-1
Content-Type: application/json

{
  "approved": false,
  "reason": "This is way too simple, add a better error handling strategy!"
}

> {%
    client.test("First review was submitted", function () {
        client.assert(response.status === 200, "Expected HTTP 200");
    });
%}

### Approve the second attempt after another delay
< {%
    await sleep(Number(request.variables.get("delayMs")));
%}
POST {{baseUrl}}/agent/review/{{issueId}}/{{taskId}}-attempt-2
Content-Type: application/json

{
  "approved": true,
  "reason": "Now, this looks good!"
}

> {%
    client.test("Second review was submitted", function () {
        client.assert(response.status === 200, "Expected HTTP 200");
    });
%}
```

### Analyzing the Session ###

We start by running our Go project:

```bash
go run main.go
```

We'll then run our *.http script using ijhttp -L VERBOSE agent-requests.http.

The first request for fixing an issue 12345 is sent:

```bash
PS sqlflow-example\requests> ijhttp -L VERBOSE agent-requests.http
┌─────────────────────────────────────────────────────────────────────────────┐
│                      Running IntelliJ HTTP Client with                      │
├────────────────────────┬────────────────────────────────────────────────────┤
│         Files          │ agent-requests.http                                │
├────────────────────────┼────────────────────────────────────────────────────┤
│   Public Environment   │                                                    │
├────────────────────────┼────────────────────────────────────────────────────┤
│  Private Environment   │                                                    │
└────────────────────────┴────────────────────────────────────────────────────┘
Request 'startAgent' POST http://localhost:8000/agent/start
= request =>
POST http://localhost:8000/agent/start
Content-Type: application/json
Content-Length: 25
User-Agent: IntelliJ HTTP Client/CLI 2026.1
Accept-Encoding: br, deflate, gzip, x-gzip
Accept: */*

{
  "issue_id": "12345"
}

###

<= response =
HTTP/1.1 200 OK
date: Sun, 23 Aug 2026 09:25:51 GMT
server: Go-http-client/1.1
content-length: 146
content-type: application/json

{"run_id":"15fa7c33-93c6-4179-92ed-2c7bd7001627","task_id":"27a11757-3ff2-4baa-9092-5e924dba5a6f","status":"Agent dispatched to fix Issue #12345"}

Response code: 200 (OK); Time: 454ms (454 ms); Content length: 146 bytes (146 B)
```

In the Go backend output, we can see our fictional agent doing its work. It goes idle and requests a human review. We'll reject the fix.

```bash
Request 'Reject the first attempt after a delay' POST http://localhost:8000/agent/review/12345/27a11757-3ff2-4baa-9092-5e924dba5a6f-attempt-1
= request =>
POST http://localhost:8000/agent/review/12345/27a11757-3ff2-4baa-9092-5e924dba5a6f-attempt-1
Content-Type: application/json
Content-Length: 100
User-Agent: IntelliJ HTTP Client/CLI 2026.1
Accept-Encoding: br, deflate, gzip, x-gzip
Accept: */*

{
  "approved": false,
  "reason": "This is way too simple, add a better error handling strategy!"
}

###

<= response =
HTTP/1.1 200 OK
date: Sun, 23 Aug 2026 09:26:22 GMT
server: Go-http-client/1.1
content-length: 175
content-type: application/json

{"message":"Fix for 27a11757-3ff2-4baa-9092-5e924dba5a6f-attempt-1 rejected. Agent tries again with feedback: 'This is way too simple, add a better error handling strategy!'"}

Response code: 200 (OK); Time: 19ms (19 ms); Content length: 175 bytes (175 B)
```

We can see the Go worker waking up, restoring state and generating another fix based on our feedback. Let's accept the fix:

```bash
Request 'Approve the second attempt after another delay' POST http://localhost:8000/agent/review/12345/27a11757-3ff2-4baa-9092-5e924dba5a6f-attempt-2
= request =>
POST http://localhost:8000/agent/review/12345/27a11757-3ff2-4baa-9092-5e924dba5a6f-attempt-2
Content-Type: application/json
Content-Length: 59
User-Agent: IntelliJ HTTP Client/CLI 2026.1
Accept-Encoding: br, deflate, gzip, x-gzip
Accept: */*

{
  "approved": true,
  "reason": "Now, this looks good!"
}

###

<= response =
HTTP/1.1 200 OK
date: Sun, 23 Aug 2026 09:26:53 GMT
server: Go-http-client/1.1
content-length: 112
content-type: application/json

{"message":"Fix for 27a11757-3ff2-4baa-9092-5e924dba5a6f-attempt-2 approved. Agent is now completing its work."}

Response code: 200 (OK); Time: 21ms (21 ms); Content length: 112 bytes (112 B)
```

In the Go backend logs, we can observe the entire lifecycle: how the workflow is suspended, fully unloaded from 
memory, and then replayed from the database checkpoints exactly when our http requests arrive:

```bash
2026/08/23 09:25:50 Worker agent-worker-1 started on queue 'ai-agent-queue'
2026/08/23 09:25:50 Background worker started.
2026/08/23 09:25:50 Web API is running at http://localhost:8000
2026/08/23 09:27:02 [Workflow] Agent starts investigation for ticket 12345
2026/08/23 09:27:02 [Workflow] Attempt 1/3: Generating fix based on: 'Initial Attempt'
2026/08/23 09:27:02 [Workflow] Review requested for 27a11757-3ff2-4baa-9092-5e924dba5a6f-attempt-1. Agent goes to sleep and waits...
2026/08/23 09:27:02 [Workflow] ❌ Attempt 1 was rejected: This is way too simple, add a better error handling strategy!
2026/08/23 09:27:02 [Workflow] Attempt 2/3: Generating fix based on: 'This is way too simple, add a better error handling strategy!'
2026/08/23 09:27:02 [Workflow] Review requested for 27a11757-3ff2-4baa-9092-5e924dba5a6f-attempt-2. Agent goes to sleep and waits...
2026/08/23 09:27:02 [Workflow] ✅ Fix approved! Creating pull request...
2026/08/23 09:27:02 [GitHubService] PR for issue #12345 has been created...
2026/08/23 09:27:03 [Workflow] Mission accomplished! PR created: https://github.com/company/repo/pull/7971
```