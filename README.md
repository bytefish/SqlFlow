# SqlFlow #

SqlFlow is a simple durable execution workflow system, for PostgreSQL and SQL Server. It handles scheduling 
and retries, without needing any other services to run in addition to PostgreSQL or SQL Server.

The SQL Script for creating the SqlFlow Database Schema is available here:

* [https://github.com/bytefish/SqlFlow/blob/main/sql/](https://github.com/bytefish/SqlFlow/blob/main/sql)

It took a large deal of inspiration from Absurd, but it features a much simpler database model. 

## 1. Setup ##

To include SqlFlowSdk in your project, install the NuGet package using the .NET CLI:

```
dotnet add package SqlFlowSdk
```

Also add the SDK Implementation for the Database Management System to use:

```
dotnet add package SqlFlowSdk.Postgres
dotnet add package SqlFlowSdk.SqlServer
```

If you want to add the Management Endpoints for the Control Panel, you need to add:

```
dotnet add package SqlFlowSdk.Management
```

Also add the Management API Implementation for the Database Management System to use:

```
dotnet add package SqlFlowSdk.Management.Postgres
dotnet add package SqlFlowSdk.Management.SqlServer
```

You'll then need to create the `ssf` Database Schema in the DBMS of your choice:

* `sql/ssf-postgres.sql`
* `sql/ssf-sqlserver.sql`

## 2. Quick Start ##

Start by creating the `ssf` Database Schema, that holds all required database objects for the Workflow system:

* `sql/ssf-postgres.sql`
* `sql/ssf-sqlserver.sql`

The define an `IJob`, which is going to model an Order Fulfillment Task:

```csharp
public class FulfillOrderJob : IJob<OrderData, FulfillOrderResult>
{
    private readonly ILogger<FulfillOrderJob> _logger;

    private readonly PaymentService _paymentService;
    private readonly ShippingService _shippingService;
    
    public FulfillOrderJob(
        PaymentService paymentService,
        ShippingService shippingService,
        ILogger<FulfillOrderJob> logger)
    {
        _paymentService = paymentService;
        _shippingService = shippingService;
        _logger = logger;
    }

    public async Task<FulfillOrderResult> ExecuteAsync(TaskContext ctx, OrderData order)
    {
        _logger.LogInformation("Processing Order {OrderId}", order.OrderId);

        // Process the Payment
        PaymentResult payment = await ctx.Step("charge-payment", async () =>
        {
            return await _paymentService.ChargeAsync(order.OrderId, order.Amount);
        });

        if (!payment.Success)
        {
            throw new Exception($"Payment failed: {payment.ErrorMessage}");
        }

        // Wait for Warehouse
        _logger.LogInformation("Waiting for pick signal...");

        JsonNode pickPayload = await ctx.AwaitEvent(
            eventName: $"order-picked:{order.OrderId}",
            stepName: "wait-for-picking"
        );

        // Ship the items
        ShippingResult shipment = await ctx.Step("ship-items", async () =>
        {
            return await _shippingService.ShipAsync(order.OrderId, order.Items);
        });

        return new FulfillOrderResult { Status = "Fulfilled", Tracking = shipment.TrackingNumber };
    }
}
```

We then register it in the `Program.cs` like this:

```csharp
// Your custom connection string
string connectionString = "Server=localhost;Database=DeinDatenbankName;Integrated Security=True;TrustServerCertificate=True";

// Add Logging
builder.Services.AddLogging();

// Register Services
builder.Services.AddSingleton<PaymentService>();
builder.Services.AddSingleton<ShippingService>();
builder.Services.AddSingleton<OrderService>();

// Register the SqlFlow SDK
builder.Services.AddSqlFlowSdk(connectionString);
```

We can then create a Background Workers for polling and executing tasks:

```csharp
// Configure Workers and Jobs. In this example, we have two different queues
// for standard and VIP orders, each with its own processing configuration.
builder.Services.AddSqlFlowWorker("standard-orders-queue", worker =>
{
    worker
        .SetConcurrency(1)
        .SetPollInterval(1);

    worker.AddJob<FulfillOrderJob, OrderData, FulfillOrderResult>("standard-fulfill", options =>
    {
        options.WithMaxAttempts(3);
    });
});

builder.Services.AddSqlFlowWorker("vip-orders-queue", worker =>
{
    worker
        .SetConcurrency(5)
        .SetPollInterval(0.5);

    worker.AddJob<FulfillOrderJob, OrderData, FulfillOrderResult>("vip-fulfill", options =>
    {
        options.WithMaxAttempts(5);
    });
});
```

And finally we define two endpoints to create an order and emit events to it:

```csharp
// A User places an order through this endpoint. Depending on whether it's a VIP order or not, it gets published
// to a different queue with different processing configurations.
app.MapPost("/order", async (IJobPublisher publisher, [FromBody] OrderData request, CancellationToken ct) =>
{
    // VIP Orders go to the VIP Queue with a different Job configuration (e.g. more retries, faster processing, etc.)
    string queueName = request.IsPremium ? "vip-orders-queue" : "standard-orders-queue";

    SpawnResult result = await publisher.PublishAsync<FulfillOrderJob, OrderData>("vip-fulfill", request, ct);

    return Results.Ok(new { RunId = result.RunId });
});

app.MapPost("/order/{orderId}/picked", async (OrderService orderService, IEventPublisher publisher, string orderId, [FromBody] PickingData data, CancellationToken ct) =>
{
    // We fetch the OrderData to determine which queue to publish the event to. In a real application, you might have this
    // information cached or included in the request to avoid an extra database call.
    OrderData orderData = await orderService.GetOrderByIdAsync(orderId);

    // Premium Customers Events go to the VIP Queue with a different Job configuration (e.g. more retries, faster processing, etc.)
    string queueName = orderData.IsPremium ? "vip-orders-queue" : "standard-orders-queue";

    // This wakes up the suspended task waiting for "order-picked:{orderId}"
    await publisher.EmitEventAsync(
        queue: queueName,
        eventName: $"order-picked:{orderId}",
        payload: data,
        ct
    );

    return Results.Ok(new { Message = "Pick signal sent. Workflow will resume." });
});
```

To kick off an Order send the JSON Payload to the endpoints:

```http
### Creates Order "ORD-123"
POST https://localhost:5000/order
Content-Type: application/json
Accept-Language: en-US,en;q=0.5
{
  "orderId": "ORD-123",
  "isPremium": true,
  "amount": 99.50,
  "items": ["Item A", "Item B"]
}

### Continues running Order "ORD-123"
POST https://localhost:5000/order/ORD-123/picked
Content-Type: application/json
{ 
  "picker": "Philipp",
  "pickedAt": "2024-06-01T10:15:30Z"
}
```

## 3. Management, Control Panel and Diagnosing the System Health ##

SqlFlow comes with a Management API and a Control Panel to understand you systems health and quickly 
find out how many tasks are being processed, which tasks are slow, which tasks currently await events, 
why tasks are failing and search for specific tasks.


<a href="https://raw.githubusercontent.com/bytefish/SqlFlow/main/doc/control-panel-event-blockades.jpg">
    <img src="https://raw.githubusercontent.com/bytefish/SqlFlow/main/doc/control-panel-event-blockades.jpg" alt="Screenshot of Event Blockades within the SqlFlow System" width="100%" />
</a>


It isn't meant to replace your Helm or Grafana dashboards, that also provide alarms and such. Think of it 
as another tool to observe your system at a much deeper level, than coarse metrics could provide.

## 4. A more complete application: Durable AI Agents ##

### What we are going to build ###

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

### Building an Agent Job ###

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

### The LLM Service ###

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

### The Autonomous Agent Job ###

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

### Putting It All Together ###

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
