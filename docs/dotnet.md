# Getting Started with the .NET SDK

To include SqlFlowSdk in your project, install the NuGet package using the .NET CLI:

```bash
dotnet add package SqlFlowSdk
```

Also add the SDK Implementation for the Database Management System to use:

```bash
dotnet add package SqlFlowSdk.Postgres
# or
dotnet add package SqlFlowSdk.SqlServer
```

### Optional: NATS JetStream Signaling & Management APIs
To eliminate database CPU fan-out and connection exhaustion at scale, you can replace the default database signaling with NATS JetStream: `dotnet add package SqlFlowSdk.Nats`

If you want to add the Management Endpoints for the Control Panel: `dotnet add package SqlFlowSdk.Management.Postgres`


## Building a Durable AI Agent

The classic examples for durable execution are usually e-commerce checkouts. But there's a rapidly growing use case developers are dealing with: Autonomous AI Agents. Building AI agents introduces severe state-management challenges:

1. **Slow API Calls:** LLM API calls are inherently slow, prone to timeouts, and expensive. If a server crashes while waiting 30 seconds for an AI generation, standard `async/await` state is lost forever.
2. **Human-in-the-Loop:** You don't want an AI pushing code to production without human review. Agents need to pause execution, ask a human for permission, and resume only when approved (hours or days later).

Traditional approaches require you to build complex state machines, database polling loops, or heavy external infrastructure. With SqlFlow, we can write our agent as standard, sequential C# code. The framework will automatically checkpoint the state, sleep without blocking server threads, and wake up exactly where it left off.

### 1. The Domain Models

Let's define the data models representing our inputs, states, and final output for an AI Agent that fixes GitHub bugs.

```csharp
public class AgentTask {
    [JsonPropertyName("issue_id")]
    public string IssueId { get; set; } = ""; 
}

public class Solution {
    [JsonPropertyName("patched_code")]
    public string PatchedCode { get; set; } = ""; 
}

public class HumanApproval {
    [JsonPropertyName("approved")]
    public bool Approved { get; set; }
    [JsonPropertyName("reason")]
    public string? Reason { get; set; } 
}

public class AgentResult {
    [JsonPropertyName("success")]
    public bool Success { get; set; }
    [JsonPropertyName("pull_request_url")]
    public string? PullRequestUrl { get; set; }
}
```

### 2. The External Services (Simulated)

The agent interacts with the outside world. We simulate a slow LLM and a GitHub service. Because these calls are wrapped by SqlFlow later, we won't lose our place if the app recycles during the `Task.Delay`.

```csharp
public interface ILlmService {
    Task<Solution> GenerateFixAsync(string log, string lastFeedback, CancellationToken ct);
}

public class LlmService : ILlmService {
    public async Task<Solution> GenerateFixAsync(string log, string lastFeedback, CancellationToken ct) {
        // Simulate a very expensive LLM call
        await Task.Delay(2500, ct);

        string code = lastFeedback.Contains("error handling")
            ? "// AI: Improved Logging & Error-Handling added\nif(data == null) throw new ArgumentNullException();" 
            : "// AI: Simple Fix for the NullReferenceException\nif(data == null) return;";
        return new Solution { PatchedCode = code };
    }
}

// Imagine IGitHubService exists with methods:
// - Task<Issue> GetIssueDetailsAsync(string issueId, CancellationToken ct)
// - Task RequestHumanReviewAsync(string issueId, Solution fix, string correlationId, CancellationToken ct)
// - Task<string> CreatePullRequestAsync(string issueId, string code, CancellationToken ct)
```

### 3. The Autonomous Agent Job (The Core Concept)

We define our logic inside an `IJob`. The magic is in the **`ctx.Step`** method: every time a step completes, its result is automatically checkpointed to the Postgres database. If the process crashes, the framework replays the job, skips the completed steps, and loads their results directly from the database.

Furthermore, we use **`ctx.AwaitEvent`** to wait for human interaction. This instructs the engine to safely suspend the workflow state to the database and free up the worker thread entirely until an external system fires the webhook event.

```csharp
public class AutonomousAgentJob : IJob<AgentTask, AgentResult>
{
    private readonly ILlmService _llmService;
    private readonly IGitHubService _gitHubService;

    public AutonomousAgentJob(ILlmService llmService, IGitHubService gitHubService)
    {
        _llmService = llmService;
        _gitHubService = gitHubService;
    }

    public async Task<AgentResult> ExecuteAsync(TaskContext ctx, AgentTask task)
    {
        var bugReport = await ctx.Step("fetch-issue-context", async () =>
            await _gitHubService.GetIssueDetailsAsync(task.IssueId, ctx.CancellationToken));

        bool isApproved = false;
        int attempt = 0;
        string lastFeedback = "Initial Attempt";

        while (!isApproved && attempt < 3)
        {
            attempt++;
            string correlationId = $"attempt-{attempt}";

            Solution proposedFix = await ctx.Step($"generate-code-fix-{attempt}", async () =>
                await _llmService.GenerateFixAsync(bugReport.StackTrace, lastFeedback, ctx.CancellationToken));

            await ctx.Step($"notify-reviewer-{attempt}", async () => {
                await _gitHubService.RequestHumanReviewAsync(task.IssueId, proposedFix, correlationId, ctx.CancellationToken);
            });

            // Wait for a human decision without blocking a thread! State is saved to DB here.
            JsonNode? review = await ctx.AwaitEvent(
                eventName: $"agent-approval:{task.IssueId}:{correlationId}",
                stepName: $"wait-for-human-review-{attempt}"
            );
            
            isApproved = review["approved"]?.GetValue<bool>() ?? false;
            lastFeedback = review["reason"]?.GetValue<string>() ?? "No feedback has been given";
        }

        if (isApproved)
        {
            string prUrl = await ctx.Step("create-pull-request", async () =>
                await _gitHubService.CreatePullRequestAsync(task.IssueId, "apply-fix", ctx.CancellationToken));

            return new AgentResult { Success = true, PullRequestUrl = prUrl };
        }

        return new AgentResult { Success = false };
    }
}
```

### 4. Dependency Injection & Endpoints (Program.cs)

We configure a background worker to process the tasks, and expose HTTP endpoints to interact with the durable execution engine.

```csharp
var builder = WebApplication.CreateBuilder(args);
string connectionString = "Host=127.0.0.1;Port=5432;Database=sqlflow;Username=postgres;Password=password;";

builder.Services.AddSingleton<ILlmService, LlmService>();
builder.Services.AddSingleton<IGitHubService, GitHubService>();

// Register the SqlFlow SDK and setup the worker
builder.Services
    .AddSqlFlowPostgres(connectionString)
    // .AddNatsSignaling("nats://localhost:4222") // Uncomment to instantly scale to NATS JetStream
    .AddWorker("ai-agent-queue", worker =>
    {
        worker.SetConcurrency(5).SetPollInterval(1);
        worker.AddJob<AutonomousAgentJob, AgentTask, AgentResult>("solve-bug");
    });

var app = builder.Build();

// Endpoint to spawn the durable task
app.MapPost("/agent/start", async (ISqlFlow client, [FromBody] AgentTask task, CancellationToken ct) =>
{
    var result = await client.SpawnAsync(new SpawnOptions { Queue = "ai-agent-queue" }, "solve-bug", task, ct);
    return Results.Ok(new { RunId = result.RunId, TaskId = result.TaskId });
});

// Webhook for the Human Reviewer to resume the suspended task
app.MapPost("/agent/review/{issueId}/{correlationId}", async (IEventPublisher publisher, string issueId, string correlationId, [FromBody] HumanApproval approval, CancellationToken ct) =>
{
    await publisher.EmitEventAsync(
        queue: "ai-agent-queue", 
        eventName: $"agent-approval:{issueId}:{correlationId}", 
        payload: approval, ct);
    return Results.Ok();
});

app.Run();
```