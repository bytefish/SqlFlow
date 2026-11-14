using Microsoft.AspNetCore.Mvc;
using SqlFlowSdk.Core;
using SqlFlowSdk.Postgres;
using SqlFlowSdk.Nats;
using SqlFlowSdk.DistributedSample.WorkerApp.Services;
using SqlFlowSdk.DistributedSample.WorkerApp.Models;

var builder = WebApplication.CreateBuilder(args);

builder.Configuration
    .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
    .AddEnvironmentVariables();

// Read connection strings from configuration (appsettings.json or environment variables)
string pgConnectionString = builder.Configuration.GetConnectionString("SqlFlowDb")
    ?? throw new InvalidOperationException("Connection string 'SqlFlowDb' fehlt.");

// Read NATS connection string from configuration (appsettings.json or environment variables)
string natsUrl = builder.Configuration.GetConnectionString("nats-broker")
    ?? throw new InvalidOperationException("Connection string 'nats-broker' fehlt.");

builder.Services.AddLogging(loggingBuilder => loggingBuilder.AddConsole());

// Register Sample Services for LLM, GitHub, and Local Notifications
builder.Services.AddSingleton<ILlmService, LlmService>();
builder.Services.AddSingleton<IGitHubService, GitHubService>();
builder.Services.AddSingleton<ILocalNotificationService, LocalNotificationService>();

// Multitenant Configuration: Each tenant has its own queue for the autonomous agent
const string tenantId = "tenant-alpha";
const string queueName = "ai-agent-queue";

// Add SqlFlow Postgres and NATS Signaling, and configure the Worker with the AutonomousAgentJob
builder.Services
    .AddSqlFlowPostgres(pgConnectionString)
    .AddNatsSignaling(natsUrl)
    .AddWorker(tenantId, queueName, worker =>
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

// API Endpoint to start the autonomous agent for a specific tenant and issue
app.MapPost("/agent/{tenantId}/start", async (
    [FromServices] IJobPublisher jobPublisher,
    [FromRoute] string tenantId,
    [FromBody] AgentTask task,
    CancellationToken ct) =>
{
    task.TenantId = tenantId;
    var result = await jobPublisher.PublishAsync<AutonomousAgentJob, AgentTask>(tenantId, "solve-bug", task, ct);

    return Results.Ok(new
    {
        TenantId = tenantId,
        RunId = result.RunId,
        TaskId = result.TaskId,
        Status = $"Agent dispatched for tenant {tenantId} to fix Issue #{task.IssueId}"
    });
});

// API Endpoint to receive human approval or rejection for the proposed fix
app.MapPost("/agent/{tenantId}/review/{issueId}/{correlationId}", async (
    [FromServices] IEventPublisher publisher,
    [FromRoute] string tenantId,
    [FromRoute] string issueId,
    [FromRoute] string correlationId,
    [FromBody] HumanApproval approval,
    CancellationToken ct) =>
{
    await publisher.EmitEventAsync(tenantId, queueName, eventName: $"agent-approval:{tenantId}:{issueId}:{correlationId}", payload: approval, ct);

    string message = approval.Approved
        ? $"Fix for tenant {tenantId}, correlation {correlationId} approved. Agent is now completing its work."
        : $"Fix for tenant {tenantId}, correlation {correlationId} rejected. Agent tries again with feedback: '{approval.Reason}'";

    return Results.Ok(new { Message = message });
});

app.Run();