using Microsoft.AspNetCore.Mvc;
using SqlFlowSdk.Core;
using SqlFlowSdk.Postgres;
using SqlFlowSdk.Nats;
using SqlFlowSdk.DistributedSample.WorkerApp.Services;
using SqlFlowSdk.DistributedSample.WorkerApp.Models;

var builder = WebApplication.CreateBuilder(args);

// 1. Connection Strings direkt von Aspire aus der Konfiguration/Umgebung lesen
// (Kein DockerContainers.StartAllContainersAsync() mehr nötig!)
string pgConnectionString = builder.Configuration.GetConnectionString("SqlFlowDb")
    ?? throw new InvalidOperationException("Connection string 'SqlFlowDb' fehlt.");

string natsUrl = builder.Configuration.GetConnectionString("nats-broker")
    ?? throw new InvalidOperationException("Connection string 'nats-broker' fehlt.");

builder.Configuration
    .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
    .AddEnvironmentVariables();

builder.Services.AddLogging(loggingBuilder => loggingBuilder.AddConsole());

// 2. Sample-Services registrieren
builder.Services.AddSingleton<ILlmService, LlmService>();
builder.Services.AddSingleton<IGitHubService, GitHubService>();
builder.Services.AddSingleton<ILocalNotificationService, LocalNotificationService>();

// Multi-Tenant Konfiguration
const string tenantId = "tenant-alpha";
const string queueName = "ai-agent-queue";

// 3. SqlFlow SDK übernimmt das komplette SQL-Locking, NATS-Signaling & Retry-Handling
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

// 4. Deine Webhooks bleiben exakt gleich
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