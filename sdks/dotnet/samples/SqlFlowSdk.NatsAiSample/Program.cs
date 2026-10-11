// Licensed under the MIT license. See LICENSE file in the project root for full license information.
using Microsoft.AspNetCore.Mvc;
using SqlFlowSdk;
using SqlFlowSdk.AiSample; // Reusing jobs from original sample
using SqlFlowSdk.AiSample.Models;
using SqlFlowSdk.AiSample.Services;
using SqlFlowSdk.Core;
using SqlFlowSdk.Postgres;
using SqlFlowSdk.Nats;
using SqlFlowSdk.NatsAiSample.Docker;

var builder = WebApplication.CreateBuilder(args);

// Start both Postgres and NATS containers
await DockerContainers.StartAllContainersAsync();
string pgConnectionString = DockerContainers.PostgresContainer.GetConnectionString();
string natsUrl = DockerContainers.GetNatsUrl();

builder.Configuration
    .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
    .AddEnvironmentVariables();

builder.Services.AddLogging(loggingBuilder => loggingBuilder.AddConsole());

// Re-register the original sample services
builder.Services.AddSingleton<ILlmService, LlmService>();
builder.Services.AddSingleton<IGitHubService, GitHubService>();
builder.Services.AddSingleton<ILocalNotificationService, LocalNotificationService>();

// Configure Multi-Tenant Tenant Identifier Example
const string tenantId = "tenant-alpha";
const string queueName = "ai-agent-queue";

// Configure SqlFlow: Postgres as Source of Truth, NATS as the Wake-Up Signal with Multi-Tenancy
builder.Services
    .AddSqlFlowPostgres(pgConnectionString)
    .AddNatsSignaling(natsUrl) // Overrides standard signalling with multi-tenant NATS JetStream
    .AddWorker(tenantId, queueName, worker =>
    {
        worker
            .SetConcurrency(1)
            // Fallback smart-polling interval if NATS is ever temporarily unavailable
            .SetPollInterval(1);

        worker.AddJob<AutonomousAgentJob, AgentTask, AgentResult>("solve-bug", options =>
        {
            options.WithMaxAttempts(3);
        });
    });

var app = builder.Build();

// Multi-Tenant Webhook to start an agent job
app.MapPost("/agent/{tenantId}/start", async (
    [FromServices] IJobPublisher jobPublisher,
    [FromRoute] string tenantId,
    [FromBody] AgentTask task,
    CancellationToken ct) =>
{
    task.TenantId = tenantId;

    var result = await jobPublisher.PublishAsync<AutonomousAgentJob, AgentTask>(tenantId, "solve-bug", task, ct);

    return Results.Ok(new { TenantId = tenantId, RunId = result.RunId, TaskId = result.TaskId, Status = $"Agent dispatched for tenant {tenantId} to fix Issue #{task.IssueId}" });
});

// Multi-Tenant Webhook to review and resume agent workflow via NATS Event publishing
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