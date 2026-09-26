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

// Configure SqlFlow: Postgres as Source of Truth, NATS as the Wake-Up Signal
builder.Services
    .AddSqlFlowPostgres(pgConnectionString)
    .AddNatsSignaling(natsUrl) // Silently overrides Postgres LISTEN/NOTIFY
    .AddWorker("ai-agent-queue", worker =>
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

app.MapPost("/agent/start", async (
    [FromServices] ISqlFlow client,
    [FromBody] AgentTask task,
    CancellationToken ct) =>
{
    var result = await client.SpawnAsync(new SpawnOptions
    {
        Queue = "ai-agent-queue"
    }, "solve-bug", task, ct);

    return Results.Ok(new { RunId = result.RunId, TaskId = result.TaskId, Status = $"Agent dispatched to fix Issue #{task.IssueId}" });
});

app.MapPost("/agent/review/{issueId}/{correlationId}", async (
    [FromServices] IEventPublisher publisher,
    [FromRoute] string issueId,
    [FromRoute] string correlationId,
    [FromBody] HumanApproval approval,
    CancellationToken ct) =>
{
    await publisher.EmitEventAsync(queue: "ai-agent-queue", eventName: $"agent-approval:{issueId}:{correlationId}", payload: approval, ct);

    string message = approval.Approved
        ? $"Fix for {correlationId} approved. Agent is now completing its work."
        : $"Fix for {correlationId} rejected. Agent tries again with feedback: '{approval.Reason}'";

    return Results.Ok(new { Message = message });
});

app.Run();