// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using Microsoft.AspNetCore.Mvc;
using SqlFlowSdk;
using SqlFlowSdk.AiSample;
using SqlFlowSdk.AiSample.Docker;
using SqlFlowSdk.AiSample.Models;
using SqlFlowSdk.AiSample.Services;
using SqlFlowSdk.Core;
using SqlFlowSdk.Extensions;
using SqlFlowSdk.Management.AspNetCore;
using SqlFlowSdk.Management.Postgres;

var builder = WebApplication.CreateBuilder(args);


// Start Docker Containers for dependencies
await DockerContainers.StartAllContainersAsync();

string connectionString = DockerContainers.PostgresContainer.GetConnectionString();

// Add Configuration
builder.Configuration
    .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
    .AddJsonFile($"appsettings.{builder.Environment.EnvironmentName}.json", optional: true, reloadOnChange: true)
    .AddEnvironmentVariables();

// Add Logging
builder.Services.AddLogging(loggingBuilder => loggingBuilder.AddConsole());

// Add Cors
const string CorsPolicyName = "FrontendCorsPolicy";

var allowedOrigins = builder.Configuration.GetSection("Cors:AllowedOrigins").Get<string[]>() ?? Array.Empty<string>();

builder.Services.AddCors(options =>
{
    options.AddPolicy(CorsPolicyName, policy =>
    {
        policy
            .WithOrigins(allowedOrigins)
            .AllowAnyHeader()
            .AllowAnyMethod();
    });
});

// Add Services
builder.Services.AddSingleton<ILlmService, LlmService>();
builder.Services.AddSingleton<IGitHubService, GitHubService>();
builder.Services.AddSingleton<ILocalNotificationService, LocalNotificationService>();

// Register the SqlFlow SDK
builder.Services.AddSqlFlowSdk(connectionString);
builder.Services.AddSqlFlowQueryApi(connectionString);
builder.Services.AddSqlFlowAdminApi(connectionString);

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

app.UseCors(CorsPolicyName);

// Map the SqlFlow Management endpoints for observing, analyzing and managing the workflow system. This
// provides a web interface to view the status of tasks, queues, and other relevant information about the
// workflow system.
app
    .MapSqlFlowQueryEndpoints()
    .MapSqlFlowAdminEndpoints();

// A Webhook triggers the Agent, such as a new JIRA ticket or GitHub issue
app.MapPost("/agent/start", async (ISqlFlow client, [FromBody] AgentTask task, CancellationToken ct) =>
{
    var result = await client.SpawnAsync(new SpawnOptions
    {
        Queue = "ai-agent-queue"
    }, "solve-bug", task, ct);

    return Results.Ok(new { RunId = result.RunId, TaskId = result.TaskId, Status = $"Agent dispatched to fix Isse #{task.IssueId}" });
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