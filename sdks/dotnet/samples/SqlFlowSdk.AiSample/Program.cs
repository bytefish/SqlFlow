// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using Microsoft.AspNetCore.Mvc;
using SqlFlowSdk;
using SqlFlowSdk.AiSample;
using SqlFlowSdk.AiSample.Docker;
using SqlFlowSdk.AiSample.Models;
using SqlFlowSdk.AiSample.Services;
using SqlFlowSdk.Core;
using SqlFlowSdk.Management.AspNetCore;
using SqlFlowSdk.Management.Postgres;
using SqlFlowSdk.Postgres;

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

// Configure Workers and Jobs with Multi-Tenancy (Example Tenant: "tenant-alpha")
const string tenantId = "tenant-alpha";
const string queueName = "ai-agent-queue";

builder.Services
    .AddSqlFlowPostgres(connectionString)
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

// Register the SqlFlow SDK Query & Admin APIs
builder.Services.AddSqlFlowQueryApi(connectionString);
builder.Services.AddSqlFlowAdminApi(connectionString);

var app = builder.Build();

app.UseCors(CorsPolicyName);

app
    .MapSqlFlowQueryEndpoints()
    .MapSqlFlowAdminEndpoints();

// A Webhook triggers the Agent for a specific tenant
app.MapPost("/agent/start/{tenantId}", async (
    [FromServices] ISqlFlow client,
    [FromRoute] string tenantId,
    [FromBody] AgentTask task,
    CancellationToken ct) =>
{
    task.TenantId = tenantId;

    var result = await client.SpawnAsync(new SpawnOptions
    {
        TenantId = tenantId,
        Queue = queueName
    }, "solve-bug", task, ct);

    return Results.Ok(new { TenantId = tenantId, RunId = result.RunId, TaskId = result.TaskId, Status = $"Agent dispatched for tenant {tenantId} to fix Issue #{task.IssueId}" });
});

// A Lead-Developer clicks on "Approve" or "Reject" for a specific tenant
app.MapPost("/agent/review/{tenantId}/{issueId}/{correlationId}", async (
    [FromServices] IEventPublisher publisher,
    [FromRoute] string tenantId,
    [FromRoute] string issueId,
    [FromRoute] string correlationId,
    [FromBody] HumanApproval approval,
    CancellationToken ct) =>
{
    // Wake up the agent working on this specific tenant's issue
    await publisher.EmitEventAsync(tenantId, queueName, eventName: $"agent-approval:{tenantId}:{issueId}:{correlationId}", payload: approval, ct);

    string message = approval.Approved
        ? $"Fix for tenant {tenantId}, correlation {correlationId} approved. Agent is completing its work."
        : $"Fix for tenant {tenantId}, correlation {correlationId} rejected. Agent retries with feedback: '{approval.Reason}'";

    return Results.Ok(new { Message = message });
});

app.Run();