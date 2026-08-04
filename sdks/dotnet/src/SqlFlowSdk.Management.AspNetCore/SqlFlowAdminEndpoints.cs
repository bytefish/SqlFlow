// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Routing;
using SqlFlowSdk.Management.Models;

namespace SqlFlowSdk.Management.AspNetCore;

/// <summary>
/// Maps all SqlFlow Admin API endpoints under a configurable route prefix (default: /api/sqlflow-admin).
/// </summary>
public static class SqlFlowAdminEndpoints
{
    public static IEndpointRouteBuilder MapSqlFlowAdminEndpoints(
        this IEndpointRouteBuilder routes,
        string routePrefix = "/api/sqlflow-admin")
    {
        var group = routes.MapGroup(routePrefix)
                          .WithTags("SqlFlow Administration");

        group.MapGet("/queues", async (ISqlFlowAdminService admin, CancellationToken ct) =>
            Results.Ok(await admin.GetAllQueuesAsync(ct)))
            .WithName("AdminGetAllQueues");

        group.MapPost("/queues", async (ISqlFlowAdminService admin, [FromBody] CreateQueueCommand req, CancellationToken ct) =>
        {
            await admin.CreateQueueAsync(req, ct);
            return Results.Ok(new { Message = $"Queue '{req.QueueName}' created successfully." });
        })
            .WithName("AdminCreateQueue");

        group.MapDelete("/queues/{queueName}", async (ISqlFlowAdminService admin, string queueName, CancellationToken ct) =>
        {
            await admin.DropQueueAsync(queueName, ct);
            return Results.Ok(new { Message = $"Queue '{queueName}' and all its data dropped." });
        })
            .WithName("AdminDropQueue");


        // Task Interventions
        group.MapPost("/tasks/cancel", async (ISqlFlowAdminService admin, [FromBody] CancelTaskCommand req, CancellationToken ct) =>
        {
            await admin.CancelTaskAsync(req, ct);
            return Results.Ok(new { Message = $"Task '{req.TaskId}' in queue '{req.QueueName}' has been cancelled." });
        })
            .WithName("AdminCancelTask");

        group.MapPost("/tasks/bulk-cancel", async (ISqlFlowAdminService admin, [FromBody] BulkCancelTasksCommand req, CancellationToken ct) =>
        {
            await admin.BulkCancelTasksAsync(req, ct);
            return Results.Ok(new { Message = $"Bulk cancelled {req.TaskIds.Count} tasks in queue '{req.QueueName}'." });
        })
            .WithName("AdminBulkCancelTasks");

        // Worker and Run Control

        group.MapPost("/workers/release-claims", async (ISqlFlowAdminService admin, [FromBody] ReleaseWorkerClaimsCommand req, CancellationToken ct) =>
        {
            await admin.ReleaseWorkerClaimsAsync(req, ct);
            return Results.Ok(new { Message = $"Claims for worker '{req.WorkerId}' released." });
        })
            .WithName("AdminReleaseWorkerClaims");

        group.MapPost("/runs/extend-claim", async (ISqlFlowAdminService admin, [FromBody] ExtendClaimCommand req, CancellationToken ct) =>
        {
            await admin.ExtendClaimAsync(req, ct);
            return Results.Ok(new { Message = $"Claim for run '{req.RunId}' extended by {req.ExtendBySeconds} seconds." });
        })
            .WithName("AdminExtendClaim");

        group.MapPost("/runs/wake", async (ISqlFlowAdminService admin, [FromBody] ScheduleWakeupCommand req, CancellationToken ct) =>
        {
            await admin.WakeRunAsync(req, ct);
            return Results.Ok(new { Message = $"Run '{req.RunId}' scheduled to wake." });
        })
            .WithName("AdminWakeRun");

        group.MapPost("/runs/complete", async (ISqlFlowAdminService admin, [FromBody] CompleteRunCommand req, CancellationToken ct) =>
        {
            await admin.ForceCompleteRunAsync(req, ct);
            return Results.Ok(new { Message = $"Run '{req.RunId}' manually marked as completed." });
        })
            .WithName("AdminCompleteRun");

        group.MapPost("/runs/fail", async (ISqlFlowAdminService admin, [FromBody] FailRunCommand req, CancellationToken ct) =>
        {
            await admin.ForceFailRunAsync(req, ct);
            return Results.Ok(new { Message = $"Run '{req.RunId}' manually failed." });
        })
            .WithName("AdminFailRun");


        // Event and Checkpoint Interventions

        group.MapPost("/events/emit", async (ISqlFlowAdminService admin, [FromBody] EmitEventCommand req, CancellationToken ct) =>
        {
            await admin.EmitEventAsync(req, ct);
            return Results.Ok(new { Message = $"Event '{req.EventName}' emitted in queue '{req.QueueName}'." });
        })
            .WithName("AdminEmitEvent");

        group.MapPost("/checkpoints", async (ISqlFlowAdminService admin, [FromBody] SetCheckpointCommand req, CancellationToken ct) =>
        {
            await admin.SetCheckpointAsync(req, ct);
            return Results.Ok(new { Message = $"Checkpoint '{req.StepName}' set for task '{req.TaskId}'." });
        })
            .WithName("AdminSetCheckpoint");

        // Cleanup and Maintenance

        group.MapPost("/cleanup/tasks", async (ISqlFlowAdminService admin, [FromBody] CleanupCommand req, CancellationToken ct) =>
        {
            int deleted = await admin.CleanupTasksAsync(req, ct);
            return Results.Ok(new { DeletedCount = deleted, Message = $"Cleaned up {deleted} old tasks." });
        })
            .WithName("AdminCleanupTasks");

        group.MapPost("/cleanup/events", async (ISqlFlowAdminService admin, [FromBody] CleanupCommand req, CancellationToken ct) =>
        {
            int deleted = await admin.CleanupEventsAsync(req, ct);
            return Results.Ok(new { DeletedCount = deleted, Message = $"Cleaned up {deleted} old events." });
        })
            .WithName("AdminCleanupEvents");

        return routes;
    }
}