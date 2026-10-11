// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Routing;
using SqlFlowSdk.Management.Models;

namespace SqlFlowSdk.Management.AspNetCore;

/// <summary>
/// Maps all SqlFlow Admin API endpoints under a configurable route prefix with tenant isolation (default: /api/sqlflow-admin/{tenantId}).
/// </summary>
public static class SqlFlowAdminEndpoints
{
    public static IEndpointRouteBuilder MapSqlFlowAdminEndpoints(
        this IEndpointRouteBuilder routes,
        string routePrefix = "/api/sqlflow-admin/{tenantId}")
    {
        var group = routes.MapGroup(routePrefix)
                          .WithTags("SqlFlow Administration");

        group.MapGet("/queues", async (string tenantId, ISqlFlowAdminService admin, CancellationToken ct) =>
            Results.Ok(await admin.GetAllQueuesAsync(tenantId, ct)))
            .WithName("AdminGetAllQueues");

        group.MapPost("/queues", async (string tenantId, ISqlFlowAdminService admin, [FromBody] CreateQueueCommand req, CancellationToken ct) =>
        {
            await admin.CreateQueueAsync(tenantId, req, ct);
            return Results.Ok(new { TenantId = tenantId, Message = $"Queue '{req.QueueName}' created successfully for tenant '{tenantId}'." });
        })
            .WithName("AdminCreateQueue");

        group.MapDelete("/queues/{queueName}", async (string tenantId, ISqlFlowAdminService admin, string queueName, CancellationToken ct) =>
        {
            await admin.DropQueueAsync(tenantId, queueName, ct);
            return Results.Ok(new { TenantId = tenantId, Message = $"Queue '{queueName}' and all its data dropped for tenant '{tenantId}'." });
        })
            .WithName("AdminDropQueue");


        // Task Interventions
        group.MapPost("/tasks/cancel", async (string tenantId, ISqlFlowAdminService admin, [FromBody] CancelTaskCommand req, CancellationToken ct) =>
        {
            await admin.CancelTaskAsync(tenantId, req, ct);
            return Results.Ok(new { TenantId = tenantId, Message = $"Task '{req.TaskId}' in queue '{req.QueueName}' has been cancelled." });
        })
            .WithName("AdminCancelTask");

        group.MapPost("/tasks/bulk-cancel", async (string tenantId, ISqlFlowAdminService admin, [FromBody] BulkCancelTasksCommand req, CancellationToken ct) =>
        {
            await admin.BulkCancelTasksAsync(tenantId, req, ct);
            return Results.Ok(new { TenantId = tenantId, Message = $"Bulk cancelled {req.TaskIds.Count} tasks in queue '{req.QueueName}'." });
        })
            .WithName("AdminBulkCancelTasks");

        // Worker and Run Control

        group.MapPost("/workers/release-claims", async (string tenantId, ISqlFlowAdminService admin, [FromBody] ReleaseWorkerClaimsCommand req, CancellationToken ct) =>
        {
            await admin.ReleaseWorkerClaimsAsync(tenantId, req, ct);
            return Results.Ok(new { TenantId = tenantId, Message = $"Claims for worker '{req.WorkerId}' released." });
        })
            .WithName("AdminReleaseWorkerClaims");

        group.MapPost("/runs/extend-claim", async (string tenantId, ISqlFlowAdminService admin, [FromBody] ExtendClaimCommand req, CancellationToken ct) =>
        {
            await admin.ExtendClaimAsync(tenantId, req, ct);
            return Results.Ok(new { TenantId = tenantId, Message = $"Claim for run '{req.RunId}' extended by {req.ExtendBySeconds} seconds." });
        })
            .WithName("AdminExtendClaim");

        group.MapPost("/runs/wake", async (string tenantId, ISqlFlowAdminService admin, [FromBody] ScheduleWakeupCommand req, CancellationToken ct) =>
        {
            await admin.WakeRunAsync(tenantId, req, ct);
            return Results.Ok(new { TenantId = tenantId, Message = $"Run '{req.RunId}' scheduled to wake." });
        })
            .WithName("AdminWakeRun");

        group.MapPost("/runs/complete", async (string tenantId, ISqlFlowAdminService admin, [FromBody] CompleteRunCommand req, CancellationToken ct) =>
        {
            await admin.ForceCompleteRunAsync(tenantId, req, ct);
            return Results.Ok(new { TenantId = tenantId, Message = $"Run '{req.RunId}' manually marked as completed." });
        })
            .WithName("AdminCompleteRun");

        group.MapPost("/runs/fail", async (string tenantId, ISqlFlowAdminService admin, [FromBody] FailRunCommand req, CancellationToken ct) =>
        {
            await admin.ForceFailRunAsync(tenantId, req, ct);
            return Results.Ok(new { TenantId = tenantId, Message = $"Run '{req.RunId}' manually failed." });
        })
            .WithName("AdminFailRun");


        // Event and Checkpoint Interventions

        group.MapPost("/events/emit", async (string tenantId, ISqlFlowAdminService admin, [FromBody] EmitEventCommand req, CancellationToken ct) =>
        {
            await admin.EmitEventAsync(tenantId, req, ct);
            return Results.Ok(new { TenantId = tenantId, Message = $"Event '{req.EventName}' emitted in queue '{req.QueueName}'." });
        })
            .WithName("AdminEmitEvent");

        group.MapPost("/checkpoints", async (string tenantId, ISqlFlowAdminService admin, [FromBody] SetCheckpointCommand req, CancellationToken ct) =>
        {
            await admin.SetCheckpointAsync(tenantId, req, ct);
            return Results.Ok(new { TenantId = tenantId, Message = $"Checkpoint '{req.StepName}' set for task '{req.TaskId}'." });
        })
            .WithName("AdminSetCheckpoint");

        // Cleanup and Maintenance

        group.MapPost("/cleanup/tasks", async (string tenantId, ISqlFlowAdminService admin, [FromBody] CleanupCommand req, CancellationToken ct) =>
        {
            int deleted = await admin.CleanupTasksAsync(tenantId, req, ct);
            return Results.Ok(new { TenantId = tenantId, DeletedCount = deleted, Message = $"Cleaned up {deleted} old tasks." });
        })
            .WithName("AdminCleanupTasks");

        group.MapPost("/cleanup/events", async (string tenantId, ISqlFlowAdminService admin, [FromBody] CleanupCommand req, CancellationToken ct) =>
        {
            int deleted = await admin.CleanupEventsAsync(tenantId, req, ct);
            return Results.Ok(new { TenantId = tenantId, DeletedCount = deleted, Message = $"Cleaned up {deleted} old events." });
        })
            .WithName("AdminCleanupEvents");

        return routes;
    }
}