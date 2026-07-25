// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using SqlFlowSdk.Management.Models;

namespace SqlFlowSdk.Management.AspNetCore;

/// <summary>
/// Provides extension methods to register SqlFlow Management endpoints as ASP.NET Core Minimal APIs.
/// </summary>
public static class SqlFlowManagementEndpoints
{
    /// <summary>
    /// Maps all SqlFlow Management endpoints under a configurable route prefix (default: /api/sqlflow-management).
    /// </summary>
    public static IEndpointRouteBuilder MapSqlFlowManagementEndpoints(
        this IEndpointRouteBuilder routes,
        string routePrefix = "/api/sqlflow-management")
    {
        var group = routes.MapGroup(routePrefix)
                          .WithTags("SqlFlow Management");

        group.MapGet("/stats", async (ISqlFlowManagementService dashboard, CancellationToken ct) =>
            Results.Ok(await dashboard.GetQueueStatsAsync(ct)))
            .WithName("GetQueueStats")
            .WithSummary("Aggregated task counts per queue and state");

        group.MapGet("/throughput", async (ISqlFlowManagementService dashboard, int? windowSeconds, CancellationToken ct) =>
        {
            var window = TimeSpan.FromSeconds(windowSeconds ?? 3600);
            return Results.Ok(await dashboard.GetThroughputHistoryAsync(window, ct));
        })
            .WithName("GetThroughputHistory")
            .WithSummary("Throughput and success rates over time");

        group.MapGet("/latency-percentiles", async (ISqlFlowManagementService dashboard, string? queueName, CancellationToken ct) =>
            Results.Ok(await dashboard.GetTaskLatencyPercentilesAsync(queueName, ct)))
            .WithName("GetTaskLatencyPercentiles")
            .WithSummary("Performance metrics (p50, p95, p99 percentiles)");

        group.MapGet("/health", async (ISqlFlowManagementService dashboard, string queueName, CancellationToken ct) =>
            Results.Ok(await dashboard.GetDatabaseHealthAsync(queueName, ct)))
            .WithName("GetDatabaseHealth")
            .WithSummary("Database health and storage consumption per queue");

        group.MapGet("/workers", async (ISqlFlowManagementService dashboard, CancellationToken ct) =>
            Results.Ok(await dashboard.GetActiveWorkersAsync(ct)))
            .WithName("GetActiveWorkers")
            .WithSummary("Active worker nodes and their current utilization");

        group.MapGet("/wait-times", async (ISqlFlowManagementService dashboard, int? limit, CancellationToken ct) =>
            Results.Ok(await dashboard.GetQueueWaitTimesAsync(limit ?? 50, ct)))
            .WithName("GetQueueWaitTimes")
            .WithSummary("Queue wait times prior to first start");

        group.MapGet("/hotspots/failures", async (ISqlFlowManagementService dashboard, int? lookbackSeconds, int? limit, CancellationToken ct) =>
        {
            TimeSpan? window = lookbackSeconds.HasValue ? TimeSpan.FromSeconds(lookbackSeconds.Value) : null;

            return Results.Ok(await dashboard.GetTaskFailureHotspotsAsync(limit ?? 50, window, ct));
        })
            .WithName("GetTaskFailureHotspots")
            .WithSummary("Tasks/agents with the highest failure rates");

        group.MapGet("/active-waits", async (ISqlFlowManagementService dashboard, int? limit, CancellationToken ct) =>
            Results.Ok(await dashboard.GetActiveWaitsAsync(limit ?? 50, ct)))
            .WithName("GetActiveWaits")
            .WithSummary("Blocking events that tasks are currently waiting on");

        group.MapGet("/backlog", async (ISqlFlowManagementService dashboard, int? limit, CancellationToken ct) =>
            Results.Ok(await dashboard.GetQueueBacklogDepthAsync(limit ?? 50, ct)))
            .WithName("GetQueueBacklogDepth")
            .WithSummary("Number of backlog tasks and age of the oldest element");

        group.MapGet("/hotspots/retries", async (ISqlFlowManagementService dashboard, int? limit, CancellationToken ct) =>
            Results.Ok(await dashboard.GetRetryHotspotsAsync(limit ?? 50, ct)))
            .WithName("GetRetryHotspots")
            .WithSummary("Tasks with frequent retry attempts (flapping detection)");

        group.MapGet("/upcoming-wakeups", async (ISqlFlowManagementService dashboard, int? limit, CancellationToken ct) =>
            Results.Ok(await dashboard.GetUpcomingWakeupsAsync(limit ?? 50, ct)))
            .WithName("GetUpcomingWakeups")
            .WithSummary("Wake-up forecast for sleeping tasks to support auto-scaling");

        group.MapGet("/slow-tasks", async (ISqlFlowManagementService dashboard, int? limit, CancellationToken ct) =>
            Results.Ok(await dashboard.GetSlowestTasksAsync(limit ?? 50, ct)))
            .WithName("GetSlowestTasks")
            .WithSummary("Slowest successfully completed tasks from the last 24 hours");

        group.MapGet("/failed-tasks", async (ISqlFlowManagementService dashboard, int? limit, CancellationToken ct) =>
            Results.Ok(await dashboard.GetFailedTasksAsync(limit ?? 50, ct)))
            .WithName("GetFailedTasks")
            .WithSummary("List of failed tasks with failure reasons");

        group.MapGet("/tasks/{queueName}/{taskId}", async (ISqlFlowManagementService dashboard, string queueName, string taskId, CancellationToken ct) =>
        {
            var result = await dashboard.GetTaskDetailsAsync(queueName, taskId, ct);
            return result is not null ? Results.Ok(result) : Results.NotFound();
        })
            .WithName("GetTaskDetails")
            .WithSummary("Detail view of an individual task including parameters and payloads");

        group.MapPost("/tasks/search", async (ISqlFlowManagementService dashboard, TaskSearchFilter filter, CancellationToken ct) =>
            Results.Ok(await dashboard.SearchTasksAsync(filter, ct)))
            .WithName("SearchTasks")
            .WithSummary("Universal task search with filtering, paging, and JSON matching");

        return routes;
    }
}