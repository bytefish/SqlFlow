// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Routing;
using SqlFlowSdk.Management.Models;

namespace SqlFlowSdk.Management.AspNetCore;

/// <summary>
/// Provides extension methods to register SqlFlow Query API endpoints as ASP.NET Core Minimal APIs with tenant isolation.
/// </summary>
public static class SqlFlowQueryEndpoints
{
    /// <summary>
    /// Maps all SqlFlow Query API endpoints under a configurable route prefix with tenant isolation (default: /api/sqlflow-query/{tenantId}).
    /// </summary>
    public static IEndpointRouteBuilder MapSqlFlowQueryEndpoints(
        this IEndpointRouteBuilder routes,
        string routePrefix = "/api/sqlflow-query/{tenantId}")
    {
        var group = routes.MapGroup(routePrefix)
                          .WithTags("SqlFlow Query API");

        group.MapGet("/stats", async (string tenantId, ISqlFlowQueryService dashboard, CancellationToken ct) =>
            Results.Ok(await dashboard.GetQueueStatsAsync(tenantId, ct)))
            .WithName("GetQueueStats")
            .WithSummary("Aggregated task counts per queue and state for a tenant");

        group.MapGet("/throughput", async (string tenantId, ISqlFlowQueryService dashboard, int? windowSeconds, CancellationToken ct) =>
        {
            var window = TimeSpan.FromSeconds(windowSeconds ?? 3600);
            return Results.Ok(await dashboard.GetThroughputHistoryAsync(tenantId, window, ct));
        })
            .WithName("GetThroughputHistory")
            .WithSummary("Throughput and success rates over time");

        group.MapGet("/latency-percentiles", async (string tenantId, ISqlFlowQueryService dashboard, string? queueName, CancellationToken ct) =>
            Results.Ok(await dashboard.GetTaskLatencyPercentilesAsync(tenantId, queueName, ct)))
            .WithName("GetTaskLatencyPercentiles")
            .WithSummary("Performance metrics (p50, p95, p99 percentiles)");

        group.MapGet("/health", async (string tenantId, ISqlFlowQueryService dashboard, string queueName, CancellationToken ct) =>
            Results.Ok(await dashboard.GetDatabaseHealthAsync(tenantId, queueName, ct)))
            .WithName("GetDatabaseHealth")
            .WithSummary("Database health and storage consumption per queue");

        group.MapGet("/workers", async (string tenantId, ISqlFlowQueryService dashboard, CancellationToken ct) =>
            Results.Ok(await dashboard.GetActiveWorkersAsync(tenantId, ct)))
            .WithName("GetActiveWorkers")
            .WithSummary("Active worker nodes and their current utilization");

        group.MapGet("/wait-times", async (string tenantId, ISqlFlowQueryService dashboard, int? limit, CancellationToken ct) =>
            Results.Ok(await dashboard.GetQueueWaitTimesAsync(tenantId, limit ?? 50, ct)))
            .WithName("GetQueueWaitTimes")
            .WithSummary("Queue wait times prior to first start");

        group.MapGet("/hotspots/failures", async (string tenantId, ISqlFlowQueryService dashboard, int? lookbackSeconds, int? limit, CancellationToken ct) =>
        {
            TimeSpan? window = lookbackSeconds.HasValue ? TimeSpan.FromSeconds(lookbackSeconds.Value) : null;

            return Results.Ok(await dashboard.GetTaskFailureHotspotsAsync(tenantId, limit ?? 50, window, ct));
        })
            .WithName("GetTaskFailureHotspots")
            .WithSummary("Tasks/agents with the highest failure rates");

        group.MapGet("/active-waits", async (string tenantId, ISqlFlowQueryService dashboard, int? limit, CancellationToken ct) =>
            Results.Ok(await dashboard.GetActiveWaitsAsync(tenantId, limit ?? 50, ct)))
            .WithName("GetActiveWaits")
            .WithSummary("Blocking events that tasks are currently waiting on");

        group.MapGet("/backlog", async (string tenantId, ISqlFlowQueryService dashboard, int? limit, CancellationToken ct) =>
            Results.Ok(await dashboard.GetQueueBacklogDepthAsync(tenantId, limit ?? 50, ct)))
            .WithName("GetQueueBacklogDepth")
            .WithSummary("Number of backlog tasks and age of the oldest element");

        group.MapGet("/hotspots/retries", async (string tenantId, ISqlFlowQueryService dashboard, int? limit, CancellationToken ct) =>
            Results.Ok(await dashboard.GetRetryHotspotsAsync(tenantId, limit ?? 50, ct)))
            .WithName("GetRetryHotspots")
            .WithSummary("Tasks with frequent retry attempts (flapping detection)");

        group.MapGet("/upcoming-wakeups", async (string tenantId, ISqlFlowQueryService dashboard, int? limit, CancellationToken ct) =>
            Results.Ok(await dashboard.GetUpcomingWakeupsAsync(tenantId, limit ?? 50, ct)))
            .WithName("GetUpcomingWakeups")
            .WithSummary("Wake-up forecast for sleeping tasks to support auto-scaling");

        group.MapGet("/slow-tasks", async (string tenantId, ISqlFlowQueryService dashboard, int? limit, CancellationToken ct) =>
            Results.Ok(await dashboard.GetSlowestTasksAsync(tenantId, limit ?? 50, ct)))
            .WithName("GetSlowestTasks")
            .WithSummary("Slowest successfully completed tasks from the last 24 hours");

        group.MapGet("/failed-tasks", async (string tenantId, ISqlFlowQueryService dashboard, int? limit, CancellationToken ct) =>
            Results.Ok(await dashboard.GetFailedTasksAsync(tenantId, limit ?? 50, ct)))
            .WithName("GetFailedTasks")
            .WithSummary("List of failed tasks with failure reasons");

        group.MapGet("/tasks/{queueName}/{taskId}", async (string tenantId, ISqlFlowQueryService dashboard, string queueName, string taskId, CancellationToken ct) =>
        {
            var result = await dashboard.GetTaskDetailsAsync(tenantId, queueName, taskId, ct);
            return result is not null ? Results.Ok(result) : Results.NotFound();
        })
            .WithName("GetTaskDetails")
            .WithSummary("Detail view of an individual task including parameters and payloads");

        group.MapPost("/tasks/search", async (string tenantId, ISqlFlowQueryService dashboard, [FromBody] TaskSearchFilter filter, CancellationToken ct) =>
        {
            // Tenant-ID in Filter absichern
            filter.TenantId = tenantId;
            return Results.Ok(await dashboard.SearchTasksAsync(filter, ct));
        })
            .WithName("SearchTasks")
            .WithSummary("Universal task search with filtering, paging, and JSON matching");

        return routes;
    }
}