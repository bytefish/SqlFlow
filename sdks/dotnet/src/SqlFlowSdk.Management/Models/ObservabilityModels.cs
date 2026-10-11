using System;
using System.Collections.Generic;
using System.Text;

namespace SqlFlowSdk.Management.Models;

/// <summary>
/// Represents an active wait item in the SQL Flow system for a specific tenant, including the queue name, event name, number of waiting tasks, 
/// and the timestamp of the oldest waiting task.
/// </summary>
/// <param name="TenantId">The identifier of the tenant.</param>
/// <param name="QueueName">The name of the queue.</param>
/// <param name="EventName">The name of the event.</param>
/// <param name="WaitingCount">The number of waiting tasks.</param>
/// <param name="OldestWaitAt">The timestamp of the oldest waiting task.</param>
public record ActiveWaitItem(string TenantId, string QueueName, string EventName, int WaitingCount, DateTimeOffset? OldestWaitAt);

/// <summary>
/// Represents an active worker in the SQL Flow system for a specific tenant, including the queue it is associated with, its 
/// unique identifier, and the number of active runs it is currently handling.
/// </summary>
/// <param name="TenantId">The identifier of the tenant.</param>
/// <param name="QueueName">Queue to which the worker is associated.</param>
/// <param name="WorkerId">Unique identifier for the worker.</param>
/// <param name="ActiveRuns">Number of active runs the worker is handling.</param>
public record ActiveWorkerItem(string TenantId, string QueueName, string WorkerId, int ActiveRuns);

/// <summary>
/// Database health information for a specific queue and tenant in the SQL Flow system, including the engine name, queue name, etc.
/// </summary>
/// <param name="TenantId">The identifier of the tenant.</param>
/// <param name="EngineName">The name of the SQL engine.</param>
/// <param name="QueueName">The name of the queue.</param>
/// <param name="TasksTableBytes">The size of the tasks table in bytes.</param>
/// <param name="RunsTableBytes">The size of the runs table in bytes.</param>
/// <param name="ActiveLocks">The number of active locks.</param>
public record DatabaseHealthItem(string TenantId, string EngineName, string QueueName, long TasksTableBytes, long RunsTableBytes, int ActiveLocks);

/// <summary>
/// Represents a failed task in the SQL Flow system for a specific tenant, including details such as the queue name, task ID, task name, number of attempts, run ID, etc.
/// </summary>
/// <param name="TenantId">The identifier of the tenant.</param>
/// <param name="QueueName">The name of the queue.</param>
/// <param name="TaskId">The ID of the failed task.</param>
/// <param name="TaskName">The name of the failed task.</param>
/// <param name="Attempts">The number of attempts made for the task.</param>
/// <param name="RunId">The ID of the run in which the task failed.</param>
/// <param name="FailedAt">The timestamp when the task failed.</param>
/// <param name="FailureReason">The reason for the failure.</param>
public record FailedTaskItem(string TenantId, string QueueName, string TaskId, string TaskName, int Attempts, string? RunId, DateTimeOffset? FailedAt, string? FailureReason);

/// <summary>
/// Represents a backlog item in the SQL Flow system for a tenant, including the queue name, the number of pending tasks, and the timestamp of the oldest pending task.
/// </summary>
/// <param name="TenantId">The identifier of the tenant.</param>
/// <param name="QueueName">The name of the queue.</param>
/// <param name="PendingCount">The number of pending tasks.</param>
/// <param name="OldestPendingAt">The timestamp of the oldest pending task.</param>
public record QueueBacklogItem(string TenantId, string QueueName, int PendingCount, DateTimeOffset? OldestPendingAt);

/// <summary>
/// Aggregated statistics for a specific queue and tenant in the SQL Flow system.
/// </summary>
/// <param name="TenantId">The identifier of the tenant.</param>
/// <param name="QueueName">The name of the queue.</param>
/// <param name="State">The state of the tasks.</param>
/// <param name="Count">The count of tasks in the specified state.</param>
public record QueueStatItem(string TenantId, string QueueName, string State, int Count);

/// <summary>
/// Represents the wait time statistics for a specific queue and tenant in the SQL Flow system.
/// </summary>
/// <param name="TenantId">The identifier of the tenant.</param>
/// <param name="QueueName">The name of the queue.</param>
/// <param name="AvgWaitTimeMs">The average wait time in milliseconds.</param>
/// <param name="MaxWaitTimeMs">The maximum wait time in milliseconds.</param>
public record QueueWaitTimeItem(string TenantId, string QueueName, double AvgWaitTimeMs, double MaxWaitTimeMs);

/// <summary>
/// Represents a retry hotspot item in the SQL Flow system for a specific tenant.
/// </summary>
/// <param name="TenantId">The identifier of the tenant.</param>
/// <param name="QueueName">The name of the queue.</param>
/// <param name="TaskId">The identifier of the task.</param>
/// <param name="TaskName">The name of the task.</param>
/// <param name="Attempts">The number of attempts.</param>
/// <param name="State">The current state of the task.</param>
public record RetryHotspotItem(string TenantId, string QueueName, string TaskId, string TaskName, int Attempts, string State);

/// <summary>
/// Represents a slow task item in the SQL Flow system for a specific tenant.
/// </summary>
/// <param name="TenantId">The identifier of the tenant.</param>
/// <param name="QueueName">The name of the queue.</param>
/// <param name="TaskId">The identifier of the task.</param>
/// <param name="TaskName">The name of the task.</param>
/// <param name="DurationMs">The duration of the task in milliseconds.</param>
/// <param name="CompletedAt">The timestamp when the task was completed.</param>
public record SlowTaskItem(string TenantId, string QueueName, string TaskId, string TaskName, double DurationMs, DateTimeOffset CompletedAt);

/// <summary>
/// Represents detailed information about a specific task in the SQL Flow system for a tenant.
/// </summary>
/// <param name="TenantId">The identifier of the tenant.</param>
/// <param name="TaskId">The identifier of the task.</param>
/// <param name="QueueName">The name of the queue.</param>
/// <param name="TaskName">The name of the task.</param>
/// <param name="State">The state of the task.</param>
/// <param name="EnqueuedAt">The timestamp when the task was enqueued.</param>
/// <param name="FirstStartedAt">The timestamp when the task was first started.</param>
/// <param name="CompletedPayload">The payload of the completed task.</param>
/// <param name="Params">The parameters associated with the task.</param>
public record TaskDetailItem(string TenantId, string TaskId, string QueueName, string TaskName, string State, DateTimeOffset EnqueuedAt, DateTimeOffset? FirstStartedAt, string? CompletedPayload, string? Params);

/// <summary>
/// Represents a task failure hotspot item in the SQL Flow system for a tenant.
/// </summary>
/// <param name="TenantId">The identifier of the tenant.</param>
/// <param name="QueueName">The name of the queue.</param>
/// <param name="TaskName">The name of the task.</param>
/// <param name="FailureCount">The number of failures.</param>
/// <param name="LastFailedAt">The timestamp of the last failure.</param>
public record TaskFailureHotspotItem(string TenantId, string QueueName, string TaskName, int FailureCount, DateTimeOffset LastFailedAt);

/// <summary>
/// Represents the percentile statistics for a specific task in the SQL Flow system for a tenant.
/// </summary>
/// <param name="TenantId">The identifier of the tenant.</param>
/// <param name="TaskName">The name of the task.</param>
/// <param name="P50Ms">The 50th percentile execution time in milliseconds.</param>
/// <param name="P95Ms">The 95th percentile execution time in milliseconds.</param>
/// <param name="P99Ms">The 99th percentile execution time in milliseconds.</param>
public record TaskPercentileItem(string TenantId, string TaskName, double P50Ms, double P95Ms, double P99Ms);

/// <summary>
/// A filter class for searching tasks across tenants.
/// </summary>
public class TaskSearchFilter
{
    /// <summary>
    /// Tenant ID to filter tasks by. Required for multi-tenancy.
    /// </summary>
    public string TenantId { get; set; } = string.Empty;

    /// <summary>
    /// Queue name to filter tasks by.
    /// </summary>
    public string QueueName { get; set; } = string.Empty;

    public List<string>? States { get; set; }
    public string? SearchTerm { get; set; }
    public int? MinAttempts { get; set; }
    public int? MaxAttempts { get; set; }
    public string? ClaimedBy { get; set; }
    public DateTimeOffset? FromDate { get; set; }
    public DateTimeOffset? ToDate { get; set; }
    public string SortBy { get; set; } = "enqueue_at";
    public bool SortDescending { get; set; } = true;
    public int Offset { get; set; } = 0;
    public int Limit { get; set; } = 50;
}

/// <summary>
/// Represents a search result item for a task in the SQL Flow system for a tenant.
/// </summary>
public record TaskSearchResultItem(
    string TenantId,
    string QueueName,
    string TaskId,
    string TaskName,
    string State,
    int Attempts,
    string? RunId,
    DateTimeOffset EnqueuedAt,
    DateTimeOffset? LastAttemptAt,
    string? FailureReason,
    string? ParamsJson
);

/// <summary>
/// Represents a throughput bucket item in the SQL Flow system for a tenant.
/// </summary>
public record ThroughputBucketItem(string TenantId, DateTimeOffset TimeBucket, string QueueName, int CompletedCount, int FailedCount, double AvgDurationMs);

/// <summary>
/// Represents an upcoming wakeup bucket item in the SQL Flow system for a tenant.
/// </summary>
public record UpcomingWakeupBucketItem(string TenantId, DateTimeOffset TimeBucket, string QueueName, int SleepingCount);