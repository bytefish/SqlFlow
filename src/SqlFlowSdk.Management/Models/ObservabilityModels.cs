using System;
using System.Collections.Generic;
using System.Text;

namespace SqlFlowSdk.Management.Models;

/// <summary>
/// Represents an active wait item in the SQL Flow system, including the queue name, event name, number of waiting tasks, 
/// and the timestamp of the oldest waiting task.
/// </summary>
/// <param name="QueueName">The name of the queue.</param>
/// <param name="EventName">The name of the event.</param>
/// <param name="WaitingCount">The number of waiting tasks.</param>
/// <param name="OldestWaitAt">The timestamp of the oldest waiting task.</param>
public record ActiveWaitItem(string QueueName, string EventName, int WaitingCount, DateTimeOffset? OldestWaitAt);

/// <summary>
/// Represents an active worker in the SQL Flow system, including the queue it is associated with, its 
/// unique identifier, and the number of active runs it is currently handling.
/// </summary>
/// <param name="QueueName">Queue to which the worker is associated.</param>
/// <param name="WorkerId">Unique identifier for the worker.</param>
/// <param name="ActiveRuns">Number of active runs the worker is handling.</param>
public record ActiveWorkerItem(string QueueName, string WorkerId, int ActiveRuns);

/// <summary>
/// Database health information for a specific queue in the SQL Flow system, including the engine name, queue name,
/// </summary>
/// <param name="EngineName">The name of the SQL engine.</param>
/// <param name="QueueName">The name of the queue.</param>
/// <param name="TasksTableBytes">The size of the tasks table in bytes.</param>
/// <param name="RunsTableBytes">The size of the runs table in bytes.</param>
/// <param name="ActiveLocks">The number of active locks.</param>
public record DatabaseHealthItem(string EngineName, string QueueName, long TasksTableBytes, long RunsTableBytes, int ActiveLocks);

/// <summary>
/// Represents a failed task in the SQL Flow system, including details such as the queue name, task ID, task name, number of attempts, run ID, 
/// </summary>
/// <param name="QueueName">The name of the queue.</param>
/// <param name="TaskId">The ID of the failed task.</param>
/// <param name="TaskName">The name of the failed task.</param>
/// <param name="Attempts">The number of attempts made for the task.</param>
/// <param name="RunId">The ID of the run in which the task failed.</param>
/// <param name="FailedAt">The timestamp when the task failed.</param>
/// <param name="FailureReason">The reason for the failure.</param>
public record FailedTaskItem(string QueueName, string TaskId, string TaskName, int Attempts, string? RunId, DateTimeOffset? FailedAt, string? FailureReason);

/// <summary>
/// Represents a backlog item in the SQL Flow system, including the queue name, the number of pending tasks, and the timestamp of the oldest pending task.
/// </summary>
/// <param name="QueueName">The name of the queue.</param>
/// <param name="PendingCount">The number of pending tasks.</param>
/// <param name="OldestPendingAt">The timestamp of the oldest pending task.</param>
public record QueueBacklogItem(string QueueName, int PendingCount, DateTimeOffset? OldestPendingAt);

/// <summary>
/// Aggregated statistics for a specific queue in the SQL Flow system, including the queue name, state, and count of tasks in that state.
/// </summary>
/// <param name="QueueName">The name of the queue.</param>
/// <param name="State">The state of the tasks.</param>
/// <param name="Count">The count of tasks in the specified state.</param>
public record QueueStatItem(string QueueName, string State, int Count);

/// <summary>
/// Represents the wait time statistics for a specific queue in the SQL Flow system, including the queue name, average wait time in milliseconds, and maximum wait time in milliseconds.
/// </summary>
/// <param name="QueueName">The name of the queue.</param>
/// <param name="AvgWaitTimeMs">The average wait time in milliseconds.</param>
/// <param name="MaxWaitTimeMs">The maximum wait time in milliseconds.</param>
public record QueueWaitTimeItem(string QueueName, double AvgWaitTimeMs, double MaxWaitTimeMs);

/// <summary>
/// Represents a retry hotspot item in the SQL Flow system, including the queue name, task identifier, task name, number of attempts, and current state of the task.
/// </summary>
/// <param name="QueueName">The name of the queue.</param>
/// <param name="TaskId">The identifier of the task.</param>
/// <param name="TaskName">The name of the task.</param>
/// <param name="Attempts">The number of attempts.</param>
/// <param name="State">The current state of the task.</param>
public record RetryHotspotItem(string QueueName, string TaskId, string TaskName, int Attempts, string State);

/// <summary>
/// Represents a slow task item in the SQL Flow system, including the queue name, task identifier, task name, duration in milliseconds, and the timestamp when the task was completed.
/// </summary>
/// <param name="QueueName">The name of the queue.</param>
/// <param name="TaskId">The identifier of the task.</param>
/// <param name="TaskName">The name of the task.</param>
/// <param name="DurationMs">The duration of the task in milliseconds.</param>
/// <param name="CompletedAt">The timestamp when the task was completed.</param>
public record SlowTaskItem(string QueueName, string TaskId, string TaskName, double DurationMs, DateTimeOffset CompletedAt);

/// <summary>
/// Represents detailed information about a specific task in the SQL Flow system, including its identifier, queue name, task name, state, timestamps for when it was enqueued 
/// and first started, any completed payload, and parameters associated with the task.
/// </summary>
/// <param name="TaskId">The identifier of the task.</param>
/// <param name="QueueName">The name of the queue.</param>
/// <param name="TaskName">The name of the task.</param>
/// <param name="State">The state of the task.</param>
/// <param name="EnqueuedAt">The timestamp when the task was enqueued.</param>
/// <param name="FirstStartedAt">The timestamp when the task was first started.</param>
/// <param name="CompletedPayload">The payload of the completed task.</param>
/// <param name="Params">The parameters associated with the task.</param>
public record TaskDetailItem(string TaskId, string QueueName, string TaskName, string State, DateTimeOffset EnqueuedAt, DateTimeOffset? FirstStartedAt, string? CompletedPayload, string? Params);

/// <summary>
/// Represents a task failure hotspot item in the SQL Flow system, including the queue name, task name, failure count, and the timestamp of the last failure.
/// </summary>
/// <param name="QueueName">The name of the queue.</param>
/// <param name="TaskName">The name of the task.</param>
/// <param name="FailureCount">The number of failures.</param>
/// <param name="LastFailedAt">The timestamp of the last failure.</param>
public record TaskFailureHotspotItem(string QueueName, string TaskName, int FailureCount, DateTimeOffset LastFailedAt);

/// <summary>
/// Represents the percentile statistics for a specific task in the SQL Flow system, including the task name and the 50th, 95th, and 99th percentile execution times in milliseconds.
/// </summary>
/// <param name="TaskName">The name of the task.</param>
/// <param name="P50Ms">The 50th percentile execution time in milliseconds.</param>
/// <param name="P95Ms">The 95th percentile execution time in milliseconds.</param>
/// <param name="P99Ms">The 99th percentile execution time in milliseconds.</param>
public record TaskPercentileItem(string TaskName, double P50Ms, double P95Ms, double P99Ms);

/// <summary>
/// A filter class for searching tasks. It allows filtering by queue name, task states, search terms, 
/// attempt counts, claimed by user, date ranges, and sorting options.
/// </summary>
public class TaskSearchFilter
{
    /// <summary>
    /// Queue name to filter tasks by. This is a required field and must be specified to search for tasks in a specific queue.
    /// </summary>
    public string QueueName { get; set; } = string.Empty;

    /// <summary>
    /// States to filter tasks by. If null, no state filtering is applied. Valid states are: "pending", "claimed", "completed", "failed".
    /// </summary>
    public List<string>? States { get; set; }

    /// <summary>
    /// Search term to filter tasks by. This can be used to search for tasks with specific attributes or content.
    /// </summary>
    public string? SearchTerm { get; set; }

    /// <summary>
    /// Minimum number of attempts to filter tasks by. If null, no minimum attempt filtering is applied.
    /// </summary>
    public int? MinAttempts { get; set; }

    /// <summary>
    /// Maximum number of attempts to filter tasks by. If null, no maximum attempt filtering is applied.
    /// </summary>
    public int? MaxAttempts { get; set; }

    /// <summary>
    /// Search for tasks claimed by a specific user. If null, no claimed by filtering is applied.
    /// </summary>
    public string? ClaimedBy { get; set; }

    /// <summary>
    /// Filters for tasks that were created or updated after this date. If null, no from date filtering is applied.
    /// </summary>
    public DateTimeOffset? FromDate { get; set; }

    /// <summary>
    /// Filters for tasks that were created or updated before this date. If null, no to date filtering is applied.
    /// </summary>
    public DateTimeOffset? ToDate { get; set; }

    /// <summary>
    /// Supported Sort Options are: enqueue_at, failed_at, attempts
    /// </summary>
    public string SortBy { get; set; } = "enqueue_at";

    /// <summary>
    /// Sort order for the results. If true, results will be sorted in descending order; if false, in ascending order.
    /// </summary>
    public bool SortDescending { get; set; } = true;

    /// <summary>
    /// Pagination offset for the results. This specifies the number of tasks to skip before starting to return results. The default value is 0.
    /// </summary>
    public int Offset { get; set; } = 0;

    /// <summary>
    /// Pagination limit for the results. This specifies the maximum number of tasks to return in the results. The default value is 50.
    /// </summary>
    public int Limit { get; set; } = 50;
}

/// <summary>
/// Represents a search result item for a task in the SQL Flow system, including details such as queue name, task ID, task name, state, attempts, run ID, timestamps for enqueuing and last attempt, failure reason, and parameters in JSON format.
/// </summary>
/// <param name="QueueName">The name of the queue.</param>
/// <param name="TaskId">The identifier of the task.</param>
/// <param name="TaskName">The name of the task.</param>
/// <param name="State">The current state of the task.</param>
/// <param name="Attempts">The number of attempts.</param>
/// <param name="RunId">The identifier of the run.</param>
/// <param name="EnqueuedAt">The timestamp when the task was enqueued.</param>
/// <param name="LastAttemptAt">The timestamp of the last attempt.</param>
/// <param name="FailureReason">The reason for the failure.</param>
/// <param name="ParamsJson">The parameters in JSON format.</param>
public record TaskSearchResultItem(
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
/// Represents a throughput bucket item in the SQL Flow system, including the time bucket, queue name, completed count, failed count, and average duration in milliseconds.
/// </summary>
/// <param name="TimeBucket">The time bucket.</param>
/// <param name="QueueName">The name of the queue.</param>
/// <param name="CompletedCount">The number of completed tasks.</param>
/// <param name="FailedCount">The number of failed tasks.</param>
/// <param name="AvgDurationMs">The average duration in milliseconds.</param>
public record ThroughputBucketItem(DateTimeOffset TimeBucket, string QueueName, int CompletedCount, int FailedCount, double AvgDurationMs);

/// <summary>
/// Represents an upcoming wakeup bucket item in the SQL Flow system, including the time bucket, queue name, and the count of sleeping tasks.
/// </summary>
/// <param name="TimeBucket">The time bucket.</param>
/// <param name="QueueName">The name of the queue.</param>
/// <param name="SleepingCount">The count of sleeping tasks.</param>
public record UpcomingWakeupBucketItem(DateTimeOffset TimeBucket, string QueueName, int SleepingCount);
