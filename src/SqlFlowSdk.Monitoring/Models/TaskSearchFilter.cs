namespace SqlFlowSdk.Monitoring.Models;

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
