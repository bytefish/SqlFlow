using SqlFlowSdk.Management.Models;

namespace SqlFlowSdk.Management;

public interface ISqlFlowQueryService
{
    Task<List<QueueStatItem>> GetQueueStatsAsync(string tenantId, CancellationToken ct = default);

    Task<List<ThroughputBucketItem>> GetThroughputHistoryAsync(string tenantId, TimeSpan window, CancellationToken ct = default);

    Task<List<TaskPercentileItem>> GetTaskLatencyPercentilesAsync(string tenantId, string? queueName = null, CancellationToken ct = default);

    Task<DatabaseHealthItem> GetDatabaseHealthAsync(string tenantId, string queueName, CancellationToken ct = default);

    Task<List<ActiveWorkerItem>> GetActiveWorkersAsync(string tenantId, CancellationToken ct = default);

    Task<List<QueueWaitTimeItem>> GetQueueWaitTimesAsync(string tenantId, int limit = 50, CancellationToken ct = default);

    Task<List<TaskFailureHotspotItem>> GetTaskFailureHotspotsAsync(string tenantId, int limit = 50, TimeSpan? lookbackWindow = null, CancellationToken ct = default);

    Task<List<ActiveWaitItem>> GetActiveWaitsAsync(string tenantId, int limit = 50, CancellationToken ct = default);

    Task<List<QueueBacklogItem>> GetQueueBacklogDepthAsync(string tenantId, int limit = 50, CancellationToken ct = default);

    Task<List<RetryHotspotItem>> GetRetryHotspotsAsync(string tenantId, int limit = 50, CancellationToken ct = default);

    Task<List<UpcomingWakeupBucketItem>> GetUpcomingWakeupsAsync(string tenantId, int limit = 50, CancellationToken ct = default);

    Task<List<SlowTaskItem>> GetSlowestTasksAsync(string tenantId, int limit = 50, CancellationToken ct = default);

    Task<List<FailedTaskItem>> GetFailedTasksAsync(string tenantId, int limit = 50, CancellationToken ct = default);

    Task<TaskDetailItem?> GetTaskDetailsAsync(string tenantId, string queueName, string taskId, CancellationToken ct = default);

    Task<List<TaskSearchResultItem>> SearchTasksAsync(TaskSearchFilter filter, CancellationToken ct = default);
}