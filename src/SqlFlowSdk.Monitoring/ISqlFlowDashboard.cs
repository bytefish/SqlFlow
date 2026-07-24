// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using SqlFlowSdk.Monitoring.Models;

namespace SqlFlowSdk.Monitoring;

/// <summary>
/// An interface for retrieving monitoring and dashboard data from a SQL Flow database. It provides methods to fetch various statistics, 
/// metrics, and details related to task queues, throughput, latency, database health, active workers, wait times, task failures, 
/// and more. 
/// 
/// Implementations of this interface can be used to build dashboards or monitoring tools for SQL Flow systems.
/// </summary>
public interface ISqlFlowDashboard
{
    /// <summary>
    /// Gets the current statistics for all task queues in the SQL Flow system. This includes information such as queue names, their states, 
    /// and the number of tasks in each queue.
    /// </summary>
    /// <param name="ct">Cancellation token to cancel the operation.</param>
    /// <returns>Queue statistics for all queues</returns>
    Task<List<QueueStatItem>> GetQueueStatsAsync(CancellationToken ct = default);

    /// <summary>
    /// Gets the throughput history of tasks processed in the SQL Flow system over a specified time window. This includes metrics such as 
    /// the number of tasks processed per unit of time, allowing for analysis of system performance and trends over time.
    /// </summary>
    /// <param name="window">The time span over which to retrieve throughput history.</param>
    /// <param name="ct">Cancellation token to cancel the operation.</param>
    /// <returns>Throughput history for the given time window</returns>
    Task<List<ThroughputBucketItem>> GetThroughputHistoryAsync(TimeSpan window, CancellationToken ct = default);

    /// <summary>
    /// Gets the latency percentiles for tasks in the SQL Flow system, optionally filtered by a specific queue name. This provides 
    /// insights into the distribution of task processing times, allowing for performance analysis and identification of potential 
    /// bottlenecks.
    /// </summary>
    /// <param name="queueName">Optional queue name to filter the latency percentiles. If null, percentiles for all queues will be returned.</param>
    /// <param name="ct">Cancellation token to cancel the operation.</param>
    /// <returns>The latency percentiles for tasks</returns>
    Task<List<TaskPercentileItem>> GetTaskLatencyPercentilesAsync(string? queueName = null, CancellationToken ct = default);

    /// <summary>
    /// Gets the health status of the SQL Flow database, including information about the database's availability, responsiveness, and any potential 
    /// issues that may affect the system's operation. This can help identify problems early and ensure the reliability of the SQL Flow system.
    /// </summary>
    /// <param name="queueName">Queue to check the health of</param>
    /// <param name="ct">Cancellation token to cancel the operation.</param>
    /// <returns>Current Database Health</returns>
    Task<DatabaseHealthItem> GetDatabaseHealthAsync(string queueName, CancellationToken ct = default);

    /// <summary>
    /// Gets the list of currently active workers in the SQL Flow system, along with their associated queue names and the number of active runs they are handling.
    /// </summary>
    /// <param name="ct">Cancellation token to cancel the operation.</param>
    /// <returns></returns>
    Task<List<ActiveWorkerItem>> GetActiveWorkersAsync(CancellationToken ct = default);

    /// <summary>
    /// Gets the wait times for tasks in the SQL Flow system, providing insights into how long tasks are waiting in queues before 
    /// being processed. This can help identify potential bottlenecks and areas for optimization.
    /// </summary>
    /// <param name="ct">Cancellation token to cancel the operation.</param>
    /// <returns>Wait Times for Tasks being processed</returns>
    Task<List<QueueWaitTimeItem>> GetQueueWaitTimesAsync(CancellationToken ct = default);

    /// <summary>
    /// Gets the Failure hotspots for tasks in the SQL Flow system over a specified lookback window. This includes information 
    /// about which tasks are failing most frequently, allowing for targeted investigation and remediation of issues.
    /// </summary>
    /// <param name="lookbackWindow">Lookback Window to calculate the failures for</param>
    /// <param name="ct">Cancellation token to cancel the operation.</param>
    /// <returns>Failure Hotspot Items to find out why tasks fail</returns>
    Task<List<TaskFailureHotspotItem>> GetTaskFailureHotspotsAsync(TimeSpan? lookbackWindow = null, CancellationToken ct = default);

    /// <summary>
    /// Gets the list of currently active waits in the SQL Flow system, providing information about tasks that are waiting for resources or 
    /// other conditions to be met before they can proceed. This can help identify potential contention points and areas for optimization 
    /// in the system.
    /// </summary>
    /// <param name="ct">Cancellation token to cancel the operation.</param>
    /// <returns></returns>
    Task<List<ActiveWaitItem>> GetActiveWaitsAsync(CancellationToken ct = default);

    /// <summary>
    /// Gets the backlog depth of task queues in the SQL Flow system, providing information about the number of pending tasks in each queue 
    /// and the age of the oldest pending task. This can help identify potential bottlenecks and areas for optimization in the system.
    /// </summary>
    /// <param name="ct">Cancellation token to cancel the operation.</param>
    /// <returns>The Backlog Depth of all task queues</returns>
    Task<List<QueueBacklogItem>> GetQueueBacklogDepthAsync(CancellationToken ct = default);

    /// <summary>
    /// Gets the retry hotspots for tasks in the SQL Flow system, providing information about which tasks are being retried most frequently.
    /// </summary>
    /// <param name="ct">Cancellation token to cancel the operation.</param>
    /// <returns>The Hotspots of most tasks being retried</returns>
    Task<List<RetryHotspotItem>> GetRetryHotspotsAsync(CancellationToken ct = default);

    /// <summary>
    /// Gets the upcoming wakeup buckets for tasks in the SQL Flow system, providing information about when tasks are scheduled to wake up 
    /// and be processed. This can help identify potential scheduling issues and scale resources accordingly.
    /// </summary>
    /// <param name="ct">Cancellation token to cancel the operation.</param>
    /// <returns></returns>
    Task<List<UpcomingWakeupBucketItem>> GetUpcomingWakeupsAsync(CancellationToken ct = default);

    /// <summary>
    /// Gets a list of the slowest tasks in the SQL Flow system, providing information about which tasks are taking the longest to complete.
    /// </summary>
    /// <param name="ct">Cancellation token to cancel the operation.</param>
    /// <returns>The list of slowest tasks in the system</returns>
    Task<List<SlowTaskItem>> GetSlowestTasksAsync(CancellationToken ct = default);

    /// <summary>
    /// Gets a list of failed tasks in the SQL Flow system, providing information about which tasks have failed, their failure reasons, 
    /// and other relevant details. Sorted by the most recent failures first.
    /// </summary>
    /// <param name="limit">Number of tasks to return, defaults to 50.</param>
    /// <param name="ct">Cancellation token to cancel the operation.</param>
    /// <returns>The list of failed tasks in the system</returns>
    Task<IEnumerable<FailedTaskItem>> GetFailedTasksAsync(int limit = 50, CancellationToken ct = default);

    /// <summary>
    /// Gets a list of the most recent completed tasks in the SQL Flow system, providing information about which tasks have completed successfully, 
    /// their completion times, and other relevant details. Sorted by the most recent completions first
    /// </summary>
    /// <param name="queueName">Queue Name to get the tasks for</param>
    /// <param name="taskId">ID of the Task being completed</param>
    /// <param name="ct">Cancellation token to cancel the operation.</param>
    /// <returns>Task details for the requested task</returns>
    Task<TaskDetailItem?> GetTaskDetailsAsync(string queueName, string taskId, CancellationToken ct = default);

    /// <summary>
    /// Searches for tasks in the SQL Flow system based on the provided filter criteria. This allows for flexible querying of tasks based on 
    /// various attributes such as queue name, state, search term, attempt counts, claimed by, date ranges, sorting options, and pagination.
    /// </summary>
    /// <param name="filter">Search Filter with filter criterias</param>
    /// <param name="ct">Cancellation token to cancel the operation.</param>
    /// <returns>Tasks matching the given Filter criteria</returns>
    Task<List<TaskSearchResultItem>> SearchTasksAsync(TaskSearchFilter filter, CancellationToken ct = default);
}