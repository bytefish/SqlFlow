using SqlFlowSdk.Monitoring.Models;
using System.Data.Common;

namespace SqlFlowSdk.Monitoring.SqlServer.Services;

/// <summary>
/// DataReader-Extensions for SQL Server-specific types, particularly for handling DateTimeOffset values.
/// </summary>
public static class DbDataReaderExtensions
{
    public static DateTimeOffset GetDateTimeOffset(this DbDataReader reader, int i)
    {
        return reader.GetFieldValue<DateTimeOffset>(i);
    }

    public static DateTimeOffset? GetNullableDateTimeOffset(this DbDataReader reader, int i)
    {
        return reader.IsDBNull(i) ? null : reader.GetFieldValue<DateTimeOffset>(i);
    }
}

/// <summary>
/// SqlServer-specific implementation of the ISqlFlowDashboard interface, providing methods to retrieve various statistics and metrics from 
/// a SQL Server database used by the SqlFlow system.
/// </summary>
public class SqlServerSqlFlowDashboard : ISqlFlowDashboard
{
    private readonly DbDataSource _dataSource;

    public SqlServerSqlFlowDashboard(DbDataSource dataSource)
    {
        _dataSource = dataSource;
    }

    private void AddParam(DbCommand cmd, string name, object? value)
    {
        DbParameter p = cmd.CreateParameter();
        p.ParameterName = name;
        p.Value = value ?? DBNull.Value;
        cmd.Parameters.Add(p);
    }

    private string? ParseJson(DbDataReader reader, int ordinal)
    {
        return reader.IsDBNull(ordinal) ? null : reader.GetString(ordinal);
    }

    public async Task<IEnumerable<QueueStatItem>> GetQueueStatsAsync(CancellationToken ct = default)
    {
        List<QueueStatItem> stats = [];

        string sql = @"
            SELECT queue_name, state, COUNT(*) as count 
            FROM relay.tasks WITH (NOLOCK)
            GROUP BY queue_name, state
            ORDER BY queue_name, state";

        await using DbConnection conn = await _dataSource
            .OpenConnectionAsync(ct)
            .ConfigureAwait(false);

        await using DbCommand cmd = conn.CreateCommand();

        cmd.CommandText = sql;

        await using DbDataReader reader = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);

        while (await reader.ReadAsync(ct).ConfigureAwait(false))
        {
            stats.Add(new QueueStatItem(reader.GetString(0), reader.GetString(1), reader.GetInt32(2)));
        }

        return stats;
    }

    public async Task<IEnumerable<ThroughputBucketItem>> GetThroughputHistoryAsync(TimeSpan window, CancellationToken ct = default)
    {
        List<ThroughputBucketItem> result = [];

        string sql = @"
            SELECT 
                DATEADD(minute, DATEDIFF(minute, 0, r.completed_at), 0) AS time_bucket, r.queue_name,
                SUM(CASE WHEN r.state = 'completed' THEN 1 ELSE 0 END) AS completed_count,
                SUM(CASE WHEN r.state = 'failed' THEN 1 ELSE 0 END) AS failed_count,
                ISNULL(AVG(CAST(DATEDIFF_BIG(millisecond, r.started_at, r.completed_at) AS FLOAT)), 0) AS avg_duration_ms
            FROM relay.runs r WITH (NOLOCK)
            WHERE r.completed_at >= DATEADD(second, -@window_seconds, SYSDATETIMEOFFSET())
            GROUP BY DATEADD(minute, DATEDIFF(minute, 0, r.completed_at), 0), r.queue_name
            ORDER BY time_bucket DESC";

        await using DbConnection conn = await _dataSource
            .OpenConnectionAsync(ct)
            .ConfigureAwait(false);

        await using DbCommand cmd = conn.CreateCommand();

        cmd.CommandText = sql;

        AddParam(cmd, "@window_seconds", (int)window.TotalSeconds);

        await using DbDataReader reader = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);

        while (await reader.ReadAsync(ct).ConfigureAwait(false))
        {
            result.Add(new ThroughputBucketItem(reader.GetDateTimeOffset(0), reader.GetString(1), reader.GetInt32(2), reader.GetInt32(3), reader.GetDouble(4)));
        }

        return result;
    }

    public async Task<IEnumerable<TaskPercentileItem>> GetTaskLatencyPercentilesAsync(string? queueName = null, CancellationToken ct = default)
    {
        List<TaskPercentileItem> result = [];
        string sql = @"
            WITH Latencies AS (
                SELECT t.task_name, DATEDIFF_BIG(millisecond, r.started_at, r.completed_at) AS duration_ms
                FROM relay.runs r WITH (NOLOCK) JOIN relay.tasks t WITH (NOLOCK) ON t.queue_name = r.queue_name AND t.task_id = r.task_id
                WHERE r.state = 'completed' AND (@queue IS NULL OR r.queue_name = @queue)
            )
            SELECT DISTINCT task_name,
                PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY duration_ms) OVER (PARTITION BY task_name) AS p50,
                PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY duration_ms) OVER (PARTITION BY task_name) AS p95,
                PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY duration_ms) OVER (PARTITION BY task_name) AS p99
            FROM Latencies";

        await using DbConnection conn = await _dataSource
            .OpenConnectionAsync(ct)
            .ConfigureAwait(false);

        await using DbCommand cmd = conn.CreateCommand();

        cmd.CommandText = sql;

        AddParam(cmd, "@queue", queueName);

        await using DbDataReader reader = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);

        while (await reader.ReadAsync(ct).ConfigureAwait(false))
        {
            result.Add(new TaskPercentileItem(reader.GetString(0), reader.IsDBNull(1) ? 0 : Convert.ToDouble(reader.GetValue(1)), reader.IsDBNull(2) ? 0 : Convert.ToDouble(reader.GetValue(2)), reader.IsDBNull(3) ? 0 : Convert.ToDouble(reader.GetValue(3))));
        }

        return result;
    }

    public async Task<DatabaseHealthItem> GetDatabaseHealthAsync(string queueName, CancellationToken ct = default)
    {
        string sql = @"
            SELECT 
                SUM(CASE WHEN t.name = 'tasks' THEN a.total_pages * 8 * 1024 ELSE 0 END) AS tasks_bytes,
                SUM(CASE WHEN t.name = 'runs' THEN a.total_pages * 8 * 1024 ELSE 0 END) AS runs_bytes
            FROM sys.tables t
            JOIN sys.indexes i ON t.object_id = i.object_id
            JOIN sys.partitions p ON i.object_id = p.object_id AND i.index_id = p.index_id
            JOIN sys.allocation_units a ON p.partition_id = a.container_id
            WHERE t.schema_id = SCHEMA_ID('relay') AND t.name IN ('tasks', 'runs')";

        await using DbConnection conn = await _dataSource
            .OpenConnectionAsync(ct)
            .ConfigureAwait(false);

        await using DbCommand cmd = conn.CreateCommand();

        cmd.CommandText = sql;

        await using DbDataReader reader = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);

        if (await reader.ReadAsync(ct).ConfigureAwait(false))
        {
            return new DatabaseHealthItem("SQL Server", queueName, reader.IsDBNull(0) ? 0 : Convert.ToInt64(reader.GetValue(0)), reader.IsDBNull(1) ? 0 : Convert.ToInt64(reader.GetValue(1)), 0);
        }

        return new DatabaseHealthItem("SQL Server", queueName, 0, 0, 0);
    }

    public async Task<IEnumerable<ActiveWorkerItem>> GetActiveWorkersAsync(CancellationToken ct = default)
    {
        List<ActiveWorkerItem> result = [];

        string sql = "SELECT queue_name, claimed_by, COUNT(*) as active_runs FROM relay.runs WITH (NOLOCK) WHERE state = 'running' AND claimed_by IS NOT NULL GROUP BY queue_name, claimed_by ORDER BY active_runs DESC";

        await using DbConnection conn = await _dataSource
            .OpenConnectionAsync(ct)
            .ConfigureAwait(false);

        await using DbCommand cmd = conn.CreateCommand();
        
        cmd.CommandText = sql;
        
        await using DbDataReader reader = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);

        while (await reader.ReadAsync(ct).ConfigureAwait(false))
        {
            result.Add(new ActiveWorkerItem(reader.GetString(0), reader.GetString(1), reader.GetInt32(2)));
        }

        return result;
    }

    public async Task<IEnumerable<QueueWaitTimeItem>> GetQueueWaitTimesAsync(CancellationToken ct = default)
    {
        List<QueueWaitTimeItem> result = [];

        string sql = "SELECT queue_name, ISNULL(AVG(CAST(DATEDIFF_BIG(millisecond, enqueue_at, first_started_at) AS FLOAT)), 0) AS avg_wait_ms, ISNULL(MAX(CAST(DATEDIFF_BIG(millisecond, enqueue_at, first_started_at) AS FLOAT)), 0) AS max_wait_ms FROM relay.tasks WITH (NOLOCK) WHERE first_started_at IS NOT NULL AND enqueue_at >= DATEADD(day, -7, SYSDATETIMEOFFSET()) GROUP BY queue_name";

        await using DbConnection conn = await _dataSource
            .OpenConnectionAsync(ct)
            .ConfigureAwait(false);

        await using DbCommand cmd = conn.CreateCommand();

        cmd.CommandText = sql;

        await using DbDataReader reader = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);

        while (await reader.ReadAsync(ct).ConfigureAwait(false))
        {
            result.Add(new QueueWaitTimeItem(reader.GetString(0), reader.GetDouble(1), reader.GetDouble(2)));
        }

        return result;
    }

    public async Task<IEnumerable<TaskFailureHotspotItem>> GetTaskFailureHotspotsAsync(TimeSpan? lookbackWindow = null, CancellationToken ct = default)
    {
        List<TaskFailureHotspotItem> hotspots = [];

        string timeFilter = lookbackWindow.HasValue ? "AND r.failed_at >= DATEADD(second, -@window_seconds, SYSDATETIMEOFFSET())" : "";
        string sql = $"SELECT TOP 50 t.queue_name, t.task_name, COUNT(*) as failure_count, MAX(r.failed_at) FROM relay.runs r WITH (NOLOCK) JOIN relay.tasks t WITH (NOLOCK) ON r.queue_name = t.queue_name AND r.task_id = t.task_id WHERE r.state = 'failed' {timeFilter} GROUP BY t.queue_name, t.task_name ORDER BY failure_count DESC";

        await using DbConnection conn = await _dataSource
            .OpenConnectionAsync(ct)
            .ConfigureAwait(false);

        await using DbCommand cmd = conn.CreateCommand();

        cmd.CommandText = sql;

        if (lookbackWindow.HasValue)
        {
            AddParam(cmd, "@window_seconds", (int)lookbackWindow.Value.TotalSeconds);
        }

        await using DbDataReader reader = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);

        while (await reader.ReadAsync(ct).ConfigureAwait(false))
        {
            hotspots.Add(new TaskFailureHotspotItem(reader.GetString(0), reader.GetString(1), reader.GetInt32(2), reader.GetDateTimeOffset(3)));
        }

        return hotspots;
    }

    public async Task<IEnumerable<ActiveWaitItem>> GetActiveWaitsAsync(CancellationToken ct = default)
    {
        List<ActiveWaitItem> waits = [];

        string sql = "SELECT queue_name, event_name, COUNT(*) as waiting_count, MIN(created_at) FROM relay.waits WITH (NOLOCK) GROUP BY queue_name, event_name ORDER BY waiting_count DESC";

        await using DbConnection conn = await _dataSource
            .OpenConnectionAsync(ct)
            .ConfigureAwait(false);

        await using DbCommand cmd = conn.CreateCommand();

        cmd.CommandText = sql;

        await using DbDataReader reader = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);

        while (await reader.ReadAsync(ct).ConfigureAwait(false))
        {
            waits.Add(new ActiveWaitItem(reader.GetString(0), reader.GetString(1), reader.GetInt32(2), reader.IsDBNull(3) ? null : reader.GetDateTimeOffset(3)));
        }

        return waits;
    }

    public async Task<IEnumerable<QueueBacklogItem>> GetQueueBacklogDepthAsync(CancellationToken ct = default)
    {
        List<QueueBacklogItem> backlog = [];

        string sql = "SELECT queue_name, COUNT(*) as pending_count, MIN(enqueue_at) FROM relay.tasks WITH (NOLOCK) WHERE state = 'pending' GROUP BY queue_name ORDER BY pending_count DESC";

        await using DbConnection conn = await _dataSource
            .OpenConnectionAsync(ct)
            .ConfigureAwait(false);

        await using DbCommand cmd = conn.CreateCommand();

        cmd.CommandText = sql;

        await using DbDataReader reader = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);

        while (await reader.ReadAsync(ct).ConfigureAwait(false))
        {
            backlog.Add(new QueueBacklogItem(reader.GetString(0), reader.GetInt32(1), reader.IsDBNull(2) ? null : reader.GetNullableDateTimeOffset(2)));
        }

        return backlog;
    }

    public async Task<IEnumerable<RetryHotspotItem>> GetRetryHotspotsAsync(CancellationToken ct = default)
    {
        List<RetryHotspotItem> hotspots = [];

        string sql = "SELECT TOP 50 queue_name, CAST(task_id AS NVARCHAR(36)), task_name, attempts, state FROM relay.tasks WITH (NOLOCK) WHERE attempts > 1 AND state IN ('pending', 'sleeping') ORDER BY attempts DESC";

        await using DbConnection conn = await _dataSource
            .OpenConnectionAsync(ct)
            .ConfigureAwait(false);

        await using DbCommand cmd = conn.CreateCommand();

        cmd.CommandText = sql;

        await using DbDataReader reader = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);

        while (await reader.ReadAsync(ct).ConfigureAwait(false))
        {
            hotspots.Add(new RetryHotspotItem(reader.GetString(0), reader.GetString(1), reader.GetString(2), reader.GetInt32(3), reader.GetString(4)));
        }

        return hotspots;
    }

    public async Task<IEnumerable<UpcomingWakeupBucketItem>> GetUpcomingWakeupsAsync(CancellationToken ct = default)
    {
        List<UpcomingWakeupBucketItem> wakeups = [];

        string sql = "SELECT TOP 50 DATEADD(hour, DATEDIFF(hour, 0, r.available_at), 0) AS time_bucket, r.queue_name, COUNT(*) AS sleeping_count FROM relay.runs r WITH (NOLOCK) WHERE r.state = 'sleeping' AND r.available_at > SYSDATETIMEOFFSET() GROUP BY DATEADD(hour, DATEDIFF(hour, 0, r.available_at), 0), r.queue_name ORDER BY time_bucket ASC";

        await using DbConnection conn = await _dataSource
            .OpenConnectionAsync(ct)
            .ConfigureAwait(false);

        await using DbCommand cmd = conn.CreateCommand();

        cmd.CommandText = sql;

        await using DbDataReader reader = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);
        while (await reader.ReadAsync(ct).ConfigureAwait(false))
        {
            wakeups.Add(new UpcomingWakeupBucketItem(reader.GetDateTimeOffset(0), reader.GetString(1), reader.GetInt32(2)));
        }

        return wakeups;
    }

    public async Task<IEnumerable<SlowTaskItem>> GetSlowestTasksAsync(CancellationToken ct = default)
    {
        List<SlowTaskItem> slowTasks = [];

        string sql = "SELECT TOP 50 t.queue_name, CAST(t.task_id AS NVARCHAR(36)), t.task_name, CAST(DATEDIFF_BIG(millisecond, r.started_at, r.completed_at) AS FLOAT) AS duration_ms, r.completed_at FROM relay.runs r WITH (NOLOCK) JOIN relay.tasks t WITH (NOLOCK) ON r.queue_name = t.queue_name AND r.task_id = t.task_id WHERE r.state = 'completed' AND r.completed_at >= DATEADD(day, -1, SYSDATETIMEOFFSET()) ORDER BY duration_ms DESC";

        await using DbConnection conn = await _dataSource
            .OpenConnectionAsync(ct)
            .ConfigureAwait(false);

        await using DbCommand cmd = conn.CreateCommand();

        cmd.CommandText = sql;

        await using DbDataReader reader = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);

        while (await reader.ReadAsync(ct).ConfigureAwait(false))
        {
            slowTasks.Add(new SlowTaskItem(reader.GetString(0), reader.GetString(1), reader.GetString(2), reader.GetDouble(3), reader.GetDateTimeOffset(4)));
        }

        return slowTasks;
    }

    public async Task<IEnumerable<FailedTaskItem>> GetFailedTasksAsync(int limit = 50, CancellationToken ct = default)
    {
        List<FailedTaskItem> tasks = [];

        string sql = "SELECT TOP (@limit) t.queue_name, CAST(t.task_id AS NVARCHAR(36)), t.task_name, t.attempts, CAST(t.last_attempt_run AS NVARCHAR(36)), r.failed_at, r.failure_reason FROM relay.tasks t WITH (NOLOCK) LEFT JOIN relay.runs r WITH (NOLOCK) ON t.queue_name = r.queue_name AND t.last_attempt_run = r.run_id WHERE t.state = 'failed' ORDER BY r.failed_at DESC";

        await using DbConnection conn = await _dataSource
            .OpenConnectionAsync(ct)
            .ConfigureAwait(false);

        await using DbCommand cmd = conn.CreateCommand();

        cmd.CommandText = sql;

        AddParam(cmd, "@limit", limit);

        await using DbDataReader reader = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);
        while (await reader.ReadAsync(ct).ConfigureAwait(false))
        {
            tasks.Add(new FailedTaskItem(reader.GetString(0), reader.GetString(1), reader.GetString(2), reader.GetInt32(3), reader.IsDBNull(4) ? null : reader.GetString(4), reader.IsDBNull(5) ? null : reader.GetNullableDateTimeOffset(5), ParseJson(reader, 6)));
        }

        return tasks;
    }

    public async Task<TaskDetailItem?> GetTaskDetailsAsync(string queueName, string taskId, CancellationToken ct = default)
    {
        string sql = "SELECT task_name, state, enqueue_at, first_started_at, completed_payload, params FROM relay.tasks WITH (NOLOCK) WHERE queue_name = @queue AND task_id = @task_id";

        await using DbConnection conn = await _dataSource
            .OpenConnectionAsync(ct)
            .ConfigureAwait(false);

        await using DbCommand cmd = conn.CreateCommand();

        cmd.CommandText = sql;

        AddParam(cmd, "@queue", queueName);
        AddParam(cmd, "@task_id", Guid.Parse(taskId));

        await using DbDataReader reader = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);

        if (await reader.ReadAsync(ct).ConfigureAwait(false))
        {
            return new TaskDetailItem(taskId, queueName, reader.GetString(0), reader.GetString(1), reader.GetDateTimeOffset(2), reader.IsDBNull(3) ? null : reader.GetNullableDateTimeOffset(3), ParseJson(reader, 4), ParseJson(reader, 5));
        }

        return null;
    }

    public async Task<IEnumerable<TaskSearchResultItem>> SearchTasksAsync(TaskSearchFilter filter, CancellationToken ct = default)
    {
        List<TaskSearchResultItem> items = [];

        string orderByCol = filter.SortBy.ToLower() switch
        {
            "failed_at" => "r.failed_at",
            "attempts" => "t.attempts",
            _ => "t.enqueue_at"
        };

        string orderDir = filter.SortDescending ? "DESC" : "ASC";

        string sql = $@"
            SELECT t.queue_name, CAST(t.task_id AS NVARCHAR(36)), t.task_name, t.state, t.attempts, CAST(t.last_attempt_run AS NVARCHAR(36)), t.enqueue_at, r.failed_at, r.failure_reason, t.params
            FROM relay.tasks t WITH (NOLOCK) LEFT JOIN relay.runs r WITH (NOLOCK) ON t.queue_name = r.queue_name AND t.last_attempt_run = r.run_id
            WHERE t.queue_name = @queue
              AND (@states_csv IS NULL OR t.state IN (SELECT value FROM STRING_SPLIT(@states_csv, ',')))
              AND (@min_att IS NULL OR t.attempts >= @min_att)
              AND (@max_att IS NULL OR t.attempts <= @max_att)
              AND (@claimed_by IS NULL OR r.claimed_by = @claimed_by)
              AND (@search_term IS NULL OR (t.task_name LIKE @search_term OR r.failure_reason LIKE @search_term OR t.params LIKE @search_term))
              AND (@from_date IS NULL OR t.enqueue_at >= @from_date)
              AND (@to_date IS NULL OR t.enqueue_at <= @to_date)
            ORDER BY {orderByCol} {orderDir} 
            OFFSET @offset ROWS FETCH NEXT @limit ROWS ONLY";

        await using DbConnection conn = await _dataSource
            .OpenConnectionAsync(ct)
            .ConfigureAwait(false);

        await using DbCommand cmd = conn.CreateCommand();

        cmd.CommandText = sql;

        string? statesCsv = filter.States != null && filter.States.Count > 0 ? string.Join(",", filter.States) : null;

        AddParam(cmd, "@queue", filter.QueueName);
        AddParam(cmd, "@states_csv", statesCsv);
        AddParam(cmd, "@min_att", filter.MinAttempts);
        AddParam(cmd, "@max_att", filter.MaxAttempts);
        AddParam(cmd, "@claimed_by", filter.ClaimedBy);
        AddParam(cmd, "@search_term", string.IsNullOrWhiteSpace(filter.SearchTerm) ? null : $"%{filter.SearchTerm}%");
        AddParam(cmd, "@from_date", filter.FromDate);
        AddParam(cmd, "@to_date", filter.ToDate);
        AddParam(cmd, "@offset", filter.Offset);
        AddParam(cmd, "@limit", filter.Limit);

        await using DbDataReader reader = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);

        while (await reader.ReadAsync(ct).ConfigureAwait(false))
        {
            items.Add(new TaskSearchResultItem(reader.GetString(0), reader.GetString(1), reader.GetString(2), reader.GetString(3), reader.GetInt32(4), reader.IsDBNull(5) ? null : reader.GetString(5), reader.GetDateTimeOffset(6), reader.IsDBNull(7) ? null : reader.GetNullableDateTimeOffset(7), ParseJson(reader, 8), ParseJson(reader, 9)));
        }

        return items;
    }
}