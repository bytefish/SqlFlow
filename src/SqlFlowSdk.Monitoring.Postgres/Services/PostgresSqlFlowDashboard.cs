using Npgsql;
using SqlFlowSdk.Monitoring.Models;
using System.Data.Common;

namespace SqlFlowSdk.Monitoring.Postgres.Services
{
    public class PostgresSqlFlowDashboard : ISqlFlowDashboard
    {
        private readonly NpgsqlDataSource _dataSource;

        public PostgresSqlFlowDashboard(NpgsqlDataSource dataSource)
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
            if (reader.IsDBNull(ordinal))
            {
                return null;
            }

            return reader.GetString(ordinal); // Postgres JSONB kommt als String zurück
        }

        public async Task<IEnumerable<QueueStatItem>> GetQueueStatsAsync(CancellationToken ct = default)
        {
            List<QueueStatItem> stats = [];

            string sql = @"
            SELECT queue_name, state, COUNT(*)::int as count 
            FROM relay.tasks 
            GROUP BY queue_name, state
            ORDER BY queue_name, state";

            await using NpgsqlConnection conn = await _dataSource
                .OpenConnectionAsync(ct)
                .ConfigureAwait(false);
            
            await using NpgsqlCommand cmd = conn.CreateCommand();
            
            cmd.CommandText = sql;

            await using NpgsqlDataReader reader = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);

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
                date_trunc('minute', r.completed_at) AS time_bucket, r.queue_name,
                COUNT(*) FILTER (WHERE r.state = 'completed')::int AS completed_count,
                COUNT(*) FILTER (WHERE r.state = 'failed')::int AS failed_count,
                COALESCE(AVG(EXTRACT(EPOCH FROM (r.completed_at - r.started_at)) * 1000), 0) AS avg_duration_ms
            FROM relay.runs r
            WHERE r.completed_at >= NOW() - (@window_seconds || ' seconds')::INTERVAL
            GROUP BY 1, 2 ORDER BY time_bucket DESC";

            await using NpgsqlConnection conn = await _dataSource
                .OpenConnectionAsync(ct)
                .ConfigureAwait(false);

            await using NpgsqlCommand cmd = conn.CreateCommand();

            cmd.CommandText = sql;
            
            AddParam(cmd, "@window_seconds", (int)window.TotalSeconds);

            await using NpgsqlDataReader reader = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);
            
            while (await reader.ReadAsync(ct).ConfigureAwait(false))
            {
                result.Add(new ThroughputBucketItem(
                    reader.GetFieldValue<DateTimeOffset>(0), reader.GetString(1),
                    reader.GetInt32(2), reader.GetInt32(3), reader.GetDouble(4)));
            }
            
            return result;
        }

        public async Task<IEnumerable<TaskPercentileItem>> GetTaskLatencyPercentilesAsync(string? queueName = null, CancellationToken ct = default)
        {
            List<TaskPercentileItem> result = [];
            string sql = @"
            SELECT 
                t.task_name,
                percentile_cont(0.50) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (r.completed_at - r.started_at)) * 1000) AS p50,
                percentile_cont(0.95) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (r.completed_at - r.started_at)) * 1000) AS p95,
                percentile_cont(0.99) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (r.completed_at - r.started_at)) * 1000) AS p99
            FROM relay.runs r JOIN relay.tasks t ON t.queue_name = r.queue_name AND t.task_id = r.task_id
            WHERE r.state = 'completed' AND (@queue IS NULL OR r.queue_name = @queue)
            GROUP BY t.task_name";

            await using NpgsqlConnection conn = await _dataSource
                .OpenConnectionAsync(ct)
                .ConfigureAwait(false);
            
            await using NpgsqlCommand cmd = conn.CreateCommand();

            cmd.CommandText = sql;

            AddParam(cmd, "@queue", queueName);

            await using NpgsqlDataReader reader = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);
            while (await reader.ReadAsync(ct).ConfigureAwait(false))
            {
                result.Add(new TaskPercentileItem(
                    reader.GetString(0), reader.IsDBNull(1) ? 0 : reader.GetDouble(1),
                    reader.IsDBNull(2) ? 0 : reader.GetDouble(2), reader.IsDBNull(3) ? 0 : reader.GetDouble(3)));
            }
            return result;
        }

        public async Task<DatabaseHealthItem> GetDatabaseHealthAsync(string queueName, CancellationToken ct = default)
        {
            string sql = @"
            SELECT pg_total_relation_size('relay.tasks') AS tasks_bytes,
                   pg_total_relation_size('relay.runs') AS runs_bytes,
                   (SELECT count(*)::int FROM pg_locks WHERE granted = false) AS blocked_locks";

            await using NpgsqlConnection conn = await _dataSource
                .OpenConnectionAsync(ct)
                .ConfigureAwait(false);
            
            await using NpgsqlCommand cmd = conn.CreateCommand();

            cmd.CommandText = sql;

            await using NpgsqlDataReader reader = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);

            if (await reader.ReadAsync(ct).ConfigureAwait(false))
            {
                return new DatabaseHealthItem("PostgreSQL", queueName, reader.GetInt64(0), reader.GetInt64(1), reader.GetInt32(2));
            }

            return new DatabaseHealthItem("PostgreSQL", queueName, 0, 0, 0);
        }

        public async Task<IEnumerable<ActiveWorkerItem>> GetActiveWorkersAsync(CancellationToken ct = default)
        {
            List<ActiveWorkerItem> result = [];

            string sql = "SELECT queue_name, claimed_by, COUNT(*)::int as active_runs FROM relay.runs WHERE state = 'running' AND claimed_by IS NOT NULL GROUP BY queue_name, claimed_by ORDER BY active_runs DESC";

            await using NpgsqlConnection conn = await _dataSource
                .OpenConnectionAsync(ct)
                .ConfigureAwait(false);

            await using NpgsqlCommand cmd = conn.CreateCommand();

            cmd.CommandText = sql;
            
            await using NpgsqlDataReader reader = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);

            while (await reader.ReadAsync(ct).ConfigureAwait(false))
            {
                result.Add(new ActiveWorkerItem(reader.GetString(0), reader.GetString(1), reader.GetInt32(2)));
            }

            return result;
        }

        public async Task<IEnumerable<QueueWaitTimeItem>> GetQueueWaitTimesAsync(CancellationToken ct = default)
        {
            List<QueueWaitTimeItem> result = [];

            string sql = "SELECT queue_name, COALESCE(AVG(EXTRACT(EPOCH FROM (first_started_at - enqueue_at)) * 1000), 0) AS avg_wait_ms, COALESCE(MAX(EXTRACT(EPOCH FROM (first_started_at - enqueue_at)) * 1000), 0) AS max_wait_ms FROM relay.tasks WHERE first_started_at IS NOT NULL AND enqueue_at >= NOW() - INTERVAL '7 days' GROUP BY queue_name";

            await using NpgsqlConnection conn = await _dataSource
                .OpenConnectionAsync(ct)
                .ConfigureAwait(false);

            await using NpgsqlCommand cmd = conn.CreateCommand();

            cmd.CommandText = sql;

            await using NpgsqlDataReader reader = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);

            while (await reader.ReadAsync(ct).ConfigureAwait(false))
            {
                result.Add(new QueueWaitTimeItem(reader.GetString(0), reader.GetDouble(1), reader.GetDouble(2)));
            }

            return result;
        }

        public async Task<IEnumerable<TaskFailureHotspotItem>> GetTaskFailureHotspotsAsync(TimeSpan? lookbackWindow = null, CancellationToken ct = default)
        {
            List<TaskFailureHotspotItem> hotspots = [];
            
            string timeFilter = lookbackWindow.HasValue ? "AND r.failed_at >= NOW() - (@window_seconds || ' seconds')::INTERVAL" : "";
            
            string sql = $"SELECT t.queue_name, t.task_name, COUNT(*)::int as failure_count, MAX(r.failed_at) FROM relay.runs r JOIN relay.tasks t ON r.queue_name = t.queue_name AND r.task_id = t.task_id WHERE r.state = 'failed' {timeFilter} GROUP BY t.queue_name, t.task_name ORDER BY failure_count DESC LIMIT 50";

            await using NpgsqlConnection conn = await _dataSource
                .OpenConnectionAsync(ct)
                .ConfigureAwait(false);

            await using NpgsqlCommand cmd = conn.CreateCommand();
            
            cmd.CommandText = sql;

            if (lookbackWindow.HasValue)
            {
                AddParam(cmd, "@window_seconds", (int)lookbackWindow.Value.TotalSeconds);
            }

            await using NpgsqlDataReader reader = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);
            while (await reader.ReadAsync(ct).ConfigureAwait(false))
            {
                hotspots.Add(new TaskFailureHotspotItem(reader.GetString(0), reader.GetString(1), reader.GetInt32(2), reader.GetFieldValue<DateTimeOffset>(3)));
            }

            return hotspots;
        }

        public async Task<IEnumerable<ActiveWaitItem>> GetActiveWaitsAsync(CancellationToken ct = default)
        {
            List<ActiveWaitItem> waits = [];
            
            string sql = "SELECT queue_name, event_name, COUNT(*)::int as waiting_count, MIN(created_at) FROM relay.waits GROUP BY queue_name, event_name ORDER BY waiting_count DESC";

            await using NpgsqlConnection conn = await _dataSource
                .OpenConnectionAsync(ct)
                .ConfigureAwait(false);
            
            await using NpgsqlCommand cmd = conn.CreateCommand();
            
            cmd.CommandText = sql;

            await using NpgsqlDataReader reader = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);
            while (await reader.ReadAsync(ct).ConfigureAwait(false))
            {
                waits.Add(new ActiveWaitItem(reader.GetString(0), reader.GetString(1), reader.GetInt32(2), reader.IsDBNull(3) ? null : reader.GetFieldValue<DateTimeOffset>(3)));
            }

            return waits;
        }

        public async Task<IEnumerable<QueueBacklogItem>> GetQueueBacklogDepthAsync(CancellationToken ct = default)
        {
            List<QueueBacklogItem> backlog = [];

            string sql = "SELECT queue_name, COUNT(*)::int as pending_count, MIN(enqueue_at) FROM relay.tasks WHERE state = 'pending' GROUP BY queue_name ORDER BY pending_count DESC";

            await using NpgsqlConnection conn = await _dataSource
                .OpenConnectionAsync(ct)
                .ConfigureAwait(false);

            await using NpgsqlCommand cmd = conn.CreateCommand();

            cmd.CommandText = sql;

            await using NpgsqlDataReader reader = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);

            while (await reader.ReadAsync(ct).ConfigureAwait(false))
            {
                backlog.Add(new QueueBacklogItem(reader.GetString(0), reader.GetInt32(1), reader.IsDBNull(2) ? null : reader.GetFieldValue<DateTimeOffset>(2)));
            }

            return backlog;
        }

        public async Task<IEnumerable<RetryHotspotItem>> GetRetryHotspotsAsync(CancellationToken ct = default)
        {
            List<RetryHotspotItem> hotspots = [];
            
            string sql = "SELECT queue_name, task_id::text, task_name, attempts, state FROM relay.tasks WHERE attempts > 1 AND state IN ('pending', 'sleeping') ORDER BY attempts DESC LIMIT 50";

            await using NpgsqlConnection conn = await _dataSource
                .OpenConnectionAsync(ct)
                .ConfigureAwait(false);

            await using NpgsqlCommand cmd = conn.CreateCommand();
            
            cmd.CommandText = sql;

            await using NpgsqlDataReader reader = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);
            
            while (await reader.ReadAsync(ct).ConfigureAwait(false))
            {
                hotspots.Add(new RetryHotspotItem(reader.GetString(0), reader.GetString(1), reader.GetString(2), reader.GetInt32(3), reader.GetString(4)));
            }

            return hotspots;
        }

        public async Task<IEnumerable<UpcomingWakeupBucketItem>> GetUpcomingWakeupsAsync(CancellationToken ct = default)
        {
            List<UpcomingWakeupBucketItem> wakeups = [];

            string sql = "SELECT date_trunc('hour', r.available_at) AS time_bucket, r.queue_name, COUNT(*)::int AS sleeping_count FROM relay.runs r WHERE r.state = 'sleeping' AND r.available_at > NOW() GROUP BY 1, 2 ORDER BY 1 ASC LIMIT 50";

            await using NpgsqlConnection conn = await _dataSource
                .OpenConnectionAsync(ct)
                .ConfigureAwait(false);
            
            await using NpgsqlCommand cmd = conn.CreateCommand();
            
            cmd.CommandText = sql;

            await using NpgsqlDataReader reader = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);
            
            while (await reader.ReadAsync(ct).ConfigureAwait(false))
            {
                wakeups.Add(new UpcomingWakeupBucketItem(reader.GetFieldValue<DateTimeOffset>(0), reader.GetString(1), reader.GetInt32(2)));
            }

            return wakeups;
        }

        public async Task<IEnumerable<SlowTaskItem>> GetSlowestTasksAsync(CancellationToken ct = default)
        {
            List<SlowTaskItem> slowTasks = [];
            
            string sql = "SELECT t.queue_name, t.task_id::text, t.task_name, EXTRACT(EPOCH FROM (r.completed_at - r.started_at)) * 1000 AS duration_ms, r.completed_at FROM relay.runs r JOIN relay.tasks t ON r.queue_name = t.queue_name AND r.task_id = t.task_id WHERE r.state = 'completed' AND r.completed_at >= NOW() - INTERVAL '24 hours' ORDER BY duration_ms DESC LIMIT 50";

            await using NpgsqlConnection conn = await _dataSource
                .OpenConnectionAsync(ct)
                .ConfigureAwait(false);

            await using NpgsqlCommand cmd = conn.CreateCommand();
            
            cmd.CommandText = sql;

            await using NpgsqlDataReader reader = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);
            while (await reader.ReadAsync(ct).ConfigureAwait(false))
            {
                slowTasks.Add(new SlowTaskItem(reader.GetString(0), reader.GetString(1), reader.GetString(2), reader.GetDouble(3), reader.GetFieldValue<DateTimeOffset>(4)));
            }

            return slowTasks;
        }

        public async Task<IEnumerable<FailedTaskItem>> GetFailedTasksAsync(int limit = 50, CancellationToken ct = default)
        {
            List<FailedTaskItem> tasks = [];
            
            string sql = "SELECT t.queue_name, t.task_id, t.task_name, t.attempts, t.last_attempt_run, r.failed_at, r.failure_reason::text FROM relay.tasks t LEFT JOIN relay.runs r ON t.queue_name = r.queue_name AND t.last_attempt_run = r.run_id WHERE t.state = 'failed' ORDER BY r.failed_at DESC NULLS LAST LIMIT @limit";

            await using NpgsqlConnection conn = await _dataSource
                .OpenConnectionAsync(ct)
                .ConfigureAwait(false);
            
            await using NpgsqlCommand cmd = conn.CreateCommand();
            
            cmd.CommandText = sql;
            AddParam(cmd, "@limit", limit);

            await using NpgsqlDataReader reader = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);
            
            while (await reader.ReadAsync(ct).ConfigureAwait(false))
            {
                tasks.Add(new FailedTaskItem(reader.GetString(0), reader.GetGuid(1).ToString(), reader.GetString(2), reader.GetInt32(3), reader.IsDBNull(4) ? null : reader.GetGuid(4).ToString(), reader.IsDBNull(5) ? null : reader.GetFieldValue<DateTimeOffset>(5), ParseJson(reader, 6)));
            }

            return tasks;
        }

        public async Task<TaskDetailItem?> GetTaskDetailsAsync(string queueName, string taskId, CancellationToken ct = default)
        {
            string sql = "SELECT task_name, state, enqueue_at, first_started_at, completed_payload, params::text FROM relay.tasks WHERE queue_name = @queue AND task_id = @task_id";

            await using NpgsqlConnection conn = await _dataSource
                .OpenConnectionAsync(ct)
                .ConfigureAwait(false);

            await using NpgsqlCommand cmd = conn.CreateCommand();
            
            cmd.CommandText = sql;
            
            AddParam(cmd, "@queue", queueName);
            AddParam(cmd, "@task_id", Guid.Parse(taskId));

            await using NpgsqlDataReader reader = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);
            
            if (await reader.ReadAsync(ct).ConfigureAwait(false))
            {
                return new TaskDetailItem(taskId, queueName, reader.GetString(0), reader.GetString(1), reader.GetFieldValue<DateTimeOffset>(2), reader.IsDBNull(3) ? null : reader.GetFieldValue<DateTimeOffset>(3), ParseJson(reader, 4), ParseJson(reader, 5));
            }

            return null;
        }

        public async Task<IEnumerable<TaskSearchResultItem>> SearchTasksAsync(TaskSearchFilter filter, CancellationToken ct = default)
        {
            List<TaskSearchResultItem> items = [];
            
            string orderByCol = filter.SortBy.ToLower() switch { "failed_at" => "r.failed_at", "attempts" => "t.attempts", _ => "t.enqueue_at" };
            string orderDir = filter.SortDescending ? "DESC" : "ASC";

            string sql = $@"
            SELECT t.queue_name, t.task_id::text, t.task_name, t.state, t.attempts, t.last_attempt_run::text, t.enqueue_at, r.failed_at, r.failure_reason::text, t.params::text
            FROM relay.tasks t LEFT JOIN relay.runs r ON t.queue_name = r.queue_name AND t.last_attempt_run = r.run_id
            WHERE t.queue_name = @queue
              AND (@states IS NULL OR t.state = ANY(@states))
              AND (@min_att IS NULL OR t.attempts >= @min_att)
              AND (@max_att IS NULL OR t.attempts <= @max_att)
              AND (@claimed_by IS NULL OR r.claimed_by = @claimed_by)
              AND (@search_term IS NULL OR (t.task_name ILIKE @search_term OR r.failure_reason::text ILIKE @search_term OR t.params::text ILIKE @search_term))
              AND (@from_date IS NULL OR t.enqueue_at >= @from_date)
              AND (@to_date IS NULL OR t.enqueue_at <= @to_date)
            ORDER BY {orderByCol} {orderDir} LIMIT @limit OFFSET @offset";

            await using NpgsqlConnection conn = await _dataSource
                .OpenConnectionAsync(ct)
                .ConfigureAwait(false);

            await using NpgsqlCommand cmd = conn.CreateCommand();
            
            cmd.CommandText = sql;

            AddParam(cmd, "@queue", filter.QueueName);
            AddParam(cmd, "@states", filter.States?.ToArray());
            AddParam(cmd, "@min_att", filter.MinAttempts);
            AddParam(cmd, "@max_att", filter.MaxAttempts);
            AddParam(cmd, "@claimed_by", filter.ClaimedBy);
            AddParam(cmd, "@search_term", string.IsNullOrWhiteSpace(filter.SearchTerm) ? null : $"%{filter.SearchTerm}%");
            AddParam(cmd, "@from_date", filter.FromDate);
            AddParam(cmd, "@to_date", filter.ToDate);
            AddParam(cmd, "@limit", filter.Limit);
            AddParam(cmd, "@offset", filter.Offset);

            await using NpgsqlDataReader reader = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);
            
            while (await reader.ReadAsync(ct).ConfigureAwait(false))
            {
                items.Add(new TaskSearchResultItem(reader.GetString(0), reader.GetString(1), reader.GetString(2), reader.GetString(3), reader.GetInt32(4), reader.IsDBNull(5) ? null : reader.GetString(5), reader.GetFieldValue<DateTimeOffset>(6), reader.IsDBNull(7) ? null : reader.GetFieldValue<DateTimeOffset>(7), ParseJson(reader, 8), ParseJson(reader, 9)));
            }
            
            return items;
        }
    }
}
