using Npgsql;
using NpgsqlTypes;
using SqlFlowSdk.Management.Models;
using System.Data;
using System.Data.Common;

namespace SqlFlowSdk.Management.Postgres.Services
{
    public class PostgresSqlFlowManagementService : ISqlFlowManagementService
    {
        private readonly NpgsqlDataSource _dataSource;

        public PostgresSqlFlowManagementService(NpgsqlDataSource dataSource)
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

        private void AddParam(NpgsqlCommand cmd, string name, NpgsqlDbType dbType, object? value)
        {
            NpgsqlParameter p = cmd.CreateParameter();
            p.ParameterName = name;
            p.NpgsqlDbType = dbType;
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

        public async Task<List<QueueStatItem>> GetQueueStatsAsync(CancellationToken ct = default)
        {
            List<QueueStatItem> stats = [];

            string sql = @"
            SELECT queue_name, state, COUNT(*)::int as count 
            FROM ssf.tasks 
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

        public async Task<List<ThroughputBucketItem>> GetThroughputHistoryAsync(TimeSpan window, CancellationToken ct = default)
        {
            List<ThroughputBucketItem> result = [];
            string sql = @"
            SELECT 
                date_trunc('minute', r.completed_at) AS time_bucket, r.queue_name,
                COUNT(*) FILTER (WHERE r.state = 'completed')::int AS completed_count,
                COUNT(*) FILTER (WHERE r.state = 'failed')::int AS failed_count,
                COALESCE(AVG(EXTRACT(EPOCH FROM (r.completed_at - r.started_at)) * 1000), 0) AS avg_duration_ms
            FROM ssf.runs r
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

        public async Task<List<TaskPercentileItem>> GetTaskLatencyPercentilesAsync(string? queueName = null, CancellationToken ct = default)
        {
            List<TaskPercentileItem> result = [];
            string sql = @"
            SELECT 
                t.task_name,
                percentile_cont(0.50) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (r.completed_at - r.started_at)) * 1000) AS p50,
                percentile_cont(0.95) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (r.completed_at - r.started_at)) * 1000) AS p95,
                percentile_cont(0.99) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (r.completed_at - r.started_at)) * 1000) AS p99
            FROM ssf.runs r JOIN ssf.tasks t ON t.queue_name = r.queue_name AND t.task_id = r.task_id
            WHERE r.state = 'completed' AND (@queue IS NULL OR r.queue_name = @queue)
            GROUP BY t.task_name";

            await using NpgsqlConnection conn = await _dataSource
                .OpenConnectionAsync(ct)
                .ConfigureAwait(false);
            
            await using NpgsqlCommand cmd = conn.CreateCommand();

            cmd.CommandText = sql;

            AddParam(cmd, "@queue", NpgsqlDbType.Text, queueName);

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
            SELECT pg_total_relation_size('ssf.tasks') AS tasks_bytes,
                   pg_total_relation_size('ssf.runs') AS runs_bytes,
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

        public async Task<List<ActiveWorkerItem>> GetActiveWorkersAsync(CancellationToken ct = default)
        {
            List<ActiveWorkerItem> result = [];

            string sql = "SELECT queue_name, claimed_by, COUNT(*)::int as active_runs FROM ssf.runs WHERE state = 'running' AND claimed_by IS NOT NULL GROUP BY queue_name, claimed_by ORDER BY active_runs DESC";

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

        public async Task<List<QueueWaitTimeItem>> GetQueueWaitTimesAsync(int limit = 50, CancellationToken ct = default)
        {
            List<QueueWaitTimeItem> result = [];

            string sql = @"
                SELECT queue_name, 
                       COALESCE(AVG(EXTRACT(EPOCH FROM (first_started_at - enqueue_at)) * 1000), 0) AS avg_wait_ms, 
                       COALESCE(MAX(EXTRACT(EPOCH FROM (first_started_at - enqueue_at)) * 1000), 0) AS max_wait_ms 
                FROM 
                    ssf.tasks 
                WHERE 
                    first_started_at IS NOT NULL AND enqueue_at >= NOW() - INTERVAL '7 days' 
                GROUP BY queue_name
                LIMIT @limit";

            await using NpgsqlConnection conn = await _dataSource
                .OpenConnectionAsync(ct)
                .ConfigureAwait(false);

            await using NpgsqlCommand cmd = conn.CreateCommand();

            cmd.CommandText = sql;

            AddParam(cmd, "@limit", NpgsqlDbType.Integer, limit);

            await using NpgsqlDataReader reader = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);

            while (await reader.ReadAsync(ct).ConfigureAwait(false))
            {
                result.Add(new QueueWaitTimeItem(reader.GetString(0), reader.GetDouble(1), reader.GetDouble(2)));
            }

            return result;
        }

        public async Task<List<TaskFailureHotspotItem>> GetTaskFailureHotspotsAsync(int limit = 50, TimeSpan? lookbackWindow = null, CancellationToken ct = default)
        {
            List<TaskFailureHotspotItem> hotspots = [];
            
            string timeFilter = lookbackWindow.HasValue ? "AND r.failed_at >= NOW() - (@window_seconds || ' seconds')::INTERVAL" : "";
            
            string sql = @$"
                SELECT 
                    t.queue_name, 
                    t.task_name, 
                    COUNT(*)::int as failure_count, 
                    MAX(r.failed_at) 
                FROM 
                    ssf.runs r 
                        JOIN ssf.tasks t ON r.queue_name = t.queue_name AND r.task_id = t.task_id WHERE r.state = 'failed' {timeFilter} 
                GROUP BY t.queue_name, t.task_name 
                ORDER BY failure_count DESC 
                LIMIT @limit";

            await using NpgsqlConnection conn = await _dataSource
                .OpenConnectionAsync(ct)
                .ConfigureAwait(false);

            await using NpgsqlCommand cmd = conn.CreateCommand();
            
            cmd.CommandText = sql;

            AddParam(cmd, "@limit", limit);

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

        public async Task<List<ActiveWaitItem>> GetActiveWaitsAsync(int limit = 50, CancellationToken ct = default)
        {
            List<ActiveWaitItem> waits = [];
            
            string sql = @"
                SELECT 
                    queue_name, 
                    event_name, 
                    COUNT(*)::int as waiting_count, 
                    MIN(created_at) 
                FROM ssf.waits 
                GROUP BY queue_name, event_name 
                ORDER BY waiting_count DESC
                LIMIT @limit";

            await using NpgsqlConnection conn = await _dataSource
                .OpenConnectionAsync(ct)
                .ConfigureAwait(false);
            
            await using NpgsqlCommand cmd = conn.CreateCommand();
            
            cmd.CommandText = sql;

            AddParam(cmd, "@limit", NpgsqlDbType.Integer, limit);

            await using NpgsqlDataReader reader = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);

            while (await reader.ReadAsync(ct).ConfigureAwait(false))
            {
                waits.Add(new ActiveWaitItem(reader.GetString(0), reader.GetString(1), reader.GetInt32(2), reader.IsDBNull(3) ? null : reader.GetFieldValue<DateTimeOffset>(3)));
            }

            return waits;
        }

        public async Task<List<QueueBacklogItem>> GetQueueBacklogDepthAsync(int limit = 50, CancellationToken ct = default)
        {
            List<QueueBacklogItem> backlog = [];

            string sql = @"
                SELECT 
                    queue_name, 
                    COUNT(*)::int as pending_count, 
                    MIN(enqueue_at) 
                FROM 
                    ssf.tasks 
                WHERE 
                    state = 'pending' 
                GROUP BY 
                    queue_name 
                ORDER BY pending_count DESC
                LIMIT @limit";

            await using NpgsqlConnection conn = await _dataSource
                .OpenConnectionAsync(ct)
                .ConfigureAwait(false);

            await using NpgsqlCommand cmd = conn.CreateCommand();

            cmd.CommandText = sql;

            AddParam(cmd, "@limit", NpgsqlDbType.Integer, limit);

            await using NpgsqlDataReader reader = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);

            while (await reader.ReadAsync(ct).ConfigureAwait(false))
            {
                backlog.Add(new QueueBacklogItem(reader.GetString(0), reader.GetInt32(1), reader.IsDBNull(2) ? null : reader.GetFieldValue<DateTimeOffset>(2)));
            }

            return backlog;
        }

        public async Task<List<RetryHotspotItem>> GetRetryHotspotsAsync(int limit = 50, CancellationToken ct = default)
        {
            List<RetryHotspotItem> hotspots = [];
            
            string sql = @"
                SELECT 
                    queue_name, 
                    task_id::text, 
                    task_name, 
                    attempts, 
                    state 
                FROM 
                    ssf.tasks 
                WHERE 
                    attempts > 1 AND state IN ('pending', 'sleeping') 
                ORDER BY attempts DESC 
                LIMIT @limit";

            await using NpgsqlConnection conn = await _dataSource
                .OpenConnectionAsync(ct)
                .ConfigureAwait(false);

            await using NpgsqlCommand cmd = conn.CreateCommand();
            
            cmd.CommandText = sql;

            AddParam(cmd, "@limit", NpgsqlDbType.Integer, limit);


            await using NpgsqlDataReader reader = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);
            
            while (await reader.ReadAsync(ct).ConfigureAwait(false))
            {
                hotspots.Add(new RetryHotspotItem(reader.GetString(0), reader.GetString(1), reader.GetString(2), reader.GetInt32(3), reader.GetString(4)));
            }

            return hotspots;
        }

        public async Task<List<UpcomingWakeupBucketItem>> GetUpcomingWakeupsAsync(int limit = 50, CancellationToken ct = default)
        {
            List<UpcomingWakeupBucketItem> wakeups = [];

            string sql = "SELECT date_trunc('hour', r.available_at) AS time_bucket, r.queue_name, COUNT(*)::int AS sleeping_count FROM ssf.runs r WHERE r.state = 'sleeping' AND r.available_at > NOW() GROUP BY 1, 2 ORDER BY 1 ASC LIMIT @limit";

            await using NpgsqlConnection conn = await _dataSource
                .OpenConnectionAsync(ct)
                .ConfigureAwait(false);
            
            await using NpgsqlCommand cmd = conn.CreateCommand();
            
            cmd.CommandText = sql;

            AddParam(cmd, "@limit", NpgsqlDbType.Integer, limit);

            await using NpgsqlDataReader reader = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);
            
            while (await reader.ReadAsync(ct).ConfigureAwait(false))
            {
                wakeups.Add(new UpcomingWakeupBucketItem(reader.GetFieldValue<DateTimeOffset>(0), reader.GetString(1), reader.GetInt32(2)));
            }

            return wakeups;
        }

        public async Task<List<SlowTaskItem>> GetSlowestTasksAsync(int limit, CancellationToken ct = default)
        {
            List<SlowTaskItem> slowTasks = [];
            
            string sql = @"
                SELECT 
                    t.queue_name, 
                    t.task_id::text, 
                    t.task_name, 
                    EXTRACT(EPOCH FROM (r.completed_at - r.started_at)) * 1000 AS duration_ms, 
                    r.completed_at 
               FROM ssf.runs r 
                    JOIN ssf.tasks t ON r.queue_name = t.queue_name AND r.task_id = t.task_id 
               WHERE r.state = 'completed' AND r.completed_at >= NOW() - INTERVAL '24 hours' 
              ORDER BY duration_ms DESC LIMIT 50";

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

        public async Task<List<FailedTaskItem>> GetFailedTasksAsync(int limit = 50, CancellationToken ct = default)
        {
            List<FailedTaskItem> tasks = [];
            
            string sql = "SELECT t.queue_name, t.task_id, t.task_name, t.attempts, t.last_attempt_run, r.failed_at, r.failure_reason::text FROM ssf.tasks t LEFT JOIN ssf.runs r ON t.queue_name = r.queue_name AND t.last_attempt_run = r.run_id WHERE t.state = 'failed' ORDER BY r.failed_at DESC NULLS LAST LIMIT @limit";

            await using NpgsqlConnection conn = await _dataSource
                .OpenConnectionAsync(ct)
                .ConfigureAwait(false);
            
            await using NpgsqlCommand cmd = conn.CreateCommand();
            
            cmd.CommandText = sql;

            AddParam(cmd, "@limit", NpgsqlDbType.Integer, limit);

            await using NpgsqlDataReader reader = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);
            
            while (await reader.ReadAsync(ct).ConfigureAwait(false))
            {
                tasks.Add(new FailedTaskItem(reader.GetString(0), reader.GetGuid(1).ToString(), reader.GetString(2), reader.GetInt32(3), reader.IsDBNull(4) ? null : reader.GetGuid(4).ToString(), reader.IsDBNull(5) ? null : reader.GetFieldValue<DateTimeOffset>(5), ParseJson(reader, 6)));
            }

            return tasks;
        }

        public async Task<TaskDetailItem?> GetTaskDetailsAsync(string queueName, string taskId, CancellationToken ct = default)
        {
            string sql = "SELECT task_name, state, enqueue_at, first_started_at, completed_payload, params::text FROM ssf.tasks WHERE queue_name = @queue AND task_id = @task_id  LIMIT @limit OFFSET @offset";

            await using NpgsqlConnection conn = await _dataSource
                .OpenConnectionAsync(ct)
                .ConfigureAwait(false);

            await using NpgsqlCommand cmd = conn.CreateCommand();
            
            cmd.CommandText = sql;
            
            AddParam(cmd, "@queue", NpgsqlDbType.Text, queueName);
            AddParam(cmd, "@task_id", NpgsqlDbType.Uuid, Guid.Parse(taskId));
            AddParam(cmd, "@limit", NpgsqlDbType.Integer, 1);
            AddParam(cmd, "@offset", NpgsqlDbType.Integer, 0);

            await using NpgsqlDataReader reader = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);
            
            if (await reader.ReadAsync(ct).ConfigureAwait(false))
            {
                return new TaskDetailItem(taskId, queueName, reader.GetString(0), reader.GetString(1), reader.GetFieldValue<DateTimeOffset>(2), reader.IsDBNull(3) ? null : reader.GetFieldValue<DateTimeOffset>(3), ParseJson(reader, 4), ParseJson(reader, 5));
            }

            return null;
        }

        public async Task<List<TaskSearchResultItem>> SearchTasksAsync(TaskSearchFilter filter, CancellationToken ct = default)
        {
            List<TaskSearchResultItem> items = [];
            
            string orderByCol = filter.SortBy.ToLower() switch { "failed_at" => "r.failed_at", "attempts" => "t.attempts", _ => "t.enqueue_at" };
            string orderDir = filter.SortDescending ? "DESC" : "ASC";

            string sql = $@"
            SELECT t.queue_name, t.task_id::text, t.task_name, t.state, t.attempts, t.last_attempt_run::text, t.enqueue_at, r.failed_at, r.failure_reason::text, t.params::text
            FROM ssf.tasks t LEFT JOIN ssf.runs r ON t.queue_name = r.queue_name AND t.last_attempt_run = r.run_id
            WHERE t.queue_name = @queue
              AND (@states::text[] IS NULL OR t.state = ANY(@states::text[]))
              AND (@min_att::int IS NULL OR t.attempts >= @min_att::int)
              AND (@max_att::int IS NULL OR t.attempts <= @max_att::int)
              AND (@claimed_by::text IS NULL OR r.claimed_by = @claimed_by::text)
              AND (@search_term::text IS NULL OR (t.task_name ILIKE @search_term::text OR r.failure_reason::text ILIKE @search_term::text OR t.params::text ILIKE @search_term::text))
              AND (@from_date::timestamptz IS NULL OR t.enqueue_at >= @from_date::timestamptz)
              AND (@to_date::timestamptz IS NULL OR t.enqueue_at <= @to_date::timestamptz)
            ORDER BY {orderByCol} {orderDir} LIMIT @limit OFFSET @offset";

            await using NpgsqlConnection conn = await _dataSource
                .OpenConnectionAsync(ct)
                .ConfigureAwait(false);

            await using NpgsqlCommand cmd = conn.CreateCommand();
            
            cmd.CommandText = sql;

            AddParam(cmd, "@queue", NpgsqlDbType.Text, filter.QueueName);
            AddParam(cmd, "@states", NpgsqlDbType.Array | NpgsqlDbType.Text, filter.States?.ToArray());
            AddParam(cmd, "@min_att", NpgsqlDbType.Integer, filter.MinAttempts);
            AddParam(cmd, "@max_att", NpgsqlDbType.Integer, filter.MaxAttempts);
            AddParam(cmd, "@claimed_by", NpgsqlDbType.Text, filter.ClaimedBy);
            AddParam(cmd, "@search_term", string.IsNullOrWhiteSpace(filter.SearchTerm) ? null : $"%{filter.SearchTerm}%");
            AddParam(cmd, "@from_date", NpgsqlDbType.TimestampTz, filter.FromDate);
            AddParam(cmd, "@to_date", NpgsqlDbType.TimestampTz, filter.ToDate);
            AddParam(cmd, "@limit", NpgsqlDbType.Integer, filter.Limit);
            AddParam(cmd, "@offset", NpgsqlDbType.Integer, filter.Offset);

            await using NpgsqlDataReader reader = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);
            
            while (await reader.ReadAsync(ct).ConfigureAwait(false))
            {
                items.Add(new TaskSearchResultItem(reader.GetString(0), reader.GetString(1), reader.GetString(2), reader.GetString(3), reader.GetInt32(4), reader.IsDBNull(5) ? null : reader.GetString(5), reader.GetFieldValue<DateTimeOffset>(6), reader.IsDBNull(7) ? null : reader.GetFieldValue<DateTimeOffset>(7), ParseJson(reader, 8), ParseJson(reader, 9)));
            }
            
            return items;
        }
    }
}
