using Npgsql;
using NpgsqlTypes;
using SqlFlowSdk.Core;
using SqlFlowSdk.Exceptions;
using System.Data;
using System.Data.Common;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace SqlFlowSdk.Database;

/// <summary>
/// Encapsulates all raw database interactions. It is used to perform all the necessary operations on the database to 
/// manage queues, tasks, checkpoints, and events in the SqlFlow system using PostgreSQL.
/// </summary>
public class PostgresFlowDatabase : ISqlFlowDatabase
{
    public async Task CreateQueueAsync(DbConnection conn, string queueName, CancellationToken cancellationToken)
    {
        using NpgsqlCommand cmd = new("CALL ssf.create_queue(@p_queue_name, 'unpartitioned')", (NpgsqlConnection)conn);

        AddParam(cmd, "@p_queue_name", NpgsqlDbType.Text, queueName);

        await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    public async Task DropQueueAsync(DbConnection conn, string queueName, CancellationToken cancellationToken)
    {
        using NpgsqlCommand cmd = new("CALL ssf.drop_queue(@p_queue_name)", (NpgsqlConnection)conn);

        AddParam(cmd, "@p_queue_name", NpgsqlDbType.Text, queueName);

        await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    public async Task<IEnumerable<string>> ListQueuesAsync(DbConnection conn, CancellationToken cancellationToken)
    {
        List<string> results = new();

        using NpgsqlCommand cmd = new("SELECT queue_name FROM ssf.queues ORDER BY queue_name", (NpgsqlConnection)conn);

        using NpgsqlDataReader reader = await cmd.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);

        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            results.Add(reader.GetString(0));
        }
        return results;
    }

    public async Task<SpawnResult> SpawnTaskAsync(DbConnection conn, string queue, string taskName, string paramsJson, string optionsJson, CancellationToken cancellationToken)
    {
        // We cast parameters to jsonb inside the SQL string so PostgreSQL parses the text correctly
        string sql = "SELECT task_id, run_id, attempt FROM ssf.spawn_task(@p_queue_name, @p_task_name, @p_params::jsonb, @p_options::jsonb)";
        using NpgsqlCommand cmd = new(sql, (NpgsqlConnection)conn);

        AddParam(cmd, "@p_queue_name", NpgsqlDbType.Text, queue);
        AddParam(cmd, "@p_task_name", NpgsqlDbType.Text, taskName);
        AddParam(cmd, "@p_params", NpgsqlDbType.Jsonb, paramsJson);
        AddParam(cmd, "@p_options", NpgsqlDbType.Jsonb, optionsJson);

        using NpgsqlDataReader reader = await cmd.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);

        if (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            return new SpawnResult
            {
                TaskId = reader.GetGuid(0).ToString(),
                RunId = reader.GetGuid(1).ToString(),
                Attempt = reader.GetInt32(2)
            };
        }
        throw new Exception("Failed to spawn task");
    }

    public async Task CancelTaskAsync(DbConnection conn, string queue, string taskId, CancellationToken cancellationToken)
    {
        using NpgsqlCommand cmd = new("CALL ssf.cancel_task(@p_queue_name, @p_task_id)", (NpgsqlConnection)conn);

        AddParam(cmd, "@p_queue_name", NpgsqlDbType.Text, queue);
        AddParam(cmd, "@p_task_id", NpgsqlDbType.Uuid, Guid.Parse(taskId));

        await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    public async Task EmitEventAsync(DbConnection conn, string queue, string eventName, string payloadJson, CancellationToken cancellationToken)
    {
        using NpgsqlCommand cmd = new("CALL ssf.emit_event(@p_queue_name, @p_event_name, @p_payload)", (NpgsqlConnection)conn);

        AddParam(cmd, "@p_queue_name", NpgsqlDbType.Text, queue);
        AddParam(cmd, "@p_event_name", NpgsqlDbType.Text, eventName);
        AddParam(cmd, "@p_payload", NpgsqlDbType.Jsonb, payloadJson);

        await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    public async Task<IEnumerable<ClaimedTask>> ClaimTasksAsync(DbConnection conn, string queue, string workerId, int timeout, int count, CancellationToken cancellationToken)
    {
        List<ClaimedTask> tasks = new();

        string sql = "SELECT run_id, task_id, attempt, task_name, params, retry_strategy, max_attempts, headers, wake_event, event_payload " +
                     "FROM ssf.claim_task(@p_queue_name, @p_worker_id, @p_claim_timeout, @p_qty)";

        using NpgsqlCommand cmd = new(sql, (NpgsqlConnection)conn);

        AddParam(cmd, "@p_queue_name", NpgsqlDbType.Text, queue);
        AddParam(cmd, "@p_worker_id", NpgsqlDbType.Text, workerId);
        AddParam(cmd, "@p_claim_timeout", NpgsqlDbType.Integer, timeout);
        AddParam(cmd, "@p_qty", NpgsqlDbType.Integer, count);

        using NpgsqlDataReader reader = await cmd.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);

        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            tasks.Add(new ClaimedTask
            {
                RunId = reader.GetGuid(0).ToString(),
                TaskId = reader.GetGuid(1).ToString(),
                Attempt = reader.GetInt32(2),
                TaskName = reader.GetString(3),
                Params = ParseJson(reader, 4),
                RetryStrategy = ParseJson(reader, 5),
                MaxAttempts = reader.IsDBNull(6) ? null : reader.GetInt32(6),
                Headers = reader.IsDBNull(7) ? null : JsonSerializer.Deserialize<JsonObject>(reader.GetString(7)),
                WakeEvent = reader.IsDBNull(8) ? null : reader.GetString(8),
                EventPayload = ParseJson(reader, 9)
            });
        }

        return tasks;
    }

    public async Task CompleteRunAsync(DbConnection conn, string queue, string runId, string resultJson, CancellationToken cancellationToken)
    {
        using NpgsqlCommand cmd = new("CALL ssf.complete_run(@p_queue_name, @p_run_id, @p_state)", (NpgsqlConnection)conn);

        AddParam(cmd, "@p_queue_name", NpgsqlDbType.Text, queue);
        AddParam(cmd, "@p_run_id", NpgsqlDbType.Uuid, Guid.Parse(runId));
        AddParam(cmd, "@p_state", NpgsqlDbType.Jsonb, resultJson);

        await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    public async Task FailRunAsync(DbConnection conn, string queue, string runId, string errorJson, CancellationToken cancellationToken)
    {
        using NpgsqlCommand cmd = new("CALL ssf.fail_run(@p_queue_name, @p_run_id, @p_reason::jsonb, @p_retry_at)", (NpgsqlConnection)conn);

        AddParam(cmd, "@p_queue_name", NpgsqlDbType.Text, queue);
        AddParam(cmd, "@p_run_id", NpgsqlDbType.Uuid, Guid.Parse(runId));
        AddParam(cmd, "@p_reason", NpgsqlDbType.Jsonb, errorJson);
        AddParam(cmd, "@p_retry_at", NpgsqlDbType.TimestampTz, DBNull.Value); // Kann optional als UTC DateTime übergeben werden

        await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    public async Task<IEnumerable<CheckpointRow>> GetCheckpointStatesAsync(DbConnection conn, string queue, string taskId, string runId, CancellationToken cancellationToken)
    {
        List<CheckpointRow> rows = new();

        string sql = "SELECT checkpoint_name, state, status, owner_run_id, updated_at " +
                     "FROM ssf.get_task_checkpoint_states(@p_queue_name, @p_task_id, @p_run_id)";
        using NpgsqlCommand cmd = new(sql, (NpgsqlConnection)conn);

        AddParam(cmd, "@p_queue_name", NpgsqlDbType.Text, queue);
        AddParam(cmd, "@p_task_id", NpgsqlDbType.Uuid, Guid.Parse(taskId));
        AddParam(cmd, "@p_run_id", NpgsqlDbType.Uuid, Guid.Parse(runId));

        using NpgsqlDataReader reader = await cmd.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);

        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            rows.Add(new CheckpointRow
            {
                CheckpointName = reader.GetString(0),
                State = ParseJson(reader, 1),
                Status = reader.GetString(2),
                OwnerRunId = reader.IsDBNull(3) ? null : reader.GetGuid(3).ToString(),
                UpdatedAt = reader.GetDateTime(4).ToUniversalTime()
            });
        }

        return rows;
    }

    public async Task<JsonNode?> GetSingleCheckpointAsync(DbConnection conn, string queue, string taskId, string checkpointName, CancellationToken cancellationToken)
    {
        string sql = "SELECT checkpoint_name, state, status, owner_run_id, updated_at FROM ssf.get_task_checkpoint_state(@p_queue_name, @p_task_id, @p_step_name, @p_include_pending)";

        using NpgsqlCommand cmd = new(sql, (NpgsqlConnection)conn);

        AddParam(cmd, "@p_queue_name", NpgsqlDbType.Text, queue);
        AddParam(cmd, "@p_task_id", NpgsqlDbType.Uuid, Guid.Parse(taskId));
        AddParam(cmd, "@p_step_name", NpgsqlDbType.Text, checkpointName);
        AddParam(cmd, "@p_include_pending", NpgsqlDbType.Integer, 0);

        using NpgsqlDataReader reader = await cmd.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);

        if (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            return ParseJson(reader, 1);
        }

        return null;
    }

    public async Task PersistCheckpointAsync(DbConnection conn, string queue, string taskId, string runId, string checkpointName, string stateJson, int timeout, CancellationToken cancellationToken)
    {
        await ExecuteWithCancelCheckAsync(async (ct) =>
        {
            using NpgsqlCommand cmd = new("CALL ssf.set_task_checkpoint_state(@p_queue_name, @p_task_id, @p_step_name, @p_state, @p_owner_run, @p_extend_claim_by)", (NpgsqlConnection)conn);

            AddParam(cmd, "@p_queue_name", NpgsqlDbType.Text, queue);
            AddParam(cmd, "@p_task_id", NpgsqlDbType.Uuid, Guid.Parse(taskId));
            AddParam(cmd, "@p_step_name", NpgsqlDbType.Text, checkpointName);
            AddParam(cmd, "@p_state", NpgsqlDbType.Jsonb, stateJson);
            AddParam(cmd, "@p_owner_run", NpgsqlDbType.Uuid, Guid.Parse(runId));
            AddParam(cmd, "@p_extend_claim_by", NpgsqlDbType.Integer, timeout);

            return await cmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
        }, cancellationToken);
    }

    public async Task ScheduleRunAsync(DbConnection conn, string queue, string runId, DateTime wakeAt, CancellationToken cancellationToken)
    {
        using NpgsqlCommand cmd = new("CALL ssf.schedule_run(@p_queue_name, @p_run_id, @p_wake_at)", (NpgsqlConnection)conn);

        AddParam(cmd, "@p_queue_name", NpgsqlDbType.Text, queue);
        AddParam(cmd, "@p_run_id", NpgsqlDbType.Uuid, Guid.Parse(runId));
        AddParam(cmd, "@p_wake_at", NpgsqlDbType.TimestampTz, wakeAt.Kind == DateTimeKind.Utc ? wakeAt : wakeAt.ToUniversalTime());

        await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    public async Task HeartbeatAsync(DbConnection conn, string queue, string runId, int seconds, CancellationToken cancellationToken)
    {
        await ExecuteWithCancelCheckAsync(async (ct) =>
        {
            using NpgsqlCommand cmd = new("CALL ssf.extend_claim(@p_queue_name, @p_run_id, @p_extend_by)", (NpgsqlConnection)conn);

            AddParam(cmd, "@p_queue_name", NpgsqlDbType.Text, queue);
            AddParam(cmd, "@p_run_id", NpgsqlDbType.Uuid, Guid.Parse(runId));
            AddParam(cmd, "@p_extend_by", NpgsqlDbType.Integer, seconds);

            return await cmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
        }, cancellationToken);
    }

    public async Task<(bool ShouldSuspend, JsonNode? Payload)> AwaitEventAsync(DbConnection conn, string queue, string taskId, string runId, string checkpointName, string eventName, int? timeout, CancellationToken cancellationToken)
    {
        return await ExecuteWithCancelCheckAsync(async (ct) =>
        {
            string sql = "SELECT should_suspend, payload FROM ssf.await_event(@p_queue_name, @p_task_id, @p_run_id, @p_step_name, @p_event_name, @p_timeout)";

            using NpgsqlCommand cmd = new(sql, (NpgsqlConnection)conn);

            AddParam(cmd, "@p_queue_name", NpgsqlDbType.Text, queue);
            AddParam(cmd, "@p_task_id", NpgsqlDbType.Uuid, Guid.Parse(taskId));
            AddParam(cmd, "@p_run_id", NpgsqlDbType.Uuid, Guid.Parse(runId));
            AddParam(cmd, "@p_step_name", NpgsqlDbType.Text, checkpointName);
            AddParam(cmd, "@p_event_name", NpgsqlDbType.Text, eventName);
            AddParam(cmd, "@p_timeout", NpgsqlDbType.Integer, timeout);

            using NpgsqlDataReader reader = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);

            if (await reader.ReadAsync(ct).ConfigureAwait(false))
            {
                return (
                    reader.GetBoolean(0),
                    ParseJson(reader, 1)
                );
            }

            throw new Exception("Failed to await event");
        }, cancellationToken);
    }


    public async Task ReleaseWorkerClaimsAsync(DbConnection conn, string queue, string workerId, CancellationToken cancellationToken)
    {
        string sql = "CALL ssf.release_worker_claims(@p_queue_name, @p_worker_id);";

        using NpgsqlCommand cmd = new(sql, (NpgsqlConnection)conn);

        cmd.CommandText = sql;

        AddParam(cmd, "@p_queue_name", NpgsqlDbType.Text, queue);
        AddParam(cmd, "@p_worker_id", NpgsqlDbType.Text, workerId);

        await cmd.ExecuteNonQueryAsync(cancellationToken);
    }


    public async Task<int> CleanupTasksAsync(DbConnection conn, string queue, int ttlSeconds, int limit, CancellationToken cancellationToken)
    {
        string sql = "SELECT deleted_tasks FROM ssf.cleanup_tasks(@p_queue_name, @p_ttl_seconds, @p_limit);";

        using NpgsqlCommand cmd = new(sql, (NpgsqlConnection)conn);

        cmd.CommandText = sql;
        
        AddParam(cmd, "@p_queue_name", NpgsqlDbType.Text, queue);
        AddParam(cmd, "@p_ttl_seconds", NpgsqlDbType.Integer, ttlSeconds);
        AddParam(cmd, "@p_limit", NpgsqlDbType.Integer, limit);

        object? result = await cmd.ExecuteScalarAsync(cancellationToken);

        return result != null && result != DBNull.Value ? Convert.ToInt32(result) : 0;
    }

    public async Task<int> CleanupEventsAsync(DbConnection conn, string queue, int ttlSeconds, int limit, CancellationToken cancellationToken)
    {
        string sql = "SELECT deleted_tasks FROM ssf.cleanup_tasks(@p_queue_name, @p_ttl_seconds, @p_limit);";

        using NpgsqlCommand cmd = new(sql, (NpgsqlConnection)conn);

        cmd.CommandText = sql;

        AddParam(cmd, "@p_queue_name", NpgsqlDbType.Text, queue);
        AddParam(cmd, "@p_ttl_seconds", NpgsqlDbType.Integer, ttlSeconds);
        AddParam(cmd, "@p_limit", NpgsqlDbType.Integer, limit);

        object? result = await cmd.ExecuteScalarAsync(cancellationToken);

        return result != null && result != DBNull.Value ? Convert.ToInt32(result) : 0;
    }

    private static JsonNode? ParseJson(NpgsqlDataReader reader, int ordinal)
    {
        if (reader.IsDBNull(ordinal))
        {
            return null;
        }

        return JsonSerializer.Deserialize<JsonNode>(reader.GetString(ordinal));
    }

    private async Task<T> ExecuteWithCancelCheckAsync<T>(Func<CancellationToken, Task<T>> action, CancellationToken ct)
    {
        try
        {
            return await action(ct).ConfigureAwait(false);
        }
        catch (PostgresException ex) when (ex.SqlState == "50011")
        {
            throw new CancelledTaskException();
        }
    }

    private void AddParam(NpgsqlCommand cmd, string name, NpgsqlDbType dbType, object? value)
    {
        cmd.Parameters.AddWithValue(name, dbType, value ?? DBNull.Value);
    }
}