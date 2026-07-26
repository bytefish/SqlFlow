// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using Microsoft.Data.SqlClient;
using SqlFlowSdk.Core;
using SqlFlowSdk.Database;
using SqlFlowSdk.Exceptions;
using System.Data;
using System.Data.Common;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace SqlFlowSdk.SqlServer.Database;

/// <summary>
/// Encapsulates all raw database interactions. It is used to perform all the necessary operations on the database to 
/// manage queues, tasks, checkpoints, and events in the SqlFlow system using SQL Server.
/// </summary>
/// <summary>
/// Encapsulates all raw database interactions. It is used to perform all the necessary operations on the database to 
/// manage queues, tasks, checkpoints, and events in the SqlFlow system using SQL Server.
/// </summary>
public class SqlServerFlowDatabase : ISqlFlowDatabase
{
    public async Task CreateQueueAsync(DbConnection conn, string queueName, CancellationToken cancellationToken)
    {
        using SqlCommand cmd = new("ssf.create_queue", (SqlConnection)conn) { CommandType = CommandType.StoredProcedure };

        AddParam(cmd, "@p_queue_name", queueName);

        await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    public async Task DropQueueAsync(DbConnection conn, string queueName, CancellationToken cancellationToken)
    {
        using SqlCommand cmd = new("ssf.drop_queue", (SqlConnection)conn) { CommandType = CommandType.StoredProcedure };

        AddParam(cmd, "@p_queue_name", queueName);

        await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    public async Task<IEnumerable<string>> ListQueuesAsync(DbConnection conn, CancellationToken cancellationToken)
    {
        List<string> results = new();

        using SqlCommand cmd = new("SELECT queue_name FROM ssf.queues ORDER BY queue_name", (SqlConnection)conn);

        using SqlDataReader reader = await cmd.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);

        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            results.Add(reader.GetString(0));
        }
        return results;
    }

    public async Task<SpawnResult> SpawnTaskAsync(DbConnection conn, string queue, string taskName, string paramsJson, string optionsJson, CancellationToken cancellationToken)
    {
        using SqlCommand cmd = new("ssf.spawn_task", (SqlConnection)conn) { CommandType = CommandType.StoredProcedure };

        AddParam(cmd, "@p_queue_name", queue);
        AddParam(cmd, "@p_task_name", taskName);
        AddParam(cmd, "@p_params", paramsJson);
        AddParam(cmd, "@p_options", optionsJson);

        using SqlDataReader reader = await cmd.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);

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
        using SqlCommand cmd = new("ssf.cancel_task", (SqlConnection)conn) { CommandType = CommandType.StoredProcedure };

        AddParam(cmd, "@p_queue_name", queue);
        AddParam(cmd, "@p_task_id", Guid.Parse(taskId));

        await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    public async Task EmitEventAsync(DbConnection conn, string queue, string eventName, string payloadJson, CancellationToken cancellationToken)
    {
        using SqlCommand cmd = new("ssf.emit_event", (SqlConnection)conn) { CommandType = CommandType.StoredProcedure };

        AddParam(cmd, "@p_queue_name", queue);
        AddParam(cmd, "@p_event_name", eventName);
        AddParam(cmd, "@p_payload", payloadJson);

        await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    public async Task<IEnumerable<ClaimedTask>> ClaimTasksAsync(DbConnection conn, string queue, string workerId, int timeout, int count, CancellationToken cancellationToken)
    {
        List<ClaimedTask> tasks = new();

        using SqlCommand cmd = new("ssf.claim_task", (SqlConnection)conn) { CommandType = CommandType.StoredProcedure };

        AddParam(cmd, "@p_queue_name", queue);
        AddParam(cmd, "@p_worker_id", workerId);
        AddParam(cmd, "@p_claim_timeout", timeout);
        AddParam(cmd, "@p_qty", count);

        using SqlDataReader reader = await cmd.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);

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
        using SqlCommand cmd = new("ssf.complete_run", (SqlConnection)conn) { CommandType = CommandType.StoredProcedure };

        AddParam(cmd, "@p_queue_name", queue);
        AddParam(cmd, "@p_run_id", Guid.Parse(runId));
        AddParam(cmd, "@p_state", resultJson);

        await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    public async Task FailRunAsync(DbConnection conn, string queue, string runId, string errorJson, CancellationToken cancellationToken)
    {
        using SqlCommand cmd = new("ssf.fail_run", (SqlConnection)conn) { CommandType = CommandType.StoredProcedure };

        AddParam(cmd, "@p_queue_name", queue);
        AddParam(cmd, "@p_run_id", Guid.Parse(runId));
        AddParam(cmd, "@p_reason", errorJson);
        AddParam(cmd, "@p_retry_at", DBNull.Value); // Kann optional als DateTimeOffset übergeben werden

        await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    public async Task<IEnumerable<CheckpointRow>> GetCheckpointStatesAsync(DbConnection conn, string queue, string taskId, string runId, CancellationToken cancellationToken)
    {
        List<CheckpointRow> rows = new();

        using SqlCommand cmd = new("ssf.get_task_checkpoint_states", (SqlConnection)conn) { CommandType = CommandType.StoredProcedure };

        AddParam(cmd, "@p_queue_name", queue);
        AddParam(cmd, "@p_task_id", Guid.Parse(taskId));
        AddParam(cmd, "@p_run_id", Guid.Parse(runId));

        using SqlDataReader reader = await cmd.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);

        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            rows.Add(new CheckpointRow
            {
                CheckpointName = reader.GetString(0),
                State = ParseJson(reader, 1),
                Status = reader.GetString(2),
                OwnerRunId = reader.IsDBNull(3) ? null : reader.GetGuid(3).ToString(),
                UpdatedAt = reader.GetDateTimeOffset(4).UtcDateTime
            });
        }

        return rows;
    }

    public async Task<JsonNode?> GetSingleCheckpointAsync(DbConnection conn, string queue, string taskId, string checkpointName, CancellationToken cancellationToken)
    {
        using SqlCommand cmd = new("ssf.get_task_checkpoint_state", (SqlConnection)conn) { CommandType = CommandType.StoredProcedure };

        AddParam(cmd, "@p_queue_name", queue);
        AddParam(cmd, "@p_task_id", Guid.Parse(taskId));
        AddParam(cmd, "@p_step_name", checkpointName);
        AddParam(cmd, "@p_include_pending", 0);

        using SqlDataReader reader = await cmd.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);

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
            using SqlCommand cmd = new("ssf.set_task_checkpoint_state", (SqlConnection)conn) { CommandType = CommandType.StoredProcedure };

            AddParam(cmd, "@p_queue_name", queue);
            AddParam(cmd, "@p_task_id", Guid.Parse(taskId));
            AddParam(cmd, "@p_step_name", checkpointName);
            AddParam(cmd, "@p_state", stateJson);
            AddParam(cmd, "@p_owner_run", Guid.Parse(runId));
            AddParam(cmd, "@p_extend_claim_by", timeout);

            return await cmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
        }, cancellationToken);
    }

    public async Task ScheduleRunAsync(DbConnection conn, string queue, string runId, DateTime wakeAt, CancellationToken cancellationToken)
    {
        using SqlCommand cmd = new("ssf.schedule_run", (SqlConnection)conn) { CommandType = CommandType.StoredProcedure };

        AddParam(cmd, "@p_queue_name", queue);
        AddParam(cmd, "@p_run_id", Guid.Parse(runId));
        AddParam(cmd, "@p_wake_at", new DateTimeOffset(wakeAt, TimeSpan.Zero));

        await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    public async Task HeartbeatAsync(DbConnection conn, string queue, string runId, int seconds, CancellationToken cancellationToken)
    {
        await ExecuteWithCancelCheckAsync(async (ct) =>
        {
            using SqlCommand cmd = new("ssf.extend_claim", (SqlConnection)conn) { CommandType = CommandType.StoredProcedure };

            AddParam(cmd, "@p_queue_name", queue);
            AddParam(cmd, "@p_run_id", Guid.Parse(runId));
            AddParam(cmd, "@p_extend_by", seconds);

            return await cmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
        }, cancellationToken);
    }

    public async Task<(bool ShouldSuspend, JsonNode? Payload)> AwaitEventAsync(DbConnection conn, string queue, string taskId, string runId, string checkpointName, string eventName, int? timeout, CancellationToken cancellationToken)
    {
        return await ExecuteWithCancelCheckAsync(async (ct) =>
        {
            using SqlCommand cmd = new("ssf.await_event", (SqlConnection)conn) { CommandType = CommandType.StoredProcedure };

            AddParam(cmd, "@p_queue_name", queue);
            AddParam(cmd, "@p_task_id", Guid.Parse(taskId));
            AddParam(cmd, "@p_run_id", Guid.Parse(runId));
            AddParam(cmd, "@p_step_name", checkpointName);
            AddParam(cmd, "@p_event_name", eventName);
            AddParam(cmd, "@p_timeout", timeout);

            using SqlDataReader reader = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);

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
        using SqlCommand cmd = new("ssf.release_worker_claims", (SqlConnection)conn) { CommandType = CommandType.StoredProcedure };

        AddParam(cmd, "@p_queue_name", queue);
        AddParam(cmd, "@p_worker_id", workerId);

        await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    public async Task<int> CleanupTasksAsync(DbConnection conn, string queue, int ttlSeconds, int limit, CancellationToken cancellationToken)
    {
        using SqlCommand cmd = new("ssf.cleanup_tasks", (SqlConnection)conn) { CommandType = CommandType.StoredProcedure };

        AddParam(cmd, "@queue", queue);
        AddParam(cmd, "@ttl", ttlSeconds);
        AddParam(cmd, "@limit", limit);

        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);

        if (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            return reader.IsDBNull(0) ? 0 : reader.GetInt32(0);
        }
        return 0;
    }

    public async Task<int> CleanupEventsAsync(DbConnection conn, string queue, int ttlSeconds, int limit, CancellationToken cancellationToken)
    {
        using SqlCommand cmd = new("ssf.cleanup_events", (SqlConnection)conn) { CommandType = CommandType.StoredProcedure };

        AddParam(cmd, "@queue", queue);
        AddParam(cmd, "@ttl", ttlSeconds);
        AddParam(cmd, "@limit", limit);

        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);

        if (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            return reader.IsDBNull(0) ? 0 : reader.GetInt32(0);
        }
        return 0;
    }

    private static JsonNode? ParseJson(SqlDataReader reader, int ordinal)
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
        catch (SqlException ex) when (ex.Number == 50011)
        {
            throw new CancelledTaskException();
        }
    }

    private void AddParam(SqlCommand cmd, string name, object? value)
    {
        cmd.Parameters.AddWithValue(name, value ?? DBNull.Value);
    }
}