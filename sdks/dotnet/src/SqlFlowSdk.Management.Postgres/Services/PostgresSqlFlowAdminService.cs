using Npgsql;
using NpgsqlTypes;
using SqlFlowSdk.Database;
using SqlFlowSdk.Management.Models;
using System.Data;
using System.Data.Common;

namespace SqlFlowSdk.Management.Postgres.Services;

public class PostgresSqlFlowAdminService : ISqlFlowAdminService
{
    private readonly DbDataSource _dataSource;
    private readonly ISqlFlowDatabase _database;

    public PostgresSqlFlowAdminService(DbDataSource dataSource, ISqlFlowDatabase database)
    {
        _dataSource = dataSource ?? throw new ArgumentNullException(nameof(dataSource));
        _database = database ?? throw new ArgumentNullException(nameof(database));
    }

    public async Task<IEnumerable<string>> GetAllQueuesAsync(string tenantId, CancellationToken ct = default)
    {
        await using DbConnection conn = await _dataSource.OpenConnectionAsync(ct);

        return await _database.ListQueuesAsync(conn, tenantId, ct);
    }

    public async Task CreateQueueAsync(string tenantId, CreateQueueCommand command, CancellationToken ct = default)
    {
        await using DbConnection conn = await _dataSource.OpenConnectionAsync(ct);

        await _database.CreateQueueAsync(conn, tenantId, command.QueueName, ct);
    }

    public async Task DropQueueAsync(string tenantId, string queueName, CancellationToken ct = default)
    {
        await using DbConnection conn = await _dataSource.OpenConnectionAsync(ct);

        await _database.DropQueueAsync(conn, tenantId, queueName, ct);
    }

    public async Task CancelTaskAsync(string tenantId, CancelTaskCommand command, CancellationToken ct = default)
    {
        await using DbConnection conn = await _dataSource.OpenConnectionAsync(ct);

        await _database.CancelTaskAsync(conn, tenantId, command.QueueName, command.TaskId, ct);
    }

    public async Task BulkCancelTasksAsync(string tenantId, BulkCancelTasksCommand command, CancellationToken ct = default)
    {
        await using DbConnection conn = await _dataSource.OpenConnectionAsync(ct);

        foreach (string taskId in command.TaskIds)
        {
            await _database.CancelTaskAsync(conn, tenantId, command.QueueName, taskId, ct);
        }
    }

    public async Task WakeRunAsync(string tenantId, ScheduleWakeupCommand command, CancellationToken ct = default)
    {
        await using DbConnection conn = await _dataSource.OpenConnectionAsync(ct);

        await _database.ScheduleRunAsync(conn, tenantId, command.QueueName, command.RunId, command.WakeAt, ct);
    }

    public async Task ForceCompleteRunAsync(string tenantId, CompleteRunCommand command, CancellationToken ct = default)
    {
        await using DbConnection conn = await _dataSource.OpenConnectionAsync(ct);

        await _database.CompleteRunAsync(conn, tenantId, command.QueueName, command.RunId, command.StateJson ?? "{}", ct);
    }

    public async Task ForceFailRunAsync(string tenantId, FailRunCommand command, CancellationToken ct = default)
    {
        await using DbConnection conn = await _dataSource.OpenConnectionAsync(ct);

        await _database.FailRunAsync(conn, tenantId, command.QueueName, command.RunId, command.ReasonJson, ct);
    }

    public async Task ExtendClaimAsync(string tenantId, ExtendClaimCommand command, CancellationToken ct = default)
    {
        await using DbConnection conn = await _dataSource.OpenConnectionAsync(ct);

        await _database.HeartbeatAsync(conn, tenantId, command.QueueName, command.RunId, command.ExtendBySeconds, ct);
    }

    public async Task EmitEventAsync(string tenantId, EmitEventCommand command, CancellationToken ct = default)
    {
        await using DbConnection conn = await _dataSource.OpenConnectionAsync(ct);
        await _database.EmitEventAsync(conn, tenantId, command.QueueName, command.EventName, command.PayloadJson ?? "null", ct);
    }

    public async Task SetCheckpointAsync(string tenantId, SetCheckpointCommand command, CancellationToken ct = default)
    {
        await using DbConnection conn = await _dataSource.OpenConnectionAsync(ct);
        await _database.PersistCheckpointAsync(conn, tenantId, command.QueueName, command.TaskId, command.OwnerRunId, command.StepName, command.StateJson, command.ExtendClaimBySeconds ?? 0, ct);
    }

    public async Task ReleaseWorkerClaimsAsync(string tenantId, ReleaseWorkerClaimsCommand command, CancellationToken ct = default)
    {
        await using var conn = await _dataSource.OpenConnectionAsync(ct);
        await _database.ReleaseWorkerClaimsAsync(conn, tenantId, command.QueueName, command.WorkerId, ct);
    }

    public async Task<int> CleanupTasksAsync(string tenantId, CleanupCommand command, CancellationToken ct = default)
    {
        await using var conn = await _dataSource.OpenConnectionAsync(ct);
        return await _database.CleanupTasksAsync(conn, tenantId, command.QueueName, command.TtlSeconds, command.Limit, ct);
    }

    public async Task<int> CleanupEventsAsync(string tenantId, CleanupCommand command, CancellationToken ct = default)
    {
        await using var conn = await _dataSource.OpenConnectionAsync(ct);
        return await _database.CleanupEventsAsync(conn, tenantId, command.QueueName, command.TtlSeconds, command.Limit, ct);
    }
}