using SqlFlowSdk.Database;
using SqlFlowSdk.Management.Models;
using System.Data.Common;

namespace SqlFlowSdk.Management.SqlServer.Services;

public class SqlServerSqlFlowAdminService : ISqlFlowAdminService
{
    private readonly DbDataSource _dataSource;
    private readonly ISqlFlowDatabase _database;

    public SqlServerSqlFlowAdminService(DbDataSource dataSource, ISqlFlowDatabase database)
    {
        _dataSource = dataSource ?? throw new ArgumentNullException(nameof(dataSource));
        _database = database ?? throw new ArgumentNullException(nameof(database));
    }

    public async Task<IEnumerable<string>> GetAllQueuesAsync(CancellationToken ct = default)
    {
        await using DbConnection conn = await _dataSource.OpenConnectionAsync(ct);

        return await _database.ListQueuesAsync(conn, ct);
    }

    public async Task CreateQueueAsync(CreateQueueCommand command, CancellationToken ct = default)
    {
        await using DbConnection conn = await _dataSource.OpenConnectionAsync(ct);

        await _database.CreateQueueAsync(conn, command.QueueName, ct);
    }

    public async Task DropQueueAsync(string queueName, CancellationToken ct = default)
    {
        await using DbConnection conn = await _dataSource.OpenConnectionAsync(ct);

        await _database.DropQueueAsync(conn, queueName, ct);
    }

    public async Task CancelTaskAsync(CancelTaskCommand command, CancellationToken ct = default)
    {
        await using DbConnection conn = await _dataSource.OpenConnectionAsync(ct);

        await _database.CancelTaskAsync(conn, command.QueueName, command.TaskId, ct);
    }

    public async Task BulkCancelTasksAsync(BulkCancelTasksCommand command, CancellationToken ct = default)
    {
        await using DbConnection conn = await _dataSource.OpenConnectionAsync(ct);

        foreach (string taskId in command.TaskIds)
        {
            await _database.CancelTaskAsync(conn, command.QueueName, taskId, ct);
        }
    }

    public async Task WakeRunAsync(ScheduleWakeupCommand command, CancellationToken ct = default)
    {
        await using DbConnection conn = await _dataSource.OpenConnectionAsync(ct);

        await _database.ScheduleRunAsync(conn, command.QueueName, command.RunId, command.WakeAt, ct);
    }

    public async Task ForceCompleteRunAsync(CompleteRunCommand command, CancellationToken ct = default)
    {
        await using DbConnection conn = await _dataSource.OpenConnectionAsync(ct);

        await _database.CompleteRunAsync(conn, command.QueueName, command.RunId, command.StateJson ?? "{}", ct);
    }

    public async Task ForceFailRunAsync(FailRunCommand command, CancellationToken ct = default)
    {
        await using DbConnection conn = await _dataSource.OpenConnectionAsync(ct);

        await _database.FailRunAsync(conn, command.QueueName, command.RunId, command.ReasonJson, ct);
    }

    public async Task ExtendClaimAsync(ExtendClaimCommand command, CancellationToken ct = default)
    {
        await using DbConnection conn = await _dataSource.OpenConnectionAsync(ct);

        await _database.HeartbeatAsync(conn, command.QueueName, command.RunId, command.ExtendBySeconds, ct);
    }

    public async Task EmitEventAsync(EmitEventCommand command, CancellationToken ct = default)
    {
        await using DbConnection conn = await _dataSource.OpenConnectionAsync(ct);
        await _database.EmitEventAsync(conn, command.QueueName, command.EventName, command.PayloadJson ?? "null", ct);
    }

    public async Task SetCheckpointAsync(SetCheckpointCommand command, CancellationToken ct = default)
    {
        await using DbConnection conn = await _dataSource.OpenConnectionAsync(ct);
        await _database.PersistCheckpointAsync(conn, command.QueueName, command.TaskId, command.OwnerRunId, command.StepName, command.StateJson, command.ExtendClaimBySeconds ?? 0, ct);
    }

    public async Task ReleaseWorkerClaimsAsync(ReleaseWorkerClaimsCommand command, CancellationToken ct = default)
    {
        await using var conn = await _dataSource.OpenConnectionAsync(ct);
        await _database.ReleaseWorkerClaimsAsync(conn, command.QueueName, command.WorkerId, ct);
    }

    public async Task<int> CleanupTasksAsync(CleanupCommand command, CancellationToken ct = default)
    {
        await using var conn = await _dataSource.OpenConnectionAsync(ct);
        return await _database.CleanupTasksAsync(conn, command.QueueName, command.TtlSeconds, command.Limit, ct);
    }

    public async Task<int> CleanupEventsAsync(CleanupCommand command, CancellationToken ct = default)
    {
        await using var conn = await _dataSource.OpenConnectionAsync(ct);
        return await _database.CleanupEventsAsync(conn, command.QueueName, command.TtlSeconds, command.Limit, ct);
    }
}
