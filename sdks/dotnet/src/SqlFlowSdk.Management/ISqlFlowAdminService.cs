using SqlFlowSdk.Management.Models;

namespace SqlFlowSdk.Management;

public interface ISqlFlowAdminService
{
    #region Queue Management

    Task<IEnumerable<string>> GetAllQueuesAsync(string tenantId, CancellationToken ct = default);

    Task CreateQueueAsync(string tenantId, CreateQueueCommand command, CancellationToken ct = default);

    Task DropQueueAsync(string tenantId, string queueName, CancellationToken ct = default);

    #endregion

    #region Task & Run Interventions

    Task CancelTaskAsync(string tenantId, CancelTaskCommand command, CancellationToken ct = default);

    Task BulkCancelTasksAsync(string tenantId, BulkCancelTasksCommand command, CancellationToken ct = default);

    Task WakeRunAsync(string tenantId, ScheduleWakeupCommand command, CancellationToken ct = default);

    Task ForceCompleteRunAsync(string tenantId, CompleteRunCommand command, CancellationToken ct = default);

    Task ForceFailRunAsync(string tenantId, FailRunCommand command, CancellationToken ct = default);

    #endregion

    #region Worker Management

    Task ExtendClaimAsync(string tenantId, ExtendClaimCommand command, CancellationToken ct = default);

    Task ReleaseWorkerClaimsAsync(string tenantId, ReleaseWorkerClaimsCommand command, CancellationToken ct = default);

    #endregion

    #region Event & Checkpoint Unblocking

    Task EmitEventAsync(string tenantId, EmitEventCommand command, CancellationToken ct = default);

    Task SetCheckpointAsync(string tenantId, SetCheckpointCommand command, CancellationToken ct = default);

    #endregion

    #region Maintenance

    Task<int> CleanupTasksAsync(string tenantId, CleanupCommand command, CancellationToken ct = default);

    Task<int> CleanupEventsAsync(string tenantId, CleanupCommand command, CancellationToken ct = default);

    #endregion
}