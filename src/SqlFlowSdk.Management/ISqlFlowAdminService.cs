using SqlFlowSdk.Database;
using SqlFlowSdk.Management.Models;
using System.Data.Common;

namespace SqlFlowSdk.Management;

public interface ISqlFlowAdminService
{
        #region Queue Management
        
        Task<IEnumerable<string>> GetAllQueuesAsync(CancellationToken ct = default);

        Task CreateQueueAsync(CreateQueueCommand command, CancellationToken ct = default);

        Task DropQueueAsync(string queueName, CancellationToken ct = default);

        #endregion

        # region Task & Run Interventions

        Task CancelTaskAsync(CancelTaskCommand command, CancellationToken ct = default);

        Task BulkCancelTasksAsync(BulkCancelTasksCommand command, CancellationToken ct = default);

        Task WakeRunAsync(ScheduleWakeupCommand command, CancellationToken ct = default);

        Task ForceCompleteRunAsync(CompleteRunCommand command, CancellationToken ct = default);

        Task ForceFailRunAsync(FailRunCommand command, CancellationToken ct = default);

        #endregion

        #region Worker Management

        Task ExtendClaimAsync(ExtendClaimCommand command, CancellationToken ct = default);

        Task ReleaseWorkerClaimsAsync(ReleaseWorkerClaimsCommand command, CancellationToken ct = default);

        #endregion

        #region Event & Checkpoint Unblocking

        Task EmitEventAsync(EmitEventCommand command, CancellationToken ct = default);

        Task SetCheckpointAsync(SetCheckpointCommand command, CancellationToken ct = default);

        #endregion

        #region Maintenance

        Task<int> CleanupTasksAsync(CleanupCommand command, CancellationToken ct = default);

        Task<int> CleanupEventsAsync(CleanupCommand command, CancellationToken ct = default);

        #endregion
}

