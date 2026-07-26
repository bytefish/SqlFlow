// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using SqlFlowSdk.Core;
using System.Data.Common;
using System.Reflection.Metadata;
using System.Text.Json.Nodes;

namespace SqlFlowSdk.Database;

/// <summary>
/// Defines all required database interactions to manage queues, tasks, checkpoints, and events in the SqlFlow system.
/// Implementations of this interface handle provider-specific (e.g., SQL Server, PostgreSQL) raw SQL operations.
/// </summary>
public interface ISqlFlowDatabase
{
    /// <summary>
    /// Creates a new queue in the database if it does not already exist.
    /// </summary>
    /// <param name="conn">The database connection to use.</param>
    /// <param name="queueName">The name of the queue to create.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    Task CreateQueueAsync(DbConnection conn, string queueName, CancellationToken cancellationToken);

    /// <summary>
    /// Drops an existing queue and all its associated data (tasks, runs, checkpoints, events) from the database.
    /// </summary>
    /// <param name="conn">The database connection to use.</param>
    /// <param name="queueName">The name of the queue to drop.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    Task DropQueueAsync(DbConnection conn, string queueName, CancellationToken cancellationToken);

    /// <summary>
    /// Retrieves a list of all queue names currently existing in the database.
    /// </summary>
    /// <param name="conn">The database connection to use.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A collection of queue names.</returns>
    Task<IEnumerable<string>> ListQueuesAsync(DbConnection conn, CancellationToken cancellationToken);

    /// <summary>
    /// Spawns a new task in the specified queue with the given parameters and options.
    /// </summary>
    /// <param name="conn">The database connection to use.</param>
    /// <param name="queue">The name of the target queue.</param>
    /// <param name="taskName">The type/name of the task to spawn.</param>
    /// <param name="paramsJson">The task payload parameters serialized as JSON.</param>
    /// <param name="optionsJson">Task execution options (e.g., headers, retry strategies) serialized as JSON.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A <see cref="SpawnResult"/> containing the generated TaskId, RunId, and Attempt number.</returns>
    Task<SpawnResult> SpawnTaskAsync(DbConnection conn, string queue, string taskName, string paramsJson, string optionsJson, CancellationToken cancellationToken);

    /// <summary>
    /// Cancels a pending, running, or sleeping task.
    /// </summary>
    /// <param name="conn">The database connection to use.</param>
    /// <param name="queue">The name of the queue where the task resides.</param>
    /// <param name="taskId">The unique identifier of the task to cancel.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    Task CancelTaskAsync(DbConnection conn, string queue, string taskId, CancellationToken cancellationToken);

    /// <summary>
    /// Emits an event to the queue that can wake up suspended tasks waiting for this specific event.
    /// </summary>
    /// <param name="conn">The database connection to use.</param>
    /// <param name="queue">The name of the queue.</param>
    /// <param name="eventName">The unique name of the event.</param>
    /// <param name="payloadJson">The event payload data serialized as JSON.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    Task EmitEventAsync(DbConnection conn, string queue, string eventName, string payloadJson, CancellationToken cancellationToken);

    /// <summary>
    /// Claims a batch of pending or scheduled tasks for processing by a worker.
    /// </summary>
    /// <param name="conn">The database connection to use.</param>
    /// <param name="queue">The name of the queue to poll.</param>
    /// <param name="workerId">The identifier of the worker claiming the tasks.</param>
    /// <param name="timeout">The lease duration in seconds for which the tasks are claimed.</param>
    /// <param name="count">The maximum number of tasks to claim.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A collection of <see cref="ClaimedTask"/> representing the tasks successfully claimed by the worker.</returns>
    Task<IEnumerable<ClaimedTask>> ClaimTasksAsync(DbConnection conn, string queue, string workerId, int timeout, int count, CancellationToken cancellationToken);

    /// <summary>
    /// Marks a task run as successfully completed and persists its final result.
    /// </summary>
    /// <param name="conn">The database connection to use.</param>
    /// <param name="queue">The name of the queue.</param>
    /// <param name="runId">The unique identifier of the task run.</param>
    /// <param name="resultJson">The final outcome/result of the task serialized as JSON.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    Task CompleteRunAsync(DbConnection conn, string queue, string runId, string resultJson, CancellationToken cancellationToken);

    /// <summary>
    /// Marks a task run as failed and records the failure reason. Depending on the retry strategy, a new run may be scheduled.
    /// </summary>
    /// <param name="conn">The database connection to use.</param>
    /// <param name="queue">The name of the queue.</param>
    /// <param name="runId">The unique identifier of the failed task run.</param>
    /// <param name="errorJson">The error details serialized as JSON.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    Task FailRunAsync(DbConnection conn, string queue, string runId, string errorJson, CancellationToken cancellationToken);

    /// <summary>
    /// Retrieves all committed checkpoint states up to the current run attempt for a specific task.
    /// </summary>
    /// <param name="conn">The database connection to use.</param>
    /// <param name="queue">The name of the queue.</param>
    /// <param name="taskId">The task identifier.</param>
    /// <param name="runId">The current run identifier requesting the checkpoints.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A collection of <see cref="CheckpointRow"/> representing the saved steps.</returns>
    Task<IEnumerable<CheckpointRow>> GetCheckpointStatesAsync(DbConnection conn, string queue, string taskId, string runId, CancellationToken cancellationToken);

    /// <summary>
    /// Retrieves the state of a single specific checkpoint for a task.
    /// </summary>
    /// <param name="conn">The database connection to use.</param>
    /// <param name="queue">The name of the queue.</param>
    /// <param name="taskId">The task identifier.</param>
    /// <param name="checkpointName">The name of the checkpoint step to retrieve.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The checkpoint state as a <see cref="JsonNode"/>, or null if it doesn't exist.</returns>
    Task<JsonNode?> GetSingleCheckpointAsync(DbConnection conn, string queue, string taskId, string checkpointName, CancellationToken cancellationToken);

    /// <summary>
    /// Persists a new checkpoint state for a specific step in a running task and optionally extends the worker's claim lease.
    /// </summary>
    /// <param name="conn">The database connection to use.</param>
    /// <param name="queue">The name of the queue.</param>
    /// <param name="taskId">The task identifier.</param>
    /// <param name="runId">The run identifier persisting the checkpoint.</param>
    /// <param name="checkpointName">The name of the step or checkpoint.</param>
    /// <param name="stateJson">The state data of the step serialized as JSON.</param>
    /// <param name="timeout">The time in seconds to extend the task claim lease by.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    Task PersistCheckpointAsync(DbConnection conn, string queue, string taskId, string runId, string checkpointName, string stateJson, int timeout, CancellationToken cancellationToken);

    /// <summary>
    /// Schedules a currently running task to be suspended and waken up at a specific future time.
    /// </summary>
    /// <param name="conn">The database connection to use.</param>
    /// <param name="queue">The name of the queue.</param>
    /// <param name="runId">The run identifier to suspend.</param>
    /// <param name="wakeAt">The UTC timestamp when the task should become available again.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    Task ScheduleRunAsync(DbConnection conn, string queue, string runId, DateTime wakeAt, CancellationToken cancellationToken);

    /// <summary>
    /// Extends the lease timeout (claim expiration) for a currently running task to prevent it from being picked up by another worker.
    /// </summary>
    /// <param name="conn">The database connection to use.</param>
    /// <param name="queue">The name of the queue.</param>
    /// <param name="runId">The run identifier to extend.</param>
    /// <param name="seconds">The number of seconds to extend the claim by.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    Task HeartbeatAsync(DbConnection conn, string queue, string runId, int seconds, CancellationToken cancellationToken);

    /// <summary>
    /// Registers a task to wait for a specific event. If the event hasn't occurred yet, the task will be suspended.
    /// If the event has already been emitted or the task is resuming from the event, it returns the payload.
    /// </summary>
    /// <param name="conn">The database connection to use.</param>
    /// <param name="queue">The name of the queue.</param>
    /// <param name="taskId">The task identifier.</param>
    /// <param name="runId">The current run identifier.</param>
    /// <param name="checkpointName">The name of the step waiting for the event.</param>
    /// <param name="eventName">The name of the event to wait for.</param>
    /// <param name="timeout">An optional timeout in seconds after which the wait should expire.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A tuple indicating whether the task should suspend its execution, and the event payload if it's already available.</returns>
    Task<(bool ShouldSuspend, JsonNode? Payload)> AwaitEventAsync(DbConnection conn, string queue, string taskId, string runId, string checkpointName, string eventName, int? timeout, CancellationToken cancellationToken);

    /// <summary>
    /// Releases all active claims held by a specific worker, putting the runs and tasks back into a pending state.
    /// </summary>
    /// <param name="conn">The database connection to use.</param>
    /// <param name="queue">The name of the queue.</param>
    /// <param name="workerId">The identifier of the worker whose claims to release.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    Task ReleaseWorkerClaimsAsync(DbConnection conn, string queue, string workerId, CancellationToken cancellationToken);

    /// <summary>
    /// Deletes old completed, failed, or cancelled tasks and their associated runs/events/checkpoints based on a TTL.
    /// </summary>
    /// <param name="conn">The database connection to use.</param>
    /// <param name="queue">The name of the queue.</param>
    /// <param name="ttlSeconds">The time-to-live in seconds.</param>
    /// <param name="limit">The maximum number of records to delete.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The number of records deleted.</returns>
    Task<int> CleanupTasksAsync(DbConnection conn, string queue, int ttlSeconds, int limit, CancellationToken cancellationToken);

    /// <summary>
    /// Deletes old unreferenced events based on a TTL.
    /// </summary>
    /// <param name="conn">The database connection to use.</param>
    /// <param name="queue">The name of the queue.</param>
    /// <param name="ttlSeconds">The time-to-live in seconds.</param>
    /// <param name="limit">The maximum number of records to delete.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The number of records deleted.</returns>
    Task<int> CleanupEventsAsync(DbConnection conn, string queue, int ttlSeconds, int limit, CancellationToken cancellationToken);
}