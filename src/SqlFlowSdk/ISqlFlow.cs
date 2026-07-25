// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using SqlFlowSdk.Core;

namespace SqlFlowSdk;

/// <summary>
/// The main interface for interacting with the SqlFlow task management system. This interface defines the core operations for registering task handlers, 
/// and managing message queues, spawning tasks, emitting events, claiming tasks for execution, and processing tasks. It serves as the primary entry 
/// point for developers using the SqlFlow SDK.
/// </summary>
public interface ISqlFlow
{
    /// <summary>
    /// Registers a task handler with the SqlFlow system. The handler will be invoked when a task with 
    /// the corresponding name is spawned.
    /// </summary>
    /// <param name="options">The options for registering the task handler.</param>
    /// <param name="handler">The task handler to register.</param>
    void RegisterTask(TaskRegistrationOptions options, TaskHandler handler);

    /// <summary>
    /// Creates a new message queue with the specified name if it does not already exist.
    /// </summary>
    /// <param name="queueName">The name of the queue to create.</param>
    /// <param name="cancellationToken">A cancellation token to cancel the operation.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    Task CreateQueueAsync(string queueName, CancellationToken cancellationToken);

    /// <summary>
    /// Deletes the message queue with the specified name and all tasks associated with it. This is a destructive operation that cannot be undone, 
    /// so use with caution. It will remove all tasks in the queue, including pending, claimed, and failed tasks. It will also remove the queue 
    /// itself, so it will no longer be available for spawning or claiming tasks.
    /// </summary>
    /// <param name="queueName">The name of the queue to delete.</param>
    /// <param name="cancellationToken">A cancellation token to cancel the operation.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    Task DropQueueAsync(string queueName, CancellationToken cancellationToken);

    /// <summary>
    /// Returns a list of all existing message queues in the SqlFlow system. This can be used to discover available queues for spawning and claiming tasks.
    /// </summary>
    /// <param name="cancellationToken">A cancellation token to cancel the operation.</param>
    /// <returns>The list of all existing message queues.</returns>
    Task<IEnumerable<string>> ListQueuesAsync(CancellationToken cancellationToken);

    /// <summary>
    /// Spawns a new task with the specified name and parameters, and enqueues it onto the specified message queue. The task will be picked up by 
    /// a registered handler for execution.
    /// </summary>
    /// <param name="options">The options for spawning the task.</param>
    /// <param name="jobName">The name of the task to spawn.</param>
    /// <param name="request">The request parameters for the task.</param>
    /// <param name="cancellationToken">A cancellation token to cancel the operation.</param>
    /// <returns>The SpawnResult, which contains information about the spawned task.</returns>
    Task<SpawnResult> SpawnAsync<TRequest>(SpawnOptions options, string jobName, TRequest request, CancellationToken cancellationToken);

    /// <summary>
    /// Emits a custom event with the specified name and payload to the specified message queue. This can be used for inter-task communication, 
    /// triggering workflows, or any other use case where you want to send a message to a queue without spawning a task. The event will be 
    /// delivered to any handlers that are listening for it on the queue.
    /// </summary>
    /// <param name="options">The options for emitting the event.</param>
    /// <param name="eventName">The name of the event to emit.</param>
    /// <param name="payload">The payload for the event.</param>
    /// <param name="cancellationToken">A cancellation token to cancel the operation.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    Task EmitEventAsync(EmitEventOptions options, string eventName, object? payload, CancellationToken cancellationToken);

    /// <summary>
    /// Cancels a pending or claimed task with the specified ID. The task will be removed from the queue and will not be executed if it has not 
    /// already started.
    /// </summary>
    /// <param name="options">The options for canceling the task.</param>
    /// <param name="taskId">The ID of the task to cancel.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    Task CancelTaskAsync(CancelTaskOptions options, string taskId, CancellationToken cancellationToken);

    /// <summary>
    /// Claims one or more tasks from the specified message queue for execution by the worker with the given ID. The claimed tasks 
    /// will be locked for the specified claim timeout duration, during which they will not be available for other workers to claim.
    /// </summary>
    /// <param name="queue">The message queue from which to claim tasks.</param>
    /// <param name="workerId">The ID of the worker claiming the tasks.</param>
    /// <param name="cancellationToken"> A cancellation token to cancel the operation.</param>
    /// <param name="claimTimeout">The duration for which the tasks will be locked.</param>
    /// <param name="batchSize">The number of tasks to claim.</param>
    /// <returns>A list of claimed tasks by the worker.</returns>
    Task<IEnumerable<ClaimedTask>> ClaimTasksAsync(string queue, string workerId, CancellationToken cancellationToken, int claimTimeout = 120, int batchSize = 1);

    /// <summary>
    /// Processes a batch of tasks from the specified message queue. This method will claim a batch of tasks and execute them 
    /// sequentially using the registered handlers.
    /// </summary>
    /// <param name="queue">The message queue from which to claim tasks.</param>
    /// <param name="workerId">The ID of the worker processing the tasks.</param>
    /// <param name="claimTimeout">The duration for which the tasks will be locked.</param>
    /// <param name="batchSize">The number of tasks to process in the batch.</param>
    /// <param name="cancellationToken"> A cancellation token to cancel the operation.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    Task WorkBatchAsync(string queue, string workerId, CancellationToken cancellationToken, int claimTimeout = 120, int batchSize = 1);

    /// <summary>
    /// Executes a single claimed task using the registered handler for its task name. This method will handle the execution of the task and 
    /// return the result. It will also handle any exceptions that occur during execution and update the task status accordingly. 
    /// </summary>
    /// <param name="task">The claimed task to execute.</param>
    /// <param name="queue">The message queue to which the task belongs.</param>
    /// <param name="claimTimeout">The duration for which the task is locked.</param>
    /// <param name="cancellationToken">A cancellation token to cancel the operation.</param>
    /// <param name="fatalOnLeaseTimeout">A value indicating whether to treat lease timeout as a fatal error.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    Task ExecuteTaskAsync(ClaimedTask task, string queue, int claimTimeout, CancellationToken cancellationToken, bool fatalOnLeaseTimeout = false);
}

/// <summary>
/// The client interface for interacting with the SqlFlow task management system. This interface defines the core operations for 
/// spawning tasks, emitting events, and canceling tasks.
/// </summary>
public interface ISqlFlowClient
{
    /// <summary>
    /// Spawns a new task with the specified name and parameters, and enqueues it onto the specified message queue. The task will be picked up by 
    /// a registered handler for execution.
    /// </summary>
    /// <param name="options">The options for spawning the task.</param>
    /// <param name="taskName">The name of the task to spawn.</param>
    /// <param name="parameters">The parameters for the task.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    Task<SpawnResult> SpawnAsync(SpawnOptions options, string taskName, object parameters);

    /// <summary>
    /// Emits a custom event with the specified name and payload to the specified message queue. This can be used for inter-task communication, 
    /// triggering workflows, or any other use case where you want to send a message to a
    /// </summary>
    /// <param name="options">The options for emitting the event.</param>
    /// <param name="eventName">The name of the event to emit.</param>
    /// <param name="payload">The payload for the event.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    Task EmitEventAsync(EmitEventOptions options, string eventName, object? payload = null);

    Task CancelTaskAsync(CancelTaskOptions options, string taskId);
}

/// <summary>
/// The management client interface for interacting with the SqlFlow task management system. This interface defines the core operations for 
/// creating, dropping, and listing message queues. It is intended for use by administrators or management tools that need to manage the 
/// queues in the system.
/// </summary>
public interface ISqlFlowManagementClient
{
    /// <summary>
    /// Creates a new message queue with the specified name if it does not already exist. This operation is typically used 
    /// by administrators or management tools to set up new queues for task processing.
    /// </summary>
    /// <param name="queueName">The name of the queue to create.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    Task CreateQueueAsync(string queueName);

    /// <summary>
    /// Drops the message queue with the specified name and all tasks associated with it. This is a destructive operation 
    /// that cannot be undone, so use with caution.
    /// </summary>
    /// <param name="queueName">The name of the queue to drop.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    Task DropQueueAsync(string queueName);

    /// <summary>
    /// Lists all existing message queues in the SqlFlow system. This can be used to discover available queues for 
    /// spawning and claiming tasks.
    /// </summary>
    /// <returns>The list of all existing message queues.</returns>
    Task<IEnumerable<string>> ListQueuesAsync();
}

/// <summary>
/// The worker client interface for interacting with the SqlFlow task management system. This interface defines the core operations for 
/// registering task handlers, claiming tasks for execution, and processing tasks. It is intended for use by worker processes that are 
/// responsible for executing tasks in the system.
/// </summary>
public interface ISqlFlowWorkerClient
{
    /// <summary>
    /// Registers a task handler with the SqlFlow system. The handler will be invoked when a task with the corresponding name is spawned.
    /// </summary>
    /// <param name="options">The options for registering the task handler.</param>
    /// <param name="handler">The task handler to register.</param>
    void RegisterTask(TaskRegistrationOptions options, TaskHandler handler);

    /// <summary>
    /// Claims one or more tasks from the specified message queue for execution by the worker with the given ID. The claimed 
    /// tasks will be locked for the specified claim timeout duration, during which they will not be available for other workers 
    /// to claim.
    /// </summary>
    /// <param name="queue">The message queue from which to claim tasks.</param>
    /// <param name="workerId">The ID of the worker claiming the tasks.</param>
    /// <param name="claimTimeout">The duration for which the tasks will be locked.</param>
    /// <param name="batchSize">The number of tasks to claim.</param>
    /// <returns>The list of claimed tasks.</returns>
    Task<IEnumerable<ClaimedTask>> ClaimTasksAsync(string queue, string workerId, int claimTimeout = 120, int batchSize = 1);

    /// <summary>
    /// Processes a batch of claimed tasks.
    /// </summary>
    /// <param name="queue">The message queue from which to claim tasks.</param>
    /// <param name="workerId">The ID of the worker processing the tasks.</param>
    /// <param name="claimTimeout">The duration for which the tasks will be locked.</param>
    /// <param name="batchSize">The number of tasks to process.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    Task WorkBatchAsync(string queue, string workerId, int claimTimeout = 120, int batchSize = 1);

    /// <summary>
    /// Executes a single claimed task using the registered handler for its task name. This method will handle the execution of the task and 
    /// return the result. It will also handle any exceptions that occur during execution and update the
    /// </summary>
    /// <param name="task">The claimed task to execute.</param>
    /// <param name="queue">The message queue from which the task was claimed.</param>
    /// <param name="claimTimeout">The duration for which the task was locked.</param>
    /// <param name="fatalOnLeaseTimeout">A value indicating whether to treat lease timeout as fatal.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    Task ExecuteTaskAsync(ClaimedTask task, string queue, int claimTimeout, bool fatalOnLeaseTimeout = false);
}
