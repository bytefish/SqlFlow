// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using Microsoft.Extensions.Logging;
using SqlFlowSdk.Core;
using SqlFlowSdk.Database;
using SqlFlowSdk.Exceptions;
using System.Collections.Concurrent;
using System.Data.Common;
using System.Text.Json;

namespace SqlFlowSdk;

/// <summary>
/// The SqlFlow client is the main entry point for interacting with the SqlFlow task queue system. It provides methods 
/// for registering tasks, spawning new tasks, emitting events, claiming and executing tasks, and managing queues. 
/// 
/// The client maintains a registry of task handlers and uses a SQL Server database to store task and event data. It 
/// also handles task execution logic, including retry strategies, cancellation policies, and error handling.
/// </summary>
public class SqlFlow : ISqlFlow, IDisposable, IAsyncDisposable
{
    private readonly ILogger _logger;
    private readonly DbDataSource _dataSource;
    private readonly ISqlFlowDatabase _db;
    private readonly ConcurrentDictionary<string, RegisteredTask> _registry = new(StringComparer.Ordinal);

    public SqlFlow(ILogger<SqlFlow> logger, DbDataSource dataSource, ISqlFlowDatabase db)
    {
        _logger = logger;
        _dataSource = dataSource;
        _db = db;
    }

    /// <summary>
    /// Registers a task handler with the SqlFlow client. This allows the client to execute tasks of the specified type when they are claimed.
    /// </summary>
public void RegisterTask(
    TaskRegistrationOptions options,
    TaskHandler handler)
{
    ArgumentNullException.ThrowIfNull(options);
    ArgumentNullException.ThrowIfNull(handler);
    ArgumentException.ThrowIfNullOrWhiteSpace(options.Name);

    RegisteredTask registration = new()
    {
        Name = options.Name,
        DefaultMaxAttempts = options.DefaultMaxAttempts,
        DefaultCancellation = options.DefaultCancellation,
        Handler = handler
    };

    if (!_registry.TryAdd(options.Name, registration))
    {
        throw new InvalidOperationException($"Task '{options.Name}' is already registered.");
    }
}

    /// <summary>
    /// Creates a new queue with the specified name. Queues are used to organize tasks and determine which workers can claim 
    /// and execute them. This method must be called before spawning tasks to a new queue or claiming tasks from it.
    /// </summary>
    public async Task CreateQueueAsync(string queueName, CancellationToken cancellationToken)
    {
        await using DbConnection conn = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);

        await _db.CreateQueueAsync(conn, queueName, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Drops the specified queue and all associated tasks and events. This is a destructive operation that cannot be 
    /// undone, so use with caution.
    /// </summary>
    public async Task DropQueueAsync(string queueName, CancellationToken cancellationToken)
    {
        await using DbConnection conn = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);

        await _db.DropQueueAsync(conn, queueName, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Returns a list of all existing queues in the SqlFlow system. This can be used to discover available queues 
    /// for spawning tasks or claiming work.
    /// </summary>
    public async Task<IEnumerable<string>> ListQueuesAsync(CancellationToken cancellationToken)
    {
        await using DbConnection conn = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);

        return await _db.ListQueuesAsync(conn, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Spawns a new task in the specified queue with the given parameters and options. The task will be picked up by workers 
    /// that have registered handlers for the specified task name. The options allow you to configure retry strategies, 
    /// cancellation policies, and other task execution parameters. 
    /// 
    /// The method returns a SpawnResult containing the task ID and run ID for tracking the task's progress.
    /// </summary>
    public async Task<SpawnResult> SpawnAsync<TRequest>(SpawnOptions options, string jobName, TRequest request, CancellationToken cancellationToken)
    {
        RegisteredTask? registration = null;
        _registry.TryGetValue(jobName, out registration);

        CancellationPolicy? cancellation = options.Cancellation ?? registration?.DefaultCancellation;
        Dictionary<string, object> normOptions = new Dictionary<string, object>();

        if (options.Headers != null) normOptions["headers"] = options.Headers;
        normOptions["max_attempts"] = options.MaxAttempts;
        if (options.RetryStrategy != null) normOptions["retry_strategy"] = options.RetryStrategy;
        if (cancellation != null) normOptions["cancellation"] = cancellation;

        await using DbConnection conn = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);

        return await _db.SpawnTaskAsync(
            conn,
            options.Queue,
            jobName,
            JsonSerializer.Serialize(request),
            JsonSerializer.Serialize(normOptions),
            cancellationToken
        ).ConfigureAwait(false);
    }

    /// <summary>
    /// Emits an event with the specified name and payload to the given queue. Events are a way to trigger actions in 
    /// response to certain conditions, such as task completions, failures, or custom application events. Workers 
    /// can listen for specific events and execute handlers when those events are emitted. 
    /// </summary>
    public async Task EmitEventAsync(EmitEventOptions options, string eventName, object? payload, CancellationToken cancellationToken)
    {
        if (string.IsNullOrEmpty(eventName))
        {
            throw new Exception("eventName required");
        }

        await using DbConnection conn = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);

        await _db.EmitEventAsync(conn, options.Queue, eventName, JsonSerializer.Serialize(payload), cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Cancels a task with the specified ID in the given queue. This will prevent the task from being executed if it has not 
    /// already been claimed by a worker. If the task is currently being executed, the cancellation policy will determine 
    /// how the worker should respond (e.g. whether to allow the task to finish, attempt to stop it, or mark it as cancelled).
    /// </summary>
    public async Task CancelTaskAsync(CancelTaskOptions options, string taskId, CancellationToken cancellationToken)
    {
        await using DbConnection conn = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);

        await _db.CancelTaskAsync(conn, options.Queue, taskId, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Claims a Task from the specified queue for execution. This method is typically called by worker processes that are 
    /// polling for work.
    /// </summary>
    public async Task<IEnumerable<ClaimedTask>> ClaimTasksAsync(string queue, string workerId, CancellationToken cancellationToken, int claimTimeout = 120, int batchSize = 1)
    {
        if (string.IsNullOrEmpty(queue))
        {
            throw new ArgumentException("Queue must be specified for claiming tasks");
        }

        await using DbConnection conn = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);

        return await _db.ClaimTasksAsync(conn, queue, workerId, claimTimeout, batchSize, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Processes a batch of claimed tasks from the specified queue. This method is typically called by worker processes that are 
    /// polling for work. It claims a batch of tasks and then executes each one using the registered handlers.
    /// </summary>
    public async Task WorkBatchAsync(string queue, string workerId, CancellationToken cancellationToken, int claimTimeout = 120, int batchSize = 1)
    {
        IEnumerable<ClaimedTask> tasks = await ClaimTasksAsync(queue, workerId, cancellationToken, claimTimeout, batchSize).ConfigureAwait(false);

        foreach (ClaimedTask task in tasks)
        {
            await ExecuteTaskAsync(task, queue, claimTimeout, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Executes a claimed task using the registered handler for its task type. This method handles the entire lifecycle of 
    /// a task execution, including invoking the handler, managing timeouts, handling exceptions, and marking the task 
    /// as completed or failed in the database.
    /// </summary>
    public async Task ExecuteTaskAsync(
     ClaimedTask task,
     string queue,
     int claimTimeout,
     CancellationToken stoppingToken,
     bool fatalOnLeaseTimeout = false)
    {
        ArgumentNullException.ThrowIfNull(task);
        ArgumentException.ThrowIfNullOrWhiteSpace(queue);

        if (claimTimeout <= 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(claimTimeout),
                "Claim timeout must be greater than zero.");
        }

        await using DbConnection connection =
            await _dataSource
                .OpenConnectionAsync(stoppingToken)
                .ConfigureAwait(false);

        using var warningCts = new CancellationTokenSource();
        using var executionCts =
            CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);

        Task warningTask = WarnAboutLeaseTimeoutAsync(
            task,
            claimTimeout,
            warningCts.Token);

        try
        {
            /*
             * Check the registration before loading checkpoints.
             *
             * If no handler is registered, throwing here causes the run to be
             * persisted as failed through the normal failure path below.
             */
            if (!_registry.TryGetValue(
                    task.TaskName,
                    out RegisteredTask? registration))
            {
                throw new UnknownTaskRegistrationException(
                    $"No handler is registered for task '{task.TaskName}'.");
            }

            TaskContext context = await TaskContext.CreateAsync(
                _logger,
                task.TaskId,
                connection,
                _db,
                queue,
                task,
                claimTimeout,
                executionCts.Token
            ).ConfigureAwait(false);

            Task<object> handlerTask = registration.Handler(
                context,
                task.Params,
                executionCts.Token);

            object result;

            if (fatalOnLeaseTimeout)
            {
                TimeSpan fatalTimeout =
                    TimeSpan.FromSeconds(checked(claimTimeout * 2L));

                try
                {
                    result = await handlerTask
                        .WaitAsync(fatalTimeout, stoppingToken)
                        .ConfigureAwait(false);
                }
                catch (TimeoutException timeoutException)
                {
                    /*
                     * Request cancellation of the handler. A handler still needs
                     * to observe its CancellationToken for cooperative shutdown.
                     */
                    await executionCts.CancelAsync().ConfigureAwait(false);

                    throw new FatalLeaseTimeoutException(
                        $"Task {task.TaskName} ({task.TaskId}) exceeded " +
                        $"the fatal lease timeout of {fatalTimeout.TotalSeconds:0} seconds.",
                        timeoutException);
                }
            }
            else
            {
                /*
                 * Without FatalOnLeaseTimeout, exceeding the lease only causes
                 * a warning. The handler continues until it finishes, fails,
                 * suspends, or the host shuts down.
                 */
                result = await handlerTask.ConfigureAwait(false);
            }

            string resultJson = JsonSerializer.Serialize(result);

            await _db.CompleteRunAsync(
                connection,
                queue,
                task.RunId,
                resultJson,
                stoppingToken
            ).ConfigureAwait(false);
        }
        catch (SuspendTaskException)
        {
            /*
             * SleepUntil or AwaitEvent already changed the run to sleeping.
             * Nothing else needs to be persisted here.
             */
        }
        catch (CancelledTaskException)
        {
            /*
             * The database reported that the task was cancelled.
             * Do not convert cancellation into a failure.
             */
        }
        catch (OperationCanceledException)
            when (stoppingToken.IsCancellationRequested)
        {
            /*
             * Normal application shutdown.
             *
             * Do not mark the task as failed. The worker shutdown path should
             * release its claims, or expired-claim recovery should make the run
             * available again.
             */
            throw;
        }
        catch (Exception exception)
        {
            _logger.LogError(
                exception,
                "Task {TaskName} ({TaskId}), run {RunId}, failed",
                task.TaskName,
                task.TaskId,
                task.RunId);

            var failure = new
            {
                name = exception.GetType().Name,
                message = exception.Message,
                stack = exception.StackTrace
            };

            try
            {
                /*
                 * Do not use executionCts here because it may have been cancelled
                 * by the fatal-timeout handling.
                 *
                 * Give failure persistence its own bounded timeout.
                 */
                using var persistenceCts =
                    new CancellationTokenSource(TimeSpan.FromSeconds(30));

                await _db.FailRunAsync(
                    connection,
                    queue,
                    task.RunId,
                    JsonSerializer.Serialize(failure),
                    persistenceCts.Token
                ).ConfigureAwait(false);
            }
            catch (Exception failException)
            {
                _logger.LogError(
                    failException,
                    "Could not mark run {RunId} as failed",
                    task.RunId);
            }

            if (exception is FatalLeaseTimeoutException)
            {
                throw;
            }
        }
        finally
        {
            await warningCts.CancelAsync().ConfigureAwait(false);

            try
            {
                await warningTask.ConfigureAwait(false);
            }
            catch (OperationCanceledException)
                when (warningCts.IsCancellationRequested)
            {
                // Expected when execution finishes before the warning timeout.
            }
        }

        async Task WarnAboutLeaseTimeoutAsync(
            ClaimedTask claimedTask,
            int timeoutSeconds,
            CancellationToken cancellationToken)
        {
            try
            {
                await Task.Delay(
                    TimeSpan.FromSeconds(timeoutSeconds),
                    cancellationToken
                ).ConfigureAwait(false);

                _logger.LogWarning(
                    "Task {TaskName} ({TaskId}), run {RunId}, " +
                    "exceeded its claim timeout of {ClaimTimeout} seconds",
                    claimedTask.TaskName,
                    claimedTask.TaskId,
                    claimedTask.RunId,
                    timeoutSeconds);
            }
            catch (OperationCanceledException)
                when (cancellationToken.IsCancellationRequested)
            {
                // The task finished before reaching the warning threshold.
            }
        }
    }

    private async Task TryFailRunAsync(
    DbConnection connection,
    string queue,
    string runId,
    Exception exception,
    CancellationToken cancellationToken)
    {
        try
        {
            string failure = JsonSerializer.Serialize(new
            {
                name = exception.GetType().Name,
                message = exception.Message,
                stack = exception.StackTrace
            });

            await _db.FailRunAsync(
                connection,
                queue,
                runId,
                failure,
                cancellationToken).ConfigureAwait(false);
        }
        catch (CancelledTaskException)
        {
            // Cancellation won the race with failure.
        }
        catch (Exception failException)
        {
            _logger.LogError(
                failException,
                "Failed to mark run {RunId} as failed.",
                runId);
        }
    }

    public async ValueTask DisposeAsync()
    {
        await Task.CompletedTask;
    }

    public void Dispose()
    {
    }
}