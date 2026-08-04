// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using Microsoft.Extensions.Logging;
using SqlFlowSdk.Core;
using SqlFlowSdk.Database;
using SqlFlowSdk.Exceptions;
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
    private readonly Dictionary<string, RegisteredTask> _registry;

    public SqlFlow(ILogger<SqlFlow> logger, DbDataSource dataSource, ISqlFlowDatabase db)
    {
        _logger = logger;
        _dataSource = dataSource;
        _db = db;
        _registry = new Dictionary<string, RegisteredTask>();
    }

    /// <summary>
    /// Registers a task handler with the SqlFlow client. This allows the client to execute tasks of the specified type when they are claimed.
    /// </summary>
    public void RegisterTask(TaskRegistrationOptions options, TaskHandler handler)
    {
        if (string.IsNullOrEmpty(options.Name))
        {
            throw new ArgumentException("Task registration requires a name");
        }

        _registry[options.Name] = new RegisteredTask
        {
            Name = options.Name,
            DefaultMaxAttempts = options.DefaultMaxAttempts,
            DefaultCancellation = options.DefaultCancellation,
            Handler = handler
        };
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
    public async Task ExecuteTaskAsync(ClaimedTask task, string queue, int claimTimeout, CancellationToken stoppingToken, bool fatalOnLeaseTimeout = false)
    {
        using CancellationTokenSource timeoutCts = new CancellationTokenSource();
        using CancellationTokenSource linkedCts = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken, timeoutCts.Token);

        _ = Task
            .Delay(claimTimeout * 1000, timeoutCts.Token)
            .ContinueWith(t =>
            {
                if (!t.IsCanceled)
                {
                    _logger.LogWarning($"Task {task.TaskName} ({task.TaskId}) exceeded claim timeout of {claimTimeout}s");
                }
            }, TaskScheduler.Default);

        await using DbConnection conn = await _dataSource.OpenConnectionAsync(stoppingToken).ConfigureAwait(false);

        try
        {
            RegisteredTask? registration = _registry.ContainsKey(task.TaskName) ? _registry[task.TaskName] : null;

            TaskContext ctx = await TaskContext.CreateAsync(_logger, task.TaskId, conn, _db, queue, task, claimTimeout, linkedCts.Token).ConfigureAwait(false);

            if (registration == null)
            {
                throw new Exception($"Unknown task: {task.TaskName}");
            }

            Task<object> handlerTask = registration.Handler(ctx, task.Params, linkedCts.Token);

            Task fatalTask = Task.Delay(Timeout.Infinite, timeoutCts.Token);

            if (fatalOnLeaseTimeout)
            {
                fatalTask = Task
                    .Delay(claimTimeout * 1000 * 2, timeoutCts.Token)
                    .ContinueWith(t =>
                    {
                        if (!t.IsCanceled)
                        {
                            throw new FatalLeaseTimeoutException($"Task {task.TaskName} ({task.TaskId}) exceeded claim timeout by 2x.");
                        }
                    }, TaskScheduler.Default);
            }

            Task finishedTask = await Task.WhenAny(handlerTask, fatalTask).ConfigureAwait(false);

            if (finishedTask == fatalTask)
            {
                await fatalTask.ConfigureAwait(false);
            }

            object result = await handlerTask.ConfigureAwait(false);

            await _db.CompleteRunAsync(conn, queue, task.RunId, JsonSerializer.Serialize(result), linkedCts.Token).ConfigureAwait(false);
        }
        catch (Exception err)
        {
            if (err is SuspendTaskException || err is CancelledTaskException) return;

            _logger.LogError($"[ssf] task execution failed: {err.Message}");

            try
            {
                var errorObj = new { name = err.GetType().Name, message = err.Message, stack = err.StackTrace };

                await _db.FailRunAsync(conn, queue, task.RunId, JsonSerializer.Serialize(errorObj), linkedCts.Token).ConfigureAwait(false);
            }
            catch (Exception failErr)
            {
                _logger.LogError($"Failed to mark run as failed: {failErr.Message}");
            }

            if (err is FatalLeaseTimeoutException) throw;
        }
        finally
        {
            timeoutCts.Cancel();
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