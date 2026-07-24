// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using Microsoft.Extensions.Logging;
using SqlFlowSdk.Database;
using SqlFlowSdk.Exceptions;
using System.Data.Common;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace SqlFlowSdk.Core;

// Delegate for Task Handler
public delegate Task<object> TaskHandler(TaskContext ctx, JsonNode? parameters, CancellationToken cancellationToken);

/// <summary>
/// The TaskContext class provides the context for a task being executed by the SqlFlow system. It contains all the necessary 
/// information and methods that a task handler needs to perform its work, including access to checkpoints, event handling, 
/// and task management functions like sleeping and heartbeating.
/// 
/// It is passed as an argument to the TaskHandler when a task is claimed for execution, and it allows the handler to 
/// interact with the SqlFlow system in a structured way, ensuring that tasks can be executed reliably and can take advantage 
/// of features like checkpoints and events to manage complex workflows.
/// </summary>
public class TaskContext
{
    private readonly Dictionary<string, int> _stepNameCounter = new();
    private readonly ILogger _logger;

    public string TaskId { get; }

    public CancellationToken CancellationToken { get; }

    private readonly DbConnection _connection;
    private readonly ISqlFlowDatabase _db;

    private readonly string _queueName;
    private readonly ClaimedTask _task;
    private readonly Dictionary<string, JsonNode?> _checkpointCache;
    private readonly int _claimTimeout;

    private TaskContext(
        ILogger logger,
        string taskId,
        DbConnection con,
        ISqlFlowDatabase db,
        string queueName,
        ClaimedTask task,
        Dictionary<string, JsonNode?> checkpointCache,
        int claimTimeout,
        CancellationToken cancellationToken)
    {
        _logger = logger;

        TaskId = taskId;
        CancellationToken = cancellationToken;

        _connection = con;
        _db = db;
        _queueName = queueName;
        _task = task;
        _checkpointCache = checkpointCache;
        _claimTimeout = claimTimeout;

    }

    public static async Task<TaskContext> CreateAsync(
        ILogger logger,
        string taskId,
        DbConnection con,
        ISqlFlowDatabase db,
        string queueName,
        ClaimedTask task,
        int claimTimeout,
        CancellationToken cancellationToken)
    {
        IEnumerable<CheckpointRow> checkpoints = await db.GetCheckpointStatesAsync(con, queueName, task.TaskId, task.RunId, cancellationToken).ConfigureAwait(false);

        Dictionary<string, JsonNode?> cache = new Dictionary<string, JsonNode?>();

        foreach (CheckpointRow cp in checkpoints)
        {
            cache[cp.CheckpointName] = cp.State;
        }

        return new TaskContext(logger, taskId, con, db, queueName, task, cache, claimTimeout, cancellationToken);
    }

    public async Task<T> Step<T>(string name, Func<Task<T>> fn)
    {
        string checkpointName = GetCheckpointName(name);

        JsonNode? state = await LookupCheckpoint(checkpointName).ConfigureAwait(false);

        if (state != null)
        {
            return state.Deserialize<T>()!;
        }

        T rv = await fn().ConfigureAwait(false);

        // Serialize result to string for DB
        JsonNode? rvJson = JsonSerializer.SerializeToNode(rv);

        string rvString = rvJson?.ToJsonString() ?? "null";

        await _db.PersistCheckpointAsync(_connection, _queueName, _task.TaskId, _task.RunId, checkpointName, rvString, _claimTimeout, CancellationToken).ConfigureAwait(false);

        _checkpointCache[checkpointName] = rvJson;

        return rv;
    }

    public Task Step(string name, Func<Task> fn)
    {
        // Wir rufen die generische Methode mit einem Dummy-Typ (bool) auf.
        return Step(name, async () =>
        {
            await fn().ConfigureAwait(false);

            return true; // Dieser Wert wird als "true" in der DB als Marker gespeichert
        });
    }

    public async Task SleepFor(string stepName, double durationSeconds)
    {
        await SleepUntil(stepName, DateTime.UtcNow.AddSeconds(durationSeconds)).ConfigureAwait(false);
    }

    public async Task SleepUntil(string stepName, DateTime wakeAt)
    {
        string checkpointName = GetCheckpointName(stepName);
        JsonNode? state = await LookupCheckpoint(checkpointName).ConfigureAwait(false);

        DateTime actualWakeAt = wakeAt;

        if (state != null && state.GetValueKind() == JsonValueKind.String)
        {
            actualWakeAt = state.GetValue<DateTime>();
        }
        else if (state == null)
        {
            // Persist the wake time as the state
            string wakeString = JsonSerializer.Serialize(wakeAt);
            await _db.PersistCheckpointAsync(_connection, _queueName, _task.TaskId, _task.RunId, checkpointName, wakeString, _claimTimeout, CancellationToken).ConfigureAwait(false);
            _checkpointCache[checkpointName] = JsonValue.Create(wakeAt);
        }

        if (DateTime.UtcNow < actualWakeAt)
        {
            await _db.ScheduleRunAsync(_connection, _queueName, _task.RunId, actualWakeAt, CancellationToken).ConfigureAwait(false);

            throw new SuspendTaskException();
        }
    }

    private string GetCheckpointName(string name)
    {
        if (!_stepNameCounter.ContainsKey(name))
        {
            _stepNameCounter[name] = 0;
        }

        _stepNameCounter[name]++;
        int count = _stepNameCounter[name];
        return count == 1 ? name : $"{name}#{count}";
    }

    private async Task<JsonNode?> LookupCheckpoint(string checkpointName)
    {
        if (_checkpointCache.TryGetValue(checkpointName, out JsonNode? cached))
        {
            return cached;
        }

        JsonNode? state = await _db.GetSingleCheckpointAsync(_connection, _queueName, _task.TaskId, checkpointName, CancellationToken).ConfigureAwait(false);

        if (state != null)
        {
            _checkpointCache[checkpointName] = state;

            return state;
        }

        return null;
    }

    public async Task<JsonNode> AwaitEvent(string eventName, string? stepName = null, double? timeoutSeconds = null)
    {
         stepName ??= $"$awaitEvent:{eventName}";
        int? timeout = timeoutSeconds.HasValue ? (int)Math.Floor(timeoutSeconds.Value) : null;

        string checkpointName = GetCheckpointName(stepName);

        JsonNode? cached = await LookupCheckpoint(checkpointName).ConfigureAwait(false);

        if (cached != null) return cached;

        if (_task.WakeEvent == eventName && _task.EventPayload == null)
        {
            _task.WakeEvent = null;
            _task.EventPayload = null;
            throw new TimeoutErrorException($"Timed out waiting for event \"{eventName}\"");
        }

        (bool ShouldSuspend, JsonNode? Payload) result = await _db.AwaitEventAsync(_connection, _queueName, _task.TaskId, _task.RunId, checkpointName, eventName, timeout, CancellationToken).ConfigureAwait(false);

        if (!result.ShouldSuspend)
        {
            _checkpointCache[checkpointName] = result.Payload;
            _task.EventPayload = null;
            return result.Payload ?? new JsonObject();
        }

        throw new SuspendTaskException();
    }

    public async Task Heartbeat(int? seconds = null)
    {
        await _db.HeartbeatAsync(_connection, _queueName, _task.RunId, seconds ?? _claimTimeout, CancellationToken).ConfigureAwait(false);
    }

    public async Task EmitEvent(string eventName, JsonNode? payload = null)
    {
        if (string.IsNullOrEmpty(eventName))
        {
            throw new ArgumentException("eventName must be a non-empty string");
        }

        await _db.EmitEventAsync(_connection, _queueName, eventName, payload?.ToJsonString() ?? "null", CancellationToken).ConfigureAwait(false);
    }
}