// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System.Text.Json.Nodes;

namespace SqlFlowSdk.Core;

/// <summary>
/// Options for spawning a new task. This is used when you want to create a new task from within an 
/// existing task or from an external trigger. It allows you to specify the queue to spawn on, 
/// retry strategy, headers, and cancellation policy.
/// </summary>
public class SpawnOptions
{
    /// <summary>
    /// Queue to spawn the task on. This determines which workers will be able to claim and 
    /// execute the task. It must match the queue specified in the RegisteredTask for the 
    /// task being spawned.
    /// </summary>
    public required string Queue { get; set; }

    /// <summary>
    /// Maximum number of attempts for this task before it is considered failed. This overrides the default 
    /// value specified in the RegisteredTask for this task type.
    /// </summary>
    public int MaxAttempts { get; set; } = 5;

    /// <summary>
    /// Strategy for retrying this task if it fails. This can be used to implement exponential backoff, 
    /// fixed delays, or other retry strategies.
    /// </summary>
    public RetryStrategy? RetryStrategy { get; set; }

    /// <summary>
    /// Headers to attach to this task. Headers are arbitrary key-value pairs that can be used to store.
    /// </summary>
    public JsonObject? Headers { get; set; }

    /// <summary>
    /// Cancellation policy for this task. This determines when and how the task can be cancelled.
    /// </summary>
    public CancellationPolicy? Cancellation { get; set; }
}
