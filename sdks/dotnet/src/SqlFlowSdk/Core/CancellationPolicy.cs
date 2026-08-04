// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System.Text.Json.Serialization;

namespace SqlFlowSdk.Core;

/// <summary>
/// Cancellation policy for a task. This determines when and how a task can be cancelled. It can specify 
/// a maximum duration for the task, after which it will be automatically cancelled, and a maximum delay 
/// for cancellation requests.
/// 
/// This allows you to control the lifecycle of your tasks and ensure that they do not run indefinitely 
/// or accept cancellation requests after a certain point.
/// </summary>
public class CancellationPolicy
{
    /// <summary>
    /// Maximum duration for the task in seconds. If the task runs longer than this duration, it will be automatically cancelled.
    /// </summary>
    [JsonPropertyName("max_duration")]
    public double? MaxDuration { get; set; }

    /// <summary>
    /// Maximum delay in seconds for accepting cancellation requests. If a cancellation request is received after this delay, it 
    /// will be ignored and the task will continue running. This allows you to specify a grace period during which cancellation 
    /// requests are accepted, and after which they are rejected.
    /// </summary>
    [JsonPropertyName("max_delay")]
    public double? MaxDelay { get; set; }
}
