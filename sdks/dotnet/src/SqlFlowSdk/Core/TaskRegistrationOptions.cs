// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace SqlFlowSdk.Core;

/// <summary>
/// Options to configure a task when it is registered with the SqlFlow system. This is used to specify the 
/// name, queue, default retry policy, and default cancellation policy for a task type. These options are 
/// used by the SqlFlow system to manage and execute tasks of this type.
/// </summary>
public class TaskRegistrationOptions
{
    /// <summary>
    /// Name of the task type. This is used to identify the task when spawning and executing tasks. It must be 
    /// unique across all registered tasks. When you spawn a task, you specify the name of
    /// </summary>
    public required string Name { get; set; }

    /// <summary>
    /// Default Number of attempts for this task before it is considered failed.
    /// </summary>
    public int DefaultMaxAttempts { get; set; } = 5;

    /// <summary>
    /// Default Cancellation policy for this task. This is used by the SqlFlow system to determine when and how 
    /// a task can be cancelled. It can be overridden on a per-task basis when spawning, but this provides 
    /// a default value for convenience.
    /// </summary>
    public CancellationPolicy? DefaultCancellation { get; set; }
}
