// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace SqlFlowSdk.Core;

/// <summary>
/// Represents a task that has been registered with the SqlFlow system. This class is used internally to store 
/// the task handler and its associated options.
/// </summary>
internal class RegisteredTask
{
    /// <summary>
    /// Name of the task. This is used to match spawned tasks with their handlers. It must be 
    /// unique across all registered tasks.
    /// </summary>
    public required string Name { get; set; }

    /// <summary>
    /// Maximum Number of attempts for this task before it is considered failed. This is used 
    /// by the SqlFlow system to determine when to stop retrying a task that keeps failing. 
    /// 
    /// It can be overridden on a per-task basis when spawning, but this provides a default value for convenience.
    /// </summary>
    public int DefaultMaxAttempts { get; set; } = 5;

    /// <summary>
    /// Default Cancellation policy for this task. This is used by the SqlFlow system to 
    /// determine when and how a task can be cancelled.
    /// </summary>
    public CancellationPolicy? DefaultCancellation { get; set; }

    /// <summary>
    /// Handler function that will be invoked when a task of this type is claimed for execution. This 
    /// function is responsible for performing the actual work of the task and returning a result. It 
    /// receives a TaskContext object that provides information about the task being executed, as 
    /// well as the parameters that were passed when the task was spawned.
    /// </summary>
    public required TaskHandler Handler { get; set; }
}
