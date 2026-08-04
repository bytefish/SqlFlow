// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace SqlFlowSdk.Core;

/// <summary>
/// The result of spawning a task. This class contains the TaskId and RunId of the spawned 
/// task, as well as the attempt number (which will be 1 for a newly spawned task).
/// </summary>
public class SpawnResult
{
    /// <summary>
    /// The unique identifier for the spawned task. This is used by the SqlFlow system to track the 
    /// task and manage its execution. It can be used to claim the task for execution, cancel the task, 
    /// or query its status.
    /// </summary>
    public required string TaskId { get; set; }

    /// <summary>
    /// The unique identifier for the run that this task belongs to. This is a unique identifier for the 
    /// entire workflow or job that this task is a part of. It can be used to group related tasks together 
    /// and track their progress as a unit.
    /// </summary>
    public required string RunId { get; set; }

    /// <summary>
    /// The attempt number for this task. This indicates how many times this task has been attempted for 
    /// execution. For a newly spawned task, this will be 1. If the task fails and is retried, this 
    /// number will be incremented with each attempt.
    /// </summary>
    public int Attempt { get; set; }
}
