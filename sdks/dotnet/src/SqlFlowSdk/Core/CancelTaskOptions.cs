// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace SqlFlowSdk.Core;

/// <summary>
/// Options for cancelling a task. This class is used when requesting cancellation of a task, and it specifies the queue that the task belongs to.
/// </summary>
public class CancelTaskOptions
{
    /// <summary>
    /// Queue that the task belongs to. This is required to identify which queue the task is in, so the system can locate and 
    /// cancel the correct task. The queue must match the one specified when the task was spawned.
    /// </summary>
    public required string Queue { get; set; }
}
