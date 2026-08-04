// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System.Text.Json.Nodes;

namespace SqlFlowSdk.Core;

/// <summary>
/// A claimed task represents a task that has been claimed by a worker and is ready to be executed. It contains all the 
/// information about the task, including its parameters, retry strategy, headers, and any wake event that may be 
/// associated with it. 
/// 
/// This is the main object that is passed to the TaskHandler when a task is being executed. It allows the worker 
/// to access all the necessary information about the task and perform the required operations to complete it.
/// </summary>
public class ClaimedTask
{
    /// <summary>
    /// The ID of the run that this task belongs to. This is a unique identifier for the entire workflow or job that 
    /// this task is a part of. It can be used to group related tasks together and track their progress as a unit.
    /// </summary>
    public required string RunId { get; set; }

    /// <summary>
    /// The ID of the task. This is a unique identifier for this specific instance of the task. It can be used to track 
    /// the progress of this task, log its execution, and report its result.
    /// </summary>
    public required string TaskId { get; set; }

    /// <summary>
    /// Name of the task type. This is used to identify which handler should be invoked to execute this task. It must match 
    /// the name of a registered task handler in the SqlFlow system.
    /// </summary>
    public required string TaskName { get; set; }

    /// <summary>
    /// Attempt number for this task. This indicates how many times this task has been attempted for execution. The 
    /// first time a task is executed, this will be 1. If the task fails and is retried, this number will be incremented 
    /// with each attempt.
    /// </summary>
    public int Attempt { get; set; }

    /// <summary>
    /// Parameters for this task. This is the data that was passed when the task was spawned. It can be any JSON-serializable 
    /// object, and it will be provided to the task handler when executing the task.
    /// </summary>
    public JsonNode? Params { get; set; }

    /// <summary>
    /// The Retry Strategy for this task. This defines how the SqlFlow system should handle retries if this task fails. It 
    /// can specify the number of retry attempts, the delay between retries, and any backoff strategy to use.
    /// </summary>
    public JsonNode? RetryStrategy { get; set; }

    /// <summary>
    /// Maximum number of attempts for this task before it is considered failed. This is used by the SqlFlow system to 
    /// determine when to stop retrying a task that keeps failing.
    /// </summary>
    public int? MaxAttempts { get; set; }

    /// <summary>
    /// Headers to attach to this task. Headers are arbitrary key-value pairs that can be used to store additional metadata about the
    /// task. 
    /// </summary>
    public JsonObject? Headers { get; set; }

    /// <summary>
    /// Optional wake event associated with this task. If specified, this event will be emitted when the task is completed, and 
    /// any tasks that are waiting for this event will be woken up and allowed to proceed. This can be used to create dependencies 
    /// between tasks and control the flow of execution in a workflow.
    /// </summary>
    public string? WakeEvent { get; set; }

    /// <summary>
    /// The Event Payload is the data that will be emitted along with the wake event when this task is completed.
    /// </summary>
    public JsonNode? EventPayload { get; set; }
}
