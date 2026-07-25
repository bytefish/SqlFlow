// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace SqlFlowSdk.Management.Models;

/// <summary>
/// Represents detailed information about a specific task in the SQL Flow system, including its identifier, queue name, task name, state, timestamps for when it was enqueued 
/// and first started, any completed payload, and parameters associated with the task.
/// </summary>
/// <param name="TaskId">The identifier of the task.</param>
/// <param name="QueueName">The name of the queue.</param>
/// <param name="TaskName">The name of the task.</param>
/// <param name="State">The state of the task.</param>
/// <param name="EnqueuedAt">The timestamp when the task was enqueued.</param>
/// <param name="FirstStartedAt">The timestamp when the task was first started.</param>
/// <param name="CompletedPayload">The payload of the completed task.</param>
/// <param name="Params">The parameters associated with the task.</param>
public record TaskDetailItem(string TaskId, string QueueName, string TaskName, string State, DateTimeOffset EnqueuedAt, DateTimeOffset? FirstStartedAt, string? CompletedPayload, string? Params);
