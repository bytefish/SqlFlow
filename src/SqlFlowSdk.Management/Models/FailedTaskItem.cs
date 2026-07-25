// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace SqlFlowSdk.Management.Models;

/// <summary>
/// Represents a failed task in the SQL Flow system, including details such as the queue name, task ID, task name, number of attempts, run ID, 
/// </summary>
/// <param name="QueueName">The name of the queue.</param>
/// <param name="TaskId">The ID of the failed task.</param>
/// <param name="TaskName">The name of the failed task.</param>
/// <param name="Attempts">The number of attempts made for the task.</param>
/// <param name="RunId">The ID of the run in which the task failed.</param>
/// <param name="FailedAt">The timestamp when the task failed.</param>
/// <param name="FailureReason">The reason for the failure.</param>
public record FailedTaskItem(string QueueName, string TaskId, string TaskName, int Attempts, string? RunId, DateTimeOffset? FailedAt, string? FailureReason);
