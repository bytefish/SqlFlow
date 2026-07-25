// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace SqlFlowSdk.Management.Models;

/// <summary>
/// Represents a search result item for a task in the SQL Flow system, including details such as queue name, task ID, task name, state, attempts, run ID, timestamps for enqueuing and last attempt, failure reason, and parameters in JSON format.
/// </summary>
/// <param name="QueueName">The name of the queue.</param>
/// <param name="TaskId">The identifier of the task.</param>
/// <param name="TaskName">The name of the task.</param>
/// <param name="State">The current state of the task.</param>
/// <param name="Attempts">The number of attempts.</param>
/// <param name="RunId">The identifier of the run.</param>
/// <param name="EnqueuedAt">The timestamp when the task was enqueued.</param>
/// <param name="LastAttemptAt">The timestamp of the last attempt.</param>
/// <param name="FailureReason">The reason for the failure.</param>
/// <param name="ParamsJson">The parameters in JSON format.</param>
public record TaskSearchResultItem(
    string QueueName,
    string TaskId,
    string TaskName,
    string State,
    int Attempts,
    string? RunId,
    DateTimeOffset EnqueuedAt,
    DateTimeOffset? LastAttemptAt,
    string? FailureReason,
    string? ParamsJson
);
