// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace SqlFlowSdk.Management.Models;

/// <summary>
/// Represents a slow task item in the SQL Flow system, including the queue name, task identifier, task name, duration in milliseconds, and the timestamp when the task was completed.
/// </summary>
/// <param name="QueueName">The name of the queue.</param>
/// <param name="TaskId">The identifier of the task.</param>
/// <param name="TaskName">The name of the task.</param>
/// <param name="DurationMs">The duration of the task in milliseconds.</param>
/// <param name="CompletedAt">The timestamp when the task was completed.</param>
public record SlowTaskItem(string QueueName, string TaskId, string TaskName, double DurationMs, DateTimeOffset CompletedAt);
