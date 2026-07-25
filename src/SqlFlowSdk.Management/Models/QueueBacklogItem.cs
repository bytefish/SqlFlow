// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace SqlFlowSdk.Management.Models;

/// <summary>
/// Represents a backlog item in the SQL Flow system, including the queue name, the number of pending tasks, and the timestamp of the oldest pending task.
/// </summary>
/// <param name="QueueName">The name of the queue.</param>
/// <param name="PendingCount">The number of pending tasks.</param>
/// <param name="OldestPendingAt">The timestamp of the oldest pending task.</param>
public record QueueBacklogItem(string QueueName, int PendingCount, DateTimeOffset? OldestPendingAt);
