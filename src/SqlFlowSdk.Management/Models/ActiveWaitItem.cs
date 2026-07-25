// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace SqlFlowSdk.Management.Models;

/// <summary>
/// Represents an active wait item in the SQL Flow system, including the queue name, event name, number of waiting tasks, 
/// and the timestamp of the oldest waiting task.
/// </summary>
/// <param name="QueueName">The name of the queue.</param>
/// <param name="EventName">The name of the event.</param>
/// <param name="WaitingCount">The number of waiting tasks.</param>
/// <param name="OldestWaitAt">The timestamp of the oldest waiting task.</param>
public record ActiveWaitItem(string QueueName, string EventName, int WaitingCount, DateTimeOffset? OldestWaitAt);
