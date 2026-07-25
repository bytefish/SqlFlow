// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace SqlFlowSdk.Management.Models;

/// <summary>
/// Represents a retry hotspot item in the SQL Flow system, including the queue name, task identifier, task name, number of attempts, and current state of the task.
/// </summary>
/// <param name="QueueName">The name of the queue.</param>
/// <param name="TaskId">The identifier of the task.</param>
/// <param name="TaskName">The name of the task.</param>
/// <param name="Attempts">The number of attempts.</param>
/// <param name="State">The current state of the task.</param>
public record RetryHotspotItem(string QueueName, string TaskId, string TaskName, int Attempts, string State);
