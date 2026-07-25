// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace SqlFlowSdk.Management.Models;

/// <summary>
/// Aggregated statistics for a specific queue in the SQL Flow system, including the queue name, state, and count of tasks in that state.
/// </summary>
/// <param name="QueueName">The name of the queue.</param>
/// <param name="State">The state of the tasks.</param>
/// <param name="Count">The count of tasks in the specified state.</param>
public record QueueStatItem(string QueueName, string State, int Count);
