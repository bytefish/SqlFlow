// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace SqlFlowSdk.Management.Models;

/// <summary>
/// Database health information for a specific queue in the SQL Flow system, including the engine name, queue name,
/// </summary>
/// <param name="EngineName">The name of the SQL engine.</param>
/// <param name="QueueName">The name of the queue.</param>
/// <param name="TasksTableBytes">The size of the tasks table in bytes.</param>
/// <param name="RunsTableBytes">The size of the runs table in bytes.</param>
/// <param name="ActiveLocks">The number of active locks.</param>
public record DatabaseHealthItem(string EngineName, string QueueName, long TasksTableBytes, long RunsTableBytes, int ActiveLocks);
