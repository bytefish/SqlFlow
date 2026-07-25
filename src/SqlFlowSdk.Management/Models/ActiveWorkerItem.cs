// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace SqlFlowSdk.Management.Models;

/// <summary>
/// Represents an active worker in the SQL Flow system, including the queue it is associated with, its 
/// unique identifier, and the number of active runs it is currently handling.
/// </summary>
/// <param name="QueueName">Queue to which the worker is associated.</param>
/// <param name="WorkerId">Unique identifier for the worker.</param>
/// <param name="ActiveRuns">Number of active runs the worker is handling.</param>
public record ActiveWorkerItem(string QueueName, string WorkerId, int ActiveRuns);
