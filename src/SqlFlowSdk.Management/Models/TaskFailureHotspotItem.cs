// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace SqlFlowSdk.Management.Models;

/// <summary>
/// Represents a task failure hotspot item in the SQL Flow system, including the queue name, task name, failure count, and the timestamp of the last failure.
/// </summary>
/// <param name="QueueName">The name of the queue.</param>
/// <param name="TaskName">The name of the task.</param>
/// <param name="FailureCount">The number of failures.</param>
/// <param name="LastFailedAt">The timestamp of the last failure.</param>
public record TaskFailureHotspotItem(string QueueName, string TaskName, int FailureCount, DateTimeOffset LastFailedAt);
