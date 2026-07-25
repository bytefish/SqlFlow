// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace SqlFlowSdk.Management.Models;

/// <summary>
/// Represents the wait time statistics for a specific queue in the SQL Flow system, including the queue name, average wait time in milliseconds, and maximum wait time in milliseconds.
/// </summary>
/// <param name="QueueName">The name of the queue.</param>
/// <param name="AvgWaitTimeMs">The average wait time in milliseconds.</param>
/// <param name="MaxWaitTimeMs">The maximum wait time in milliseconds.</param>
public record QueueWaitTimeItem(string QueueName, double AvgWaitTimeMs, double MaxWaitTimeMs);
