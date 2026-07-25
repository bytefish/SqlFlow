// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace SqlFlowSdk.Management.Models;

/// <summary>
/// Represents a throughput bucket item in the SQL Flow system, including the time bucket, queue name, completed count, failed count, and average duration in milliseconds.
/// </summary>
/// <param name="TimeBucket">The time bucket.</param>
/// <param name="QueueName">The name of the queue.</param>
/// <param name="CompletedCount">The number of completed tasks.</param>
/// <param name="FailedCount">The number of failed tasks.</param>
/// <param name="AvgDurationMs">The average duration in milliseconds.</param>
public record ThroughputBucketItem(DateTimeOffset TimeBucket, string QueueName, int CompletedCount, int FailedCount, double AvgDurationMs);
