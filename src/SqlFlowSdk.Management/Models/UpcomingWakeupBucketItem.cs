// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace SqlFlowSdk.Management.Models;

/// <summary>
/// Represents an upcoming wakeup bucket item in the SQL Flow system, including the time bucket, queue name, and the count of sleeping tasks.
/// </summary>
/// <param name="TimeBucket">The time bucket.</param>
/// <param name="QueueName">The name of the queue.</param>
/// <param name="SleepingCount">The count of sleeping tasks.</param>
public record UpcomingWakeupBucketItem(DateTimeOffset TimeBucket, string QueueName, int SleepingCount);
