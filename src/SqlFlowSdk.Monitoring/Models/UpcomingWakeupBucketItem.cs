namespace SqlFlowSdk.Monitoring.Models
{
    public record UpcomingWakeupBucketItem(DateTimeOffset TimeBucket, string QueueName, int SleepingCount);
}
