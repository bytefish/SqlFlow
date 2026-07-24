namespace SqlFlowSdk.Monitoring.Models
{
    public record ThroughputBucketItem(DateTimeOffset TimeBucket, string QueueName, int CompletedCount, int FailedCount, double AvgDurationMs);
}
