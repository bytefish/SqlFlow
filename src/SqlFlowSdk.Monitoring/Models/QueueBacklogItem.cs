namespace SqlFlowSdk.Monitoring.Models
{
    public record QueueBacklogItem(string QueueName, int PendingCount, DateTimeOffset? OldestPendingAt);
}
