namespace SqlFlowSdk.Monitoring.Models
{
    public record TaskFailureHotspotItem(string QueueName, string TaskName, int FailureCount, DateTimeOffset LastFailedAt);
}
