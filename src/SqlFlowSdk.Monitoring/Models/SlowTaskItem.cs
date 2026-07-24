namespace SqlFlowSdk.Monitoring.Models
{
    public record SlowTaskItem(string QueueName, string TaskId, string TaskName, double DurationMs, DateTimeOffset CompletedAt);
}
