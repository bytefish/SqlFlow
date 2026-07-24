namespace SqlFlowSdk.Monitoring.Models
{
    public record FailedTaskItem(string QueueName, string TaskId, string TaskName, int Attempts, string? RunId, DateTimeOffset? FailedAt, string? FailureReason);
}
