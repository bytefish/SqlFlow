namespace SqlFlowSdk.Monitoring.Models
{
    public record TaskDetailItem(string TaskId, string QueueName, string TaskName, string State, DateTimeOffset EnqueuedAt, DateTimeOffset? FirstStartedAt, string? CompletedPayload, string? Params);
}
