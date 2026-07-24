namespace SqlFlowSdk.Monitoring.Models
{
    public record TaskSearchResultItem(
        string QueueName,
        string TaskId,
        string TaskName,
        string State,
        int Attempts,
        string? RunId,
        DateTimeOffset EnqueuedAt,
        DateTimeOffset? LastAttemptAt,
        string? FailureReason,
        string? ParamsJson
    );
}
