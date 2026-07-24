namespace SqlFlowSdk.Monitoring.Models
{
    public record ActiveWaitItem(string QueueName, string EventName, int WaitingCount, DateTimeOffset? OldestWaitAt);
}
