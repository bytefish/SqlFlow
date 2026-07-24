namespace SqlFlowSdk.Monitoring.Models
{
    // 1. Aggregierte Status-Statistiken
    public record QueueStatItem(string QueueName, string State, int Count);
}
