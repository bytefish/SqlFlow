namespace SqlFlowSdk.Monitoring.Models
{
    public record DatabaseHealthItem(string EngineName, string QueueName, long TasksTableBytes, long RunsTableBytes, int ActiveLocks);
}
