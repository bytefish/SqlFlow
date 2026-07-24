namespace SqlFlowSdk.Monitoring.Models
{
    // 2. Monitoring & Engpässe
    public record ActiveWorkerItem(string QueueName, string WorkerId, int ActiveRuns);
}
