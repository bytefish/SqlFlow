namespace SqlFlowSdk.Monitoring.Models
{
    // 3. Erweiterte Analysen
    public record RetryHotspotItem(string QueueName, string TaskId, string TaskName, int Attempts, string State);
}
