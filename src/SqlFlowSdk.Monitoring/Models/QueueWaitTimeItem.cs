namespace SqlFlowSdk.Monitoring.Models
{
    public record QueueWaitTimeItem(string QueueName, double AvgWaitTimeMs, double MaxWaitTimeMs);
}
