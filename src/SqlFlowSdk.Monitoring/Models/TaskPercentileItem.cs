namespace SqlFlowSdk.Monitoring.Models
{
    public record TaskPercentileItem(string TaskName, double P50Ms, double P95Ms, double P99Ms);
}
