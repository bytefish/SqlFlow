namespace SqlFlowSdk.Core;

public sealed class QueueSignalOptions
{
    /// <summary>
    /// Maximum time between authoritative database inspections when no
    /// notification is received.
    /// </summary>
    public TimeSpan ReconciliationInterval { get; init; } =
        TimeSpan.FromSeconds(60);

    /// <summary>
    /// Delay before reconnecting after a listener connection failure.
    /// </summary>
    public TimeSpan ReconnectDelay { get; init; } =
        TimeSpan.FromSeconds(5);

    public void Validate()
    {
        if (ReconciliationInterval <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(
                nameof(ReconciliationInterval),
                "The reconciliation interval must be greater than zero.");
        }

        if (ReconnectDelay < TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(
                nameof(ReconnectDelay),
                "The reconnect delay cannot be negative.");
        }
    }
}