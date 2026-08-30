namespace SqlFlowSdk.Workers;

/// <summary>
/// Coordinates claiming and execution for registered SqlFlow queues.
/// </summary>
public interface ISqlFlowDispatcher
{
    /// <summary>
    /// Runs a worker for the supplied queue until cancellation is requested.
    /// Only one worker runtime per queue is allowed within the process.
    /// </summary>
    Task RunWorkerAsync(
        WorkerOptions options,
        CancellationToken cancellationToken);
}
