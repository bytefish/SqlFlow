namespace SqlFlowSdk.Core;

/// <summary>
/// Receives best-effort notifications indicating that a queue may
/// contain runnable work.
/// </summary>
/// <remarks>
/// Queue signals are hints only. They are not durable messages and do not
/// represent individual tasks.
///
/// Implementations may coalesce duplicate signals. The worker must always
/// use the database claim operation as the authoritative source of work.
///
/// The fallback timeout ensures that missed notifications, scheduled work,
/// expired leases, and temporary listener outages cannot block processing
/// indefinitely.
/// </remarks>
public interface IQueueSignalListener
{
    /// <summary>
    /// Registers a queue with the listener.
    /// </summary>
    /// <param name="queueName">The queue to register.</param>
    /// <remarks>
    /// Registration must be idempotent.
    ///
    /// Implementations should arrange for an initial database inspection
    /// after registration so that work created before the listener started
    /// is not overlooked.
    /// </remarks>
    void RegisterQueue(string queueName);

    /// <summary>
    /// Waits until the queue is signaled or the fallback timeout elapses.
    /// </summary>
    /// <param name="queueName">The queue to wait for.</param>
    /// <param name="fallbackTimeout">
    /// Maximum time to wait before allowing the worker to inspect the
    /// database again.
    /// </param>
    /// <param name="cancellationToken">
    /// Token used to stop the wait operation.
    /// </param>
    /// <returns>
    /// <see langword="true"/> if a queue signal was received;
    /// otherwise <see langword="false"/> if the fallback timeout elapsed.
    /// </returns>
    ValueTask<bool> WaitAsync(
        string queueName,
        TimeSpan fallbackTimeout,
        CancellationToken cancellationToken);
}

/// <summary>
/// Provides timer-based queue wake-ups for database providers that do not
/// support notifications.
/// </summary>
public sealed class PollingQueueSignalListener : IQueueSignalListener
{
    public void RegisterQueue(string queueName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(queueName);
    }

    public async ValueTask<bool> WaitAsync(
        string queueName,
        TimeSpan fallbackTimeout,
        CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(queueName);

        if (fallbackTimeout <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(
                nameof(fallbackTimeout),
                "The fallback timeout must be greater than zero.");
        }

        await Task.Delay(
                fallbackTimeout,
                cancellationToken)
            .ConfigureAwait(false);

        return false;
    }
}