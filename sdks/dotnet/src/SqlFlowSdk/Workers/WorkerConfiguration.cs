// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace SqlFlowSdk.Workers;

/// <summary>
/// Represents the configuration settings for a background worker that processes items from a queue.
/// </summary>
/// <remarks>Use this class to specify operational parameters such as queue name, concurrency level, polling
/// interval, batch size, and error handling behavior for a worker. These settings control how the worker interacts with
/// the queue and manages task execution.</remarks>
public class WorkerConfiguration
{
    /// <summary>
    /// Gets or sets the name of the queue to which messages are sent or from which they are received.
    /// </summary>
    public string QueueName { get; set; } = "";

    /// <summary>
    /// Gets or sets the maximum number of concurrent operations allowed.
    /// </summary>
    public int Concurrency { get; set; } = 4;

    /// <summary>
    /// Gets or sets the interval, in seconds, between polling operations.
    /// </summary>
    /// <remarks>Set this value to control how frequently the system checks for updates or new data. A smaller
    /// interval increases polling frequency but may impact performance.</remarks>
    public double PollIntervalInSeconds { get; set; } = 0.5;
    
    /// <summary>
    /// Gets or sets the claim timeout period, in seconds, for pending operations.
    /// </summary>
    /// <remarks>This value determines how long a claim remains valid before it expires. Adjust this setting
    /// to control the maximum duration an operation can be claimed before it is considered timed out and eligible for
    /// reprocessing.</remarks>
    public int ClaimTimeoutInSeconds { get; set; } = 120;
    
    /// <summary>
    /// Gets or sets the maximum number of items to include in each batch operation.
    /// </summary>
    /// <remarks>If the value is null, the default batch size defined by the system or operation will be used.
    /// Setting this property allows control over the size of each batch, which can affect performance and resource
    /// usage.</remarks>
    public int? BatchSize { get; set; }
    
    /// <summary>
    /// Gets or sets a value indicating whether a lease timeout should be treated as a fatal error.
    /// </summary>
    /// <remarks>When set to <see langword="true"/>, the system will consider a lease timeout as a
    /// non-recoverable error and may terminate the operation or process. When set to <see langword="false"/>, lease
    /// timeouts are handled as recoverable events, allowing for retry or recovery logic.</remarks>
    public bool FatalOnLeaseTimeout { get; set; } = true;
    
    /// <summary>
    /// Gets or sets the callback to invoke when an unhandled exception occurs.
    /// </summary>
    /// <remarks>Assign a delegate to handle errors that are not caught elsewhere. The provided exception
    /// parameter contains details about the error. If not set, unhandled exceptions may propagate or be handled by
    /// default error handling mechanisms.</remarks>
    public Action<Exception>? OnError { get; set; }
}
