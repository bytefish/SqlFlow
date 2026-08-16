// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using SqlFlowSdk;

namespace SqlFlowSdk.Core;

/// <summary>
/// Provides an implementation of the IEventPublisher interface that publishes events to an SqlFlow event queue.
/// </summary>
/// <remarks>This class is intended for internal use to integrate with the SqlFlow event system. It delegates event
/// publishing to an underlying ISqlFlow client. Thread safety and error handling depend on the behavior of the provided
/// ISqlFlow implementation.</remarks>
public class SqlFlowEventPublisher : IEventPublisher
{
    private readonly ISqlFlow _client;

    /// <summary>
    /// Initializes a new instance of the SqlFlowEventPublisher class using the specified SqlFlow client.
    /// </summary>
    /// <param name="client">The client instance used to communicate with the SqlFlow service. Cannot be null.</param>
    public SqlFlowEventPublisher(ISqlFlow client)
    {
        _client = client;
    }

    /// <summary>
    /// Asynchronously emits an event with the specified payload to the given queue.
    /// </summary>
    /// <typeparam name="TPayload">The type of the payload to include with the event.</typeparam>
    /// <param name="queue">The name of the queue to which the event will be emitted. Cannot be null or empty.</param>
    /// <param name="eventName">The name of the event to emit. Cannot be null or empty.</param>
    /// <param name="payload">The payload data to include with the event.</param>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    /// <returns>A task that represents the asynchronous emit operation.</returns>
    public async Task EmitEventAsync<TPayload>(string queue, string eventName, TPayload payload, CancellationToken cancellationToken)
    {
        await _client.EmitEventAsync(new EmitEventOptions { Queue = queue }, eventName, payload, cancellationToken);
    }
}