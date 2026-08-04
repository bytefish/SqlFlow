namespace SqlFlowSdk.Core;

/// <summary>
/// An Event Publisher is responsible for emitting events to a specified queue. It allows for decoupled communication between 
/// different parts of the system by sending events that can be handled by any interested subscribers.
/// </summary>
public interface IEventPublisher
{
    /// <summary>
    /// Emits an event to the specified queue with the given event name and payload.
    /// </summary>
    /// <typeparam name="TPayload">Type of the payload to send</typeparam>
    /// <param name="queue">Queue to publish to</param>
    /// <param name="eventName">Name of the Event being published</param>
    /// <param name="payload">Payload of the Event being published</param>
    /// <param name="cancellationToken">Cancellation Token to cancel the operation</param>
    /// <returns>An awaitable Task signalling completion</returns>
    Task EmitEventAsync<TPayload>(string queue, string eventName, TPayload payload, CancellationToken cancellationToken);
}
