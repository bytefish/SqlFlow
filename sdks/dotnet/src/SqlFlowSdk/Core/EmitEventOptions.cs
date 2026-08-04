// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace SqlFlowSdk.Core;

/// <summary>
/// Options for emitting an event. This class allows you to specify the queue that the event should be emitted to.
/// </summary>
public class EmitEventOptions
{
    /// <summary>
    /// Queue to which the event should be emitted. This must match the queue that handlers are listening 
    /// on for this event. The event will be delivered to all handlers that are registered to listen for 
    /// it on the specified queue.
    /// </summary>
    public required string Queue { get; set; }
}
