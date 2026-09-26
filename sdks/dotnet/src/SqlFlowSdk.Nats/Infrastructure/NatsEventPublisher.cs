
using SqlFlowSdk.Core;
using Microsoft.Extensions.Logging;
using NATS.Client.JetStream;

namespace SqlFlowSdk.Nats.Infrastructure;

internal class NatsEventPublisher : IEventPublisher
{
    private readonly IEventPublisher _innerPublisher;
    private readonly INatsJSContext _js;
    private readonly ILogger<NatsEventPublisher> _logger;

    public NatsEventPublisher(
        IEventPublisher innerPublisher,
        INatsJSContext js,
        ILogger<NatsEventPublisher> logger)
    {
        _innerPublisher = innerPublisher;
        _js = js;
        _logger = logger;
    }

    public async Task EmitEventAsync<TPayload>(string queue, string eventName, TPayload payload, CancellationToken cancellationToken)
    {
        await _innerPublisher.EmitEventAsync(queue, eventName, payload, cancellationToken);

        try
        {
            string subject = $"ssf.queues.{queue}";

            await _js.PublishAsync(subject, data: "ping", cancellationToken: cancellationToken);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to publish NATS wake-up signal for queue '{Queue}' after event emission.", queue);
        }
    }
}