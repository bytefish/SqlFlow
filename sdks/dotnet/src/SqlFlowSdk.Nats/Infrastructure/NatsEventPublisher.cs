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

    public async Task EmitEventAsync<TPayload>(string tenantId, string queue, string eventName, TPayload payload, CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(tenantId);
        ArgumentException.ThrowIfNullOrWhiteSpace(queue);

        await _innerPublisher.EmitEventAsync(tenantId, queue, eventName, payload, cancellationToken);

        try
        {
            string subject = $"ssf.tenants.{tenantId}.queues.{queue}";

            await _js.PublishAsync(subject, data: "ping", cancellationToken: cancellationToken);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to publish NATS wake-up signal for tenant '{TenantId}' and queue '{Queue}' after event emission.", tenantId, queue);
        }
    }
}