using Microsoft.Extensions.Logging;
using NATS.Client.JetStream;
using SqlFlowSdk.Core;

namespace SqlFlowSdk.Nats.Infrastructure;

internal class NatsJobPublisher : IJobPublisher
{
    private readonly ISqlFlow _client;
    private readonly SqlFlowRegistry _registry;
    private readonly INatsJSContext _js;
    private readonly ILogger<NatsJobPublisher> _logger;

    public NatsJobPublisher(
        ISqlFlow client,
        SqlFlowRegistry registry,
        INatsJSContext js,
        ILogger<NatsJobPublisher> logger)
    {
        _client = client;
        _registry = registry;
        _js = js;
        _logger = logger;
    }

    public async Task<SpawnResult> PublishAsync<TJob, TRequest>(string jobName, TRequest request, CancellationToken cancellationToken)
        where TRequest : notnull
    {
        if (!_registry.Routes.TryGetValue(jobName, out (Type JobType, string Queue) routing))
            throw new InvalidOperationException($"No Job found for name '{jobName}'.");

        if (routing.JobType != typeof(TJob))
            throw new InvalidOperationException($"Type-Mismatch for Job '{jobName}'.");

        var result = await _client.SpawnAsync(new SpawnOptions { Queue = routing.Queue }, jobName, request, cancellationToken);

        try
        {
            string subject = $"ssf.queues.{routing.Queue}";

            // V3 API: PublishAsync directly on INatsJSContext
            await _js.PublishAsync(subject, data: "ping", cancellationToken: cancellationToken);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to publish NATS wake-up signal for queue '{Queue}'. Falling back to smart polling.", routing.Queue);
        }

        return result;
    }
}