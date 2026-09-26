// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System.Collections.Concurrent;
using System.Threading.Channels;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;
using SqlFlowSdk.Core;

namespace SqlFlowSdk.Nats.Database;

public sealed class NatsQueueSignalListener :
    BackgroundService,
    IQueueSignalListener
{
    private readonly INatsJSContext _js;
    private readonly QueueSignalOptions _options;
    private readonly ILogger<NatsQueueSignalListener> _logger;

    private readonly ConcurrentDictionary<string, Channel<bool>> _queueSignals =
        new(StringComparer.Ordinal);

    public NatsQueueSignalListener(
        INatsJSContext js,
        IOptions<QueueSignalOptions> options,
        ILogger<NatsQueueSignalListener> logger)
    {
        ArgumentNullException.ThrowIfNull(js);
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(logger);

        _js = js;
        _options = options.Value;
        _logger = logger;

        _options.Validate();
    }

    public void RegisterQueue(string queueName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(queueName);

        Channel<bool> newChannel = CreateSignalChannel();

        Channel<bool> actualChannel =
            _queueSignals.GetOrAdd(
                queueName,
                newChannel);

        if (ReferenceEquals(actualChannel, newChannel))
        {
            // Exactly one initial reconciliation for a newly registered queue.
            actualChannel.Writer.TryWrite(true);
        }
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
                "Fallback timeout must be greater than zero.");
        }

        if (!_queueSignals.TryGetValue(
                queueName,
                out Channel<bool>? channel))
        {
            RegisterQueue(queueName);
            channel = _queueSignals[queueName];
        }

        using CancellationTokenSource timeout =
            CancellationTokenSource.CreateLinkedTokenSource(
                cancellationToken);

        timeout.CancelAfter(fallbackTimeout);

        try
        {
            await channel.Reader
                .ReadAsync(timeout.Token)
                .ConfigureAwait(false);

            return true;
        }
        catch (OperationCanceledException)
            when (!cancellationToken.IsCancellationRequested)
        {
            return false;
        }
    }

    protected override async Task ExecuteAsync(
        CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await ConsumeAsync(stoppingToken)
                    .ConfigureAwait(false);
            }
            catch (OperationCanceledException)
                when (stoppingToken.IsCancellationRequested)
            {
                return;
            }
            catch (Exception exception)
            {
                _logger.LogError(
                    exception,
                    "NATS JetStream queue listener disconnected.");

                SignalAllQueues();

                if (_options.ReconnectDelay > TimeSpan.Zero)
                {
                    await DelayBeforeReconnectAsync(
                            stoppingToken)
                        .ConfigureAwait(false);
                }
            }
        }
    }

    private async Task ConsumeAsync(
        CancellationToken cancellationToken)
    {
        // 1. Ensure the Stream exists
        var streamConfig = new StreamConfig("SSF_QUEUES", subjects: new[] { "ssf.queues.>" });
        var stream = await _js.CreateStreamAsync(streamConfig, cancellationToken: cancellationToken)
            .ConfigureAwait(false);

        _logger.LogInformation("NATS JetStream stream 'SSF_QUEUES' verified.");

        // 2. Perform an initial reconciliation right as the listener connects
        SignalAllQueues();

        // Note: NATS handles message streaming dynamically per-queue or via a wildcards, 
        // but since workers subscribe dynamically to specific queues as they register, 
        // we consume globally or iterate over registered queues. 
        // To match the exact robust pattern, we listen to all queue subjects using wildcard.
        string subject = "ssf.queues.>";

        var consumerConfig = new ConsumerConfig(name: "ssf-worker-group")
        {
            FilterSubject = subject,
            DeliverPolicy = ConsumerConfigDeliverPolicy.New
        };

        var consumer = await stream.CreateOrUpdateConsumerAsync(consumerConfig, cancellationToken: cancellationToken)
            .ConfigureAwait(false);

        var consumeOpts = new NatsJSConsumeOpts { MaxMsgs = 100 };

        await foreach (var msg in consumer.ConsumeAsync<string>(opts: consumeOpts, cancellationToken: cancellationToken))
        {
            await msg.AckAsync(cancellationToken: cancellationToken)
                .ConfigureAwait(false);

            // Extract queue name from subject (e.g. ssf.queues.ai-agent-queue -> ai-agent-queue)
            string queueName = ExtractQueueName(msg.Subject);

            if (!string.IsNullOrEmpty(queueName))
            {
                SignalQueue(queueName);
            }
        }
    }

    private void SignalQueue(string queueName)
    {
        if (_queueSignals.TryGetValue(
                queueName,
                out Channel<bool>? channel))
        {
            channel.Writer.TryWrite(true);
        }
    }

    private void SignalAllQueues()
    {
        foreach (Channel<bool> channel in _queueSignals.Values)
        {
            channel.Writer.TryWrite(true);
        }
    }

    private static string ExtractQueueName(string subject)
    {
        // Subject format: ssf.queues.<queueName>
        const string prefix = "ssf.queues.";
        if (subject.StartsWith(prefix, StringComparison.Ordinal))
        {
            return subject.Substring(prefix.Length);
        }
        return string.Empty;
    }

    private async Task DelayBeforeReconnectAsync(
        CancellationToken cancellationToken)
    {
        try
        {
            await Task.Delay(
                    _options.ReconnectDelay,
                    cancellationToken)
                .ConfigureAwait(false);
        }
        catch (OperationCanceledException)
            when (cancellationToken.IsCancellationRequested)
        {
            // Normal shutdown.
        }
    }

    private static Channel<bool> CreateSignalChannel()
    {
        return Channel.CreateBounded<bool>(
            new BoundedChannelOptions(1)
            {
                SingleReader = true,
                SingleWriter = false,
                FullMode = BoundedChannelFullMode.DropWrite,
                AllowSynchronousContinuations = false
            });
    }
}