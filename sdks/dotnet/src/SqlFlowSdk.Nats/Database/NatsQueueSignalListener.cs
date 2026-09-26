using System.Collections.Concurrent;
using System.Threading.Channels;
using Microsoft.Extensions.Logging;
using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;
using SqlFlowSdk.Core;

namespace SqlFlowSdk.Signaling
{
    public class NatsQueueSignalListener : IQueueSignalListener, IAsyncDisposable
    {
        private readonly INatsJSContext _js;
        private readonly ILogger<NatsQueueSignalListener> _logger;

        private readonly ConcurrentDictionary<string, Channel<bool>> _queueChannels = new();
        private readonly ConcurrentDictionary<string, Task> _subscriptions = new();
        private readonly CancellationTokenSource _cts = new();

        public NatsQueueSignalListener(INatsJSContext js, ILogger<NatsQueueSignalListener> logger)
        {
            _js = js;
            _logger = logger;
        }

        public void RegisterQueue(string queueName)
        {
            _queueChannels.GetOrAdd(queueName, _ => Channel.CreateBounded<bool>(new BoundedChannelOptions(1)
            {
                FullMode = BoundedChannelFullMode.DropOldest
            }));

            // Launch the V3 Consumer task in the background since registration is synchronous
            _subscriptions.GetOrAdd(queueName, q => Task.Run(() => StartConsumingAsync(q)));
        }

        public async ValueTask<bool> WaitAsync(string queueName, TimeSpan fallbackTimeout, CancellationToken cancellationToken)
        {
            if (!_queueChannels.TryGetValue(queueName, out var channel))
            {
                await Task.Delay(fallbackTimeout, cancellationToken);
                return false;
            }

            using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            linkedCts.CancelAfter(fallbackTimeout);

            try
            {
                // ReadAsync natively returns a ValueTask. Awaiting this avoids allocations 
                // if the signal was already in the channel when the worker called WaitAsync.
                return await channel.Reader.ReadAsync(linkedCts.Token);
            }
            catch (OperationCanceledException)
            {
                // Timeout expired naturally, fallback mechanism takes over
                return false;
            }
        }

        private async Task StartConsumingAsync(string queueName)
        {
            string subject = $"ssf.queues.{queueName}";

            try
            {
                // 1. Ensure the Stream exists
                var streamConfig = new StreamConfig("SSF_QUEUES", subjects: new[] { "ssf.queues.>" });
                var stream = await _js.CreateStreamAsync(streamConfig, cancellationToken: _cts.Token);

                // 2. Create the Ephemeral Consumer
                var consumerConfig = new ConsumerConfig
                {
                    FilterSubject = subject,
                    DeliverPolicy = ConsumerConfigDeliverPolicy.New
                };

                var consumer = await stream.CreateOrUpdateConsumerAsync(consumerConfig, cancellationToken: _cts.Token);

                // 3. Consume with named parameters to bypass the INatsDeserialize<T> overload compiler error
                var consumeOpts = new NatsJSConsumeOpts { MaxMsgs = 100 };

                await foreach (var msg in consumer.ConsumeAsync<string>(opts: consumeOpts, cancellationToken: _cts.Token))
                {
                    await msg.AckAsync(cancellationToken: _cts.Token);

                    if (_queueChannels.TryGetValue(queueName, out var channel))
                    {
                        channel.Writer.TryWrite(true);
                    }
                }
            }
            catch (OperationCanceledException)
            {
                // Normal shutdown
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error consuming NATS JetStream for queue {Queue}", queueName);
            }
        }

        public async ValueTask DisposeAsync()
        {
            _cts.Cancel();
            await Task.WhenAll(_subscriptions.Values);
            _cts.Dispose();
        }
    }
}