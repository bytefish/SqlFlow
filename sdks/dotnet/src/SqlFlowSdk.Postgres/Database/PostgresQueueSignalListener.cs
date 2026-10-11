using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Npgsql;
using SqlFlowSdk.Core;
using System.Collections.Concurrent;
using System.Threading.Channels;

namespace SqlFlowSdk.Postgres;

public sealed class PostgresQueueSignalListener :
    BackgroundService, IQueueSignalListener
{
    public const string NotificationChannel = "ssf_work_available";

    private readonly NpgsqlDataSource _dataSource;
    private readonly QueueSignalOptions _options;
    private readonly ILogger<PostgresQueueSignalListener> _logger;


    private readonly ConcurrentDictionary<string, Channel<bool>>
        _queueSignals = new(StringComparer.Ordinal);

    public PostgresQueueSignalListener(
        NpgsqlDataSource dataSource,
        IOptions<QueueSignalOptions> options,
        ILogger<PostgresQueueSignalListener> logger)
    {
        ArgumentNullException.ThrowIfNull(dataSource);
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(logger);

        _dataSource = dataSource;
        _options = options.Value;
        _logger = logger;

        _options.Validate();
    }

    // Helper to generate the combined payload identifier
    private static string GetSignalKey(string tenantId, string queueName) => $"{tenantId}:{queueName}";


    public void RegisterQueue(string tenantId, string queueName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(tenantId);
        ArgumentException.ThrowIfNullOrWhiteSpace(queueName);

        string signalKey = GetSignalKey(tenantId, queueName);

        Channel<bool> newChannel = CreateSignalChannel();

        Channel<bool> actualChannel =
            _queueSignals.GetOrAdd(
                signalKey,
                newChannel);

        if (ReferenceEquals(actualChannel, newChannel))
        {
            // Exactly one initial reconciliation for a newly registered
            // queue.
            actualChannel.Writer.TryWrite(true);
        }
    }

    public async ValueTask<bool> WaitAsync(
        string tenantId,
        string queueName,
        TimeSpan fallbackTimeout,
        CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(tenantId);
        ArgumentException.ThrowIfNullOrWhiteSpace(queueName);

        if (fallbackTimeout <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(
                nameof(fallbackTimeout),
                "Fallback timeout must be greater than zero.");
        }

        string signalKey = GetSignalKey(tenantId, queueName);

        if (!_queueSignals.TryGetValue(
                signalKey,
                out Channel<bool>? channel))
        {
            RegisterQueue(tenantId, queueName);
            channel = _queueSignals[signalKey];
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
                await ListenAsync(stoppingToken)
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
                    "PostgreSQL queue notification listener disconnected.");

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

    private async Task ListenAsync(
        CancellationToken cancellationToken)
    {
        await using NpgsqlConnection connection =
            await _dataSource
                .OpenConnectionAsync(cancellationToken)
                .ConfigureAwait(false);

        connection.Notification += OnNotification;

        try
        {
            await using NpgsqlCommand command = new(
                $"LISTEN {NotificationChannel};",
                connection);

            await command
                .ExecuteNonQueryAsync(cancellationToken)
                .ConfigureAwait(false);

            _logger.LogInformation(
                "Listening for PostgreSQL queue notifications on channel {Channel}.",
                NotificationChannel);

            // LISTEN is active now. Reconcile state that may have existed
            // before the listener connection was established.
            SignalAllQueues();

            while (!cancellationToken.IsCancellationRequested)
            {
                await connection
                    .WaitAsync(cancellationToken)
                    .ConfigureAwait(false);
            }
        }
        finally
        {
            connection.Notification -= OnNotification;
        }
    }


    private void OnNotification(
        object? sender,
        NpgsqlNotificationEventArgs args)
    {
        if (!string.Equals(
                args.Channel,
                NotificationChannel,
                StringComparison.Ordinal))
        {
            return;
        }

        if (string.IsNullOrWhiteSpace(args.Payload))
        {
            SignalAllQueues();
            return;
        }

        // The Payload from pg_notify is now 'tenant_id:queue_name'
        SignalQueue(args.Payload);
    }

    private void SignalQueue(string payloadKey)
    {
        // Directly matches the pattern in GetSignalKey
        if (_queueSignals.TryGetValue(
                payloadKey,
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