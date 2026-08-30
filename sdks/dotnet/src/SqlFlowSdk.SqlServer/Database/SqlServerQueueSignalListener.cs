using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using SqlFlowSdk.Core;
using System.Collections.Concurrent;
using System.Data;
using System.Threading.Channels;
using Microsoft.Data.SqlClient;

namespace SqlFlowSdk.SqlServer;

public sealed class SqlServerQueueSignalListener :
    BackgroundService,
    IQueueSignalListener
{
    public const string NotificationServiceName = "ssf_NotificationService";
    public const string NotificationQueueName = "ssf.NotificationQueue";

    private readonly string _connectionString;
    private readonly QueueSignalOptions _options;
    private readonly ILogger<SqlServerQueueSignalListener> _logger;

    private readonly ConcurrentDictionary<string, Channel<bool>>
        _queueSignals = new(StringComparer.Ordinal);

    public SqlServerQueueSignalListener(
        string connectionString,
        IOptions<QueueSignalOptions> options,
        ILogger<SqlServerQueueSignalListener> logger)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(connectionString);
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(logger);

        _connectionString = connectionString;
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
            // Initial reconciliation for newly registered queue
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
                    "SQL Server Service Broker notification listener disconnected.");

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
        await using SqlConnection connection = new(_connectionString);
        await connection.OpenAsync(cancellationToken).ConfigureAwait(false);

        _logger.LogInformation(
            "Listening for SQL Server Service Broker queue notifications on {Queue}.",
            NotificationQueueName);

        // Reconcile state that may have existed before connection
        SignalAllSurroundingWork();

        // Using WAITFOR (RECEIVE) on the Service Broker Queue to block efficiently without CPU spinning
        string waitSql = @"
            WAITFOR (
                RECEIVE TOP(1) message_body 
                FROM ssf.NotificationQueue
            ), TIMEOUT 60000;";

        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                await using SqlCommand command = new(waitSql, connection);
                command.CommandTimeout = 70; // Slightly higher than WAITFOR timeout

                await using SqlDataReader reader = await command
                    .ExecuteReaderAsync(CommandBehavior.SequentialAccess, cancellationToken)
                    .ConfigureAwait(false);

                if (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                {
                    if (!reader.IsDBNull(0))
                    {
                        string jsonPayload = reader.GetString(0);
                        ParseAndSignal(jsonPayload);
                    }
                }
            }
            catch (SqlException ex) when (ex.Number == -2) // Timeout expired
            {
                // Normal timeout, loop again
            }
        }
    }

    private void ParseAndSignal(string jsonPayload)
    {
        try
        {
            // Simple string extraction or JSON parse for {"queue":"...","event":"..."}
            int queueIdx = jsonPayload.IndexOf("\"queue\":", StringComparison.Ordinal);
            if (queueIdx >= 0)
            {
                int start = jsonPayload.IndexOf('"', queueIdx + 8);
                if (start >= 0)
                {
                    int end = jsonPayload.IndexOf('"', start + 1);
                    if (end > start)
                    {
                        string queueName = jsonPayload.Substring(start + 1, end - start - 1);
                        SignalQueue(queueName);
                        return;
                    }
                }
            }
        }
        catch
        {
            // Fallback if payload cannot be parsed
        }

        SignalAllQueues();
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

    private void SignalAllSurroundingWork()
    {
        SignalAllQueues();
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