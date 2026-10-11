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

        SignalAllSurroundingWork();

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
                command.CommandTimeout = 70;

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
            catch (SqlException ex) when (ex.Number == -2)
            {
                // Timeout expired, loop again
            }
        }
    }

    private void ParseAndSignal(string jsonPayload)
    {
        try
        {
            string? tenantId = ExtractJsonField(jsonPayload, "tenant_id");
            string? queueName = ExtractJsonField(jsonPayload, "queue");

            if (!string.IsNullOrEmpty(tenantId) && !string.IsNullOrEmpty(queueName))
            {
                SignalQueue(GetSignalKey(tenantId, queueName));
                return;
            }
            else if (!string.IsNullOrEmpty(queueName))
            {
                // Fallback for legacy messages containing only queue name
                foreach (var key in _queueSignals.Keys)
                {
                    if (key.EndsWith(":" + queueName, StringComparison.Ordinal))
                    {
                        SignalQueue(key);
                    }
                }
                return;
            }
        }
        catch
        {
            // Fallback
        }

        SignalAllQueues();
    }

    private static string? ExtractJsonField(string json, string fieldName)
    {
        int fieldIdx = json.IndexOf($"\"{fieldName}\":", StringComparison.Ordinal);
        if (fieldIdx < 0) return null;

        int start = json.IndexOf('"', fieldIdx + fieldName.Length + 3);
        if (start < 0) return null;

        int end = json.IndexOf('"', start + 1);
        if (end <= start) return null;

        return json.Substring(start + 1, end - start - 1);
    }

    private void SignalQueue(string signalKey)
    {
        if (_queueSignals.TryGetValue(
                signalKey,
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

    private async Task DelayBeforeReconnectAsync(CancellationToken cancellationToken)
    {
        try
        {
            await Task.Delay(_options.ReconnectDelay, cancellationToken)
                .ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
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