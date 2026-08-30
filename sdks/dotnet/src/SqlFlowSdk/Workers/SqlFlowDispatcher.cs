using Microsoft.Extensions.Logging;
using SqlFlowSdk.Core;
using System.Collections.Concurrent;
using System.Threading.Channels;

namespace SqlFlowSdk.Workers;

public sealed class SqlFlowDispatcher : ISqlFlowDispatcher
{
    private readonly ISqlFlow _client;
    private readonly IQueueSignalListener _signals;
    private readonly QueueSignalOptions _signalOptions;
    private readonly ILogger<SqlFlowDispatcher> _logger;

    private readonly ConcurrentDictionary<string, byte> _activeQueues =
        new(StringComparer.Ordinal);

    public SqlFlowDispatcher(
        ISqlFlow client,
        IQueueSignalListener signals,
        QueueSignalOptions signalOptions,
        ILogger<SqlFlowDispatcher> logger)
    {
        ArgumentNullException.ThrowIfNull(client);
        ArgumentNullException.ThrowIfNull(signals);
        ArgumentNullException.ThrowIfNull(signalOptions);
        ArgumentNullException.ThrowIfNull(logger);

        signalOptions.Validate();

        _client = client;
        _signals = signals;
        _signalOptions = signalOptions;
        _logger = logger;
    }

    public async Task RunWorkerAsync(
        WorkerOptions options,
        CancellationToken cancellationToken)
    {
        ValidateOptions(options);

        if (!_activeQueues.TryAdd(options.Queue, 0))
        {
            throw new InvalidOperationException(
                $"A worker for queue '{options.Queue}' is already running.");
        }

        try
        {
            _signals.RegisterQueue(options.Queue);

            await RunQueueAsync(
                    options,
                    cancellationToken)
                .ConfigureAwait(false);
        }
        finally
        {
            _activeQueues.TryRemove(options.Queue, out _);
        }
    }

    private async Task RunQueueAsync(
        WorkerOptions options,
        CancellationToken cancellationToken)
    {
        int capacity = options.Concurrency;

        Channel<ClaimedTask> executionQueue =
            Channel.CreateBounded<ClaimedTask>(
                new BoundedChannelOptions(capacity)
                {
                    SingleWriter = true,
                    SingleReader = capacity == 1,
                    FullMode = BoundedChannelFullMode.Wait,
                    AllowSynchronousContinuations = false
                });

        using SemaphoreSlim availableCapacity =
            new(capacity, capacity);

        Task producer = ProduceAsync(
            options,
            executionQueue.Writer,
            availableCapacity,
            cancellationToken);

        Task[] consumers = Enumerable
            .Range(0, capacity)
            .Select(_ => ConsumeAsync(
                options,
                executionQueue.Reader,
                availableCapacity,
                cancellationToken))
            .ToArray();

        try
        {
            await producer.ConfigureAwait(false);
        }
        finally
        {
            executionQueue.Writer.TryComplete();
        }

        await Task.WhenAll(consumers).ConfigureAwait(false);
    }

    private async Task ProduceAsync(
        WorkerOptions options,
        ChannelWriter<ClaimedTask> writer,
        SemaphoreSlim availableCapacity,
        CancellationToken cancellationToken)
    {
        int maximumBatchSize = Math.Min(
            options.BatchSize ?? options.Concurrency,
            options.Concurrency);

        bool queueMayContainWork = true;

        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                if (!queueMayContainWork)
                {
                    await _signals.WaitAsync(
                            options.Queue,
                            _signalOptions.ReconciliationInterval,
                            cancellationToken)
                        .ConfigureAwait(false);

                    queueMayContainWork = true;
                }

                await availableCapacity
                    .WaitAsync(cancellationToken)
                    .ConfigureAwait(false);

                int reservedSlots = 1;
                int submittedTasks = 0;

                try
                {
                    while (reservedSlots < maximumBatchSize &&
                           availableCapacity.Wait(0))
                    {
                        reservedSlots++;
                    }

                    IReadOnlyList<ClaimedTask> tasks =
                        (await _client.ClaimTasksAsync(
                                options.Queue,
                                options.WorkerId,
                                cancellationToken,
                                options.ClaimTimeout,
                                reservedSlots)
                            .ConfigureAwait(false))
                        .ToList();

                    int unusedSlots =
                        reservedSlots - tasks.Count;

                    if (unusedSlots > 0)
                    {
                        availableCapacity.Release(unusedSlots);
                        reservedSlots -= unusedSlots;
                    }

                    foreach (ClaimedTask task in tasks)
                    {
                        await writer.WriteAsync(
                                task,
                                cancellationToken)
                            .ConfigureAwait(false);

                        submittedTasks++;
                    }

                    // No task was found. Wait for NOTIFY or reconciliation.
                    if (tasks.Count == 0)
                    {
                        queueMayContainWork = false;
                        continue;
                    }

                    // A full claim result means there may be more database
                    // work. Loop immediately, subject to available capacity.
                    queueMayContainWork = tasks.Count == maximumBatchSize;
                }
                catch
                {
                    int slotsNotSubmitted =
                        reservedSlots - submittedTasks;

                    if (slotsNotSubmitted > 0)
                    {
                        availableCapacity.Release(
                            slotsNotSubmitted);
                    }

                    throw;
                }
            }
            catch (OperationCanceledException)
                when (cancellationToken.IsCancellationRequested)
            {
                return;
            }
            catch (Exception exception)
            {
                ReportError(options, exception);

                queueMayContainWork = false;
            }
        }
    }

    private async Task ConsumeAsync(
        WorkerOptions options,
        ChannelReader<ClaimedTask> reader,
        SemaphoreSlim availableCapacity,
        CancellationToken cancellationToken)
    {
        try
        {
            await foreach (
                ClaimedTask task in
                reader.ReadAllAsync(cancellationToken)
                    .ConfigureAwait(false))
            {
                try
                {
                    await _client.ExecuteTaskAsync(
                            task,
                            options.Queue,
                            options.ClaimTimeout,
                            cancellationToken,
                            options.FatalOnLeaseTimeout)
                        .ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                    when (cancellationToken.IsCancellationRequested)
                {
                    return;
                }
                catch (Exception exception)
                {
                    ReportError(options, exception);
                }
                finally
                {
                    // This wakes the producer if all execution slots
                    // were previously occupied.
                    availableCapacity.Release();
                }
            }
        }
        catch (OperationCanceledException)
            when (cancellationToken.IsCancellationRequested)
        {
            // Normal shutdown.
        }
    }

    private void ReportError(
        WorkerOptions options,
        Exception exception)
    {
        if (options.OnError is not null)
        {
            try
            {
                options.OnError(exception);
            }
            catch (Exception callbackException)
            {
                _logger.LogError(
                    callbackException,
                    "The worker error callback failed for queue {Queue}.",
                    options.Queue);
            }

            return;
        }

        _logger.LogError(
            exception,
            "Worker error in queue {Queue}.",
            options.Queue);
    }

    private static void ValidateOptions(
        WorkerOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentException.ThrowIfNullOrWhiteSpace(options.Queue);
        ArgumentException.ThrowIfNullOrWhiteSpace(options.WorkerId);

        if (options.Concurrency <= 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(options.Concurrency),
                "Concurrency must be greater than zero.");
        }

        if (options.BatchSize is <= 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(options.BatchSize),
                "BatchSize must be greater than zero.");
        }

        if (options.ClaimTimeout <= 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(options.ClaimTimeout),
                "ClaimTimeout must be greater than zero.");
        }
    }
}