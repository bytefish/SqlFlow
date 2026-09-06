using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using SqlFlowSdk.Core;

namespace SqlFlowSdk.Workers;

internal sealed class GenericSqlFlowWorker : BackgroundService
{
    private readonly ISqlFlow _client;
    private readonly ISqlFlowDispatcher _dispatcher;
    private readonly IServiceProvider _provider;
    private readonly SqlFlowRegistry _registry;
    private readonly ILogger<GenericSqlFlowWorker> _logger;
    private readonly string _queueName;

    public GenericSqlFlowWorker(
        ISqlFlow client,
        ISqlFlowDispatcher dispatcher,
        IServiceProvider provider,
        SqlFlowRegistry registry,
        ILogger<GenericSqlFlowWorker> logger,
        string queueName)
    {
        _client = client;
        _dispatcher = dispatcher;
        _provider = provider;
        _registry = registry;
        _logger = logger;
        _queueName = queueName;
    }

    protected override async Task ExecuteAsync(
        CancellationToken stoppingToken)
    {
        WorkerConfiguration config = _registry.WorkerConfigs.Single(x => x.QueueName == _queueName);

        _logger.LogInformation(
            "Creating queue {Queue} if it does not exist.",
            _queueName);

        await _client.CreateQueueAsync(
                _queueName,
                stoppingToken)
            .ConfigureAwait(false);

        await RegisterTasksAsync().ConfigureAwait(false);

        WorkerOptions options = new()
        {
            Queue = config.QueueName,
            WorkerId = CreateWorkerId(config.QueueName),
            Concurrency = config.Concurrency,
            BatchSize = config.BatchSize,
            ClaimTimeout = config.ClaimTimeoutInSeconds,
            FatalOnLeaseTimeout = config.FatalOnLeaseTimeout,
            OnError = config.OnError ??
                (exception => _logger.LogError(
                    exception,
                    "Worker error in queue {Queue}.",
                    config.QueueName))
        };

        _logger.LogInformation(
            "Starting worker for queue {Queue} with concurrency {Concurrency}.",
            options.Queue,
            options.Concurrency);

        await _dispatcher.RunWorkerAsync(
                options,
                stoppingToken)
            .ConfigureAwait(false);
    }

    private async Task RegisterTasksAsync()
    {
        if (!_registry.JobRegistrationsByQueue.TryGetValue(
                _queueName,
                out var registrations))
        {
            return;
        }

        foreach (var registration in registrations)
        {
            await registration(_client, _provider)
                .ConfigureAwait(false);
        }
    }

    private static string CreateWorkerId(
        string queueName)
    {
        string suffix = Guid.NewGuid()
            .ToString("N")[..6];

        return $"worker-{queueName}-{suffix}";
    }
}