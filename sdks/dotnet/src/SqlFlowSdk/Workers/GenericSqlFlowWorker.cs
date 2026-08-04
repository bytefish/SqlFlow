// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using SqlFlowSdk;
using SqlFlowSdk.Core;
using SqlFlowSdk.Workers;

namespace SqlFlowSdk.Workers;

/// <summary>
/// A Generic Worker that is responsible for polling a specific queue and executing tasks from that queue using the SqlFlow Client.
/// </summary>
internal class GenericSqlFlowWorker : BackgroundService
{
    private readonly ISqlFlow _client;
    private readonly IServiceProvider _provider;
    private readonly SqlFlowRegistry _registry;
    private readonly ILogger<GenericSqlFlowWorker> _logger;
    private readonly string _queueName;

    public GenericSqlFlowWorker(
        ISqlFlow client,
        IServiceProvider provider,
        SqlFlowRegistry registry,
        ILogger<GenericSqlFlowWorker> logger,
        string queueName)
    {
        _client = client;
        _provider = provider;
        _registry = registry;
        _logger = logger;
        _queueName = queueName;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("Create Queue if not exists: '{Queue}'", _queueName);
        
        await _client.CreateQueueAsync(_queueName, stoppingToken);

        var config = _registry.WorkerConfigs.First(w => w.QueueName == _queueName);

        _logger.LogInformation("Starting Worker for Queue '{Queue}' (Concurrency: {Concurrency})",
            config.QueueName, config.Concurrency);

        if (_registry.JobRegistrationsByQueue.TryGetValue(_queueName, out var registrations))
        {
            foreach (var registration in registrations)
            {
                await registration(_client, _provider);
            }
        }

        var workerOptions = new WorkerOptions
        {
            Queue = config.QueueName,
            WorkerId = $"worker-{_queueName}-{Guid.NewGuid().ToString("N")[..6]}",
            Concurrency = config.Concurrency,
            PollInterval = config.PollIntervalInSeconds,
            BatchSize = config.BatchSize,
            ClaimTimeout = config.ClaimTimeoutInSeconds,
            FatalOnLeaseTimeout = config.FatalOnLeaseTimeout,
            OnError = config.OnError ?? (ex => _logger.LogError(ex, "Critical error raised in Queue {Queue}", config.QueueName))
        };

        var innerWorker = new SqlFlowWorker(workerOptions, _client);

        await innerWorker.ExecuteAsync(stoppingToken);
    }
}