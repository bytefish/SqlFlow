using Microsoft.Extensions.DependencyInjection;
using SqlFlowSdk.Configuration;
using SqlFlowSdk.Core;
using SqlFlowSdk.Workers;

namespace SqlFlowSdk.Extensions;

public class SqlFlowWorkerBuilder
{
    private readonly IServiceCollection _services;
    private readonly SqlFlowRegistry _registry;
    private readonly WorkerConfiguration _workerConfig;

    public SqlFlowWorkerBuilder(IServiceCollection services, SqlFlowRegistry registry, WorkerConfiguration workerConfig)
    {
        _services = services;
        _registry = registry;
        _workerConfig = workerConfig;

        if (!_registry.JobRegistrationsByQueue.ContainsKey(_workerConfig.QueueName))
        {
            _registry.JobRegistrationsByQueue[_workerConfig.QueueName] = new();
        }
    }

    public SqlFlowWorkerBuilder SetConcurrency(int concurrency) { _workerConfig.Concurrency = concurrency; return this; }

    public SqlFlowWorkerBuilder SetPollInterval(double pollIntervalInSeconds) { _workerConfig.PollIntervalInSeconds = pollIntervalInSeconds; return this; }

    public SqlFlowWorkerBuilder SetClaimTimeout(int seconds) { _workerConfig.ClaimTimeoutInSeconds = seconds; return this; }

    public SqlFlowWorkerBuilder SetBatchSize(int batchSize) { _workerConfig.BatchSize = batchSize; return this; }

    public SqlFlowWorkerBuilder SetFatalOnLeaseTimeout(bool fatal) { _workerConfig.FatalOnLeaseTimeout = fatal; return this; }

    public SqlFlowWorkerBuilder SetOnError(Action<Exception> handler) { _workerConfig.OnError = handler; return this; }

    public SqlFlowWorkerBuilder AddJob<TJob, TRequest, TResult>(string jobName, Action<JobOptionsBuilder>? configure = null)
        where TJob : class, IJob<TRequest, TResult>
    {
        var options = new JobOptions(jobName);
        configure?.Invoke(new JobOptionsBuilder(options));

        _services.AddTransient<TJob>();

        if (_registry.Routes.ContainsKey(options.Name))
        {
            throw new InvalidOperationException($"Job name '{options.Name}' has already been used.");
        }

        _registry.Routes[options.Name] = (typeof(TJob), _workerConfig.QueueName);

        _registry.JobRegistrationsByQueue[_workerConfig.QueueName].Add((client, provider) =>
        {
            client.UseJob<TJob, TRequest, TResult>(provider, options.Name, options.MaxAttempts);
            return Task.CompletedTask;
        });

        return this;
    }
}