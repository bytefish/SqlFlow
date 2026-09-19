using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using SqlFlowSdk.Core;
using SqlFlowSdk.Workers;

namespace SqlFlowSdk.Extensions;

public sealed class SqlFlowServiceBuilder
{
    public SqlFlowServiceBuilder(
        IServiceCollection services)
    {
        Services = services
            ?? throw new ArgumentNullException(nameof(services));
    }

    internal IServiceCollection Services { get; }

    /// <summary>
    /// Adds a worker for the specified queue.
    /// </summary>
    public SqlFlowServiceBuilder AddWorker(
        string queueName,
        Action<SqlFlowWorkerBuilder> configure)
    {
        Services.AddSqlFlowWorker(
            queueName,
            configure);

        return this;
    }

    /// <summary>
    /// Adds a worker using the default worker configuration.
    /// </summary>
    public SqlFlowServiceBuilder AddWorker(
        string queueName)
    {
        return AddWorker(
            queueName,
            static _ => { });
    }
}

public static class SqlFlowRequiredServices
{
    public static IServiceCollection AddRequiredServices(
        this IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);

        /*
         * Register options through the normal options infrastructure.
         *
         * Validation is performed when QueueSignalOptions is first resolved.
         */
        services
            .AddOptions<QueueSignalOptions>()
            .Validate(
                options =>
                {
                    try
                    {
                        options.Validate();
                        return true;
                    }
                    catch
                    {
                        return false;
                    }
                },
                "The SqlFlow queue signal configuration is invalid.");

        /*
         * Expose the options value directly for classes that do not need
         * IOptions<QueueSignalOptions>.
         */
        services.TryAddSingleton(serviceProvider =>
            serviceProvider
                .GetRequiredService<IOptions<QueueSignalOptions>>()
                .Value);

        /*
         * This is the provider-independent in-process signal dispatcher.
         *
         * It does not contain Npgsql or SQL Server-specific code.
         */
        services.TryAddSingleton<
            ISqlFlowDispatcher,
            SqlFlowDispatcher>();

        services.TryAddTransient<
            IJobPublisher,
            SqlFlowJobPublisher>();

        services.TryAddTransient<
            IEventPublisher,
            SqlFlowEventPublisher>();

        EnsureRegistry(services);

        return services;
    }

    internal static SqlFlowRegistry EnsureRegistry(
        IServiceCollection services)
    {
        ServiceDescriptor? descriptor =
            services.FirstOrDefault(
                candidate =>
                    candidate.ServiceType ==
                    typeof(SqlFlowRegistry));

        if (descriptor?.ImplementationInstance
            is SqlFlowRegistry registry)
        {
            return registry;
        }

        if (descriptor is not null)
        {
            throw new InvalidOperationException(
                $"{nameof(SqlFlowRegistry)} must be registered as an " +
                "implementation instance because workers are configured " +
                "while the service collection is being built.");
        }

        registry = new SqlFlowRegistry();

        services.AddSingleton(registry);

        return registry;
    }
}

internal static class SqlFlowWorkerServiceCollectionExtensions
{
    internal static IServiceCollection AddSqlFlowWorker(
        this IServiceCollection services,
        string queueName,
        Action<SqlFlowWorkerBuilder> configure)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentException.ThrowIfNullOrWhiteSpace(queueName);
        ArgumentNullException.ThrowIfNull(configure);

        SqlFlowRegistry registry =
            SqlFlowRequiredServices.EnsureRegistry(services);

        if (registry.WorkerConfigs.Any(
                configuration =>
                    string.Equals(
                        configuration.QueueName,
                        queueName,
                        StringComparison.Ordinal)))
        {
            throw new InvalidOperationException(
                $"A SqlFlow worker for queue '{queueName}' " +
                "is already registered.");
        }

        var workerConfiguration =
            new WorkerConfiguration
            {
                QueueName = queueName
            };

        var builder = new SqlFlowWorkerBuilder(
            services,
            registry,
            workerConfiguration);

        /*
         * First apply and validate the user configuration.
         *
         * Do not add the configuration to the registry before this call.
         * Otherwise, an exception raised by configure could leave a
         * partially configured worker in the registry.
         */
        configure(builder);

        ValidateWorkerConfiguration(workerConfiguration);

        registry.WorkerConfigs.Add(workerConfiguration);

        /*
         * AddSingleton is intentional.
         *
         * Multiple workers are registered using the same IHostedService
         * service type. The host will resolve and start all of them.
         */
        services.AddSingleton<IHostedService>(
            serviceProvider =>
                new GenericSqlFlowWorker(
                    client: serviceProvider
                        .GetRequiredService<ISqlFlow>(),

                    dispatcher: serviceProvider
                        .GetRequiredService<ISqlFlowDispatcher>(),

                    provider: serviceProvider,

                    registry: serviceProvider
                        .GetRequiredService<SqlFlowRegistry>(),

                    logger: serviceProvider
                        .GetRequiredService<
                            ILogger<GenericSqlFlowWorker>>(),

                    queueName: queueName));

        return services;
    }

    private static void ValidateWorkerConfiguration(
        WorkerConfiguration configuration)
    {
        if (configuration.Concurrency <= 0)
        {
            throw new InvalidOperationException(
                $"Worker concurrency for queue " +
                $"'{configuration.QueueName}' must be greater than zero.");
        }

        if (configuration.BatchSize is <= 0)
        {
            throw new InvalidOperationException(
                $"Worker batch size for queue " +
                $"'{configuration.QueueName}' must be greater than zero.");
        }

        if (configuration.ClaimTimeoutInSeconds <= 0)
        {
            throw new InvalidOperationException(
                $"Worker claim timeout for queue " +
                $"'{configuration.QueueName}' must be greater than zero.");
        }
    }
}