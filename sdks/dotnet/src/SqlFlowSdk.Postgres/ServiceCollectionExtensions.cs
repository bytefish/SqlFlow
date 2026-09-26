using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Npgsql;
using SqlFlowSdk.Core;
using SqlFlowSdk.Database;
using SqlFlowSdk.Extensions;
using SqlFlowSdk.Workers;
using System.Data.Common;

namespace SqlFlowSdk.Postgres;

public static class PostgresServiceCollectionExtensions
{
    /// <summary>
    /// Adds the PostgreSQL SqlFlow provider using an NpgsqlDataSource
    /// that is already registered with dependency injection.
    /// </summary>
    public static SqlFlowServiceBuilder AddSqlFlowPostgres(
        this IServiceCollection services,
        Action<QueueSignalOptions>? configureSignals = null)
    {
        ArgumentNullException.ThrowIfNull(services);

        ConfigureSignalOptions(
            services,
            configureSignals);

        services.TryAddSingleton<DbDataSource>(
            serviceProvider =>
                serviceProvider.GetRequiredService<
                    NpgsqlDataSource>());

        AddPostgresServices(services);

        return new SqlFlowServiceBuilder(services);
    }

    /// <summary>
    /// Adds the PostgreSQL SqlFlow provider using a connection string.
    /// </summary>
    public static SqlFlowServiceBuilder AddSqlFlowPostgres(
            this IServiceCollection services,
            string connectionString,
            Action<QueueSignalOptions>? configureSignals = null)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentException.ThrowIfNullOrWhiteSpace(connectionString);

        ConfigureSignalOptions(services, configureSignals);

        services.TryAddSingleton(_ =>
        {
            var builder = new NpgsqlDataSourceBuilder(connectionString);
            return builder.Build();
        });

        services.TryAddSingleton<DbDataSource>(
            serviceProvider => serviceProvider.GetRequiredService<NpgsqlDataSource>());

        AddPostgresServices(services);

        return new SqlFlowServiceBuilder(services);
    }

    /// <summary>
    /// Adds the PostgreSQL SqlFlow provider using an existing
    /// NpgsqlDataSource instance.
    /// </summary>
    public static SqlFlowServiceBuilder AddSqlFlowPostgres(
        this IServiceCollection services,
        NpgsqlDataSource dataSource,
        Action<QueueSignalOptions>? configureSignals = null)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(dataSource);

        ConfigureSignalOptions(
            services,
            configureSignals);

        /*
         * The explicitly supplied data source replaces any earlier
         * NpgsqlDataSource registration.
         */
        services.Replace(
            ServiceDescriptor.Singleton(dataSource));

        services.Replace(
            ServiceDescriptor.Singleton<DbDataSource>(
                serviceProvider =>
                    serviceProvider.GetRequiredService<
                        NpgsqlDataSource>()));

        AddPostgresServices(services);

        return new SqlFlowServiceBuilder(services);
    }
    private static void AddPostgresServices(IServiceCollection services)
    {
        services.AddRequiredServices();

        services.TryAddSingleton<ISqlFlow, SqlFlow>();

        services.Replace(ServiceDescriptor.Singleton<ISqlFlowDatabase, PostgresFlowDatabase>());

        services.TryAddSingleton<PostgresQueueSignalListener>();

        services.Replace(
            ServiceDescriptor.Singleton<IQueueSignalListener>(
                serviceProvider => serviceProvider.GetRequiredService<PostgresQueueSignalListener>()));

        AddListenerHostedService(services);
    }

    private static void ConfigureSignalOptions(IServiceCollection services, Action<QueueSignalOptions>? configureSignals)
    {
        services.AddOptions<QueueSignalOptions>();
        if (configureSignals is not null)
        {
            services.Configure(configureSignals);
        }
    }

    private static void AddListenerHostedService(IServiceCollection services)
    {
        if (services.Any(descriptor => descriptor.ServiceType == typeof(PostgresQueueSignalListenerRegistrationMarker)))
        {
            return;
        }

        services.AddSingleton<PostgresQueueSignalListenerRegistrationMarker>();

        /*
         * THE FIX:
         * We conditionally return the Postgres listener ONLY if it wasn't replaced by NATS.
         */
        services.AddSingleton<IHostedService>(serviceProvider =>
        {
            var activeListener = serviceProvider.GetRequiredService<IQueueSignalListener>();

            if (activeListener is PostgresQueueSignalListener pgListener)
            {
                return pgListener;
            }

            // NATS (or another provider) took over. Return a dummy service so we 
            // don't waste a database connection running an unused Postgres LISTEN loop.
            return new NoOpHostedService();
        });
    }

    private sealed class PostgresQueueSignalListenerRegistrationMarker;

    /// <summary>
    /// A dummy service that does nothing, used to gracefully cancel out 
    /// the Postgres background loop when another signaling provider is active.
    /// </summary>
    private sealed class NoOpHostedService : IHostedService
    {
        public Task StartAsync(CancellationToken cancellationToken) => Task.CompletedTask;
        public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
    }
}