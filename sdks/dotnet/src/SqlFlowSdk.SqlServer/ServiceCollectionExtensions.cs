// Licensed under the MIT license.
// See LICENSE file in the project root for full license information.

using Microsoft.Data.SqlClient;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using SqlFlowSdk.Core;
using SqlFlowSdk.Database;
using SqlFlowSdk.Extensions;
using SqlFlowSdk.SqlServer.Database;
using System.Data.Common;

namespace SqlFlowSdk.SqlServer;

public static class SqlServerServiceCollectionExtensions
{
    /// <summary>
    /// Adds the SQL Server SqlFlow provider using a connection string.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="connectionString">
    /// The SQL Server connection string.
    /// </param>
    /// <param name="configureSignals">
    /// Optional configuration for queue signals and reconciliation.
    /// </param>
    /// <returns>
    /// A fluent builder for registering SqlFlow workers.
    /// </returns>
    public static SqlFlowServiceBuilder AddSqlFlowSqlServer(
        this IServiceCollection services,
        string connectionString,
        Action<QueueSignalOptions>? configureSignals = null)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentException.ThrowIfNullOrWhiteSpace(connectionString);

        ConfigureSignalOptions(
            services,
            configureSignals);

        services.TryAddSingleton<DbDataSource>(
            _ => SqlClientFactory.Instance
                .CreateDataSource(connectionString));

        AddSqlServerServices(services);

        return new SqlFlowServiceBuilder(services);
    }

    /// <summary>
    /// Adds the SQL Server SqlFlow provider using an existing
    /// DbDataSource.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="dataSource">
    /// An existing SQL Server DbDataSource.
    /// </param>
    /// <param name="configureSignals">
    /// Optional configuration for queue signals and reconciliation.
    /// </param>
    /// <returns>
    /// A fluent builder for registering SqlFlow workers.
    /// </returns>
    public static SqlFlowServiceBuilder AddSqlFlowSqlServer(
        this IServiceCollection services,
        DbDataSource dataSource,
        Action<QueueSignalOptions>? configureSignals = null)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(dataSource);

        ValidateDataSource(dataSource);

        ConfigureSignalOptions(
            services,
            configureSignals);

        services.Replace(ServiceDescriptor.Singleton(dataSource));

        AddSqlServerServices(services);

        return new SqlFlowServiceBuilder(services);
    }

    /// <summary>
    /// Adds the SQL Server SqlFlow provider using a DbDataSource that
    /// has already been registered in dependency injection.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configureSignals">
    /// Optional configuration for queue signals and reconciliation.
    /// </param>
    /// <returns>
    /// A fluent builder for registering SqlFlow workers.
    /// </returns>
    public static SqlFlowServiceBuilder AddSqlFlowSqlServer(
        this IServiceCollection services,
        Action<QueueSignalOptions>? configureSignals = null)
    {
        ArgumentNullException.ThrowIfNull(services);

        ConfigureSignalOptions(
            services,
            configureSignals);

        AddSqlServerServices(services);

        return new SqlFlowServiceBuilder(services);
    }

    private static void AddSqlServerServices(
        IServiceCollection services)
    {
        services.AddRequiredServices();

        services.TryAddSingleton<ISqlFlow, SqlFlow>();

        services.TryAddSingleton<
            IEventPublisher,
            SqlFlowEventPublisher>();

        services.Replace(
            ServiceDescriptor.Singleton<
                ISqlFlowDatabase,
                SqlServerFlowDatabase>());

        services.TryAddSingleton<
            SqlServerQueueSignalListener>();

        services.Replace(
            ServiceDescriptor.Singleton<IQueueSignalListener>(
                serviceProvider =>
                    serviceProvider.GetRequiredService<
                        SqlServerQueueSignalListener>()));

        AddListenerHostedService(services);
    }

    private static void ConfigureSignalOptions(
        IServiceCollection services,
        Action<QueueSignalOptions>? configureSignals)
    {
        services.AddOptions<QueueSignalOptions>();

        if (configureSignals is not null)
        {
            services.Configure(configureSignals);
        }
    }

    private static void AddListenerHostedService(
        IServiceCollection services)
    {
        bool alreadyRegistered = services.Any(
            descriptor =>
                descriptor.ServiceType ==
                typeof(
                    SqlServerQueueSignalListenerRegistrationMarker));

        if (alreadyRegistered)
        {
            return;
        }

        services.AddSingleton<
            SqlServerQueueSignalListenerRegistrationMarker>();

        services.AddSingleton<IHostedService>(
            serviceProvider =>
                serviceProvider.GetRequiredService<
                    SqlServerQueueSignalListener>());
    }

    private static void ValidateDataSource(
        DbDataSource dataSource)
    {
        if (string.IsNullOrWhiteSpace(dataSource.ConnectionString))
        {
            throw new ArgumentException(
                "The SQL Server data source must have a connection string.",
                nameof(dataSource));
        }
    }

    private sealed class
        SqlServerQueueSignalListenerRegistrationMarker;
}