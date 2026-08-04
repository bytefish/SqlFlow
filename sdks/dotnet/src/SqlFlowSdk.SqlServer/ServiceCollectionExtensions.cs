// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using Microsoft.Data.SqlClient;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using SqlFlowSdk.Core;
using SqlFlowSdk.Database;
using SqlFlowSdk.SqlServer.Database;
using System.Data.Common;

namespace SqlFlowSdk.Extensions;

public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Registers the SQL Server implementation for SqlFlow.
    /// This configures a <see cref="DbDataSource"/> and the <see cref="ISqlFlowDatabase"/> as singletons.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="connectionString">The SQL Server connection string.</param>
    /// <returns>The service collection for chaining.</returns>
    public static IServiceCollection AddSqlFlowSdk(this IServiceCollection services, string connectionString)
    {
        if (string.IsNullOrWhiteSpace(connectionString))
        {
            throw new ArgumentException("Connection string cannot be null or empty.", nameof(connectionString));
        }

        services.AddSingleton<IEventPublisher, SqlFlowEventPublisher>();

        services.TryAddSingleton<ISqlFlow>(sp =>
        {
            ILogger<SqlFlow> logger = sp.GetRequiredService<ILogger<SqlFlow>>();

            DbDataSource dataSource = SqlClientFactory.Instance.CreateDataSource(connectionString);

            return new SqlFlow(logger, dataSource, new SqlServerFlowDatabase());
        });

        return services;
    }
}