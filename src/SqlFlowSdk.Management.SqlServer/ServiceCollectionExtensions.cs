// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using Microsoft.Data.SqlClient;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using SqlFlowSdk.Database;
using SqlFlowSdk.Management;
using SqlFlowSdk.Management.SqlServer.Services;
using System.Data.Common;

namespace SqlFlowSdk;

public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Registers the SQL Server implementation for the SqlFlow Dashboard.
    /// This configures a <see cref="DbDataSource"/> and the <see cref="ISqlFlowDatabase"/> as singletons.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="connectionString">The SQL Server connection string.</param>
    /// <returns>The service collection for chaining.</returns>
    public static IServiceCollection AddSqlFlowDashboard(this IServiceCollection services, string connectionString)
    {
        if (string.IsNullOrWhiteSpace(connectionString))
        {
            throw new ArgumentException("Connection string cannot be null or empty.", nameof(connectionString));
        }

        services.TryAddSingleton<ISqlFlowQueryService>(sp =>
        {
            var dataSource = SqlClientFactory.Instance.CreateDataSource(connectionString);

            return new SqlServerSqlManagementService(dataSource);
        });

        return services;
    }

    /// <summary>
    /// Registers the SQL Server implementation for the SqlFlow Dashboard.
    /// This configures a <see cref="DbDataSource"/> and the <see cref="ISqlFlowDatabase"/> as singletons.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="connectionString">The SQL Server connection string.</param>
    /// <returns>The service collection for chaining.</returns>
    public static IServiceCollection AddSqlFlowAdminApi(this IServiceCollection services, string connectionString)
    {
        if (string.IsNullOrWhiteSpace(connectionString))
        {
            throw new ArgumentException("Connection string cannot be null or empty.", nameof(connectionString));
        }

        services.TryAddSingleton<ISqlFlowAdminService>(sp =>
        {
            var dataSource = SqlClientFactory.Instance.CreateDataSource(connectionString);

            return new SqlServerSqlFlowAdminService(dataSource, sp.GetRequiredService<ISqlFlowDatabase>());
        });

        return services;
    }
}