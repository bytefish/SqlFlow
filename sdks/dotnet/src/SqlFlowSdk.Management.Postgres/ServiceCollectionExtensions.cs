// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Npgsql;
using SqlFlowSdk.Database;
using SqlFlowSdk.Management.Postgres.Services;
using System.Data.Common;

namespace SqlFlowSdk.Management.Postgres;

public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Registers the PostgreSQL implementation for the SqlFlow Dashboard.
    /// This configures a <see cref="DbDataSource"/> and the <see cref="ISqlFlowDatabase"/> as singletons.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="connectionString">The PostgreSQL connection string.</param>
    /// <returns>The service collection for chaining.</returns>
    public static IServiceCollection AddSqlFlowQueryApi(this IServiceCollection services, string connectionString)
    {
        if (string.IsNullOrWhiteSpace(connectionString))
        {
            throw new ArgumentException("Connection string cannot be null or empty.", nameof(connectionString));
        }

        services.TryAddSingleton<ISqlFlowQueryService>(sp =>
        {
            NpgsqlDataSource dataSource = NpgsqlDataSource.Create(connectionString);

            return new PostgresSqlFlowQueryService(dataSource);
        });

        return services;
    }

    /// <summary>
    /// Registers the PostgreSQL implementation for the SqlFlow Dashboard.
    /// This configures a <see cref="DbDataSource"/> and the <see cref="ISqlFlowDatabase"/> as singletons.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="connectionString">The PostgreSQL connection string.</param>
    /// <returns>The service collection for chaining.</returns>
    public static IServiceCollection AddSqlFlowAdminApi(this IServiceCollection services, string connectionString)
    {
        if (string.IsNullOrWhiteSpace(connectionString))
        {
            throw new ArgumentException("Connection string cannot be null or empty.", nameof(connectionString));
        }

        services.TryAddSingleton<ISqlFlowAdminService>(sp =>
        {
            NpgsqlDataSource dataSource = NpgsqlDataSource.Create(connectionString);

            return new PostgresSqlFlowAdminService(dataSource, new PostgresFlowDatabase());
        });

        return services;
    }
}