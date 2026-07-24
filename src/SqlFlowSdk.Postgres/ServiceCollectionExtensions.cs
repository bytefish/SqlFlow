// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using Npgsql;
using SqlFlowSdk.Core;
using SqlFlowSdk.Database;
using System.Data.Common;

namespace SqlFlowSdk.Extensions;

public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Registers the PostgreSQL implementation for SqlFlow.
    /// This configures a <see cref="DbDataSource"/> and the <see cref="ISqlFlowDatabase"/> as singletons.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="connectionString">The PostgreSQL connection string.</param>
    /// <returns>The service collection for chaining.</returns>
    public static IServiceCollection AddSqlFlowSdk(this IServiceCollection services, string connectionString)
    {
        if (string.IsNullOrWhiteSpace(connectionString))
        {
            throw new ArgumentException("Connection string cannot be null or empty.", nameof(connectionString));
        }

        // Register Publish Abstraction
        services.AddSingleton<IEventPublisher, SqlFlowEventPublisher>();

        services.TryAddSingleton<ISqlFlow>(sp =>
        {            
            NpgsqlDataSource dataSource = NpgsqlDataSource.Create(connectionString);

            return new SqlFlow(sp.GetRequiredService<ILogger<SqlFlow>>(), dataSource, new PostgresFlowDatabase());
        });

        return services;
    }
}