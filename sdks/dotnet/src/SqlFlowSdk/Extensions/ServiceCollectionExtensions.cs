// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using SqlFlowSdk.Core;
using SqlFlowSdk.Database;
using SqlFlowSdk.Workers;

namespace SqlFlowSdk.Extensions;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddSqlFlowWorker(this IServiceCollection services, string queueName, Action<SqlFlowWorkerBuilder> configure)
    {
        var registryDescriptor = services.FirstOrDefault(d => d.ServiceType == typeof(SqlFlowRegistry));

        SqlFlowRegistry registry;

        if (registryDescriptor == null)
        {
            registry = new SqlFlowRegistry();
            services.AddSingleton(registry);
            services.AddTransient<IJobPublisher, SqlFlowJobPublisher>();
        }
        else
        {
            registry = (SqlFlowRegistry)registryDescriptor.ImplementationInstance!;
        }

        var workerConfig = new WorkerConfiguration { QueueName = queueName };

        registry.WorkerConfigs.Add(workerConfig);

        var builder = new SqlFlowWorkerBuilder(services, registry, workerConfig);

        configure(builder);

        // One to One Pattern. One Worker per Queue. This simplifies the design and
        // avoids complexities of multiple workers consuming from the same queue.
        services.AddSingleton<IHostedService>(sp =>
        {
            return new GenericSqlFlowWorker(
                client: sp.GetRequiredService<ISqlFlow>(),
                provider: sp,
                registry: sp.GetRequiredService<SqlFlowRegistry>(),
                logger: sp.GetRequiredService<ILogger<GenericSqlFlowWorker>>(),
                queueName: queueName
            );
        });

        return services;
    }
}