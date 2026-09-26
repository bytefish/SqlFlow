// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using NATS.Client.Core;
using NATS.Client.JetStream;
using NATS.Net;
using SqlFlowSdk.Core;
using SqlFlowSdk.Extensions;
using SqlFlowSdk.Nats.Database;
using SqlFlowSdk.Nats.Infrastructure;

namespace SqlFlowSdk.Nats;

public static class NatsServiceCollectionExtensions
{
    /// <summary>
    /// Overrides the default PostgreSQL LISTEN/NOTIFY signaling layer 
    /// with NATS JetStream using a NATS URL.
    /// </summary>
    public static SqlFlowServiceBuilder AddNatsSignaling(
        this SqlFlowServiceBuilder builder,
        string natsUrl)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentException.ThrowIfNullOrWhiteSpace(natsUrl);

        builder.Services.TryAddSingleton<INatsConnection>(_ =>
            new NatsConnection(new NatsOpts { Url = natsUrl }));

        AddNatsServices(builder.Services);

        return builder;
    }

    /// <summary>
    /// Overrides the default PostgreSQL LISTEN/NOTIFY signaling layer 
    /// with NATS JetStream using explicit NatsOpts.
    /// </summary>
    public static SqlFlowServiceBuilder AddNatsSignaling(
        this SqlFlowServiceBuilder builder,
        NatsOpts options)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(options);

        builder.Services.TryAddSingleton<INatsConnection>(_ =>
            new NatsConnection(options));

        AddNatsServices(builder.Services);

        return builder;
    }

    /// <summary>
    /// Overrides the default PostgreSQL LISTEN/NOTIFY signaling layer 
    /// using an INatsConnection that is already registered with dependency injection.
    /// </summary>
    public static SqlFlowServiceBuilder AddNatsSignaling(
        this SqlFlowServiceBuilder builder)
    {
        ArgumentNullException.ThrowIfNull(builder);

        AddNatsServices(builder.Services);

        return builder;
    }

    private static void AddNatsServices(IServiceCollection services)
    {
        /*
         * Register the JetStream V3 context derived from the NATS connection.
         */
        services.TryAddSingleton<INatsJSContext>(serviceProvider =>
        {
            var connection = serviceProvider.GetRequiredService<INatsConnection>();
            return connection.CreateJetStreamContext();
        });

        /*
         * Replace the default IJobPublisher using a Transient lifetime 
         * to match SqlFlowRequiredServices, whilst using NatsJobPublisher 
         * to fire the NATS wake-up signal alongside the DB commit.
         */
        services.Replace(
            ServiceDescriptor.Transient<IJobPublisher, NatsJobPublisher>());

        /*
         * Register the NATS listener implementation.
         */
        services.TryAddSingleton<NatsQueueSignalListener>();

        /*
         * Replace IQueueSignalListener to resolve to the NATS listener
         * instead of PostgresQueueSignalListener.
         */
        services.Replace(
            ServiceDescriptor.Singleton<IQueueSignalListener>(
                serviceProvider =>
                    serviceProvider.GetRequiredService<NatsQueueSignalListener>()));

        AddListenerHostedService(services);
    }

    private static void AddListenerHostedService(IServiceCollection services)
    {
        if (services.Any(
                descriptor =>
                    descriptor.ServiceType ==
                    typeof(NatsQueueSignalListenerRegistrationMarker)))
        {
            return;
        }

        services.AddSingleton<NatsQueueSignalListenerRegistrationMarker>();

        /*
         * Register the NATS listener as a hosted background service, 
         * matching the lifecycle pattern used by Postgres.
         */
        services.AddSingleton<IHostedService>(
            serviceProvider =>
                serviceProvider.GetRequiredService<
                    NatsQueueSignalListener>());
    }

    private sealed class NatsQueueSignalListenerRegistrationMarker;
}