using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using NATS.Client.Core;
using NATS.Client.JetStream;
using NATS.Net;
using SqlFlowSdk.Core;
using SqlFlowSdk.Extensions; // Required for SqlFlowServiceBuilder
using SqlFlowSdk.Nats.Infrastructure;
using SqlFlowSdk.Signaling;

namespace SqlFlowSdk.Nats;

public static class NatsServiceCollectionExtensions
{
    public static SqlFlowServiceBuilder AddNatsSignaling(this SqlFlowServiceBuilder builder, string natsUrl)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentException.ThrowIfNullOrWhiteSpace(natsUrl);

        builder.Services.TryAddSingleton<INatsConnection>(_ =>
            new NatsConnection(new NatsOpts { Url = natsUrl }));

        AddNatsServices(builder.Services);

        return builder;
    }

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

    private static void AddNatsServices(IServiceCollection services)
    {
        services.TryAddSingleton<INatsJSContext>(serviceProvider =>
        {
            var connection = serviceProvider.GetRequiredService<INatsConnection>();
            return connection.CreateJetStreamContext();
        });

        // Replace the Publisher (Must be Transient to match SqlFlowRequiredServices)
        services.Replace(
            ServiceDescriptor.Transient<IJobPublisher, NatsJobPublisher>());

        services.TryAddSingleton<NatsQueueSignalListener>();

        // Replace the Listener (Must be Singleton)
        services.Replace(
            ServiceDescriptor.Singleton<IQueueSignalListener>(
                serviceProvider => serviceProvider.GetRequiredService<NatsQueueSignalListener>()));
    }
}