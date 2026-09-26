// Licensed under the MIT license. See LICENSE file in the project root for full license information.
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using Testcontainers.PostgreSql;

namespace SqlFlowSdk.NatsAiSample.Docker;

public static class DockerContainers
{
    public static PostgreSqlContainer PostgresContainer = new PostgreSqlBuilder("postgres:18")
        .WithBindMount(Path.Combine(Directory.GetCurrentDirectory(), "../../../../sql/ssf-postgres.sql"), "/docker-entrypoint-initdb.d/1-ssf-postgres.sql")
        .WithUsername("postgres")
        .WithPassword("password")
        .Build();

    // Use the generic ContainerBuilder to easily append the "-js" command needed for JetStream
    public static IContainer NatsContainer = new ContainerBuilder("nats:2.10")
        .WithPortBinding(4222, true)
        .WithCommand("-js")
        .Build();

    public static async Task StartAllContainersAsync()
    {
        await Task.WhenAll(
            PostgresContainer.StartAsync(),
            NatsContainer.StartAsync()
        );
    }

    public static async Task StopAllContainersAsync()
    {
        await Task.WhenAll(
            PostgresContainer.StopAsync(),
            NatsContainer.StopAsync()
        );
    }

    public static string GetNatsUrl()
    {
        return $"nats://{NatsContainer.Hostname}:{NatsContainer.GetMappedPublicPort(4222)}";
    }
}