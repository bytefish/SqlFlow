// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using Testcontainers.PostgreSql;

namespace SqlFlowSdk.AiSample.Docker
{
    public static class DockerContainers
    {
        public static PostgreSqlContainer PostgresContainer = new PostgreSqlBuilder("postgres:18")
            // Mount SQL Scripts 
            .WithBindMount(Path.Combine(Directory.GetCurrentDirectory(), "../../../../sql/ssf-postgres.sql"), "/docker-entrypoint-initdb.d/1-ssf-postgres.sql")
            // Set Username and Password
            .WithUsername("postgres")
            .WithPassword("password")
            .Build();

        public static async Task StartAllContainersAsync()
        {
            await PostgresContainer.StartAsync();
        }

        public static async Task StopAllContainersAsync()
        {
            await PostgresContainer.StopAsync();
        }
    }
}