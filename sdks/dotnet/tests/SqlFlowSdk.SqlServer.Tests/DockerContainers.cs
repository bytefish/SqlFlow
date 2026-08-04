// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using DotNet.Testcontainers;
using Testcontainers.MsSql;

namespace SqlFlowSdk.SqlServer.Tests
{
    public class DockerContainers
    {
        public static MsSqlContainer SqlServerContainer = new MsSqlBuilder("mcr.microsoft.com/mssql/server:2022-latest")
            .WithPassword("P@ssw0rd123!")
            .WithLogger(ConsoleLogger.Instance)
            .Build();

        public static async Task StartAllContainersAsync()
        {
            await SqlServerContainer.StartAsync();

            string scriptContent = await File.ReadAllTextAsync(Path.Combine(AppContext.BaseDirectory, "Resources\\sql\\ssf-sqlserver.sql"));

            await SqlServerContainer.ExecScriptAsync(scriptContent);
        }

        public static async Task StopAllContainersAsync()
        {
            await SqlServerContainer.StopAsync();
        }
    }
}