// Licensed under the MIT license.
// See LICENSE file in the project root for full license information.

using DotNet.Testcontainers;
using Microsoft.Data.SqlClient;
using Testcontainers.MsSql;

namespace SqlFlowSdk.SqlServer.Tests;

public static class DockerContainers
{
    private const string DatabaseName = "SqlFlowTests";

    public static MsSqlContainer SqlServerContainer { get; } =
        new MsSqlBuilder(
                "mcr.microsoft.com/mssql/server:2022-latest")
            .WithPassword("P@ssw0rd123!")
            .WithLogger(ConsoleLogger.Instance)
            .Build();

    /// <summary>
    /// Gets the connection string for the SqlFlow test database.
    /// </summary>
    public static string ConnectionString
    {
        get
        {
            var builder = new SqlConnectionStringBuilder(
                SqlServerContainer.GetConnectionString())
            {
                InitialCatalog = DatabaseName
            };

            return builder.ConnectionString;
        }
    }

    public static async Task StartAllContainersAsync()
    {
        await SqlServerContainer
            .StartAsync()
            .ConfigureAwait(false);

        await CreateDatabaseAsync()
            .ConfigureAwait(false);

        await EnableServiceBrokerAsync()
            .ConfigureAwait(false);

        await InstallSchemaAsync()
            .ConfigureAwait(false);

        await VerifyServiceBrokerAsync()
            .ConfigureAwait(false);
    }

    public static async Task StopAllContainersAsync()
    {
        await SqlServerContainer
            .StopAsync()
            .ConfigureAwait(false);
    }

    private static async Task CreateDatabaseAsync()
    {
        /*
         * The container connection string initially targets master.
         * CREATE DATABASE must be executed from that connection.
         */
        string masterConnectionString =
            SqlServerContainer.GetConnectionString();

        await using var connection =
            new SqlConnection(masterConnectionString);

        await connection
            .OpenAsync()
            .ConfigureAwait(false);

        const string sql = """
            IF DB_ID(N'SqlFlowTests') IS NULL
            BEGIN
                CREATE DATABASE [SqlFlowTests];
            END;
            """;

        await using var command =
            new SqlCommand(sql, connection);

        await command
            .ExecuteNonQueryAsync()
            .ConfigureAwait(false);
    }

    private static async Task EnableServiceBrokerAsync()
    {
        /*
         * ALTER DATABASE is executed from master.
         *
         * For an isolated disposable test container,
         * WITH ROLLBACK IMMEDIATE prevents initialization from hanging
         * if another test connection already targets SqlFlowTests.
         */
        string masterConnectionString =
            SqlServerContainer.GetConnectionString();

        await using var connection =
            new SqlConnection(masterConnectionString);

        await connection
            .OpenAsync()
            .ConfigureAwait(false);

        const string sql = """
            IF EXISTS
            (
                SELECT 1
                FROM sys.databases
                WHERE name = N'SqlFlowTests'
                  AND is_broker_enabled = 0
            )
            BEGIN
                ALTER DATABASE [SqlFlowTests]
                    SET ENABLE_BROKER
                    WITH ROLLBACK IMMEDIATE;
            END;
            """;

        await using var command =
            new SqlCommand(sql, connection);

        await command
            .ExecuteNonQueryAsync()
            .ConfigureAwait(false);
    }

    private static async Task InstallSchemaAsync()
    {
        string scriptPath = Path.Combine(
            AppContext.BaseDirectory,
            "Resources",
            "sql",
            "ssf-sqlserver.sql");

        string scriptContent =
            await File.ReadAllTextAsync(scriptPath)
                .ConfigureAwait(false);

        /*
         * ExecScriptAsync normally uses the container's default connection,
         * which targets master. Prefix the script so that all schema objects
         * are installed in SqlFlowTests.
         */
        string databaseScript = $$"""
            USE [{{DatabaseName}}];
            GO

            {{scriptContent}}
            """;

        var result = await SqlServerContainer
            .ExecScriptAsync(databaseScript)
            .ConfigureAwait(false);

        if (result.ExitCode != 0)
        {
            throw new InvalidOperationException(
                "Could not install the SQL Server SqlFlow schema." +
                Environment.NewLine +
                result.Stderr);
        }
    }

    private static async Task VerifyServiceBrokerAsync()
    {
        string masterConnectionString =
            SqlServerContainer.GetConnectionString();

        await using var connection =
            new SqlConnection(masterConnectionString);

        await connection
            .OpenAsync()
            .ConfigureAwait(false);

        const string sql = """
            SELECT CONVERT(bit, is_broker_enabled)
            FROM sys.databases
            WHERE name = N'SqlFlowTests';
            """;

        await using var command =
            new SqlCommand(sql, connection);

        object? result = await command
            .ExecuteScalarAsync()
            .ConfigureAwait(false);

        if (result is not bool brokerEnabled || !brokerEnabled)
        {
            throw new InvalidOperationException(
                "Service Broker is not enabled for database " +
                "'SqlFlowTests'.");
        }
    }
}