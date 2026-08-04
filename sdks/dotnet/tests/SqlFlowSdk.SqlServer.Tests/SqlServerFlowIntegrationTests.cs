// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging.Abstractions;
using SqlFlowSdk.Core;
using SqlFlowSdk.Database;
using SqlFlowSdk.SqlServer.Database;
using SqlFlowSdk.Workers;
using System.Data.Common;

namespace SqlFlowSdk.SqlServer.Tests;

[TestClass]
public class PostgresFlowIntegrationTests
{
    private static string ConnectionString = null!;

    /// <summary>
    /// Starts the Containers for the Tests.
    /// </summary>
    /// <param name="context">Required Test Context</param>
    /// <returns>Awaitable Task</returns>
    [AssemblyInitialize]
    public static async Task AssemblyInitializeAsync(TestContext context)
    {
        await DockerContainers.StartAllContainersAsync();

        // Updated to use the PostgresContainer from the DockerContainers setup
        ConnectionString = DockerContainers.SqlServerContainer.GetConnectionString();
    }

    [TestMethod]
    public async Task Test_BasicTaskExecution_Flow()
    {
        // ARRANGE

        await using DbDataSource dataSource = SqlClientFactory.Instance.CreateDataSource(ConnectionString);

        ISqlFlowDatabase db = new SqlServerFlowDatabase();

        ISqlFlow client = new SqlFlow(NullLogger<SqlFlow>.Instance, dataSource, db);

        // We use a TCS to signal when the background worker has actually finished the task
        var completionSource = new TaskCompletionSource<int>();

        // Ensure the test queue exists
        await client.CreateQueueAsync("test-queue", default);

        // Define the Task Logic
        client.RegisterTask(new TaskRegistrationOptions
        {
            Name = "add-numbers"
        }, async (ctx, parameters, ct) =>
        {
            if (parameters == null)
            {
                throw new InvalidOperationException("Expected JsonObject parameters");
            }
            // Extract inputs
            int a = parameters["a"]?.GetValue<int>() ?? 0;
            int b = parameters["b"]?.GetValue<int>() ?? 0;

            var sum = a + b;

            // Signal the test that we are done
            completionSource.SetResult(sum);

            return new { result = sum };
        });

        // ACT

        await client.SpawnAsync(new SpawnOptions { Queue = "test-queue" }, "add-numbers", new { a = 10, b = 20 }, default);

        // 4. Worker exakt nach deiner Konstruktor-Signatur instanziieren
        SqlFlowWorker worker = new SqlFlowWorker(new WorkerOptions
        {
            Queue = "test-queue",
            PollInterval = 0.1, // Fast polling for tests
            Concurrency = 1,
            WorkerId = "test-worker"
        }, client);

        using CancellationTokenSource cts = new CancellationTokenSource();

        // Run worker in background
        Task workerTask = worker.ExecuteAsync(cts.Token);

        // Wait for the task to complete (or timeout after 5s)
        Task completedTask = await Task.WhenAny(completionSource.Task, Task.Delay(5000));

        // Stop worker
        cts.Cancel();

        try
        {
            await workerTask;
        }
        catch (OperationCanceledException) { }

        if (completedTask != completionSource.Task)
        {
            Assert.Fail("Task execution timed out.");
        }

        int result = await completionSource.Task;

        Assert.AreEqual(30, result, "The worker should have summed 10 + 20 to get 30.");
    }
}