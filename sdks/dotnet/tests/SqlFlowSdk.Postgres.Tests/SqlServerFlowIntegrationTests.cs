// Licensed under the MIT license.
// See LICENSE file in the project root for full license information.

using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using SqlFlowSdk.Core;
using SqlFlowSdk.Database;
using SqlFlowSdk.Workers;
using System.Collections.Concurrent;
using System.Data.Common;
using System.Threading.Channels;

namespace SqlFlowSdk.Postgres.Tests;

[TestClass]
public class PostgresFlowIntegrationTests
{
    private static string ConnectionString = null!;

    /// <summary>
    /// Starts the containers for the tests.
    /// </summary>
    [AssemblyInitialize]
    public static async Task AssemblyInitializeAsync(
        TestContext context)
    {
        await DockerContainers.StartAllContainersAsync();

        ConnectionString =
            DockerContainers.PostgresContainer.GetConnectionString();
    }

    [TestMethod]
    public async Task Test_BasicTaskExecution_Flow()
    {
        // Arrange
        const string queueName = "test-queue";
        const string taskName = "add-numbers";

        await using NpgsqlDataSource dataSource =
            NpgsqlDataSource.Create(ConnectionString);

        ISqlFlowDatabase database =
            new PostgresFlowDatabase();

        await using var client = new SqlFlow(
            NullLogger<SqlFlow>.Instance,
            dataSource,
            database);

        var dispatcher = new TestSqlFlowDispatcher();

        var completionSource =
            new TaskCompletionSource<int>(
                TaskCreationOptions.RunContinuationsAsynchronously);

        await client.CreateQueueAsync(
            queueName,
            CancellationToken.None);

        client.RegisterTask(
            new TaskRegistrationOptions
            {
                Name = taskName
            },
            (context, parameters, cancellationToken) =>
            {
                if (parameters is null)
                {
                    throw new InvalidOperationException(
                        "Expected task parameters.");
                }

                int a =
                    parameters["a"]?.GetValue<int>() ?? 0;

                int b =
                    parameters["b"]?.GetValue<int>() ?? 0;

                int sum = a + b;

                completionSource.TrySetResult(sum);

                return Task.FromResult<object>(
                    new
                    {
                        result = sum
                    });
            });

        var workerOptions = new WorkerOptions
        {
            Queue = queueName,
            Concurrency = 1,
            WorkerId = $"test-worker-{Guid.NewGuid():N}",
            BatchSize = 1,
            ClaimTimeout = 30,
            FatalOnLeaseTimeout = false,
            OnError = exception =>
                completionSource.TrySetException(exception)
        };

        /*
         * New worker constructor:
         *
         * - WorkerOptions
         * - ISqlFlow
         * - ISqlFlowDispatcher
         */
        var worker = new SqlFlowWorker(
            workerOptions,
            client,
            dispatcher);

        using var workerCancellation =
            new CancellationTokenSource();

        dispatcher.RegisterQueue(queueName);

        Task workerTask =
            worker.ExecuteAsync(workerCancellation.Token);

        try
        {
            // Act
            await client.SpawnAsync(
                new SpawnOptions
                {
                    Queue = queueName
                },
                taskName,
                new
                {
                    a = 10,
                    b = 20
                },
                CancellationToken.None);

            /*
             * In production, PostgresQueueSignalListener receives the
             * PostgreSQL NOTIFY and calls the dispatcher.
             *
             * This test invokes that boundary directly so that it tests
             * worker execution independently of the listener lifecycle.
             */
            dispatcher.Signal(queueName);

            int result;

            try
            {
                result = await completionSource.Task.WaitAsync(
                    TimeSpan.FromSeconds(5));
            }
            catch (TimeoutException)
            {
                Assert.Fail(
                    "Task execution timed out after five seconds.");

                return;
            }

            // Assert
            Assert.AreEqual(
                30,
                result,
                "The worker should have summed 10 + 20 to get 30.");
        }
        finally
        {
            dispatcher.UnregisterQueue(queueName);

            await workerCancellation.CancelAsync();

            try
            {
                await workerTask;
            }
            catch (OperationCanceledException)
                when (workerCancellation.IsCancellationRequested)
            {
                // Expected during worker shutdown.
            }
        }
    }

    private sealed class TestSqlFlowDispatcher
        : ISqlFlowDispatcher
    {
        private readonly ConcurrentDictionary<
            string,
            Channel<bool>> _queueSignals =
                new(StringComparer.Ordinal);

        public void RegisterQueue(string queueName)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(queueName);

            Channel<bool> channel = _queueSignals.GetOrAdd(
                queueName,
                static _ => CreateChannel());

            /*
             * Schedule one initial reconciliation. This also covers work
             * that was created before the worker was started.
             */
            channel.Writer.TryWrite(true);
        }

        public void UnregisterQueue(string queueName)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(queueName);

            if (_queueSignals.TryRemove(
                    queueName,
                    out Channel<bool>? channel))
            {
                channel.Writer.TryComplete();
            }
        }

        public void Signal(string queueName)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(queueName);

            Channel<bool> channel = _queueSignals.GetOrAdd(
                queueName,
                static _ => CreateChannel());

            /*
             * Capacity is one, so repeated notifications are coalesced.
             */
            channel.Writer.TryWrite(true);
        }

        public async ValueTask WaitForWorkAsync(
            string queueName,
            TimeSpan timeout,
            CancellationToken cancellationToken)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(queueName);

            Channel<bool> channel = _queueSignals.GetOrAdd(
                queueName,
                static _ => CreateChannel());

            using var timeoutCancellation =
                CancellationTokenSource.CreateLinkedTokenSource(
                    cancellationToken);

            timeoutCancellation.CancelAfter(timeout);

            try
            {
                await channel.Reader
                    .ReadAsync(timeoutCancellation.Token)
                    .ConfigureAwait(false);
            }
            catch (OperationCanceledException)
                when (!cancellationToken.IsCancellationRequested)
            {
                /*
                 * This was only the reconciliation timeout.
                 * Return normally so the worker attempts another claim.
                 */
            }
        }

        private static Channel<bool> CreateChannel()
        {
            return Channel.CreateBounded<bool>(
                new BoundedChannelOptions(1)
                {
                    SingleReader = false,
                    SingleWriter = false,
                    AllowSynchronousContinuations = false,
                    FullMode = BoundedChannelFullMode.DropWrite
                });
        }
    }
}