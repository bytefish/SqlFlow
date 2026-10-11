// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging.Abstractions;
using SqlFlowSdk.Core;
using SqlFlowSdk.Database;
using SqlFlowSdk.SqlServer.Database;
using SqlFlowSdk.Workers;
using System.Collections.Concurrent;
using System.Data.Common;
using System.Threading.Channels;

namespace SqlFlowSdk.SqlServer.Tests;

[TestClass]
public class SqlServerFlowIntegrationTests
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

        ConnectionString = DockerContainers.ConnectionString;
    }

    [TestMethod]
    public async Task Test_BasicTaskExecution_Flow()
    {
        // Arrange
        const string tenantId = "tenant-alpha";
        const string queueName = "test-queue";
        const string taskName = "add-numbers";

        await using DbDataSource dataSource = SqlClientFactory.Instance.CreateDataSource(ConnectionString);

        ISqlFlowDatabase database = new SqlServerFlowDatabase();

        await using var client = new SqlFlow(
            NullLogger<SqlFlow>.Instance,
            dataSource,
            database);

        var signalListener =
            new TestQueueSignalListener();

        var signalOptions =
            new QueueSignalOptions
            {
                ReconciliationInterval =
                    TimeSpan.FromSeconds(30),

                ReconnectDelay =
                    TimeSpan.Zero
            };

        var dispatcher =
            new SqlFlowDispatcher(
                client,
                signalListener,
                signalOptions,
                NullLogger<SqlFlowDispatcher>.Instance);

        var completionSource =
            new TaskCompletionSource<int>(
                TaskCreationOptions.RunContinuationsAsynchronously);

        await client.CreateQueueAsync(
            tenantId,
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

        var workerOptions =
            new WorkerOptions
            {
                TenantId = tenantId,
                Queue = queueName,
                Concurrency = 1,
                WorkerId =
                    $"test-worker-{Guid.NewGuid():N}",
                BatchSize = 1,
                ClaimTimeout = 30,
                FatalOnLeaseTimeout = false,
                OnError = exception =>
                    completionSource.TrySetException(exception)
            };

        using var workerCancellation =
            new CancellationTokenSource();

        /*
         * The dispatcher replaces the old SqlFlowWorker.
         *
         * RunWorkerAsync starts the producer and consumer loops for
         * this queue and continues running until cancellation.
         */
        Task dispatcherTask =
            dispatcher.RunWorkerAsync(
                workerOptions,
                workerCancellation.Token);

        try
        {
            // Act
            await client.SpawnAsync(
                new SpawnOptions
                {
                    TenantId = tenantId,
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
             * In production, SqlServerQueueSignalListener receives
             * Service Broker notifications and wakes the dispatcher.
             *
             * This test injects a controllable signal listener and
             * triggers the wake-up explicitly using tenant and queue.
             */
            signalListener.Signal(tenantId, queueName);

            int result;

            try
            {
                result =
                    await completionSource.Task.WaitAsync(
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
            await workerCancellation.CancelAsync();

            try
            {
                await dispatcherTask;
            }
            catch (OperationCanceledException)
                when (workerCancellation.IsCancellationRequested)
            {
                // Expected during dispatcher shutdown.
            }
        }
    }

    private sealed class TestQueueSignalListener :
        IQueueSignalListener
    {
        private readonly ConcurrentDictionary<
            string,
            Channel<bool>> _queueSignals =
                new(StringComparer.Ordinal);

        private static string GetSignalKey(string tenantId, string queueName) => $"{tenantId}:{queueName}";

        public void RegisterQueue(string tenantId, string queueName)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(tenantId);
            ArgumentException.ThrowIfNullOrWhiteSpace(queueName);

            string signalKey = GetSignalKey(tenantId, queueName);
            Channel<bool> newChannel =
                CreateChannel();

            Channel<bool> actualChannel =
                _queueSignals.GetOrAdd(
                    signalKey,
                    newChannel);

            if (ReferenceEquals(
                    newChannel,
                    actualChannel))
            {
                /*
                 * Perform one initial reconciliation when the queue is
                 * registered for the first time.
                 *
                 * This covers work that existed before the dispatcher
                 * started.
                 */
                actualChannel.Writer.TryWrite(true);
            }
        }

        public async ValueTask<bool> WaitAsync(
            string tenantId,
            string queueName,
            TimeSpan fallbackTimeout,
            CancellationToken cancellationToken)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(tenantId);
            ArgumentException.ThrowIfNullOrWhiteSpace(queueName);

            if (fallbackTimeout <= TimeSpan.Zero)
            {
                throw new ArgumentOutOfRangeException(
                    nameof(fallbackTimeout),
                    "The fallback timeout must be greater than zero.");
            }

            string signalKey = GetSignalKey(tenantId, queueName);

            if (!_queueSignals.TryGetValue(
                    signalKey,
                    out Channel<bool>? channel))
            {
                RegisterQueue(tenantId, queueName);

                channel = _queueSignals[signalKey];
            }

            using CancellationTokenSource timeoutCancellation =
                CancellationTokenSource.CreateLinkedTokenSource(
                    cancellationToken);

            timeoutCancellation.CancelAfter(
                fallbackTimeout);

            try
            {
                await channel.Reader
                    .ReadAsync(timeoutCancellation.Token)
                    .ConfigureAwait(false);

                return true;
            }
            catch (OperationCanceledException)
                when (!cancellationToken.IsCancellationRequested)
            {
                /*
                 * Only the reconciliation timeout elapsed.
                 * The dispatcher should perform another claim attempt.
                 */
                return false;
            }
        }

        public void Signal(string tenantId, string queueName)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(tenantId);
            ArgumentException.ThrowIfNullOrWhiteSpace(queueName);

            string signalKey = GetSignalKey(tenantId, queueName);

            if (!_queueSignals.TryGetValue(
                    signalKey,
                    out Channel<bool>? channel))
            {
                RegisterQueue(tenantId, queueName);

                channel = _queueSignals[signalKey];
            }

            /*
             * The channel has capacity one, so repeated signals are
             * intentionally coalesced.
             */
            channel.Writer.TryWrite(true);
        }

        private static Channel<bool> CreateChannel()
        {
            return Channel.CreateBounded<bool>(
                new BoundedChannelOptions(1)
                {
                    SingleReader = true,
                    SingleWriter = false,
                    AllowSynchronousContinuations = false,
                    FullMode =
                        BoundedChannelFullMode.DropWrite
                });
        }
    }
}