// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using SqlFlowSdk;
using SqlFlowSdk.Core;
using System.Collections.Concurrent;

namespace SqlFlowSdk.Workers
{
    /// <summary>
    /// A standalone worker that polls for tasks and executes them using the provided SqlFlow Client.
    /// </summary>
    public class SqlFlowWorker
    {
        private readonly ISqlFlow _client;
        private readonly WorkerOptions _options;

        public SqlFlowWorker(WorkerOptions options, ISqlFlow client)
        {
            _client = client;
            _options = options;
        }

        /// <summary>
        /// Starts the polling loop. This method returns a Task that runs until cancellation is requested.
        /// </summary>
        public async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            var semaphore = new SemaphoreSlim(_options.Concurrency);
            var executing = new ConcurrentDictionary<Task, bool>();
            var batchSize = _options.BatchSize ?? _options.Concurrency;

            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    if (semaphore.CurrentCount == 0)
                    {
                        await Task.Delay((int)(_options.PollInterval * 1000), stoppingToken).ConfigureAwait(false);
                        continue;
                    }

                    int toClaim = Math.Min(batchSize, semaphore.CurrentCount);
                    if (toClaim <= 0)
                    {
                        await Task.Delay((int)(_options.PollInterval * 1000), stoppingToken).ConfigureAwait(false);
                        continue;
                    }

                    var messages = await _client.ClaimTasksAsync(_options.Queue, _options.WorkerId, stoppingToken, _options.ClaimTimeout, toClaim).ConfigureAwait(false);

                    var msgList = new List<ClaimedTask>(messages);

                    if (msgList.Count == 0)
                    {
                        await Task.Delay((int)(_options.PollInterval * 1000), stoppingToken).ConfigureAwait(false);
                        continue;
                    }

                    foreach (var task in msgList)
                    {
                        await semaphore.WaitAsync(stoppingToken).ConfigureAwait(false);

                        // Fire and forget (tracked by dictionary)
                        Task taskExecution = ExecuteTaskWrapper(task, stoppingToken, semaphore);

                        executing.TryAdd(taskExecution, true);

                        _ = taskExecution
                            .ContinueWith(t => executing.TryRemove(t, out _));
                    }
                }
                catch (OperationCanceledException) { break; }
                catch (Exception ex)
                {
                    _options.OnError?.Invoke(ex);

                    await Task.Delay((int)(_options.PollInterval * 1000), stoppingToken).ConfigureAwait(false);
                }
            }

            await Task.WhenAll(executing.Keys).ConfigureAwait(false);
        }

        private async Task ExecuteTaskWrapper(ClaimedTask task, CancellationToken stoppingToken, SemaphoreSlim semaphore)
        {
            try
            {
                await _client.ExecuteTaskAsync(task, _options.Queue, _options.ClaimTimeout, stoppingToken, _options.FatalOnLeaseTimeout).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                _options.OnError?.Invoke(ex);
            }
            finally
            {
                semaphore.Release();
            }
        }
    }
}
