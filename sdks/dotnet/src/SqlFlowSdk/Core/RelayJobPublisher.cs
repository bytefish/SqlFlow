// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using SqlFlowSdk;

namespace SqlFlowSdk.Core;

/// <summary>
/// Provides an implementation of the IJobPublisher interface that publishes jobs using the SqlFlow job system.
/// </summary>
/// <remarks>This class is intended for internal use within the job publishing infrastructure. It validates job
/// names and types against the provided SqlFlowRegistry before dispatching jobs to the underlying ISqlFlow
/// client.</remarks>
internal class SqlFlowJobPublisher : IJobPublisher
{
    private readonly ISqlFlow _client;
    private readonly SqlFlowRegistry _registry;

    public SqlFlowJobPublisher(ISqlFlow client, SqlFlowRegistry registry)
    {
        _client = client;
        _registry = registry;
    }

    public Task<SpawnResult> PublishAsync<TJob, TRequest>(string jobName, TRequest request, CancellationToken cancellationToken)
        where TRequest : notnull
    {
        if (!_registry.Routes.TryGetValue(jobName, out (Type JobType, string Queue) routing))
        {
            throw new InvalidOperationException($"No Job found for name '{jobName}' .");
        }

        if (routing.JobType != typeof(TJob))
        {
            throw new InvalidOperationException($"Type-Mismatch for Job '{jobName}'.");
        }

        return _client.SpawnAsync(new SpawnOptions { Queue = routing.Queue }, jobName, request, cancellationToken);
    }
}
