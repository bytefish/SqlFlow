// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using SqlFlowSdk;
using SqlFlowSdk.Workers;

namespace SqlFlowSdk.Core;

/// <summary>
/// Registry that holds all the necessary information about job routes, worker configurations, and job registrations.
/// </summary>
public class SqlFlowRegistry
{
    /// <summary>
    /// Gets the collection of job route mappings, associating route names with their corresponding job type and queue.
    /// </summary>
    /// <remarks>Each entry in the dictionary maps a route name to a tuple containing the job type and the
    /// name of the queue to which jobs of that type are assigned. The collection is read-only; to modify the routes,
    /// use the appropriate methods provided by the containing class, if available.</remarks>
    public Dictionary<string, (Type JobType, string Queue)> Routes { get; } = new();

    /// <summary>
    /// Gets the collection of job registrations grouped by queue name.
    /// </summary>
    /// <remarks>Each key in the dictionary represents a queue name, and the associated value is a list of job
    /// registration delegates for that queue. The delegates define the logic to execute a job, accepting an
    /// implementation of ISqlFlow, an IServiceProvider for dependency resolution, and returning a Task representing the
    /// asynchronous operation.</remarks>
    public Dictionary<string, List<Func<ISqlFlow, IServiceProvider, Task>>> JobRegistrationsByQueue { get; } = new();

    /// <summary>
    /// Gets the collection of worker configurations used by the system.
    /// </summary>
    public List<WorkerConfiguration> WorkerConfigs { get; } = new();
}
