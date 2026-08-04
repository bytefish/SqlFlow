// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace SqlFlowSdk.Workers;

/// <summary>
/// Options for configuring the behavior of an SqlFlowWorker. This class allows you to specify the worker ID, 
/// queue to poll, claim timeout, batch size for claiming tasks, concurrency level, polling interval, 
/// error handling, and whether to treat lease timeouts as fatal errors. 
/// 
/// These options are used by the SqlFlowWorker to manage how it claims and executes tasks from the SqlFlow system.
/// </summary>
public class WorkerOptions
{
    /// <summary>
    /// The unique identifier for this worker. This is used by the SqlFlow system to track which worker 
    /// has claimed which tasks, and to manage task leases. It must be unique across all workers that 
    /// are polling the same queue, but can be any string value (e.g. a hostname, a GUID, etc.).
    /// </summary>
    public required string WorkerId { get; set; }

    /// <summary>
    /// The queue this worker will poll for tasks to claim and execute. This must match the queue specified 
    /// in the RegisteredTask for the tasks that this worker is intended to execute. The worker will only 
    /// claim tasks from this queue.
    /// </summary>
    public required string Queue { get; set; }

    /// <summary>
    /// Timeout in seconds for claiming a task. When a worker claims a task, it is locked for that 
    /// worker for the duration of the claim timeout. The default value is 120 seconds, which means 
    /// that if a worker claims a task and does not complete it within 2 minutes, the task will be 
    /// released back to the queue and made available for other workers to claim.
    /// </summary>
    public int ClaimTimeout { get; set; } = 120;

    /// <summary>
    /// An optional batch size for claiming tasks. If specified, the worker will attempt to claim 
    /// up to this many tasks at once when polling the queue. This can improve efficiency by 
    /// reducing the number of claim requests made.
    /// </summary>
    public int? BatchSize { get; set; }

    /// <summary>
    /// Level of concurrency for executing tasks. This determines how many tasks the worker will execute 
    /// in parallel. The default value is 1, which means that the worker will execute tasks sequentially. 
    /// 
    /// Setting this to a higher value allows the worker to process multiple tasks at the same time, 
    /// which can improve throughput if the tasks are I/O-bound or if the worker has sufficient resources 
    /// to handle multiple tasks concurrently.
    /// </summary>
    public int Concurrency { get; set; } = 1;

    /// <summary>
    /// Polling interval in seconds for checking the queue for new tasks. This determines how frequently the 
    /// worker will poll the queue to claim new tasks. The default value is 0.25 seconds, which means that 
    /// the worker will check for new tasks every 250 milliseconds.
    /// </summary>
    public double PollInterval { get; set; } = 0.25;

    /// <summary>
    /// An optional error handler that will be invoked if an exception occurs while processing a task. This 
    /// allows a developer to implement custom error handling logic, such as logging the error, sending 
    /// notifications, or performing cleanup actions. 
    /// 
    /// If not specified, exceptions will be thrown and may cause the worker to crash.
    /// </summary>
    public Action<Exception>? OnError { get; set; }

    /// <summary>
    /// The Timeout after which a task lease is considered expired. If a worker claims a task and does 
    /// not complete it within this time, the task will be released back to the queue.
    /// </summary>
    public bool FatalOnLeaseTimeout { get; set; } = true;
}
