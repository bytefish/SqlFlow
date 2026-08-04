// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace SqlFlowSdk.Core;

/// <summary>
/// Defines a job that can be executed asynchronously with specific parameters and returns a result. This interface is intended to 
/// be implemented by classes that represent jobs in the SqlFlow job system. The ExecuteAsync method is called to perform the 
/// job's work, taking a TaskContext, the job's parameters, and a CancellationToken for cooperative cancellation.
/// </summary>
/// <typeparam name="TParams">Type of the Job Parameters</typeparam>
/// <typeparam name="TResult">Result of the Job Execution</typeparam>
public interface IJob<TParams, TResult>
{
    /// <summary>
    /// Executes the job asynchronously with the provided context, parameters, and cancellation token. Implementations of this method should
    /// </summary>
    /// <param name="ctx">Context of the given Task, such as a TaskId</param>
    /// <param name="args">Parameters to pass to the Task being executed</param>
    /// <param name="cancellationToken">Cancellation Token to cancel the operation</param>
    /// <returns>The result of the Task execution</returns>
    Task<TResult> ExecuteAsync(TaskContext ctx, TParams args);
}
