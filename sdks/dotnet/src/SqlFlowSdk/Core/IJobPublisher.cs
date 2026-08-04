namespace SqlFlowSdk.Core;

public interface IJobPublisher
{
    /// <summary>
    /// Typesafe method to publish a job. The job will be serialized and sent to the worker to be processed. The worker will deserialize the job 
    /// and execute it. The result of the job will be returned to the caller.
    /// </summary>
    /// <typeparam name="TJob">Type of the Job</typeparam>
    /// <typeparam name="TRequest">Type of the Request</typeparam>
    /// <param name="jobName">The Jobs name to publish to</param>
    /// <param name="request">Thre event being published</param>
    /// <param name="cancellationToken">Cancellation token to cancel the operation</param>
    /// <returns></returns>
    Task<SpawnResult> PublishAsync<TJob, TRequest>(string jobName, TRequest request, CancellationToken cancellationToken)
        where TRequest : notnull;
}
