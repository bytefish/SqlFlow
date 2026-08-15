package de.bytefish.sqlflow.core;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.bytefish.sqlflow.core.infrastructure.Job;
import de.bytefish.sqlflow.core.infrastructure.JobFactory;
import de.bytefish.sqlflow.core.infrastructure.TaskHandler;
import de.bytefish.sqlflow.core.models.*;

import java.util.List;


public interface ISqlFlow {

    void createQueue(String queueName);

    void dropQueue(String queueName);

    List<String> listQueues();

    /**
     * Registers a functional task handler with the SqlFlow client.
     */
    void registerTask(TaskRegistrationOptions options, TaskHandler handler);

    <TRequest> SpawnResult spawn(SpawnOptions options, String taskName, TRequest request);

    void emitEvent(EmitEventOptions options, String eventName, Object payload);

    void cancelTask(CancelTaskOptions options, String taskId);

    List<ClaimedTask> claimTasks(String queue, String workerId, int claimTimeout, int batchSize);

    void workBatch(String queue, String workerId, int claimTimeout, int batchSize);

    void executeTask(ClaimedTask task, String queue, int claimTimeout, boolean fatalOnLeaseTimeout);

    /**
     * Provides an elegant default method to bridge the core functional SDK with strongly-typed Job interfaces and Dependency Injection.
     *
     * @param provider           The JobFactory (equivalent to IServiceProvider in .NET) for DI resolution.
     * @param mapper             Jackson ObjectMapper for deserialization.
     * @param taskName           The logical name of the task to register.
     * @param defaultMaxAttempts The default max attempts for retries.
     * @param jobClass           The Class definition of the Job.
     * @param paramsClass        The Class definition of the Parameters expected by the Job.
     */
    default <TParams, TResult> void useJob(
            JobFactory provider,
            ObjectMapper mapper,
            String taskName,
            int defaultMaxAttempts,
            Class<? extends Job<TParams, TResult>> jobClass,
            Class<TParams> paramsClass) {

        TaskRegistrationOptions options = new TaskRegistrationOptions(taskName, defaultMaxAttempts);

        this.registerTask(options, (ctx, jsonParams) -> {
            Job<TParams, TResult> job = provider.getJob(jobClass);

            TParams typedParams = null;

            if (jsonParams != null && !jsonParams.isNull()) {
                typedParams = mapper.convertValue(jsonParams, paramsClass);
            }

            return job.execute(ctx, typedParams);
        });
    }
}