package de.bytefish.sqlflow.core;

import de.bytefish.sqlflow.core.infrastructure.Job;
import de.bytefish.sqlflow.core.models.*;

import java.util.List;

public interface SqlFlow {
    void createQueue(String queueName);

    void dropQueue(String queueName);

    List<String> listQueues();

    <TParams, TResult> void registerTask(TaskRegistrationOptions options, Class<? extends Job<TParams, TResult>> jobClass);

    <TRequest> SpawnResult spawn(SpawnOptions options, String jobName, TRequest request);

    void emitEvent(EmitEventOptions options, String eventName, Object payload);

    void cancelTask(CancelTaskOptions options, String taskId);

    List<ClaimedTask> claimTasks(String queue, String workerId, int claimTimeout, int batchSize);

    void workBatch(String queue, String workerId, int claimTimeout, int batchSize);

    void executeTask(ClaimedTask task, String queue, int claimTimeout, boolean fatalOnLeaseTimeout);
}