package de.bytefish.sqlflow.core.workers;

import java.util.function.Consumer;

public record WorkerConfiguration(
    String queueName,
    int concurrency,
    int claimTimeoutInSeconds,
    Integer batchSize,
    Integer maxTasksPerSecond,
    Integer rateLimitBurstSize,
    boolean fatalOnLeaseTimeout,
    Consumer<Exception> onError
) {

}