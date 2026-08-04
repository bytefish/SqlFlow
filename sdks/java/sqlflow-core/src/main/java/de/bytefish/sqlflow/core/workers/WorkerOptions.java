package de.bytefish.sqlflow.core.workers;

import java.util.function.Consumer;

public record WorkerOptions(
    String workerId,
    String queue,
    int claimTimeout,
    Integer batchSize,
    int concurrency,
    double pollInterval,
    Consumer<Exception> onError,
    boolean fatalOnLeaseTimeout
) {}