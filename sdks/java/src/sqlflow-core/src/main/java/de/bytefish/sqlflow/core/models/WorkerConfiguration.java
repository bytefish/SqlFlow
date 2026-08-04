package de.bytefish.sqlflow.core.models;

import java.util.function.Consumer;

public record WorkerConfiguration(
        String queueName,
        int concurrency,
        double pollIntervalInSeconds,
        int claimTimeoutInSeconds,
        Integer batchSize,
        boolean fatalOnLeaseTimeout,
        Consumer<Exception> onError
) {
}