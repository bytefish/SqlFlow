package de.bytefish.sqlflow.core.workers;

import java.util.Objects;
import java.util.function.Consumer;

public record WorkerOptions(
    String workerId,
    String queue,
    int claimTimeout,
    int batchSize,
    int concurrency,
    double pollInterval,
    Consumer<Exception> onError,
    boolean fatalOnLeaseTimeout
) {
    public WorkerOptions {
        Objects.requireNonNull(workerId, "workerId cannot be null");
        Objects.requireNonNull(queue, "queue cannot be null");
        if (concurrency < 1) throw new IllegalArgumentException("concurrency must be >= 1");
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String workerId;
        private String queue;
        private int claimTimeout = 120;
        private int batchSize = 1;
        private int concurrency = 1;
        private double pollInterval = 0.25;
        private Consumer<Exception> onError = ex -> {}; 
        private boolean fatalOnLeaseTimeout = true;

        public Builder workerId(String workerId) { this.workerId = workerId; return this; }
        public Builder queue(String queue) { this.queue = queue; return this; }
        public Builder claimTimeout(int claimTimeout) { this.claimTimeout = claimTimeout; return this; }
        public Builder batchSize(int batchSize) { this.batchSize = batchSize; return this; }
        public Builder concurrency(int concurrency) { this.concurrency = concurrency; return this; }
        public Builder pollInterval(double pollInterval) { this.pollInterval = pollInterval; return this; }
        public Builder onError(Consumer<Exception> onError) { this.onError = onError; return this; }
        public Builder fatalOnLeaseTimeout(boolean fatal) { this.fatalOnLeaseTimeout = fatal; return this; }

        public WorkerOptions build() {
            return new WorkerOptions(workerId, queue, claimTimeout, batchSize, concurrency, pollInterval, onError, fatalOnLeaseTimeout);
        }
    }
}