package de.bytefish.sqlflow.core.workers;

import java.util.Objects;
import java.util.function.Consumer;

public record WorkerOptions(
        String workerId,
        String queue,
        int claimTimeout,
        int batchSize,
        int concurrency,
        Integer maxTasksPerSecond,
        Integer rateLimitBurstSize,
        Consumer<Exception> onError,
        boolean fatalOnLeaseTimeout
) {
    public WorkerOptions
    {
        Objects.requireNonNull(workerId);
        Objects.requireNonNull(queue);

        if (concurrency <= 0) {
            throw new IllegalArgumentException(
                    "concurrency must be > 0");
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {

        private String workerId;

        private String queue;

        private int claimTimeout = 120;

        private int batchSize = 1;

        private int concurrency = 1;

        private Integer maxTasksPerSecond;

        private Integer rateLimitBurstSize;

        private Consumer<Exception> onError = ex -> { };

        private boolean fatalOnLeaseTimeout = true;

        public Builder workerId(String workerId) {
            this.workerId = workerId;
            return this;
        }

        public Builder queue(
                String queue
        ) {
            this.queue = queue;
            return this;
        }

        public Builder claimTimeout(
                int claimTimeout
        ) {
            this.claimTimeout = claimTimeout;
            return this;
        }

        public Builder batchSize(
                int batchSize
        ) {
            this.batchSize = batchSize;
            return this;
        }

        public Builder concurrency(
                int concurrency
        ) {
            this.concurrency = concurrency;
            return this;
        }

        public Builder maxTasksPerSecond(
                Integer value
        ) {
            this.maxTasksPerSecond = value;
            return this;
        }

        public Builder rateLimitBurstSize(
                Integer value
        ) {
            this.rateLimitBurstSize = value;
            return this;
        }

        public Builder onError(
                Consumer<Exception> onError
        ) {
            this.onError = onError;
            return this;
        }

        public Builder fatalOnLeaseTimeout(
                boolean fatal
        ) {
            this.fatalOnLeaseTimeout = fatal;
            return this;
        }

        public WorkerOptions build() {
            return new WorkerOptions(
                    workerId,
                    queue,
                    claimTimeout,
                    batchSize,
                    concurrency,
                    maxTasksPerSecond,
                    rateLimitBurstSize,
                    onError,
                    fatalOnLeaseTimeout
            );
        }
    }
}