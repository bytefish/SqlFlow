package de.bytefish.sqlflow.core.workers;

import de.bytefish.sqlflow.core.ISqlFlow;
import de.bytefish.sqlflow.core.infrastructure.QueueSignalOptions;
import de.bytefish.sqlflow.core.infrastructure.TokenBucketRateLimiter;
import de.bytefish.sqlflow.core.models.ClaimedTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

public final class DefaultSqlFlowDispatcher implements SqlFlowDispatcher {

    private static final Logger logger = LoggerFactory.getLogger(DefaultSqlFlowDispatcher.class);

    private final ISqlFlow client;
    private final QueueSignalListener signals;
    private final QueueSignalOptions signalOptions;
    private final Set<String> activeQueues = ConcurrentHashMap.newKeySet();

    public DefaultSqlFlowDispatcher(
            ISqlFlow client,
            QueueSignalListener signals,
            QueueSignalOptions signalOptions) {
        this.client = client;
        this.signals = signals;
        this.signalOptions = signalOptions;
    }

    @Override
    public void runWorker(
            WorkerOptions options) {
        validateOptions(options);

        if (!activeQueues.add(
                options.queue())) {
            throw new IllegalStateException(
                    "A worker for queue '"
                            + options.queue()
                            + "' is already running.");
        }

        try {
            signals.registerQueue(
                    options.queue());

            runQueue(
                    options);
        } finally {
            activeQueues.remove(
                    options.queue());
        }
    }

    private void runQueue(
            WorkerOptions options) {
        TokenBucketRateLimiter rateLimiter =
                createRateLimiter(
                        options);

        int capacity =
                options.concurrency();

        BlockingQueue<ClaimedTask> executionQueue =
                new ArrayBlockingQueue<>(
                        capacity);

        Semaphore availableCapacity =
                new Semaphore(
                        capacity);

        AtomicBoolean running =
                new AtomicBoolean(
                        true);

        try(ExecutorService consumers = Executors.newVirtualThreadPerTaskExecutor()) {

            Thread producer = Thread.ofVirtual().start(() ->
                    produce(
                            options,
                            rateLimiter,
                            executionQueue,
                            availableCapacity,
                            running));

            for (int i = 0; i < capacity; i++) {
                consumers.submit(() ->
                        consume(
                                options,
                                executionQueue,
                                availableCapacity,
                                running));
            }

            try {
                producer.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                running.set(false);

                consumers.shutdownNow();
            }
        }
    }

    private void produce(
            WorkerOptions options,
            TokenBucketRateLimiter rateLimiter,
            BlockingQueue<ClaimedTask> writer,
            Semaphore availableCapacity,
            AtomicBoolean running) {
        boolean queueMayContainWork =
                true;

        while (running.get() && !Thread.currentThread().isInterrupted()) {
            try {
                if (!queueMayContainWork) {
                    signals.waitForSignal(
                            options.queue(),
                            signalOptions.reconciliationInterval());
                }

                queueMayContainWork =
                        executeOneWorkCycle(
                                options,
                                rateLimiter,
                                writer,
                                availableCapacity);
            } catch (InterruptedException ex) {
                Thread.currentThread()
                        .interrupt();

                return;
            } catch (Exception ex) {
                reportError(
                        options,
                        ex);

                queueMayContainWork =
                        false;
            }
        }
    }

    private boolean executeOneWorkCycle(
            WorkerOptions options,
            TokenBucketRateLimiter rateLimiter,
            BlockingQueue<ClaimedTask> writer,
            Semaphore availableCapacity)
            throws Exception {
        int maximumBatchSize =
                Math.min(
                        options.batchSize(),
                        options.concurrency());

        availableCapacity.acquire();

        int reservedSlots = 1;

        int submittedTasks = 0;

        try {
            while (
                    reservedSlots < maximumBatchSize && availableCapacity.tryAcquire()) {
                reservedSlots++;
            }

            if (rateLimiter != null) {
                rateLimiter.acquire(
                        reservedSlots);
            }

            List<ClaimedTask> tasks = client.claimTasks(
                    options.queue(),
                    options.workerId(),
                    options.claimTimeout(),
                    reservedSlots);

            int unusedSlots = reservedSlots - tasks.size();

            if (unusedSlots > 0) {
                availableCapacity.release(unusedSlots);

                reservedSlots -= unusedSlots;
            }

            for (ClaimedTask task : tasks) {
                writer.put(task);

                submittedTasks++;
            }

            return tasks.size() == maximumBatchSize;
        } catch (Exception ex) {
            int notSubmitted = reservedSlots - submittedTasks;

            if (notSubmitted > 0) {
                availableCapacity.release(notSubmitted);
            }

            throw ex;
        }
    }

    private void consume(
            WorkerOptions options,
            BlockingQueue<ClaimedTask> reader,
            Semaphore availableCapacity,
            AtomicBoolean running) {
        while (running.get()) {
            try {
                ClaimedTask task = reader.take();

                try {
                    client.executeTask(
                            task,
                            options.queue(),
                            options.claimTimeout(),
                            options.fatalOnLeaseTimeout());
                } catch (Exception ex) {
                    reportError(options, ex);
                } finally {
                    availableCapacity.release();
                }
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();

                return;
            }
        }
    }

    private TokenBucketRateLimiter createRateLimiter(WorkerOptions options) {
        if (options.maxTasksPerSecond() == null || options.maxTasksPerSecond() <= 0) {
            return null;
        }

        int burst = options.rateLimitBurstSize() != null
                ? options.rateLimitBurstSize()
                : options.maxTasksPerSecond();

        return new TokenBucketRateLimiter(options.maxTasksPerSecond(), burst);
    }

    private void reportError(
            WorkerOptions options,
            Exception exception) {
        try {
            if (options.onError() != null) {
                options.onError().accept(exception);

                return;
            }
        } catch (Exception callbackException) {
            logger.error(
                    "Worker error callback failed for queue '{}'",
                    options.queue(),
                    callbackException);
        }

        logger.error(
                "Worker error in queue '{}'",
                options.queue(),
                exception);
    }

    private static void validateOptions(
            WorkerOptions options) {
        if (options.concurrency() <= 0) {
            throw new IllegalArgumentException("Concurrency must be > 0");
        }

        if (options.claimTimeout() <= 0) {
            throw new IllegalArgumentException("ClaimTimeout must be > 0");
        }

        if (options.batchSize() <= 0) {
            throw new IllegalArgumentException("BatchSize must be > 0");
        }
    }
}