package de.bytefish.sqlflow.core.workers;

import de.bytefish.sqlflow.core.ISqlFlow;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import de.bytefish.sqlflow.core.models.ClaimedTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SqlFlowWorker implements AutoCloseable, Runnable {
    private static final Logger logger = LoggerFactory.getLogger(SqlFlowWorker.class);

    private final ISqlFlow client;
    private final WorkerOptions options;
    private final AtomicBoolean isRunning = new AtomicBoolean(false);
    private final ExecutorService executorService;
    private final Semaphore concurrencyLimiter;

    public SqlFlowWorker(WorkerOptions options, ISqlFlow client) {
        this.client = client;
        this.options = options;
        this.concurrencyLimiter = new Semaphore(options.concurrency());
        this.executorService = Executors.newVirtualThreadPerTaskExecutor();
    }

    @Override
    public void run() {
        if (!isRunning.compareAndSet(false, true)) {
            throw new IllegalStateException("Worker is already running");
        }

        logger.info("SqlFlow Worker [{}] started for queue '{}'", options.workerId(), options.queue());

        while (isRunning.get() && !Thread.currentThread().isInterrupted()) {
            try {
                if (concurrencyLimiter.availablePermits() == 0) {
                    Thread.sleep((long) (options.pollInterval() * 1000));
                    continue;
                }

                int toClaim = Math.min(options.batchSize(), concurrencyLimiter.availablePermits());
                List<ClaimedTask> messages = client.claimTasks(options.queue(), options.workerId(), options.claimTimeout(), toClaim);

                if (messages.isEmpty()) {
                    Thread.sleep((long) (options.pollInterval() * 1000));
                    continue;
                }

                for (ClaimedTask task : messages) {
                    concurrencyLimiter.acquire();
                    executorService.submit(() -> executeTaskSafely(task));
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.debug("Worker polling loop interrupted.");
                break;
            } catch (Exception ex) {
                options.onError().accept(ex);
                sleepQuietly(1000); // Backoff on generic errors
            }
        }
    }

    private void executeTaskSafely(ClaimedTask task) {
        try {
            client.executeTask(task, options.queue(), options.claimTimeout(), options.fatalOnLeaseTimeout());
        } catch (Exception ex) {
            options.onError().accept(ex);
        } finally {
            concurrencyLimiter.release();
        }
    }

    private void sleepQuietly(long millis) {
        try { Thread.sleep(millis); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
    }

    @Override
    public void close() {
        if (isRunning.compareAndSet(true, false)) {
            logger.info("Shutting down worker [{}]...", options.workerId());
            executorService.shutdown();
            try {
                if (!executorService.awaitTermination(30, TimeUnit.SECONDS)) {
                    executorService.shutdownNow();
                }
            } catch (InterruptedException e) {
                executorService.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
    }
}