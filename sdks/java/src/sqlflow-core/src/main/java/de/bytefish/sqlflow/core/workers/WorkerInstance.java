package de.bytefish.sqlflow.core.workers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

public final class WorkerInstance implements AutoCloseable
{
    private static final Logger logger =
            LoggerFactory.getLogger(
                    WorkerInstance.class);

    private final WorkerOptions options;

    private final SqlFlowDispatcher dispatcher;

    private final AtomicBoolean started =
            new AtomicBoolean(false);

    private volatile Thread thread;

    public WorkerInstance(
            WorkerOptions options,
            SqlFlowDispatcher dispatcher)
    {
        this.options =
                Objects.requireNonNull(
                        options);

        this.dispatcher =
                Objects.requireNonNull(
                        dispatcher);
    }

    public void start()
    {
        if (!started.compareAndSet(false, true))
        {
            throw new IllegalStateException(
                    "Worker is already running.");
        }

        logger.info(
                "Starting worker '{}' for queue '{}'.",
                options.workerId(),
                options.queue());

        thread =
                Thread.ofVirtual()
                        .name(
                                "sqlflow-worker-"
                                        + options.queue())
                        .start(() ->
                        {
                            try
                            {
                                dispatcher.runWorker(
                                        options);
                            }
                            catch (Exception ex)
                            {
                                logger.error(
                                        "Worker '{}' terminated unexpectedly.",
                                        options.workerId(),
                                        ex);
                            }
                            finally
                            {
                                started.set(false);
                            }
                        });
    }

    public boolean isRunning()
    {
        Thread workerThread =
                this.thread;

        return workerThread != null
                && workerThread.isAlive();
    }

    public String workerId()
    {
        return options.workerId();
    }

    public String queueName()
    {
        return options.queue();
    }

    public WorkerOptions options()
    {
        return options;
    }

    public void stop()
    {
        Thread workerThread =
                this.thread;

        if (workerThread == null)
        {
            return;
        }

        logger.info(
                "Stopping worker '{}' for queue '{}'.",
                options.workerId(),
                options.queue());

        workerThread.interrupt();
    }

    @Override
    public void close()
            throws Exception
    {
        stop();

        Thread workerThread =
                this.thread;

        if (workerThread != null)
        {
            workerThread.join(
                    10_000);
        }
    }
}