package de.bytefish.sqlflow.core.workers;

import java.time.Duration;

public interface QueueSignalListener
{
    void registerQueue(
            String queueName);

    boolean waitForSignal(
            String queueName,
            Duration timeout)
            throws InterruptedException;
}