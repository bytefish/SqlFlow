package de.bytefish.sqlflow.core.workers;

import java.time.Duration;

public interface QueueSignalListener
{
    void registerQueue(String tenantId, String queueName);

    boolean waitForSignal(
            String tenantId,
            String queueName,
            Duration timeout)
            throws InterruptedException;
}