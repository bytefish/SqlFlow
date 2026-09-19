package de.bytefish.sqlflow.core.workers;

public interface SqlFlowDispatcher
{
    void runWorker(WorkerOptions options);
}