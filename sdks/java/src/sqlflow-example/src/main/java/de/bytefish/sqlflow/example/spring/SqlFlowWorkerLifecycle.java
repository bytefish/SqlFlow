package de.bytefish.sqlflow.example.spring;

import de.bytefish.sqlflow.core.workers.WorkerInstance;
import org.springframework.context.SmartLifecycle;

public final class SqlFlowWorkerLifecycle implements SmartLifecycle
{
    private final WorkerInstance worker;

    public SqlFlowWorkerLifecycle(
            WorkerInstance worker)
    {
        this.worker = worker;
    }

    @Override
    public void start()
    {
        worker.start();
    }

    @Override
    public void stop()
    {
        try
        {
            worker.close();
        }
        catch (Exception ignored)
        {
        }
    }

    @Override
    public boolean isRunning()
    {
        return worker.isRunning();
    }

    @Override
    public boolean isAutoStartup()
    {
        return true;
    }

    @Override
    public int getPhase()
    {
        return Integer.MAX_VALUE;
    }
}