package de.bytefish.sqlflow.sqlserver;

import de.bytefish.sqlflow.core.workers.QueueSignalListener;

import javax.sql.DataSource;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import java.time.Duration;

import java.util.Map;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;

public final class SqlServerQueueSignalListener implements QueueSignalListener, AutoCloseable
{
    private final DataSource dataSource;

    private final Map<String, Semaphore> queueSignals =
            new ConcurrentHashMap<>();

    private volatile boolean running = true;

    private volatile Thread listenerThread;

    public SqlServerQueueSignalListener(
            DataSource dataSource)
    {
        this.dataSource = dataSource;

        startListener();
    }

    @Override
    public void registerQueue(
            String queueName)
    {
        queueSignals.computeIfAbsent(
                queueName,
                ignored -> new Semaphore(0));
    }

    @Override
    public boolean waitForSignal(
            String queueName,
            Duration timeout)
            throws InterruptedException
    {
        Semaphore semaphore =
                queueSignals.computeIfAbsent(
                        queueName,
                        ignored ->
                                new Semaphore(0));

        return semaphore.tryAcquire(
                timeout.toMillis(),
                java.util.concurrent.TimeUnit.MILLISECONDS);
    }

    private void startListener()
    {
        listenerThread =
                Thread.ofVirtual()
                        .name(
                                "sqlflow-service-broker-listener")
                        .start(
                                this::listenerLoop);
    }

    private void listenerLoop()
    {
        while (running)
        {
            try (
                    Connection connection =
                            dataSource.getConnection())
            {
                while (running)
                {
                    SignalMessage signal =
                            waitForSignalMessage(
                                    connection);

                    if (signal == null)
                    {
                        continue;
                    }

                    Semaphore semaphore =
                            queueSignals.get(
                                    signal.queueName());

                    if (semaphore != null)
                    {
                        semaphore.release();
                    }
                }
            }
            catch (Exception ex)
            {
                try
                {
                    Thread.sleep(5000);
                }
                catch (InterruptedException ignored)
                {
                    Thread.currentThread()
                            .interrupt();

                    return;
                }
            }
        }
    }

    private SignalMessage waitForSignalMessage(
            Connection connection)
            throws Exception
    {
        try (
                PreparedStatement stmt =
                        connection.prepareStatement(
                                """
                                EXEC ssf.wait_for_queue_signal
                                    @p_timeout_ms = ?
                                """))
        {
            stmt.setInt(
                    1,
                    60000);

            try (
                    ResultSet rs =
                            stmt.executeQuery())
            {
                if (!rs.next())
                {
                    return null;
                }

                boolean signaled =
                        rs.getBoolean(
                                "signaled");

                if (!signaled)
                {
                    return null;
                }

                String queue =
                        rs.getString(
                                "queue_name");

                if (queue == null ||
                        queue.isBlank())
                {
                    return null;
                }

                return new SignalMessage(
                        true,
                        queue);
            }
        }
    }

    @Override
    public void close()
            throws Exception
    {
        running = false;

        if (listenerThread != null)
        {
            listenerThread.interrupt();

            listenerThread.join(
                    10_000);
        }
    }

    private record SignalMessage(
            boolean signaled,
            String queueName)
    {
    }
}