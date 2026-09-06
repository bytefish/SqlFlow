package de.bytefish.sqlflow.postgres;

import de.bytefish.sqlflow.core.workers.QueueSignalListener;

import org.postgresql.PGConnection;
import org.postgresql.PGNotification;

import javax.sql.DataSource;

import java.sql.Connection;
import java.sql.Statement;

import java.time.Duration;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;

public final class PostgresQueueSignalListener implements QueueSignalListener, AutoCloseable
{
    private final DataSource dataSource;

    private final Map<String, Semaphore> queueSignals =
            new ConcurrentHashMap<>();

    private volatile boolean running = true;

    private volatile Thread listenerThread;

    public PostgresQueueSignalListener(
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
                key ->
                {
                    registerListenChannel(
                            key);

                    return new Semaphore(0);
                });
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
                        key -> new Semaphore(0));

        return semaphore.tryAcquire(
                timeout.toMillis(),
                java.util.concurrent.TimeUnit.MILLISECONDS);
    }

    private void startListener()
    {
        listenerThread =
                Thread.ofVirtual()
                        .name("sqlflow-postgres-listener")
                        .start(this::listenerLoop);
    }

    private void listenerLoop()
    {
        while (running)
        {
            try (
                    Connection connection =
                            dataSource.getConnection();
            )
            {
                PGConnection pgConnection =
                        connection.unwrap(
                                PGConnection.class);

                //
                // Subscribe to all already known queues.
                //
                for (String queue :
                        queueSignals.keySet())
                {
                    executeListen(
                            connection,
                            queue);
                }

                while (running)
                {
                    PGNotification[] notifications =
                            pgConnection.getNotifications(
                                    10_000);

                    if (notifications == null)
                    {
                        continue;
                    }

                    for (PGNotification notification :
                            notifications)
                    {
                        String channel =
                                notification.getName();

                        String queueName =
                                extractQueueName(
                                        channel);

                        Semaphore semaphore =
                                queueSignals.get(
                                        queueName);

                        if (semaphore != null)
                        {
                            semaphore.release();
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                try
                {
                    Thread.sleep(
                            5_000);
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

    private void registerListenChannel(
            String queueName)
    {
        try (
                Connection connection =
                        dataSource.getConnection()
        )
        {
            executeListen(
                    connection,
                    queueName);
        }
        catch (Exception ex)
        {
            throw new RuntimeException(
                    "Failed to register LISTEN channel for queue "
                            + queueName,
                    ex);
        }
    }

    private static void executeListen(
            Connection connection,
            String queueName)
            throws Exception
    {
        String channel =
                channelName(
                        queueName);

        try (
                Statement statement =
                        connection.createStatement()
        )
        {
            statement.execute(
                    "LISTEN \"" +
                            channel +
                            "\"");
        }
    }

    private static String channelName(
            String queueName)
    {
        return "ssf_" + queueName;
    }

    private static String extractQueueName(
            String channel)
    {
        if (channel.startsWith("ssf_"))
        {
            return channel.substring(4);
        }

        return channel;
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
}