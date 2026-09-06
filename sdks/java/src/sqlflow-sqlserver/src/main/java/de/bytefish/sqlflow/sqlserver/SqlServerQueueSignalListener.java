package de.bytefish.sqlflow.sqlserver;

import de.bytefish.sqlflow.core.workers.DefaultSqlFlowDispatcher;
import de.bytefish.sqlflow.core.workers.QueueSignalListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;

import java.net.SocketException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import java.time.Duration;

import java.util.Map;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;

public final class SqlServerQueueSignalListener implements QueueSignalListener, AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(SqlServerQueueSignalListener.class);

    private final DataSource dataSource;

    private final Map<String, Semaphore> queueSignals = new ConcurrentHashMap<>();

    private volatile boolean running = true;

    private volatile Thread listenerThread;

    public SqlServerQueueSignalListener(DataSource dataSource) {
        this.dataSource = dataSource;
        signalAllQueues();
        startListener();
    }

    @Override
    public void registerQueue(String queueName) {
        queueSignals.computeIfAbsent(queueName, ignored -> new Semaphore(0));
    }

    @Override
    public boolean waitForSignal(
            String queueName,
            Duration timeout)
            throws InterruptedException {
        Semaphore semaphore =
                queueSignals.computeIfAbsent(queueName, ignored -> new Semaphore(0));

        return semaphore.tryAcquire(
                timeout.toMillis(),
                java.util.concurrent.TimeUnit.MILLISECONDS);
    }

    private void startListener() {
        listenerThread =
                Thread.ofVirtual()
                        .name("sqlflow-service-broker-listener")
                        .start(this::listenerLoop);
    }

    private void listenerLoop()
    {
        String waitSql = """
        WAITFOR (
            RECEIVE TOP(1)
                CAST(message_body AS NVARCHAR(MAX))
            FROM ssf.NotificationQueue
        ), TIMEOUT 60000;
        """;

        while (running)
        {
            try (Connection connection = dataSource.getConnection())
            {
                PreparedStatement stmt = connection.prepareStatement(waitSql);
                while (running)
                {
                    try (ResultSet rs = stmt.executeQuery())
                    {
                        if (!rs.next())
                        {
                            continue;
                        }

                        String payload = rs.getString(1);

                        if (payload != null)
                        {
                            parseAndSignal(payload);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                //
                // Expected during shutdown.
                //
                if (!running)
                {
                    return;
                }

                Throwable root = ex;

                while (root.getCause() != null)
                {
                    root = root.getCause();
                }

                if (root instanceof SocketException && "Closed by interrupt".equals(root.getMessage()))
                {
                    return;
                }

                logger.error(
                        "Listener connection failed",
                        ex);

                try
                {
                    Thread.sleep(2000);
                }
                catch (InterruptedException ignored)
                {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }
    }

    private void parseAndSignal(String jsonPayload)
    {
        try
        {
            int queueIdx = jsonPayload.indexOf("\"queue\":");

            if (queueIdx >= 0)
            {
                int start = jsonPayload.indexOf('"', queueIdx + 8);

                if (start >= 0)
                {
                    int end = jsonPayload.indexOf('"', start + 1);

                    if (end > start)
                    {
                        String queueName = jsonPayload.substring(start + 1, end);

                        signalQueue(queueName);

                        return;
                    }
                }
            }
        }
        catch (Exception ignored)
        {
        }

        signalAllQueues();
    }

    private void signalQueue(
            String queueName)
    {
        Semaphore semaphore = queueSignals.get(queueName);

        if (semaphore != null)
        {
            semaphore.release();
        }
    }

    private void signalAllQueues()
    {
        for (Semaphore semaphore : queueSignals.values())
        {
            semaphore.release();
        }
    }


    @Override
    public void close() throws Exception {
        running = false;

        if (listenerThread != null) {
            listenerThread.interrupt();

            listenerThread.join(10_000);
        }
    }
}