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

public final class PostgresQueueSignalListener implements QueueSignalListener, AutoCloseable {

    private final DataSource dataSource;

    private final Map<String, Semaphore> queueSignals = new ConcurrentHashMap<>();

    private volatile boolean running = true;

    private volatile Thread listenerThread;

    public PostgresQueueSignalListener(DataSource dataSource) {
        this.dataSource = dataSource;

        startListener();
    }

    @Override
    public void registerQueue(String queueName) {
        queueSignals.computeIfAbsent(
                queueName,
                ignored -> new Semaphore(0));
    }

    @Override
    public boolean waitForSignal(
            String queueName,
            Duration timeout)
            throws InterruptedException {
        Semaphore semaphore = queueSignals.computeIfAbsent(queueName, key -> new Semaphore(0));

        return semaphore.tryAcquire(
                timeout.toMillis(),
                java.util.concurrent.TimeUnit.MILLISECONDS);
    }

    private void startListener() {
        listenerThread = Thread.ofVirtual()
                .name("sqlflow-postgres-listener")
                .start(this::listenerLoop);
    }

    private void listenerLoop() {
        while (running) {
            try (Connection connection = dataSource.getConnection()) {
                PGConnection pgConnection = connection.unwrap(PGConnection.class);

                try (Statement stmt = connection.createStatement()) {
                    stmt.execute("LISTEN ssf_work_available");
                }

                while (running) {
                    PGNotification[] notifications = pgConnection.getNotifications(1000);

                    if (notifications == null) {
                        continue;
                    }

                    for (PGNotification notification : notifications) {
                        handleNotification(notification);
                    }
                }
            } catch (Exception ex) {
                if (!running) {
                    return;
                }

                try {
                    Thread.sleep(5000);
                } catch (InterruptedException ignored) {
                    Thread.currentThread()
                            .interrupt();

                    return;
                }
            }
        }
    }

    private void handleNotification(
            PGNotification notification) {
        if (!"ssf_work_available".equals(
                notification.getName())) {
            return;
        }

        String queueName = notification.getParameter();

        if (queueName == null || queueName.isBlank()) {
            return;
        }

        Semaphore semaphore = queueSignals.get(queueName);

        if (semaphore != null) {
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