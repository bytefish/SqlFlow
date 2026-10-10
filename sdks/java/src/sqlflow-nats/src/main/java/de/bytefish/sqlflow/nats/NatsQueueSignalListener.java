package de.bytefish.sqlflow.nats;

import de.bytefish.sqlflow.core.workers.QueueSignalListener;

import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.nats.client.JetStream;
import io.nats.client.JetStreamApiException;
import io.nats.client.JetStreamManagement;
import io.nats.client.Message;
import io.nats.client.PushSubscribeOptions;
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.api.DeliverPolicy;
import io.nats.client.api.StreamConfiguration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

public final class NatsQueueSignalListener implements QueueSignalListener, AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(NatsQueueSignalListener.class);

    private final Connection connection;

    // Tracks the semaphores used to wake up sleeping worker threads
    private final Map<String, Semaphore> queueSignals = new ConcurrentHashMap<>();

    // Tracks active NATS dispatchers so we can close them cleanly on shutdown
    private final Map<String, Dispatcher> dispatchers = new ConcurrentHashMap<>();

    public NatsQueueSignalListener(Connection connection) {
        this.connection = connection;
    }

    @Override
    public void registerQueue(String queueName) {
        // Ensure the semaphore exists for local worker threads to block on
        queueSignals.computeIfAbsent(queueName, ignored -> new Semaphore(0));

        // Spin up the NATS JetStream subscription exactly once per queue
        dispatchers.computeIfAbsent(queueName, this::setupSubscription);
    }

    @Override
    public boolean waitForSignal(String queueName, Duration timeout) throws InterruptedException {
        Semaphore semaphore = queueSignals.computeIfAbsent(queueName, key -> new Semaphore(0));

        // Wait for a permit (a ping) up to the specified reconciliation timeout
        return semaphore.tryAcquire(timeout.toMillis(), TimeUnit.MILLISECONDS);
    }

    private Dispatcher setupSubscription(String queueName) {
        try {
            JetStreamManagement jsm = connection.jetStreamManagement();
            JetStream js = connection.jetStream();
            String subject = "ssf.queues." + queueName;

            // 1. Ensure the globally shared Stream exists
            StreamConfiguration streamConfig = StreamConfiguration.builder()
                    .name("SSF_QUEUES")
                    .subjects("ssf.queues.>")
                    .build();

            try {
                jsm.addStream(streamConfig);
            } catch (JetStreamApiException ex) {
                // Error code 10058 indicates the stream name is already in use, which is fine
                if (ex.getApiErrorCode() != 10058) {
                    log.error("Failed to add NATS stream", ex);
                }
            }

            // 2. Create a NAMED Consumer (Crucial for Consumer Groups)
            // By explicitly naming the consumer, NATS ensures that out of a fleet
            // of 100 workers, a single ping wakes up exactly ONE idle worker.
            ConsumerConfiguration consumerConfig = ConsumerConfiguration.builder()
                    .durable("ssf-worker-" + queueName)
                    .filterSubject(subject)
                    .deliverPolicy(DeliverPolicy.New)
                    .build();

            PushSubscribeOptions options = PushSubscribeOptions.builder()
                    .stream("SSF_QUEUES")
                    .configuration(consumerConfig)
                    .build();

            // The dispatcher utilizes the NATS client's highly optimized async thread pool
            Dispatcher dispatcher = connection.createDispatcher();

            js.subscribe(subject, dispatcher, (Message msg) -> {
                msg.ack();
                Semaphore semaphore = queueSignals.get(queueName);

                // Cap permits at 1. If 1000 pings arrive while the worker is busy,
                // we drop 999 of them to prevent memory/permit leaks, ensuring the
                // next Wait cycle still wakes up instantly to pick up the batch.
                if (semaphore != null && semaphore.availablePermits() == 0) {
                    semaphore.release();
                }
            }, false, options);

            return dispatcher;

        } catch (IOException | JetStreamApiException e) {
            log.error("Failed to setup NATS subscription for queue: {}", queueName, e);
            throw new RuntimeException("Could not initialize NATS listener", e);
        }
    }

    @Override
    public void close() throws Exception {
        for (Dispatcher dispatcher : dispatchers.values()) {
            connection.closeDispatcher(dispatcher);
        }
        dispatchers.clear();
    }
}