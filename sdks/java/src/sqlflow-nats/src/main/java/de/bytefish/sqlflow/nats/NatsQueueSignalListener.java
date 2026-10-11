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

    // Tracks the semaphores used to wake up sleeping worker threads, keyed by "tenantId:queueName"
    private final Map<String, Semaphore> queueSignals = new ConcurrentHashMap<>();

    // Tracks active NATS dispatchers so we can close them cleanly on shutdown, keyed by "tenantId:queueName"
    private final Map<String, Dispatcher> dispatchers = new ConcurrentHashMap<>();

    public NatsQueueSignalListener(Connection connection) {
        this.connection = connection;
    }

    @Override
    public void registerQueue(String tenantId, String queueName) {
        String key = tenantId + ":" + queueName;

        // Ensure the semaphore exists for local worker threads to block on[cite: 88]
        queueSignals.computeIfAbsent(key, ignored -> new Semaphore(0));

        // Spin up the NATS JetStream subscription exactly once per tenant and queue[cite: 88]
        dispatchers.computeIfAbsent(key, k -> setupSubscription(tenantId, queueName));
    }

    @Override
    public boolean waitForSignal(String tenantId, String queueName, Duration timeout) throws InterruptedException {
        String key = tenantId + ":" + queueName;
        Semaphore semaphore = queueSignals.computeIfAbsent(key, k -> new Semaphore(0));

        // Wait for a permit (a ping) up to the specified reconciliation timeout[cite: 88]
        return semaphore.tryAcquire(timeout.toMillis(), TimeUnit.MILLISECONDS);
    }

    private Dispatcher setupSubscription(String tenantId, String queueName) {
        try {
            JetStreamManagement jsm = connection.jetStreamManagement();
            JetStream js = connection.jetStream();
            String subject = "ssf.tenants." + tenantId + ".queues." + queueName;

            // 1. Ensure the globally shared Stream exists[cite: 88]
            StreamConfiguration streamConfig = StreamConfiguration.builder()
                    .name("SSF_QUEUES")
                    .subjects("ssf.tenants.>")
                    .build();

            try {
                jsm.addStream(streamConfig);
            } catch (JetStreamApiException ex) {
                // Error code 10058 indicates the stream name is already in use, which is fine[cite: 88]
                if (ex.getApiErrorCode() != 10058) {
                    log.error("Failed to add NATS stream", ex);
                }
            }

            // 2. Create a NAMED Consumer (Crucial for Consumer Groups)[cite: 88]
            String durableName = "ssf-worker-" + tenantId + "-" + queueName;
            ConsumerConfiguration consumerConfig = ConsumerConfiguration.builder()
                    .durable(durableName)
                    .filterSubject(subject)
                    .deliverPolicy(DeliverPolicy.New)
                    .build();

            PushSubscribeOptions options = PushSubscribeOptions.builder()
                    .stream("SSF_QUEUES")
                    .configuration(consumerConfig)
                    .build();

            // The dispatcher utilizes the NATS client's highly optimized async thread pool[cite: 88]
            Dispatcher dispatcher = connection.createDispatcher();

            String key = tenantId + ":" + queueName;
            js.subscribe(subject, dispatcher, (Message msg) -> {
                msg.ack();
                Semaphore semaphore = queueSignals.get(key);

                // Cap permits at 1. If 1000 pings arrive while the worker is busy,[cite: 88]
                // we drop 999 of them to prevent memory/permit leaks, ensuring the[cite: 88]
                // next Wait cycle still wakes up instantly to pick up the batch.[cite: 88]
                if (semaphore != null && semaphore.availablePermits() == 0) {
                    semaphore.release();
                }
            }, false, options);

            return dispatcher;

        } catch (IOException | JetStreamApiException e) {
            log.error("Failed to setup NATS subscription for tenant '{}' and queue: {}", tenantId, queueName, e);
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