package de.bytefish.sqlflow.nats;

import de.bytefish.sqlflow.core.infrastructure.IEventPublisher;
import io.nats.client.Connection;
import io.nats.client.JetStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;

public final class NatsEventPublisher implements IEventPublisher {

    private static final Logger log = LoggerFactory.getLogger(NatsEventPublisher.class);

    private final IEventPublisher innerPublisher;
    private final Connection connection;

    public NatsEventPublisher(IEventPublisher innerPublisher, Connection connection) {
        this.innerPublisher = innerPublisher;
        this.connection = connection;
    }

    @Override
    public <T> void emitEvent(String tenantId, String queue, String eventName, T payload) {
        // 1. Durably save the event to the Database (The Source of Truth)[cite: 86]
        innerPublisher.emitEvent(tenantId, queue, eventName, payload);

        // 2. Fire the NATS wake-up ping so the worker resumes instantly[cite: 86]
        try {
            JetStream js = connection.jetStream();
            String subject = "ssf.tenants." + tenantId + ".queues." + queue;

            js.publish(subject, "ping".getBytes(StandardCharsets.UTF_8));
        } catch (Exception ex) {
            // NATS is an optimization layer. If it's down, we log a warning and move on.[cite: 86]
            // The worker's smart delta polling will pick up the task on its next cycle.[cite: 86]
            log.warn("Failed to publish NATS wake-up signal for tenant '{}' and queue '{}'", tenantId, queue, ex);
        }
    }
}