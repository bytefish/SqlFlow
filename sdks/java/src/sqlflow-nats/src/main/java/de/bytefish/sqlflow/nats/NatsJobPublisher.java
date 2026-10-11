package de.bytefish.sqlflow.nats;

import de.bytefish.sqlflow.core.infrastructure.IJobPublisher;
import de.bytefish.sqlflow.core.models.SpawnOptions;
import de.bytefish.sqlflow.core.models.SpawnResult;
import io.nats.client.Connection;
import io.nats.client.JetStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;

public final class NatsJobPublisher implements IJobPublisher {

    private static final Logger log = LoggerFactory.getLogger(NatsJobPublisher.class);

    private final IJobPublisher innerPublisher;
    private final Connection connection;

    public NatsJobPublisher(IJobPublisher innerPublisher, Connection connection) {
        this.innerPublisher = innerPublisher;
        this.connection = connection;
    }

    @Override
    public <TRequest> SpawnResult spawn(SpawnOptions options, String taskName, TRequest request) {
        // 1. Durably save the task to the Database[cite: 87]
        SpawnResult result = innerPublisher.spawn(options, taskName, request);

        // 2. Fire the NATS wake-up ping so an idle worker picks up the new job instantly[cite: 87]
        try {
            JetStream js = connection.jetStream();
            String subject = "ssf.tenants." + options.tenantId() + ".queues." + options.queue();

            js.publish(subject, "ping".getBytes(StandardCharsets.UTF_8));
        } catch (Exception ex) {
            log.warn("Failed to publish NATS wake-up signal for spawned task in tenant '{}' and queue '{}'", options.tenantId(), options.queue(), ex);
        }

        return result;
    }
}