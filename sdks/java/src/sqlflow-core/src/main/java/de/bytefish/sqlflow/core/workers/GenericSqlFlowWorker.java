package de.bytefish.sqlflow.core.workers;

import de.bytefish.sqlflow.core.SqlFlow;
import de.bytefish.sqlflow.core.infrastructure.JobFactory;
import de.bytefish.sqlflow.core.infrastructure.SqlFlowRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GenericSqlFlowWorker implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(GenericSqlFlowWorker.class);
    private final SqlFlow client;
    private final SqlFlowRegistry registry;
    private final JobFactory jobFactory;
    private final String queueName;
    private SqlFlowWorker innerWorker;

    public GenericSqlFlowWorker(SqlFlow client, SqlFlowRegistry registry, JobFactory jobFactory, String queueName) {
        this.client = client;
        this.registry = registry;
        this.jobFactory = jobFactory;
        this.queueName = queueName;
    }

    public void start() {
        logger.info("Initializing worker for queue: {}", queueName);
        client.createQueue(queueName);

        WorkerConfiguration config = registry.getWorkerConfigs().stream()
                .filter(c -> c.queueName().equals(queueName))
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("No config found for queue: " + queueName));

        // Register handlers for this queue
        var registrations = registry.getJobRegistrationsByQueue().get(queueName);
        if (registrations != null) {
            for (var reg : registrations) {
                reg.register(client, jobFactory);
            }
        }

        WorkerOptions options = WorkerOptions.builder()
                .workerId("worker-" + queueName + "-" + System.nanoTime())
                .queue(queueName)
                .claimTimeout(config.claimTimeoutInSeconds())
                .batchSize(config.batchSize() != null ? config.batchSize() : config.concurrency())
                .concurrency(config.concurrency())
                .pollInterval(config.pollIntervalInSeconds())
                .onError(config.onError() != null ? config.onError() : ex -> logger.error("Worker error", ex))
                .fatalOnLeaseTimeout(config.fatalOnLeaseTimeout())
                .build();

        SqlFlowWorker worker = new SqlFlowWorker(options, client);
        this.innerWorker = worker;
        
        Thread.ofVirtual().start(worker::run);
    }

    @Override
    public void close() throws Exception {
        if (this.innerWorker != null) {
            this.innerWorker.close();
        }
    }
}