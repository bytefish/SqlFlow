package de.bytefish.sqlflow.core.workers;

import de.bytefish.sqlflow.core.SqlFlow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GenericSqlFlowWorker {
    private static final Logger logger = LoggerFactory.getLogger(GenericSqlFlowWorker.class);
    private final SqlFlow client;
    private final SqlFlowRegistry registry;
    private final String queueName;

    public GenericSqlFlowWorker(SqlFlow client, SqlFlowRegistry registry, String queueName) {
        this.client = client;
        this.registry = registry;
        this.queueName = queueName;
    }

    public void start() {
        logger.info("Initializing worker for queue: {}", queueName);
        client.createQueue(queueName);

        // Fetch config from registry
        var config = registry.getWorkerConfigs().stream()
                .filter(c -> c.queueName().equals(queueName))
                .findFirst()
                .orElseThrow();

        WorkerOptions options = new WorkerOptions(
                "worker-" + queueName + "-" + System.nanoTime(),
                queueName,
                config.claimTimeoutInSeconds(),
                config.batchSize(),
                config.concurrency(),
                config.pollIntervalInSeconds(),
                config.onError(),
                config.fatalOnLeaseTimeout()
        );

        SqlFlowWorker worker = new SqlFlowWorker(options, client);

        // Start the worker in a dedicated Virtual Thread
        Thread.ofVirtual().start(worker::execute);
    }
}