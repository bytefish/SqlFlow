package de.bytefish.sqlflow.core.infrastructure;

import de.bytefish.sqlflow.core.ISqlFlow;
import de.bytefish.sqlflow.core.models.JobRoute;
import de.bytefish.sqlflow.core.workers.WorkerConfiguration;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

public class SqlFlowRegistry {

    private final Map<String, JobRoute> routes = new ConcurrentHashMap<>();
    private final Map<String, List<RegistrationDelegate>> jobRegistrationsByQueue = new ConcurrentHashMap<>();
    private final List<WorkerConfiguration> workerConfigs = new CopyOnWriteArrayList<>();

    @FunctionalInterface
    public interface RegistrationDelegate {
        void register(ISqlFlow client, JobFactory factory);
    }

    public Map<String, JobRoute> getRoutes() {
        return routes;
    }

    public Map<String, List<RegistrationDelegate>> getJobRegistrationsByQueue() {
        return jobRegistrationsByQueue;
    }

    public List<WorkerConfiguration> getWorkerConfigs() {
        return workerConfigs;
    }
    
    public void addRegistration(String queue, RegistrationDelegate delegate) {
        jobRegistrationsByQueue.computeIfAbsent(queue, k -> new CopyOnWriteArrayList<>()).add(delegate);
    }
}