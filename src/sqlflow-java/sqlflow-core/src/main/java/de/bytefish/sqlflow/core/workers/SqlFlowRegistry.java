package de.bytefish.sqlflow.core.workers;

import de.bytefish.sqlflow.core.SqlFlow;
import de.bytefish.sqlflow.core.models.JobRoute;
import de.bytefish.sqlflow.core.models.WorkerConfiguration;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SqlFlowRegistry {

    // Ersatz für Dictionary<string, (Type, string)>
    private final Map<String, JobRoute> routes = new HashMap<>();

    // Ersatz für Dictionary<string, List<Func<ISqlFlow, IServiceProvider, Task>>>
    // Wir nutzen hier ein Functional Interface, da wir keine asynchronen Tasks mehr zurückgeben müssen.
    private final Map<String, List<RegistrationDelegate>> jobRegistrationsByQueue = new HashMap<>();

    private final List<WorkerConfiguration> workerConfigs = new ArrayList<>();

    @FunctionalInterface
    public interface RegistrationDelegate {
        void register(SqlFlow client, Object serviceProvider); // Object statt IServiceProvider
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
}