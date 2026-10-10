package de.bytefish.sqlflow.core.infrastructure;


import de.bytefish.sqlflow.core.models.SpawnOptions;
import de.bytefish.sqlflow.core.models.SpawnResult;

public interface IJobPublisher {
    <TRequest> SpawnResult spawn(SpawnOptions options, String taskName, TRequest request);
}