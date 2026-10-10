package de.bytefish.sqlflow.core.infrastructure;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.bytefish.sqlflow.core.db.SqlFlowDatabase;
import de.bytefish.sqlflow.core.models.SpawnOptions;
import de.bytefish.sqlflow.core.models.SpawnResult;

public class JobPublisher implements IJobPublisher {
    private final SqlFlowDatabase db;
    private final ObjectMapper mapper;

    public JobPublisher(SqlFlowDatabase db, ObjectMapper mapper) {
        this.db = db;
        this.mapper = mapper;
    }

    @Override
    public <TRequest> SpawnResult spawn(SpawnOptions options, String taskName, TRequest request) {
        try {
            String paramsJson = mapper.writeValueAsString(request);
            String optionsJson = mapper.writeValueAsString(options);
            return db.spawnTask(options.queue(), taskName, paramsJson, optionsJson);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to serialize spawn request", e);
        }
    }
}