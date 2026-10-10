package de.bytefish.sqlflow.core.infrastructure;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.bytefish.sqlflow.core.db.SqlFlowDatabase;

public class EventPublisher implements IEventPublisher {
    private final SqlFlowDatabase db;
    private final ObjectMapper mapper;

    public EventPublisher(SqlFlowDatabase db, ObjectMapper mapper) {
        this.db = db;
        this.mapper = mapper;
    }

    @Override
    public <T> void emitEvent(String queue, String eventName, T payload) {
        if (eventName == null || eventName.isEmpty()) {
            throw new IllegalArgumentException("eventName required");
        }
        try {
            String payloadJson = mapper.writeValueAsString(payload);

            db.emitEvent(queue, eventName, payloadJson);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to serialize event payload", e);
        }
    }
}