package de.bytefish.sqlflow.core.infrastructure;

public interface EventPublisher {
    <T> void emitEvent(String queue, String eventName, T payload);
}