package de.bytefish.sqlflow.core.infrastructure;

public interface IEventPublisher {
    <T> void emitEvent(String queue, String eventName, T payload);
}
