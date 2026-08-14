package de.bytefish.sqlflow.core.infrastructure;

import com.fasterxml.jackson.databind.JsonNode;

@FunctionalInterface
public interface TaskHandler {
    Object handle(TaskContext ctx, JsonNode parameters) throws Exception;
}