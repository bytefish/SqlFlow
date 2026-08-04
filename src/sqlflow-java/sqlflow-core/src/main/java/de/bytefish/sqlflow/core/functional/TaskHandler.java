package de.bytefish.sqlflow.core.functional;

import com.fasterxml.jackson.databind.JsonNode;
import de.bytefish.sqlflow.core.infrastructure.TaskContext;

@FunctionalInterface
public interface TaskHandler {
    Object handle(TaskContext ctx, JsonNode parameters) throws Exception;
}