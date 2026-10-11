package de.bytefish.sqlflow.core.infrastructure;

@FunctionalInterface
public interface JobFactory {
    <T> T getJob(String tenantId, Class<T> jobType);
}