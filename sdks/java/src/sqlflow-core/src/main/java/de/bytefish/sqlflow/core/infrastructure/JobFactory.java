package de.bytefish.sqlflow.core.infrastructure;

@FunctionalInterface
public interface JobFactory {
    <T> T getJob(Class<T> jobType);
}