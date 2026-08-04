package de.bytefish.sqlflow.core.functional;

@FunctionalInterface
public interface JobFactory {
    <T> T getJob(Class<T> jobType);
}