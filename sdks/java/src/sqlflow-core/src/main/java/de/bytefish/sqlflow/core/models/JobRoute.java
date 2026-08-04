package de.bytefish.sqlflow.core.models;

public record JobRoute(Class<?> jobType, String queue) {}