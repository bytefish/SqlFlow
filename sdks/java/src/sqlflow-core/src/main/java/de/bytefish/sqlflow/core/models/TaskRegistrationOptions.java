package de.bytefish.sqlflow.core.models;

public record TaskRegistrationOptions(
            String name,
            int defaultMaxAttempts,
            Object defaultCancellation
    ) {
        public TaskRegistrationOptions(String name) {
            this(name, 5, null);
        }
    }
