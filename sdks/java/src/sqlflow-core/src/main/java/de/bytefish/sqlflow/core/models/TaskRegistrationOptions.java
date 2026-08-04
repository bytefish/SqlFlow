package de.bytefish.sqlflow.core.models;

public record TaskRegistrationOptions(
        String name,
        int defaultMaxAttempts
        // Add CancellationPolicy here if needed in the future
    ) {}
