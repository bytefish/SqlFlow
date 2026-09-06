package de.bytefish.sqlflow.core.infrastructure;

import java.time.Duration;

public record QueueSignalOptions(Duration reconciliationInterval) {
    public QueueSignalOptions {
        if (reconciliationInterval == null) {
            throw new IllegalArgumentException(
                    "reconciliationInterval");
        }

        if (reconciliationInterval.isNegative()
                || reconciliationInterval.isZero()) {
            throw new IllegalArgumentException(
                    "reconciliationInterval must be positive");
        }
    }
}