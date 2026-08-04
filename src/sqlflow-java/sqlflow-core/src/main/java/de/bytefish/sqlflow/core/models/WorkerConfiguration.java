package de.bytefish.sqlflow.core.models;

import java.util.function.Consumer;

public record WorkerConfiguration(
        String queueName,
        int concurrency,
        double pollIntervalInSeconds,
        int claimTimeoutInSeconds,
        Integer batchSize,
        boolean fatalOnLeaseTimeout,
        Consumer<Exception> onError
) {
    // Falls du Standardwerte brauchst, kannst du einen statischen Builder
    // oder einen "with"-ähnlichen Konstruktor-Ansatz wählen.
    // Da wir hier ein Record haben, ist es am saubersten,
    // die Konfiguration beim Start einmal komplett zu instanziieren.
}