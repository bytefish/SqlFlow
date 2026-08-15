package de.bytefish.sqlflow.example.services.impl;

public class ServiceUtils {

    private ServiceUtils() {
    }

    public static void simulateDelay(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Service call canceled", e);
        }
    }
}
