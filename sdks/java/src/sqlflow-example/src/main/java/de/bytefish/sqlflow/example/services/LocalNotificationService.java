package de.bytefish.sqlflow.example.services;

public interface LocalNotificationService {
    void notifyReviewer(String issueId, String correlationId);
}
