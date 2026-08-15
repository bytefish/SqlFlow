package de.bytefish.sqlflow.example.services;

import de.bytefish.sqlflow.example.models.Solution;

public interface LlmService {
    Solution generateFix(String log, String lastFeedback) throws InterruptedException;
}
