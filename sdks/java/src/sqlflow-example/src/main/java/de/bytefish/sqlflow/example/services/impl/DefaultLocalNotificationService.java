package de.bytefish.sqlflow.example.services.impl;

import de.bytefish.sqlflow.example.services.LocalNotificationService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class DefaultLocalNotificationService implements LocalNotificationService {

    private static final Logger logger = LoggerFactory.getLogger(DefaultLocalNotificationService.class);

    @Override
    public void notifyReviewer(String issueId, String correlationId) {
        String notification = """
                
                ======================================================
                SqlServerFlow LLM AGENT PAUSED...
                
                The issue '%s' requires human approval.
                To approve the code, execute the following request:
                
                POST http://localhost:8080/agent/review/%s/%s
                Content-Type: application/json
                
                {
                  "approved": true,
                  "reason": "LGTM!"
                }
                ======================================================
                """.formatted(issueId, issueId, correlationId);

        logger.info(notification);
    }
}
