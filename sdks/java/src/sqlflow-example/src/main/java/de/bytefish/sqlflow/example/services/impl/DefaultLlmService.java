package de.bytefish.sqlflow.example.services;

import de.bytefish.sqlflow.example.models.Solution;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
    public  class DefaultLlmService implements LlmService {
        private static final Logger logger = LoggerFactory.getLogger(DefaultLlmService.class);

        @Override
        public Solution generateFix(String log, String lastFeedback) throws InterruptedException {
            logger.info("Agent is thinking: 'Learned from feedback: {}'", lastFeedback);

            // Simulate expensive LLM call
            Thread.sleep(2500);

            String code = lastFeedback.contains("error handling")
                    ? "// AI: Improved Logging & Error-Handling added\nif(data == null) throw new IllegalArgumentException();"
                    : "// AI: Simple Fix for the NullReferenceException\nif(data == null) return;";

            logger.info("LLM has generated a potential fix: \n{}", code);
            return new Solution(code);
        }
    }
