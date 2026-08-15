package de.bytefish.sqlflow.example;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import de.bytefish.sqlflow.core.ISqlFlow;
import de.bytefish.sqlflow.core.SqlFlow;
import de.bytefish.sqlflow.core.infrastructure.JobFactory;
import de.bytefish.sqlflow.core.models.EmitEventOptions;
import de.bytefish.sqlflow.core.models.SpawnOptions;
import de.bytefish.sqlflow.core.models.SpawnResult;
import de.bytefish.sqlflow.core.workers.SqlFlowWorker;
import de.bytefish.sqlflow.core.workers.WorkerOptions;
import de.bytefish.sqlflow.example.models.AgentTask;
import de.bytefish.sqlflow.example.models.HumanApproval;
import de.bytefish.sqlflow.postgres.PostgresFlowDatabase;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.web.bind.annotation.*;

import javax.sql.DataSource;
import java.util.Map;

@SpringBootApplication
@RestController
@RequestMapping("/agent")
public class AiSampleApplication {

    public static void main(String[] args) {
        SpringApplication.run(AiSampleApplication.class, args);
    }

    // ==========================================
    // 1. SqlFlow Konfiguration (Dependency Injection)
    // ==========================================

    @Bean
    public ObjectMapper objectMapper() {
        return new ObjectMapper().registerModule(new JavaTimeModule());
    }

    @Bean
    public JobFactory springJobFactory(ApplicationContext context) {
        // Der Geniestreich: Spring Boot ist unsere JobFactory!
        return context::getBean;
    }

    @Bean
    public ISqlFlow sqlFlow(DataSource dataSource, ObjectMapper mapper, JobFactory jobFactory) {
        PostgresFlowDatabase db = new PostgresFlowDatabase(dataSource, mapper);
        ISqlFlow client = new SqlFlow(db, mapper);

        // Queue initialisieren
        client.createQueue("ai-agent-queue");

        // Job registrieren (genau wie in C# UseJob)
        client.useJob(jobFactory, mapper, "solve-bug", 3, AutonomousAgentJob.class, AgentTask.class);

        return client;
    }

    @Bean
    public SqlFlowWorker sqlFlowWorker(ISqlFlow client) {
        // Worker konfigurieren
        SqlFlowWorker worker = new SqlFlowWorker(WorkerOptions.builder()
                .workerId("spring-worker-1")
                .queue("ai-agent-queue")
                .pollInterval(1.0)
                .concurrency(1)
                .build(), client);

        // Worker im Hintergrund starten (als Java 21 Virtual Thread)
        Thread.ofVirtual().start(worker);
        return worker;
    }

    // ==========================================
    // 2. REST API Endpunkte
    // ==========================================

    private final ISqlFlow sqlFlow;

    public AiSampleApplication(ISqlFlow sqlFlow) {
        this.sqlFlow = sqlFlow;
    }

    @PostMapping("/start")
    public Map<String, String> startAgent(@RequestBody AgentTask task) {
        // Spawnt den Job (asynchron abgearbeitet durch den Worker)
        SpawnResult result = sqlFlow.spawn(
                new SpawnOptions("ai-agent-queue", null, null, null),
                "solve-bug",
                task
        );

        return Map.of(
                "RunId", result.runId(),
                "TaskId", result.taskId(),
                "Status", "Agent dispatched to fix Issue #" + task.issueId()
        );
    }

    @PostMapping("/review/{issueId}/{correlationId}")
    public Map<String, String> review(
            @PathVariable String issueId,
            @PathVariable String correlationId,
            @RequestBody HumanApproval approval) {

        // Weckt den Agenten auf!
        String eventName = "agent-approval:" + issueId + ":" + correlationId;

        sqlFlow.emitEvent(
                new EmitEventOptions("ai-agent-queue"),
                eventName,
                approval
        );

        String message = approval.approved()
                ? "Fix for " + correlationId + " approved. Agent is now completing its work."
                : "Fix for " + correlationId + " rejected. Agent tries again with feedback: '" + approval.reason() + "'";

        return Map.of("Message", message);
    }
}