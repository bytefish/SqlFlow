package de.bytefish.sqlflow.example.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import de.bytefish.sqlflow.core.ISqlFlow;
import de.bytefish.sqlflow.core.SqlFlow;
import de.bytefish.sqlflow.core.infrastructure.JobFactory;
import de.bytefish.sqlflow.core.workers.SqlFlowWorker;
import de.bytefish.sqlflow.core.workers.WorkerOptions;
import de.bytefish.sqlflow.example.models.AgentTask;
import de.bytefish.sqlflow.example.workflows.AutonomousAgentJob;
import de.bytefish.sqlflow.postgres.PostgresFlowDatabase;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.testcontainers.containers.PostgreSQLContainer;

import javax.sql.DataSource;

@Configuration
public class SqlFlowConfiguration {

    @Bean(destroyMethod = "stop")
    public PostgreSQLContainer<?> postgresContainer() {
        PostgreSQLContainer<?> postgres =
                new PostgreSQLContainer<>("postgres:18")
                        .withInitScript("ssf-postgres.sql");

        postgres.start();
        return postgres;
    }

    @Bean
    public DataSource dataSource(
            PostgreSQLContainer<?> postgresContainer) {

        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(postgresContainer.getJdbcUrl());
        config.setUsername(postgresContainer.getUsername());
        config.setPassword(postgresContainer.getPassword());
        config.setMaximumPoolSize(10);

        return new HikariDataSource(config);
    }

    @Bean
    public ObjectMapper objectMapper() {
        return new ObjectMapper()
                .registerModule(new JavaTimeModule());
    }

    @Bean
    public JobFactory springJobFactory(ApplicationContext context) {
        return context::getBean;
    }

    @Bean
    public ISqlFlow sqlFlow(
            DataSource dataSource,
            ObjectMapper mapper,
            JobFactory jobFactory) {

        PostgresFlowDatabase db = new PostgresFlowDatabase(dataSource, mapper);

        ISqlFlow client = new SqlFlow(db, mapper);

        client.createQueue("ai-agent-queue");
        client.useJob(
                jobFactory,
                mapper,
                "solve-bug",
                3,
                AutonomousAgentJob.class,
                AgentTask.class);

        return client;
    }

    @Bean
    public SqlFlowWorker sqlFlowWorker(ISqlFlow client) {
        SqlFlowWorker worker = new SqlFlowWorker(
                WorkerOptions.builder()
                        .workerId("spring-worker-1")
                        .queue("ai-agent-queue")
                        .pollInterval(1.0)
                        .concurrency(1)
                        .build(),
                client);

        Thread.ofVirtual().start(worker);
        return worker;
    }
}