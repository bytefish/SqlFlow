package de.bytefish.sqlflow.sqlserver;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import de.bytefish.sqlflow.core.db.SqlFlowDatabase;
import de.bytefish.sqlflow.core.exceptions.CancelledTaskException;
import de.bytefish.sqlflow.core.models.CheckpointRow;
import de.bytefish.sqlflow.core.models.ClaimedTask;
import de.bytefish.sqlflow.core.models.SpawnResult;

import javax.sql.DataSource;
import java.sql.*;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class SqlServerFlowDatabase implements SqlFlowDatabase {

    private final DataSource dataSource;
    private final ObjectMapper mapper;

    public SqlServerFlowDatabase(DataSource dataSource, ObjectMapper mapper) {
        this.dataSource = dataSource;
        this.mapper = mapper;
    }

    @Override
    public void createQueue(String tenantId, String queueName) {
        execute(conn -> {
            try (PreparedStatement cmd = conn.prepareStatement("EXEC ssf.create_queue ?, ?, 'unpartitioned'")) {
                cmd.setString(1, tenantId);
                cmd.setString(2, queueName);
                cmd.executeUpdate();
                return null;
            }
        });
    }

    @Override
    public void dropQueue(String tenantId, String queueName) {
        execute(conn -> {
            try (PreparedStatement cmd = conn.prepareStatement("EXEC ssf.drop_queue ?, ?")) {
                cmd.setString(1, tenantId);
                cmd.setString(2, queueName);
                cmd.executeUpdate();
                return null;
            }
        });
    }

    @Override
    public List<String> listQueues(String tenantId) {
        return execute(conn -> {
            List<String> results = new ArrayList<>();
            try (PreparedStatement cmd = conn.prepareStatement("SELECT queue_name FROM ssf.queues WHERE tenant_id = ? ORDER BY queue_name")) {
                cmd.setString(1, tenantId);
                try (ResultSet rs = cmd.executeQuery()) {
                    while (rs.next()) results.add(rs.getString(1));
                    return results;
                }
            }
        });
    }

    @Override
    public OffsetDateTime getNextAvailableAt(String tenantId, String queue) {
        return execute(conn -> {
            String sql = "EXEC ssf.get_next_available_at ?, ?";

            try (PreparedStatement cmd = conn.prepareStatement(sql)) {
                cmd.setString(1, tenantId);
                cmd.setString(2, queue);

                try (ResultSet rs = cmd.executeQuery()) {
                    if (rs.next()) {
                        return rs.getObject(1, OffsetDateTime.class);
                    }
                    return null;
                }
            }
        });
    }

    @Override
    public SpawnResult spawnTask(String tenantId, String queue, String taskName, String paramsJson, String optionsJson) {
        return execute(conn -> {
            String sql = "EXEC ssf.spawn_task ?, ?, ?, ?, ?";
            try (PreparedStatement cmd = conn.prepareStatement(sql)) {
                cmd.setString(1, tenantId);
                cmd.setString(2, queue);
                cmd.setString(3, taskName);
                cmd.setString(4, paramsJson);
                cmd.setString(5, optionsJson);

                try (ResultSet rs = cmd.executeQuery()) {
                    if (rs.next()) return new SpawnResult(tenantId, rs.getString(1), rs.getString(2), rs.getInt(3));
                    throw new RuntimeException("Failed to spawn task");
                }
            }
        });
    }

    @Override
    public List<ClaimedTask> claimTasks(String tenantId, String queue, String workerId, int timeout, int count) {
        return execute(conn -> {
            List<ClaimedTask> tasks = new ArrayList<>();
            String sql = "EXEC ssf.claim_task ?, ?, ?, ?, ?";
            try (PreparedStatement cmd = conn.prepareStatement(sql)) {
                cmd.setString(1, tenantId);
                cmd.setString(2, queue);
                cmd.setString(3, workerId);
                cmd.setInt(4, timeout);
                cmd.setInt(5, count);

                try (ResultSet rs = cmd.executeQuery()) {
                    while (rs.next()) {
                        int maxAttempts = rs.getInt(7);
                        Integer finalMaxAttempts = rs.wasNull() ? null : maxAttempts;

                        JsonNode parsedHeaders = parseJson(rs, 8);
                        ObjectNode headersNode = (parsedHeaders instanceof ObjectNode obj) ? obj : null;

                        tasks.add(new ClaimedTask(
                                tenantId,
                                rs.getString(1), rs.getString(2), rs.getString(4), rs.getInt(3),
                                parseJson(rs, 5), parseJson(rs, 6), finalMaxAttempts, headersNode,
                                rs.getString(9), parseJson(rs, 10)
                        ));
                    }
                }
                return tasks;
            }
        });
    }

    @Override
    public void persistCheckpoint(String tenantId, String queue, String taskId, String runId, String checkpointName, String stateJson, int timeout) {
        execute(conn -> {
            try (PreparedStatement cmd = conn.prepareStatement("EXEC ssf.set_task_checkpoint_state ?, ?, ?, ?, ?, ?, ?")) {
                cmd.setString(1, tenantId);
                cmd.setString(2, queue);
                cmd.setString(3, UUID.fromString(taskId).toString());
                cmd.setString(4, checkpointName);
                cmd.setString(5, stateJson);
                cmd.setString(6, UUID.fromString(runId).toString());
                cmd.setInt(7, timeout);
                cmd.executeUpdate();
                return null;
            }
        });
    }

    @Override
    public EventResult awaitEvent(String tenantId, String queue, String taskId, String runId, String checkpointName, String eventName, Integer timeout) {
        return execute(conn -> {
            String sql = "EXEC ssf.await_event ?, ?, ?, ?, ?, ?, ?";
            try (PreparedStatement cmd = conn.prepareStatement(sql)) {
                cmd.setString(1, tenantId);
                cmd.setString(2, queue);
                cmd.setString(3, UUID.fromString(taskId).toString());
                cmd.setString(4, UUID.fromString(runId).toString());
                cmd.setString(5, checkpointName);
                cmd.setString(6, eventName);
                if (timeout != null) cmd.setInt(7, timeout);
                else cmd.setNull(7, Types.INTEGER);

                try (ResultSet rs = cmd.executeQuery()) {
                    if (rs.next()) return new EventResult(rs.getBoolean(1), parseJson(rs, 2));
                    throw new RuntimeException("Failed to await event");
                }
            }
        });
    }

    @Override
    public void emitEvent(String tenantId, String queue, String eventName, String payloadJson) {
        execute(conn -> {
            try (PreparedStatement cmd = conn.prepareStatement("EXEC ssf.emit_event ?, ?, ?, ?")) {
                cmd.setString(1, tenantId);
                cmd.setString(2, queue);
                cmd.setString(3, eventName);
                cmd.setString(4, payloadJson);
                cmd.executeUpdate();
                return null;
            }
        });
    }

    @Override
    public void cancelTask(String tenantId, String queue, String taskId) {
        execute(conn -> {
            try (PreparedStatement cmd = conn.prepareStatement("EXEC ssf.cancel_task ?, ?, ?")) {
                cmd.setString(1, tenantId);
                cmd.setString(2, queue);
                cmd.setString(3, UUID.fromString(taskId).toString());
                cmd.executeUpdate();
                return null;
            }
        });
    }

    @Override
    public void completeRun(String tenantId, String queue, String runId, String resultJson) {
        execute(conn -> {
            try (PreparedStatement cmd = conn.prepareStatement("EXEC ssf.complete_run ?, ?, ?, ?")) {
                cmd.setString(1, tenantId);
                cmd.setString(2, queue);
                cmd.setString(3, UUID.fromString(runId).toString());
                cmd.setString(4, resultJson);
                cmd.executeUpdate();
                return null;
            }
        });
    }

    @Override
    public void failRun(String tenantId, String queue, String runId, String errorJson) {
        execute(conn -> {
            try (PreparedStatement cmd = conn.prepareStatement("EXEC ssf.fail_run ?, ?, ?, ?, ?")) {
                cmd.setString(1, tenantId);
                cmd.setString(2, queue);
                cmd.setString(3, UUID.fromString(runId).toString());
                cmd.setString(4, errorJson);
                cmd.setNull(5, Types.TIMESTAMP);
                cmd.executeUpdate();
                return null;
            }
        });
    }

    @Override
    public List<CheckpointRow> getCheckpointStates(String tenantId, String queue, String taskId, String runId) {
        return execute(conn -> {
            List<CheckpointRow> rows = new ArrayList<>();
            try (PreparedStatement cmd = conn.prepareStatement("EXEC ssf.get_task_checkpoint_states ?, ?, ?, ?")) {
                cmd.setString(1, tenantId);
                cmd.setString(2, queue);
                cmd.setString(3, UUID.fromString(taskId).toString());
                cmd.setString(4, UUID.fromString(runId).toString());

                try (ResultSet rs = cmd.executeQuery()) {
                    while (rs.next()) {
                        Timestamp ts = rs.getTimestamp(5);
                        rows.add(new CheckpointRow(
                                tenantId,
                                rs.getString(1), parseJson(rs, 2), rs.getString(3),
                                rs.getString(4), ts != null ? ts.toInstant() : null
                        ));
                    }
                }
                return rows;
            }
        });
    }

    @Override
    public JsonNode getSingleCheckpoint(String tenantId, String queue, String taskId, String checkpointName) {
        return execute(conn -> {
            try (PreparedStatement cmd = conn.prepareStatement("EXEC ssf.get_task_checkpoint_state ?, ?, ?, ?, ?")) {
                cmd.setString(1, tenantId);
                cmd.setString(2, queue);
                cmd.setString(3, UUID.fromString(taskId).toString());
                cmd.setString(4, checkpointName);
                cmd.setInt(5, 0);

                try (ResultSet rs = cmd.executeQuery()) {
                    if (rs.next()) return parseJson(rs, 2);
                    return null;
                }
            }
        });
    }

    @Override
    public void scheduleRun(String tenantId, String queue, String runId, Instant wakeAt) {
        execute(conn -> {
            try (PreparedStatement cmd = conn.prepareStatement("EXEC ssf.schedule_run ?, ?, ?, ?")) {
                cmd.setString(1, tenantId);
                cmd.setString(2, queue);
                cmd.setString(3, UUID.fromString(runId).toString());
                cmd.setTimestamp(4, Timestamp.from(wakeAt));
                cmd.executeUpdate();
                return null;
            }
        });
    }

    @Override
    public void heartbeat(String tenantId, String queue, String runId, int seconds) {
        execute(conn -> {
            try (PreparedStatement cmd = conn.prepareStatement("EXEC ssf.extend_claim ?, ?, ?, ?")) {
                cmd.setString(1, tenantId);
                cmd.setString(2, queue);
                cmd.setString(3, UUID.fromString(runId).toString());
                cmd.setInt(4, seconds);
                cmd.executeUpdate();
                return null;
            }
        });
    }

    @Override
    public void releaseWorkerClaims(String tenantId, String queue, String workerId) {
        execute(conn -> {
            try (PreparedStatement cmd = conn.prepareStatement("EXEC ssf.release_worker_claims ?, ?, ?")) {
                cmd.setString(1, tenantId);
                cmd.setString(2, queue);
                cmd.setString(3, workerId);
                cmd.executeUpdate();
                return null;
            }
        });
    }

    @Override
    public Integer cleanupTasks(String tenantId, String queue, int ttlSeconds, int limit) {
        return execute(conn -> {
            try (PreparedStatement cmd = conn.prepareStatement("EXEC ssf.cleanup_tasks ?, ?, ?, ?")) {
                cmd.setString(1, tenantId);
                cmd.setString(2, queue);
                cmd.setInt(3, ttlSeconds);
                cmd.setInt(4, limit);
                try (ResultSet rs = cmd.executeQuery()) {
                    if (rs.next()) return rs.getInt(1);
                    return 0;
                }
            }
        });
    }

    @Override
    public Integer cleanupEvents(String tenantId, String queue, int ttlSeconds, int limit) {
        return cleanupTasks(tenantId, queue, ttlSeconds, limit);
    }

    private JsonNode parseJson(ResultSet rs, int ordinal) throws SQLException {
        String json = rs.getString(ordinal);
        if (rs.wasNull() || json == null) return null;
        try {
            return mapper.readTree(json);
        } catch (JsonProcessingException e) {
            throw new SQLException("Failed to parse JSON string from database", e);
        }
    }

    @FunctionalInterface
    private interface SqlConnectionAction<T> {
        T execute(Connection conn) throws SQLException;
    }

    private <T> T execute(SqlConnectionAction<T> action) {
        if (Thread.currentThread().isInterrupted()) {
            throw new CancelledTaskException();
        }

        try (Connection conn = dataSource.getConnection()) {
            return action.execute(conn);
        } catch (SQLException ex) {
            if (ex.getErrorCode() == 50011) {
                throw new CancelledTaskException();
            }
            throw new RuntimeException("SQL Server operation failed: " + ex.getMessage(), ex);
        }
    }
}