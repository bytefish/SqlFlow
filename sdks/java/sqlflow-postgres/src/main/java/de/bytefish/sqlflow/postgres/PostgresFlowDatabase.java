package de.bytefish.sqlflow.postgres;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import de.bytefish.sqlflow.core.db.SqlFlowDatabase;
import de.bytefish.sqlflow.core.exceptions.CancelledTaskException;
import de.bytefish.sqlflow.core.models.CheckpointRow;
import de.bytefish.sqlflow.core.models.ClaimedTask;
import de.bytefish.sqlflow.core.models.SpawnResult;
import org.postgresql.util.PSQLException;

import java.sql.*;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * PostgreSQL implementation. Completely synchronous, meant to be consumed by Java 21 Virtual Threads.
 * CancellationToken semantics are inherently mapped to standard Java Thread interruption.
 */
public class PostgresFlowDatabase implements SqlFlowDatabase {

    private final ObjectMapper mapper = new ObjectMapper();

    @Override
    public void createQueue(Connection conn, String queueName) {
        executeWithCancelCheck(() -> {
            try (PreparedStatement cmd = conn.prepareStatement("CALL ssf.create_queue(?, 'unpartitioned')")) {
                cmd.setString(1, queueName);
                cmd.executeUpdate();
                return null;
            }
        });
    }

    @Override
    public void dropQueue(Connection conn, String queueName) {
        executeWithCancelCheck(() -> {
            try (PreparedStatement cmd = conn.prepareStatement("CALL ssf.drop_queue(?)")) {
                cmd.setString(1, queueName);
                cmd.executeUpdate();
                return null;
            }
        });
    }

    @Override
    public List<String> listQueues(Connection conn) {
        return executeWithCancelCheck(() -> {
            List<String> results = new ArrayList<>();
            try (PreparedStatement cmd = conn.prepareStatement("SELECT queue_name FROM ssf.queues ORDER BY queue_name");
                 ResultSet rs = cmd.executeQuery()) {
                while (rs.next()) {
                    results.add(rs.getString(1));
                }
                return results;
            }
        });
    }

    @Override
    public SpawnResult spawnTask(Connection conn, String queue, String taskName, String paramsJson, String optionsJson) {
        return executeWithCancelCheck(() -> {
            String sql = "SELECT task_id, run_id, attempt FROM ssf.spawn_task(?, ?, ?::jsonb, ?::jsonb)";
            try (PreparedStatement cmd = conn.prepareStatement(sql)) {
                cmd.setString(1, queue);
                cmd.setString(2, taskName);
                cmd.setString(3, paramsJson);
                cmd.setString(4, optionsJson);
                try (ResultSet rs = cmd.executeQuery()) {
                    if (rs.next()) {
                        return new SpawnResult(rs.getString(1), rs.getString(2), rs.getInt(3));
                    }
                    throw new RuntimeException("Failed to spawn task");
                }
            }
        });
    }

    @Override
    public void cancelTask(Connection conn, String queue, String taskId) {
        executeWithCancelCheck(() -> {
            try (PreparedStatement cmd = conn.prepareStatement("CALL ssf.cancel_task(?, ?)")) {
                cmd.setString(1, queue);
                cmd.setObject(2, UUID.fromString(taskId));
                cmd.executeUpdate();
                return null;
            }
        });
    }

    @Override
    public void emitEvent(Connection conn, String queue, String eventName, String payloadJson) {
        executeWithCancelCheck(() -> {
            try (PreparedStatement cmd = conn.prepareStatement("CALL ssf.emit_event(?, ?, ?)")) {
                cmd.setString(1, queue);
                cmd.setString(2, eventName);
                cmd.setString(3, payloadJson);
                cmd.executeUpdate();
                return null;
            }
        });
    }

    @Override
    public List<ClaimedTask> claimTasks(Connection conn, String queue, String workerId, int timeout, int count) {
        return executeWithCancelCheck(() -> {
            List<ClaimedTask> tasks = new ArrayList<>();
            String sql = "SELECT run_id, task_id, attempt, task_name, params, retry_strategy, max_attempts, headers, wake_event, event_payload " +
                    "FROM ssf.claim_task(?, ?, ?, ?)";
            try (PreparedStatement cmd = conn.prepareStatement(sql)) {
                cmd.setString(1, queue);
                cmd.setString(2, workerId);
                cmd.setInt(3, timeout);
                cmd.setInt(4, count);

                try (ResultSet rs = cmd.executeQuery()) {
                    while (rs.next()) {
                        int maxAttempts = rs.getInt(7);
                        Integer finalMaxAttempts = rs.wasNull() ? null : maxAttempts;

                        String headersStr = rs.getString(8);
                        ObjectNode headersNode = headersStr != null ? (ObjectNode) mapper.readTree(headersStr) : null;

                        tasks.add(new ClaimedTask(
                                rs.getString(1),      // runId
                                rs.getString(2),      // taskId
                                rs.getString(4),      // taskName
                                rs.getInt(3),         // attempt
                                parseJson(rs, 5),     // params
                                parseJson(rs, 6),     // retryStrategy
                                finalMaxAttempts,     // maxAttempts
                                headersNode,          // headers
                                rs.getString(9),      // wakeEvent
                                parseJson(rs, 10)     // eventPayload
                        ));
                    }
                } catch (JsonMappingException e) {
                    throw new RuntimeException(e);
                } catch (JsonProcessingException e) {
                    throw new RuntimeException(e);
                }
                return tasks;
            }
        });
    }

    @Override
    public void completeRun(Connection conn, String queue, String runId, String resultJson) {
        executeWithCancelCheck(() -> {
            try (PreparedStatement cmd = conn.prepareStatement("CALL ssf.complete_run(?, ?, ?)")) {
                cmd.setString(1, queue);
                cmd.setObject(2, UUID.fromString(runId));
                cmd.setString(3, resultJson);
                cmd.executeUpdate();
                return null;
            }
        });
    }

    @Override
    public void failRun(Connection conn, String queue, String runId, String errorJson) {
        executeWithCancelCheck(() -> {
            try (PreparedStatement cmd = conn.prepareStatement("CALL ssf.fail_run(?, ?, ?::jsonb, ?)")) {
                cmd.setString(1, queue);
                cmd.setObject(2, UUID.fromString(runId));
                cmd.setString(3, errorJson);
                cmd.setNull(4, Types.TIMESTAMP);
                cmd.executeUpdate();
                return null;
            }
        });
    }

    @Override
    public List<CheckpointRow> getCheckpointStates(Connection conn, String queue, String taskId, String runId) {
        return executeWithCancelCheck(() -> {
            List<CheckpointRow> rows = new ArrayList<>();
            String sql = "SELECT checkpoint_name, state, status, owner_run_id, updated_at " +
                    "FROM ssf.get_task_checkpoint_states(?, ?, ?)";
            try (PreparedStatement cmd = conn.prepareStatement(sql)) {
                cmd.setString(1, queue);
                cmd.setObject(2, UUID.fromString(taskId));
                cmd.setObject(3, UUID.fromString(runId));

                try (ResultSet rs = cmd.executeQuery()) {
                    while (rs.next()) {
                        Timestamp ts = rs.getTimestamp(5);
                        rows.add(new CheckpointRow(
                                rs.getString(1),      // checkpointName
                                parseJson(rs, 2),     // state
                                rs.getString(3),      // status
                                rs.getString(4),      // ownerRunId
                                ts != null ? ts.toInstant() : null // updatedAt
                        ));
                    }
                }
                return rows;
            }
        });
    }

    @Override
    public JsonNode getSingleCheckpoint(Connection conn, String queue, String taskId, String checkpointName) {
        return executeWithCancelCheck(() -> {
            String sql = "SELECT checkpoint_name, state, status, owner_run_id, updated_at " +
                    "FROM ssf.get_task_checkpoint_state(?, ?, ?, ?)";
            try (PreparedStatement cmd = conn.prepareStatement(sql)) {
                cmd.setString(1, queue);
                cmd.setObject(2, UUID.fromString(taskId));
                cmd.setString(3, checkpointName);
                cmd.setInt(4, 0);

                try (ResultSet rs = cmd.executeQuery()) {
                    if (rs.next()) {
                        return parseJson(rs, 2);
                    }
                    return null;
                }
            }
        });
    }

    @Override
    public void persistCheckpoint(Connection conn, String queue, String taskId, String runId, String checkpointName, String stateJson, int timeout) {
        executeWithCancelCheck(() -> {
            try (PreparedStatement cmd = conn.prepareStatement("CALL ssf.set_task_checkpoint_state(?, ?, ?, ?, ?, ?)")) {
                cmd.setString(1, queue);
                cmd.setObject(2, UUID.fromString(taskId));
                cmd.setString(3, checkpointName);
                cmd.setString(4, stateJson);
                cmd.setObject(5, UUID.fromString(runId));
                cmd.setInt(6, timeout);
                cmd.executeUpdate();
                return null;
            }
        });
    }

    @Override
    public void scheduleRun(Connection conn, String queue, String runId, Instant wakeAt) {
        executeWithCancelCheck(() -> {
            try (PreparedStatement cmd = conn.prepareStatement("CALL ssf.schedule_run(?, ?, ?)")) {
                cmd.setString(1, queue);
                cmd.setObject(2, UUID.fromString(runId));
                cmd.setTimestamp(3, Timestamp.from(wakeAt));
                cmd.executeUpdate();
                return null;
            }
        });
    }

    @Override
    public void heartbeat(Connection conn, String queue, String runId, int seconds) {
        executeWithCancelCheck(() -> {
            try (PreparedStatement cmd = conn.prepareStatement("CALL ssf.extend_claim(?, ?, ?)")) {
                cmd.setString(1, queue);
                cmd.setObject(2, UUID.fromString(runId));
                cmd.setInt(3, seconds);
                cmd.executeUpdate();
                return null;
            }
        });
    }

    @Override
    public EventResult awaitEvent(Connection conn, String queue, String taskId, String runId, String checkpointName, String eventName, Integer timeout) {
        return executeWithCancelCheck(() -> {
            String sql = "SELECT should_suspend, payload FROM ssf.await_event(?, ?, ?, ?, ?, ?)";
            try (PreparedStatement cmd = conn.prepareStatement(sql)) {
                cmd.setString(1, queue);
                cmd.setObject(2, UUID.fromString(taskId));
                cmd.setObject(3, UUID.fromString(runId));
                cmd.setString(4, checkpointName);
                cmd.setString(5, eventName);
                if (timeout != null) cmd.setInt(6, timeout);
                else cmd.setNull(6, Types.INTEGER);

                try (ResultSet rs = cmd.executeQuery()) {
                    if (rs.next()) {
                        return new EventResult(rs.getBoolean(1), parseJson(rs, 2));
                    }
                    throw new RuntimeException("Failed to await event");
                }
            }
        });
    }

    @Override
    public void releaseWorkerClaims(Connection conn, String queue, String workerId) {
        executeWithCancelCheck(() -> {
            try (PreparedStatement cmd = conn.prepareStatement("CALL ssf.release_worker_claims(?, ?)")) {
                cmd.setString(1, queue);
                cmd.setString(2, workerId);
                cmd.executeUpdate();
                return null;
            }
        });
    }

    @Override
    public Integer cleanupTasks(Connection conn, String queue, int ttlSeconds, int limit) {
        return executeWithCancelCheck(() -> {
            try (PreparedStatement cmd = conn.prepareStatement("SELECT deleted_tasks FROM ssf.cleanup_tasks(?, ?, ?)")) {
                cmd.setString(1, queue);
                cmd.setInt(2, ttlSeconds);
                cmd.setInt(3, limit);
                try (ResultSet rs = cmd.executeQuery()) {
                    if (rs.next()) return rs.getInt(1);
                    return 0;
                }
            }
        });
    }

    @Override
    public Integer cleanupEvents(Connection conn, String queue, int ttlSeconds, int limit) {
        return cleanupTasks(conn, queue, ttlSeconds, limit);
    }

    private JsonNode parseJson(ResultSet rs, int ordinal) throws SQLException {
        String json = rs.getString(ordinal);
        if (rs.wasNull() || json == null) {
            return null;
        }
        try {
            return mapper.readTree(json);
        } catch (JsonProcessingException e) {
            throw new SQLException("Failed to parse JSON", e);
        }
    }

    @FunctionalInterface
    private interface SqlAction<T> {
        T execute() throws SQLException;
    }

    private <T> T executeWithCancelCheck(SqlAction<T> action) {
        if (Thread.currentThread().isInterrupted()) {
            throw new CancelledTaskException();
        }
        try {
            return action.execute();
        } catch (PSQLException ex) {
            if ("50011".equals(ex.getSQLState())) {
                throw new CancelledTaskException();
            }
            throw new RuntimeException(ex);
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
    }
}