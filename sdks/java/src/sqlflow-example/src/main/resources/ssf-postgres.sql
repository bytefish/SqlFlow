CREATE SCHEMA IF NOT EXISTS ssf;

-- ==========================================
-- FUNCTIONS & HELPERS
-- ==========================================
CREATE OR REPLACE FUNCTION ssf.current_time_fn()
RETURNS TIMESTAMPTZ
LANGUAGE plpgsql
AS $$
DECLARE
    v_fake TEXT;
BEGIN
    BEGIN
        v_fake := current_setting('ssf.fake_now', true);
    EXCEPTION WHEN OTHERS THEN
        v_fake := NULL;
    END;

    IF v_fake IS NOT NULL AND length(trim(v_fake)) > 0 THEN
        RETURN v_fake::TIMESTAMPTZ;
    END IF;
    
    RETURN clock_timestamp();
END;
$$;

CREATE OR REPLACE FUNCTION ssf.validate_queue_name(p_queue_name TEXT)
RETURNS VARCHAR(57)
LANGUAGE plpgsql
AS $$
BEGIN
    IF p_queue_name IS NULL OR length(trim(p_queue_name)) = 0 THEN
        RETURN NULL;
    END IF;
    
    IF octet_length(p_queue_name) > 114 THEN
        RETURN NULL;
    END IF;

    RETURN p_queue_name::VARCHAR(57);
END;
$$;

CREATE OR REPLACE FUNCTION ssf.get_schema_version()
RETURNS VARCHAR(50)
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN 'main-postgres-singletable';
END;
$$;

-- ==========================================
-- STATIC TABLE DEFINITIONS
-- ==========================================
CREATE TABLE IF NOT EXISTS ssf.queues (
    queue_name VARCHAR(57) PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL DEFAULT ssf.current_time_fn(),
    storage_mode VARCHAR(50) NOT NULL DEFAULT 'unpartitioned' 
        CONSTRAINT chk_storage_mode CHECK (storage_mode IN ('unpartitioned', 'partitioned')),
    default_partition VARCHAR(50) NOT NULL DEFAULT 'enabled' 
        CONSTRAINT chk_default_partition CHECK (default_partition IN ('enabled', 'disabled')),
    partition_lookahead_sec INT NOT NULL DEFAULT 2419200, -- 28 days
        CONSTRAINT chk_partition_lookahead_sec CHECK (partition_lookahead_sec >= 0),
    partition_lookback_sec INT NOT NULL DEFAULT 86400, -- 1 day
        CONSTRAINT chk_partition_lookback_sec CHECK (partition_lookback_sec >= 0),
    cleanup_ttl_sec INT NOT NULL DEFAULT 2592000, -- 30 days
        CONSTRAINT chk_cleanup_ttl_sec CHECK (cleanup_ttl_sec >= 0),
    cleanup_limit INT NOT NULL DEFAULT 1000 
        CONSTRAINT chk_cleanup_limit CHECK (cleanup_limit >= 1),
    detach_mode VARCHAR(50) NOT NULL DEFAULT 'none' 
        CONSTRAINT chk_detach_mode CHECK (detach_mode IN ('none', 'empty')),
    detach_min_age_sec INT NOT NULL DEFAULT 2592000 -- 30 days
        CONSTRAINT chk_detach_min_age_sec CHECK (detach_min_age_sec >= 0)
);

CREATE TABLE IF NOT EXISTS ssf.tasks (
    queue_name VARCHAR(57) NOT NULL,
    task_id UUID NOT NULL,
    task_name TEXT NOT NULL,
    params JSONB NOT NULL,
    headers JSONB,
    retry_strategy JSONB,
    max_attempts INT,
    cancellation JSONB,
    enqueue_at TIMESTAMPTZ NOT NULL DEFAULT ssf.current_time_fn(),
    first_started_at TIMESTAMPTZ,
    state VARCHAR(50) NOT NULL CONSTRAINT chk_task_state CHECK (state IN ('pending', 'running', 'sleeping', 'completed', 'failed', 'cancelled')),
    attempts INT NOT NULL DEFAULT 0,
    last_attempt_run UUID,
    completed_payload JSONB,
    cancelled_at TIMESTAMPTZ,
    idempotency_key VARCHAR(450),

    PRIMARY KEY (queue_name, task_id)
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_ssf_tasks_idempotency
    ON ssf.tasks (queue_name, idempotency_key) WHERE idempotency_key IS NOT NULL;

CREATE TABLE IF NOT EXISTS ssf.runs (
    queue_name VARCHAR(57) NOT NULL,
    run_id UUID NOT NULL,
    task_id UUID NOT NULL,
    attempt INT NOT NULL,
    state VARCHAR(50) NOT NULL CONSTRAINT chk_run_state CHECK (state IN ('pending', 'running', 'sleeping', 'completed', 'failed', 'cancelled')),
    claimed_by TEXT,
    claim_expires_at TIMESTAMPTZ,
    available_at TIMESTAMPTZ NOT NULL,
    wake_event TEXT,
    event_payload JSONB,
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    failed_at TIMESTAMPTZ,
    result JSONB,
    failure_reason JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT ssf.current_time_fn(),

    PRIMARY KEY (queue_name, run_id)
);

CREATE INDEX IF NOT EXISTS ix_ssf_runs_sai ON ssf.runs (queue_name, state, available_at);
CREATE INDEX IF NOT EXISTS ix_ssf_runs_cei ON ssf.runs (queue_name, claim_expires_at) WHERE state = 'running' AND claim_expires_at IS NOT NULL;

CREATE TABLE IF NOT EXISTS ssf.checkpoints (
    queue_name VARCHAR(57) NOT NULL,
    task_id UUID NOT NULL,
    checkpoint_name VARCHAR(450) NOT NULL,
    state JSONB,
    status VARCHAR(50) NOT NULL DEFAULT 'committed',
    owner_run_id UUID,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT ssf.current_time_fn(),

    PRIMARY KEY (queue_name, task_id, checkpoint_name)
);

CREATE TABLE IF NOT EXISTS ssf.events (
    queue_name VARCHAR(57) NOT NULL,
    event_name VARCHAR(450) NOT NULL,
    payload JSONB,
    emitted_at TIMESTAMPTZ NOT NULL DEFAULT ssf.current_time_fn(),

    PRIMARY KEY (queue_name, event_name)
);

CREATE TABLE IF NOT EXISTS ssf.waits (
    queue_name VARCHAR(57) NOT NULL,
    task_id UUID NOT NULL,
    run_id UUID NOT NULL,
    step_name VARCHAR(450) NOT NULL,
    event_name VARCHAR(450) NOT NULL,
    timeout_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT ssf.current_time_fn(),

    PRIMARY KEY (queue_name, run_id, step_name)
);


-- ==========================================
-- STORED PROCEDURES (as PL/pgSQL Functions)
-- ==========================================

CREATE OR REPLACE PROCEDURE ssf.create_queue(
    p_queue_name TEXT,
    p_storage_mode TEXT DEFAULT 'unpartitioned'
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_existing_mode VARCHAR(50);
BEGIN
    IF ssf.validate_queue_name(p_queue_name) IS NULL THEN
        RAISE EXCEPTION 'Invalid queue name.' USING ERRCODE = '50001';
    END IF;

    IF p_storage_mode NOT IN ('unpartitioned', 'partitioned') THEN
        RAISE EXCEPTION 'Unsupported queue storage mode.' USING ERRCODE = '50002';
    END IF;

    BEGIN
        INSERT INTO ssf.queues (queue_name, storage_mode)
        VALUES (p_queue_name, p_storage_mode);
    EXCEPTION WHEN unique_violation THEN
        -- ON CONFLICT DO NOTHING equivalent
        NULL;
    END;

    SELECT storage_mode INTO v_existing_mode
    FROM ssf.queues
    WHERE queue_name = p_queue_name;

    IF v_existing_mode <> p_storage_mode THEN
        RAISE EXCEPTION 'Queue already exists with different storage mode.' USING ERRCODE = '50003';
    END IF;
END;
$$;


CREATE OR REPLACE PROCEDURE ssf.drop_queue(p_queue_name TEXT)
LANGUAGE plpgsql
AS $$
DECLARE
    v_existing_queue VARCHAR(57);
BEGIN
    SELECT queue_name INTO v_existing_queue
    FROM ssf.queues
    WHERE queue_name = p_queue_name;

    IF v_existing_queue IS NULL THEN
        RETURN;
    END IF;

    DELETE FROM ssf.waits WHERE queue_name = p_queue_name;
    DELETE FROM ssf.events WHERE queue_name = p_queue_name;
    DELETE FROM ssf.checkpoints WHERE queue_name = p_queue_name;
    DELETE FROM ssf.runs WHERE queue_name = p_queue_name;
    DELETE FROM ssf.tasks WHERE queue_name = p_queue_name;
    DELETE FROM ssf.queues WHERE queue_name = p_queue_name;
END;
$$;


CREATE OR REPLACE FUNCTION ssf.spawn_task(
    p_queue_name TEXT,
    p_task_name TEXT,
    p_params JSONB,
    p_options JSONB DEFAULT '{}'::jsonb
)
RETURNS TABLE (
    task_id UUID,
    run_id UUID,
    attempt INT,
    created BOOLEAN
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_task_id UUID := gen_random_uuid();
    v_run_id UUID := gen_random_uuid();
    v_attempt INT := 1;
    v_now TIMESTAMPTZ := ssf.current_time_fn();

    v_headers JSONB := p_options->'headers';
    v_retry_strategy JSONB := p_options->'retry_strategy';
    v_max_attempts INT := (p_options->>'max_attempts')::INT;
    v_cancellation JSONB := p_options->'cancellation';
    v_idempotency_key VARCHAR(450) := p_options->>'idempotency_key';

    v_existing_task_id UUID;
    v_existing_run_id UUID;
    v_existing_attempt INT;
BEGIN
    IF p_task_name IS NULL OR length(trim(p_task_name)) = 0 THEN
        RAISE EXCEPTION 'task_name must be provided' USING ERRCODE = '50004';
    END IF;

    IF v_idempotency_key IS NOT NULL THEN
        BEGIN
            INSERT INTO ssf.tasks (queue_name, task_id, task_name, params, headers, retry_strategy, max_attempts, cancellation, enqueue_at, state, attempts, last_attempt_run, idempotency_key)
            VALUES (p_queue_name, v_task_id, p_task_name, p_params, v_headers, v_retry_strategy, v_max_attempts, v_cancellation, v_now, 'pending', v_attempt, v_run_id, v_idempotency_key);
        EXCEPTION WHEN unique_violation THEN
            SELECT t.task_id, t.last_attempt_run, t.attempts
            INTO v_existing_task_id, v_existing_run_id, v_existing_attempt
            FROM ssf.tasks t
            WHERE t.queue_name = p_queue_name AND t.idempotency_key = v_idempotency_key;
        END;

        IF v_existing_task_id IS NOT NULL THEN
            RETURN QUERY SELECT v_existing_task_id, v_existing_run_id, v_existing_attempt, FALSE;
            RETURN;
        END IF;
    ELSE
        INSERT INTO ssf.tasks (queue_name, task_id, task_name, params, headers, retry_strategy, max_attempts, cancellation, enqueue_at, state, attempts, last_attempt_run)
        VALUES (p_queue_name, v_task_id, p_task_name, p_params, v_headers, v_retry_strategy, v_max_attempts, v_cancellation, v_now, 'pending', v_attempt, v_run_id);
    END IF;

    INSERT INTO ssf.runs (queue_name, run_id, task_id, attempt, state, available_at)
    VALUES (p_queue_name, v_run_id, v_task_id, v_attempt, 'pending', v_now);

    RETURN QUERY SELECT v_task_id, v_run_id, v_attempt, TRUE;
END;
$$;


CREATE OR REPLACE FUNCTION ssf.claim_task(
    p_queue_name TEXT,
    p_worker_id TEXT DEFAULT 'worker',
    p_claim_timeout INT DEFAULT 30,
    p_qty INT DEFAULT 1
)
RETURNS TABLE (
    run_id UUID,
    task_id UUID,
    attempt INT,
    task_name TEXT,
    params JSONB,
    retry_strategy JSONB,
    max_attempts INT,
    headers JSONB,
    wake_event TEXT,
    event_payload JSONB
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_now TIMESTAMPTZ := ssf.current_time_fn();
    v_claim_until TIMESTAMPTZ := v_now + (p_claim_timeout || ' seconds')::INTERVAL;
BEGIN

    CREATE TEMP TABLE IF NOT EXISTS claimed_temp (
        run_id UUID,
        task_id UUID,
        attempt INT
    ) ON COMMIT DROP;

    TRUNCATE claimed_temp;

    WITH candidate AS (
        SELECT r.run_id, r.task_id, r.attempt
        FROM ssf.runs r
        JOIN ssf.tasks t ON t.queue_name = r.queue_name AND t.task_id = r.task_id
        WHERE r.queue_name = p_queue_name
          AND r.state IN ('pending', 'sleeping')
          AND t.state IN ('pending', 'sleeping', 'running')
          AND r.available_at <= v_now
        ORDER BY r.available_at, r.run_id
        LIMIT p_qty
        FOR UPDATE OF r SKIP LOCKED
    )
    INSERT INTO claimed_temp (run_id, task_id, attempt)
    SELECT c.run_id, c.task_id, c.attempt FROM candidate c;

    -- Update Runs
    UPDATE ssf.runs r
    SET state = 'running',
        claimed_by = p_worker_id,
        claim_expires_at = v_claim_until,
        started_at = v_now,
        available_at = v_now
    FROM claimed_temp c
    WHERE r.queue_name = p_queue_name AND r.run_id = c.run_id;

    -- Update  Tasks
    UPDATE ssf.tasks t
    SET state = 'running',
        attempts = CASE WHEN t.attempts > c.attempt THEN t.attempts ELSE c.attempt END,
        first_started_at = COALESCE(t.first_started_at, v_now),
        last_attempt_run = c.run_id
    FROM claimed_temp c
    WHERE t.queue_name = p_queue_name AND t.task_id = c.task_id;

    -- Select Results
    RETURN QUERY
    SELECT c.run_id, c.task_id, c.attempt, t.task_name, t.params, t.retry_strategy,
           t.max_attempts, t.headers, r.wake_event, r.event_payload
    FROM claimed_temp c
    JOIN ssf.runs r ON r.queue_name = p_queue_name AND r.run_id = c.run_id
    JOIN ssf.tasks t ON t.queue_name = p_queue_name AND t.task_id = c.task_id
    ORDER BY r.available_at, c.run_id;
END;
$$;


CREATE OR REPLACE PROCEDURE ssf.complete_run(
    p_queue_name TEXT,
    p_run_id UUID,
    p_state JSONB DEFAULT NULL
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_task_id UUID;
    v_state VARCHAR(50);
BEGIN
    SELECT task_id, state INTO v_task_id, v_state
    FROM ssf.runs
    WHERE queue_name = p_queue_name AND run_id = p_run_id
    FOR UPDATE;

    IF v_task_id IS NULL THEN
        RAISE EXCEPTION 'Run not found.' USING ERRCODE = '50005';
    END IF;

    IF v_state <> 'running' THEN
        RAISE EXCEPTION 'Run is not currently running.' USING ERRCODE = '50006';
    END IF;

    UPDATE ssf.runs
    SET state = 'completed', completed_at = ssf.current_time_fn(), result = p_state
    WHERE queue_name = p_queue_name AND run_id = p_run_id;

    UPDATE ssf.tasks
    SET state = 'completed', completed_payload = p_state, last_attempt_run = p_run_id
    WHERE queue_name = p_queue_name AND task_id = v_task_id;

    DELETE FROM ssf.waits
    WHERE queue_name = p_queue_name AND run_id = p_run_id;
END;
$$;


CREATE OR REPLACE PROCEDURE ssf.schedule_run(
    p_queue_name TEXT,
    p_run_id UUID,
    p_wake_at TIMESTAMPTZ
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_task_id UUID;
BEGIN
    SELECT task_id INTO v_task_id
    FROM ssf.runs
    WHERE queue_name = p_queue_name AND run_id = p_run_id AND state = 'running'
    FOR UPDATE;

    IF v_task_id IS NULL THEN
        RAISE EXCEPTION 'Run is not currently running or does not exist.' USING ERRCODE = '50007';
    END IF;

    UPDATE ssf.runs
    SET state = 'sleeping', claimed_by = NULL, claim_expires_at = NULL, available_at = p_wake_at, wake_event = NULL
    WHERE queue_name = p_queue_name AND run_id = p_run_id;

    UPDATE ssf.tasks
    SET state = 'sleeping'
    WHERE queue_name = p_queue_name AND task_id = v_task_id;
END;
$$;


CREATE OR REPLACE PROCEDURE ssf.fail_run(
    p_queue_name TEXT,
    p_run_id UUID,
    p_reason JSONB,
    p_retry_at TIMESTAMPTZ DEFAULT NULL
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_now TIMESTAMPTZ := ssf.current_time_fn();

    v_task_id UUID;
    v_attempt INT;
    v_retry_strategy JSONB;
    v_max_attempts INT;
    v_first_started TIMESTAMPTZ;
    v_cancellation JSONB;

    v_next_attempt INT;
    v_delay_seconds DOUBLE PRECISION := 0;
    v_next_available TIMESTAMPTZ;
    v_retry_kind TEXT;
    v_base DOUBLE PRECISION;
    v_factor DOUBLE PRECISION;
    v_max_seconds DOUBLE PRECISION;
    v_max_duration BIGINT;
    v_task_cancel BOOLEAN := FALSE;

    v_new_run_id UUID;
    v_task_state_after VARCHAR(50);
    v_recorded_attempt INT;
    v_last_attempt_run UUID := p_run_id;
    v_cancelled_at TIMESTAMPTZ := NULL;
BEGIN
    SELECT task_id, attempt INTO v_task_id, v_attempt
    FROM ssf.runs
    WHERE queue_name = p_queue_name AND run_id = p_run_id AND state IN ('running', 'sleeping')
    FOR UPDATE;

    IF v_task_id IS NULL THEN
        RAISE EXCEPTION 'Run cannot be failed.' USING ERRCODE = '50008';
    END IF;

    SELECT retry_strategy, max_attempts, first_started_at, cancellation
    INTO v_retry_strategy, v_max_attempts, v_first_started, v_cancellation
    FROM ssf.tasks
    WHERE queue_name = p_queue_name AND task_id = v_task_id
    FOR UPDATE;

    v_next_attempt := v_attempt + 1;
    v_task_state_after := 'failed';
    v_recorded_attempt := v_attempt;

    IF v_max_attempts IS NULL OR v_next_attempt <= v_max_attempts THEN
        IF p_retry_at IS NOT NULL THEN
            v_next_available := p_retry_at;
        ELSE
            v_retry_kind := COALESCE(v_retry_strategy->>'kind', 'none');
            IF v_retry_kind = 'fixed' THEN
                v_base := COALESCE((v_retry_strategy->>'base_seconds')::DOUBLE PRECISION, 60.0);
                v_delay_seconds := v_base;
            ELSIF v_retry_kind = 'exponential' THEN
                v_base := COALESCE((v_retry_strategy->>'base_seconds')::DOUBLE PRECISION, 30.0);
                v_factor := COALESCE((v_retry_strategy->>'factor')::DOUBLE PRECISION, 2.0);
                v_delay_seconds := v_base * power(v_factor, CASE WHEN v_attempt - 1 > 0 THEN v_attempt - 1 ELSE 0 END);

                v_max_seconds := (v_retry_strategy->>'max_seconds')::DOUBLE PRECISION;
                IF v_max_seconds IS NOT NULL AND v_delay_seconds > v_max_seconds THEN
                    v_delay_seconds := v_max_seconds;
                END IF;
            ELSE
                v_delay_seconds := 0;
            END IF;

            v_next_available := v_now + (v_delay_seconds || ' seconds')::INTERVAL;
        END IF;

        IF v_next_available < v_now THEN
            v_next_available := v_now;
        END IF;

        IF v_cancellation IS NOT NULL THEN
            v_max_duration := (v_cancellation->>'max_duration')::BIGINT;
            IF v_max_duration IS NOT NULL AND v_first_started IS NOT NULL THEN
                IF EXTRACT(EPOCH FROM (v_next_available - v_first_started)) >= v_max_duration THEN
                    v_task_cancel := TRUE;
                END IF;
            END IF;
        END IF;

        IF v_task_cancel = FALSE THEN
            v_task_state_after := CASE WHEN v_next_available > v_now THEN 'sleeping' ELSE 'pending' END;
            v_new_run_id := gen_random_uuid();
            v_recorded_attempt := v_next_attempt;
            v_last_attempt_run := v_new_run_id;
        END IF;
    END IF;

    IF v_task_cancel = TRUE THEN
        v_task_state_after := 'cancelled';
        v_cancelled_at := v_now;
        v_recorded_attempt := CASE WHEN v_recorded_attempt > v_attempt THEN v_recorded_attempt ELSE v_attempt END;
        v_last_attempt_run := p_run_id;
    END IF;

    UPDATE ssf.runs
    SET state = 'failed', wake_event = NULL, failed_at = v_now, failure_reason = p_reason
    WHERE queue_name = p_queue_name AND run_id = p_run_id;

    IF v_new_run_id IS NOT NULL THEN
        INSERT INTO ssf.runs (queue_name, run_id, task_id, attempt, state, available_at)
        VALUES (p_queue_name, v_new_run_id, v_task_id, v_next_attempt, v_task_state_after, v_next_available);
    END IF;

    UPDATE ssf.tasks
    SET state = v_task_state_after,
        attempts = CASE WHEN attempts > v_recorded_attempt THEN attempts ELSE v_recorded_attempt END,
        last_attempt_run = v_last_attempt_run,
        cancelled_at = COALESCE(cancelled_at, v_cancelled_at)
    WHERE queue_name = p_queue_name AND task_id = v_task_id;

    DELETE FROM ssf.waits WHERE queue_name = p_queue_name AND run_id = p_run_id;
END;
$$;


CREATE OR REPLACE PROCEDURE ssf.set_task_checkpoint_state(
    p_queue_name TEXT,
    p_task_id UUID,
    p_step_name TEXT,
    p_state JSONB,
    p_owner_run UUID,
    p_extend_claim_by INT DEFAULT NULL
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_now TIMESTAMPTZ := ssf.current_time_fn();
    v_new_attempt INT;
    v_run_state VARCHAR(50);
    v_task_state VARCHAR(50);
    v_existing_owner UUID;
    v_existing_attempt INT;
BEGIN
    IF p_step_name IS NULL OR length(trim(p_step_name)) = 0 THEN
        RAISE EXCEPTION 'step_name must be provided' USING ERRCODE = '50009';
    END IF;

    SELECT r.attempt, r.state, t.state
    INTO v_new_attempt, v_run_state, v_task_state
    FROM ssf.runs r
    JOIN ssf.tasks t ON t.queue_name = r.queue_name AND t.task_id = r.task_id
    WHERE r.queue_name = p_queue_name AND r.run_id = p_owner_run
    FOR UPDATE;

    IF v_new_attempt IS NULL THEN RAISE EXCEPTION 'Run not found for checkpoint' USING ERRCODE = '50010'; END IF;
    IF v_task_state = 'cancelled' THEN RAISE EXCEPTION 'Task has been cancelled' USING ERRCODE = '50011'; END IF;
    IF v_run_state = 'failed' THEN RAISE EXCEPTION 'Run has already failed' USING ERRCODE = '50012'; END IF;

    IF p_extend_claim_by IS NOT NULL AND p_extend_claim_by > 0 THEN
        UPDATE ssf.runs
        SET claim_expires_at = v_now + (p_extend_claim_by || ' seconds')::INTERVAL;
    END IF;

    SELECT c.owner_run_id, r.attempt
    INTO v_existing_owner, v_existing_attempt
    FROM ssf.checkpoints c
    LEFT JOIN ssf.runs r ON r.queue_name = c.queue_name AND r.run_id = c.owner_run_id
    WHERE c.queue_name = p_queue_name AND c.task_id = p_task_id AND c.checkpoint_name = p_step_name
    FOR UPDATE OF c;

    IF v_existing_owner IS NULL OR v_existing_attempt IS NULL OR v_new_attempt >= v_existing_attempt THEN
        INSERT INTO ssf.checkpoints (queue_name, task_id, checkpoint_name, state, status, owner_run_id, updated_at)
        VALUES (p_queue_name, p_task_id, p_step_name, p_state, 'committed', p_owner_run, v_now)
        ON CONFLICT (queue_name, task_id, checkpoint_name) DO UPDATE
        SET state = EXCLUDED.state, status = EXCLUDED.status, owner_run_id = EXCLUDED.owner_run_id, updated_at = EXCLUDED.updated_at;
    END IF;
END;
$$;


CREATE OR REPLACE PROCEDURE ssf.extend_claim(
    p_queue_name TEXT,
    p_run_id UUID,
    p_extend_by INT
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_now TIMESTAMPTZ := ssf.current_time_fn();
    v_run_state VARCHAR(50);
    v_claim_expires_at TIMESTAMPTZ;
    v_task_state VARCHAR(50);
BEGIN
    IF p_extend_by IS NULL OR p_extend_by <= 0 THEN
        RAISE EXCEPTION 'extend_by must be > 0' USING ERRCODE = '50013';
    END IF;

    SELECT r.state, r.claim_expires_at, t.state
    INTO v_run_state, v_claim_expires_at, v_task_state
    FROM ssf.runs r
    JOIN ssf.tasks t ON t.queue_name = r.queue_name AND t.task_id = r.task_id
    WHERE r.queue_name = p_queue_name AND r.run_id = p_run_id
    FOR UPDATE;

    IF v_run_state IS NULL THEN RAISE EXCEPTION 'Run not found' USING ERRCODE = '50014'; END IF;
    IF v_task_state = 'cancelled' THEN RAISE EXCEPTION 'Task cancelled' USING ERRCODE = '50011'; END IF;
    IF v_run_state <> 'running' THEN RAISE EXCEPTION 'Run not running' USING ERRCODE = '50015'; END IF;
    IF v_claim_expires_at IS NULL THEN RAISE EXCEPTION 'No active claim' USING ERRCODE = '50016'; END IF;

    UPDATE ssf.runs
    SET claim_expires_at = v_now + (p_extend_by || ' seconds')::INTERVAL;
END;
$$;


CREATE OR REPLACE FUNCTION ssf.get_task_checkpoint_state(
    p_queue_name TEXT,
    p_task_id UUID,
    p_step_name TEXT,
    p_include_pending INT DEFAULT 0
)
RETURNS TABLE (
    checkpoint_name VARCHAR(450),
    state JSONB,
    status VARCHAR(50),
    owner_run_id UUID,
    updated_at TIMESTAMPTZ
)
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN QUERY
    SELECT c.checkpoint_name, c.state, c.status, c.owner_run_id, c.updated_at
    FROM ssf.checkpoints c
    WHERE c.queue_name = p_queue_name
      AND c.task_id = p_task_id
      AND c.checkpoint_name = p_step_name
      AND (p_include_pending = 1 OR c.status = 'committed');
END;
$$;


CREATE OR REPLACE FUNCTION ssf.get_task_checkpoint_states(
    p_queue_name TEXT,
    p_task_id UUID,
    p_run_id UUID
)
RETURNS TABLE (
    checkpoint_name VARCHAR(450),
    state JSONB,
    status VARCHAR(50),
    owner_run_id UUID,
    updated_at TIMESTAMPTZ
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_run_task_id UUID;
    v_run_attempt INT;
BEGIN
    SELECT task_id, attempt INTO v_run_task_id, v_run_attempt
    FROM ssf.runs
    WHERE queue_name = p_queue_name AND run_id = p_run_id;

    IF v_run_task_id IS NULL THEN RAISE EXCEPTION 'Run not found' USING ERRCODE = '50014'; END IF;
    IF v_run_task_id <> p_task_id THEN RAISE EXCEPTION 'Run does not belong to task mismatch' USING ERRCODE = '50017'; END IF;

    RETURN QUERY
    SELECT c.checkpoint_name, c.state, c.status, c.owner_run_id, c.updated_at
    FROM ssf.checkpoints c
    LEFT JOIN ssf.runs r ON r.queue_name = c.queue_name AND r.run_id = c.owner_run_id
    WHERE c.queue_name = p_queue_name
      AND c.task_id = p_task_id
      AND c.status = 'committed'
      AND (r.attempt IS NULL OR r.attempt <= v_run_attempt)
    ORDER BY c.updated_at ASC;
END;
$$;


CREATE OR REPLACE FUNCTION ssf.await_event(
    p_queue_name TEXT,
    p_task_id UUID,
    p_run_id UUID,
    p_step_name TEXT,
    p_event_name TEXT,
    p_timeout INT DEFAULT NULL
)
RETURNS TABLE (
    should_suspend BOOLEAN,
    payload JSONB
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_now TIMESTAMPTZ := ssf.current_time_fn();
    v_timeout_at TIMESTAMPTZ := NULL;
    v_run_state VARCHAR(50);
    v_existing_payload JSONB;
    v_wake_event TEXT;
    v_task_state VARCHAR(50);
    v_checkpoint_payload JSONB;
    v_event_payload JSONB;
    v_resolved_payload JSONB;
    v_dummy INT;
BEGIN
    IF p_event_name IS NULL OR length(trim(p_event_name)) = 0 THEN
        RAISE EXCEPTION 'event_name must be provided' USING ERRCODE = '50018';
    END IF;

    IF p_timeout IS NOT NULL THEN
        IF p_timeout < 0 THEN RAISE EXCEPTION 'timeout must be non-negative' USING ERRCODE = '50019'; END IF;
        v_timeout_at := v_now + (p_timeout || ' seconds')::INTERVAL;
    END IF;

    SELECT state INTO v_checkpoint_payload
    FROM ssf.checkpoints
    WHERE queue_name = p_queue_name AND task_id = p_task_id AND checkpoint_name = p_step_name;

    IF v_checkpoint_payload IS NOT NULL THEN
        RETURN QUERY SELECT FALSE, v_checkpoint_payload;
        RETURN;
    END IF;

    BEGIN
        INSERT INTO ssf.events (queue_name, event_name, payload, emitted_at)
        VALUES (p_queue_name, p_event_name, NULL, '1970-01-01');
    EXCEPTION WHEN unique_violation THEN
        NULL;
    END;

    SELECT 1 INTO v_dummy FROM ssf.events WHERE queue_name = p_queue_name AND event_name = p_event_name FOR UPDATE;

    SELECT r.state, r.event_payload, r.wake_event, t.state
    INTO v_run_state, v_existing_payload, v_wake_event, v_task_state
    FROM ssf.runs r
    JOIN ssf.tasks t ON t.queue_name = r.queue_name AND t.task_id = r.task_id
    WHERE r.queue_name = p_queue_name AND r.run_id = p_run_id
    FOR UPDATE;

    IF v_run_state IS NULL THEN RAISE EXCEPTION 'Run not found' USING ERRCODE = '50014'; END IF;
    IF v_task_state = 'cancelled' THEN RAISE EXCEPTION 'Task cancelled' USING ERRCODE = '50011'; END IF;

    SELECT e.payload INTO v_event_payload FROM ssf.events e WHERE e.queue_name = p_queue_name AND e.event_name = p_event_name;

    IF v_existing_payload IS NOT NULL THEN
        UPDATE ssf.runs SET event_payload = NULL WHERE queue_name = p_queue_name AND run_id = p_run_id;
        IF v_event_payload IS NOT NULL AND v_event_payload = v_existing_payload THEN
            v_resolved_payload := v_existing_payload;
        END IF;
    END IF;

    IF v_run_state <> 'running' THEN RAISE EXCEPTION 'Run must be running to await events' USING ERRCODE = '50020'; END IF;

    IF v_resolved_payload IS NULL AND v_event_payload IS NOT NULL THEN
        v_resolved_payload := v_event_payload;
    END IF;

    IF v_resolved_payload IS NOT NULL THEN
        INSERT INTO ssf.checkpoints (queue_name, task_id, checkpoint_name, state, status, owner_run_id, updated_at)
        VALUES (p_queue_name, p_task_id, p_step_name, v_resolved_payload, 'committed', p_run_id, v_now)
        ON CONFLICT (queue_name, task_id, checkpoint_name) DO UPDATE
        SET state = EXCLUDED.state, status = EXCLUDED.status, owner_run_id = EXCLUDED.owner_run_id, updated_at = EXCLUDED.updated_at;

        RETURN QUERY SELECT FALSE, v_resolved_payload;
        RETURN;
    END IF;

    IF v_resolved_payload IS NULL AND v_wake_event = p_event_name AND v_existing_payload IS NULL THEN
        UPDATE ssf.runs SET wake_event = NULL WHERE queue_name = p_queue_name AND run_id = p_run_id;
        RETURN QUERY SELECT FALSE, NULL::JSONB
        RETURN;
    END IF;

    INSERT INTO ssf.waits (queue_name, task_id, run_id, step_name, event_name, timeout_at, created_at)
    VALUES (p_queue_name, p_task_id, p_run_id, p_step_name, p_event_name, v_timeout_at, v_now)
    ON CONFLICT (queue_name, run_id, step_name) DO UPDATE
    SET event_name = EXCLUDED.event_name, timeout_at = EXCLUDED.timeout_at, created_at = EXCLUDED.created_at;

    UPDATE ssf.runs
    SET state = 'sleeping', claimed_by = NULL, claim_expires_at = NULL,
        available_at = COALESCE(v_timeout_at, '9999-12-31 23:59:59.999999+00'::TIMESTAMPTZ),
        wake_event = p_event_name, event_payload = NULL
    WHERE queue_name = p_queue_name AND run_id = p_run_id;

    UPDATE ssf.tasks SET state = 'sleeping' WHERE queue_name = p_queue_name AND task_id = p_task_id;

    RETURN QUERY SELECT TRUE, NULL::JSONB;
END;
$$;


CREATE OR REPLACE PROCEDURE ssf.emit_event(
    p_queue_name TEXT,
    p_event_name TEXT,
    p_payload JSONB DEFAULT 'null'::jsonb
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_now TIMESTAMPTZ := ssf.current_time_fn();
    v_payload JSONB := COALESCE(p_payload, 'null'::jsonb);
    v_emit_applied INT;
BEGIN
    IF p_event_name IS NULL OR length(trim(p_event_name)) = 0 THEN
        RAISE EXCEPTION 'event_name must be provided' USING ERRCODE = '50021';
    END IF;

    UPDATE ssf.events
    SET payload = v_payload, emitted_at = v_now
    WHERE queue_name = p_queue_name AND event_name = p_event_name AND payload IS NULL;

    GET DIAGNOSTICS v_emit_applied = ROW_COUNT;

    IF v_emit_applied = 0 THEN
        BEGIN
            INSERT INTO ssf.events (queue_name, event_name, payload, emitted_at)
            VALUES (p_queue_name, p_event_name, v_payload, v_now);
            v_emit_applied := 1;
        EXCEPTION WHEN unique_violation THEN
            v_emit_applied := 0; 
        END;
    END IF;

    IF v_emit_applied = 0 THEN RETURN; END IF;

    CREATE TEMP TABLE IF NOT EXISTS affected_runs_temp (
        run_id UUID, 
        task_id UUID, 
        step_name TEXT
    ) ON COMMIT DROP;
    
    TRUNCATE affected_runs_temp;

    DELETE FROM ssf.waits
    WHERE queue_name = p_queue_name AND event_name = p_event_name AND timeout_at IS NOT NULL AND timeout_at <= v_now;

    INSERT INTO affected_runs_temp (run_id, task_id, step_name)
    SELECT run_id, task_id, step_name FROM ssf.waits
    WHERE queue_name = p_queue_name AND event_name = p_event_name AND (timeout_at IS NULL OR timeout_at > v_now);

    CREATE TEMP TABLE IF NOT EXISTS updated_runs_temp (
        run_id UUID, 
        task_id UUID
    ) ON COMMIT DROP;
    
    TRUNCATE updated_runs_temp;

    WITH upd AS (
        UPDATE ssf.runs r
        SET state = 'pending', available_at = v_now, wake_event = NULL, event_payload = v_payload, claimed_by = NULL, claim_expires_at = NULL
        FROM affected_runs_temp a 
        WHERE r.queue_name = p_queue_name AND r.run_id = a.run_id AND r.state = 'sleeping'
        RETURNING r.run_id, r.task_id
    )
    INSERT INTO updated_runs_temp (run_id, task_id)
    SELECT run_id, task_id FROM upd;

    -- Merge Checkpoints (UPSERT)
    INSERT INTO ssf.checkpoints (queue_name, task_id, checkpoint_name, state, status, owner_run_id, updated_at)
    SELECT p_queue_name, a.task_id, a.step_name, v_payload, 'committed', u.run_id, v_now
    FROM affected_runs_temp a 
    JOIN updated_runs_temp u ON a.run_id = u.run_id
    ON CONFLICT (queue_name, task_id, checkpoint_name) DO UPDATE 
    SET state = EXCLUDED.state, status = EXCLUDED.status, owner_run_id = EXCLUDED.owner_run_id, updated_at = EXCLUDED.updated_at;

    UPDATE ssf.tasks t
    SET state = 'pending'
    FROM updated_runs_temp u 
    WHERE t.queue_name = p_queue_name AND t.task_id = u.task_id;

    DELETE FROM ssf.waits w
    USING updated_runs_temp u 
    WHERE w.queue_name = p_queue_name AND w.run_id = u.run_id AND w.event_name = p_event_name;
END;
$$;


CREATE OR REPLACE PROCEDURE ssf.cancel_task(
    p_queue_name TEXT,
    p_task_id UUID
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_now TIMESTAMPTZ := ssf.current_time_fn();
    v_task_state VARCHAR(50);
    v_dummy UUID;
BEGIN
    SELECT run_id INTO v_dummy
    FROM ssf.runs
    WHERE queue_name = p_queue_name AND task_id = p_task_id AND state NOT IN ('completed', 'failed', 'cancelled')
    LIMIT 1
    FOR UPDATE;

    SELECT state INTO v_task_state
    FROM ssf.tasks
    WHERE queue_name = p_queue_name AND task_id = p_task_id
    FOR UPDATE;

    IF v_task_state IS NULL THEN
        RAISE EXCEPTION 'Task not found in queue' USING ERRCODE = '50024';
    END IF;

    IF v_task_state IN ('completed', 'failed', 'cancelled') THEN
        RETURN;
    END IF;

    UPDATE ssf.tasks
    SET state = 'cancelled', cancelled_at = COALESCE(cancelled_at, v_now)
    WHERE queue_name = p_queue_name AND task_id = p_task_id;

    UPDATE ssf.runs
    SET state = 'cancelled', claimed_by = NULL, claim_expires_at = NULL
    WHERE queue_name = p_queue_name AND task_id = p_task_id AND state NOT IN ('completed', 'failed', 'cancelled');

    DELETE FROM ssf.waits
    WHERE queue_name = p_queue_name AND task_id = p_task_id;
END;
$$;


CREATE OR REPLACE FUNCTION ssf.cleanup_tasks(
    p_queue_name TEXT,
    p_ttl_seconds INT,
    p_limit INT DEFAULT 1000
)
RETURNS TABLE (deleted_tasks INT)
LANGUAGE plpgsql
AS $$
DECLARE
    v_now TIMESTAMPTZ := ssf.current_time_fn();
    v_cutoff TIMESTAMPTZ;
    v_deleted_count INT := 0;
BEGIN
    IF p_ttl_seconds IS NULL OR p_ttl_seconds < 0 THEN
        RAISE EXCEPTION 'TTL must be a non-negative number of seconds' USING ERRCODE = '50022';
    END IF;

    v_cutoff := v_now - (p_ttl_seconds || ' seconds')::INTERVAL;

    CREATE TEMP TABLE IF NOT EXISTS to_delete_temp (task_id UUID) ON COMMIT DROP;
    TRUNCATE to_delete_temp;

    INSERT INTO to_delete_temp (task_id)
    SELECT t.task_id
    FROM ssf.tasks t
    LEFT JOIN ssf.runs r ON r.queue_name = t.queue_name AND r.run_id = t.last_attempt_run
    WHERE t.queue_name = p_queue_name
      AND t.state IN ('completed', 'failed', 'cancelled')
      AND (
        (t.state = 'completed' AND r.completed_at < v_cutoff) OR
        (t.state = 'failed' AND r.failed_at < v_cutoff) OR
        (t.state = 'cancelled' AND t.cancelled_at < v_cutoff)
      )
    LIMIT p_limit;

    DELETE FROM ssf.waits w USING to_delete_temp d WHERE w.queue_name = p_queue_name AND w.task_id = d.task_id;
    DELETE FROM ssf.checkpoints c USING to_delete_temp d WHERE c.queue_name = p_queue_name AND c.task_id = d.task_id;
    DELETE FROM ssf.runs r USING to_delete_temp d WHERE r.queue_name = p_queue_name AND r.task_id = d.task_id;
    
    WITH del AS (
        DELETE FROM ssf.tasks t USING to_delete_temp d WHERE t.queue_name = p_queue_name AND t.task_id = d.task_id
        RETURNING 1
    )
    SELECT count(*) INTO v_deleted_count FROM del;

    RETURN QUERY SELECT v_deleted_count;
END;
$$;


CREATE OR REPLACE FUNCTION ssf.cleanup_events(
    p_queue_name TEXT,
    p_ttl_seconds INT,
    p_limit INT DEFAULT 1000
)
RETURNS TABLE (deleted_events INT)
LANGUAGE plpgsql
AS $$
DECLARE
    v_now TIMESTAMPTZ := ssf.current_time_fn();
    v_cutoff TIMESTAMPTZ;
    v_deleted_count INT := 0;
BEGIN
    IF p_ttl_seconds IS NULL OR p_ttl_seconds < 0 THEN
        RAISE EXCEPTION 'TTL must be a non-negative number of seconds' USING ERRCODE = '50023';
    END IF;

    v_cutoff := v_now - (p_ttl_seconds || ' seconds')::INTERVAL;

    CREATE TEMP TABLE IF NOT EXISTS to_delete_events_temp (event_name VARCHAR(450)) ON COMMIT DROP;
    TRUNCATE to_delete_events_temp;

    INSERT INTO to_delete_events_temp (event_name)
    SELECT event_name
    FROM ssf.events
    WHERE queue_name = p_queue_name AND emitted_at < v_cutoff
    ORDER BY emitted_at
    LIMIT p_limit;

    WITH del AS (
        DELETE FROM ssf.events e USING to_delete_events_temp d WHERE e.queue_name = p_queue_name AND e.event_name = d.event_name
        RETURNING 1
    )
    SELECT count(*) INTO v_deleted_count FROM del;

    RETURN QUERY SELECT v_deleted_count;
END;
$$;

CREATE OR REPLACE FUNCTION ssf.cleanup_events(
    p_queue_name TEXT,
    p_ttl_seconds INT,
    p_limit INT DEFAULT 1000
)
RETURNS TABLE (deleted_events INT)
LANGUAGE plpgsql
AS $$
DECLARE
    v_now TIMESTAMPTZ := ssf.current_time_fn();
    v_cutoff TIMESTAMPTZ;
    v_deleted_count INT := 0;
BEGIN
    IF p_ttl_seconds IS NULL OR p_ttl_seconds < 0 THEN
        RAISE EXCEPTION 'TTL must be a non-negative number of seconds' USING ERRCODE = '50023';
    END IF;

    v_cutoff := v_now - (p_ttl_seconds || ' seconds')::INTERVAL;

    CREATE TEMP TABLE IF NOT EXISTS to_delete_events_temp (event_name VARCHAR(450)) ON COMMIT DROP;
    TRUNCATE to_delete_events_temp;

    INSERT INTO to_delete_events_temp (event_name)
    SELECT event_name
    FROM ssf.events
    WHERE queue_name = p_queue_name AND emitted_at < v_cutoff
    ORDER BY emitted_at
    LIMIT p_limit;

    WITH del AS (
        DELETE FROM ssf.events e USING to_delete_events_temp d WHERE e.queue_name = p_queue_name AND e.event_name = d.event_name
        RETURNING 1
    )
    SELECT count(*) INTO v_deleted_count FROM del;

    RETURN QUERY SELECT v_deleted_count;
END;
$$;