IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'ssf')
BEGIN
    EXEC('CREATE SCHEMA ssf');
END
GO

SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
GO

-- Returns either the actual current timestamp or a fake one if
-- the session sets `ssf.fake_now`.
CREATE OR ALTER FUNCTION ssf.current_time_fn()
RETURNS DATETIMEOFFSET
AS
BEGIN
    DECLARE @v_fake NVARCHAR(128) = CAST(SESSION_CONTEXT(N'ssf.fake_now') AS NVARCHAR(128));
    IF @v_fake IS NOT NULL AND LEN(LTRIM(RTRIM(@v_fake))) > 0
    BEGIN
        RETURN CAST(@v_fake AS DATETIMEOFFSET);
    END
    RETURN SYSDATETIMEOFFSET();
END;
GO

SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
GO

CREATE OR ALTER FUNCTION ssf.get_schema_version()
RETURNS NVARCHAR(50)
AS
BEGIN
    RETURN 'main-singletable';
END;
GO

SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
GO

CREATE OR ALTER FUNCTION ssf.validate_queue_name(@p_queue_name NVARCHAR(MAX))
RETURNS NVARCHAR(57)
AS
BEGIN
    IF @p_queue_name IS NULL OR LEN(LTRIM(RTRIM(@p_queue_name))) = 0
        RETURN NULL; 
        
    IF DATALENGTH(@p_queue_name) > 114
        RETURN NULL; 

    RETURN CAST(@p_queue_name AS NVARCHAR(57));
END;
GO

-- ==========================================
-- STATIC TABLE DEFINITIONS
-- ==========================================

IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'queues' AND schema_id = SCHEMA_ID('ssf'))
BEGIN
    CREATE TABLE ssf.queues (
        queue_name NVARCHAR(57) PRIMARY KEY,
        created_at DATETIMEOFFSET NOT NULL DEFAULT ssf.current_time_fn(),
        storage_mode NVARCHAR(50) NOT NULL DEFAULT 'unpartitioned' 
            CHECK (storage_mode IN ('unpartitioned', 'partitioned')),
        default_partition NVARCHAR(50) NOT NULL DEFAULT 'enabled' 
            CHECK (default_partition IN ('enabled', 'disabled')),
        partition_lookahead_sec INT NOT NULL DEFAULT 2419200, -- 28 days
            CHECK (partition_lookahead_sec >= 0),
        partition_lookback_sec INT NOT NULL DEFAULT 86400, -- 1 day
            CHECK (partition_lookback_sec >= 0),
        cleanup_ttl_sec INT NOT NULL DEFAULT 2592000, -- 30 days
            CHECK (cleanup_ttl_sec >= 0),
        cleanup_limit INT NOT NULL DEFAULT 1000 
            CHECK (cleanup_limit >= 1),
        detach_mode NVARCHAR(50) NOT NULL DEFAULT 'none' 
            CHECK (detach_mode IN ('none', 'empty')),
        detach_min_age_sec INT NOT NULL DEFAULT 2592000 -- 30 days
            CHECK (detach_min_age_sec >= 0)
    );
END
GO

IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'tasks' AND schema_id = SCHEMA_ID('ssf'))
BEGIN
    CREATE TABLE ssf.tasks (
        queue_name NVARCHAR(57) NOT NULL,
        task_id UNIQUEIDENTIFIER NOT NULL,
        task_name NVARCHAR(MAX) NOT NULL,
        params NVARCHAR(MAX) NOT NULL CHECK (ISJSON(params) = 1),
        headers NVARCHAR(MAX) CHECK (ISJSON(headers) = 1),
        retry_strategy NVARCHAR(MAX) CHECK (ISJSON(retry_strategy) = 1),
        max_attempts INT,
        cancellation NVARCHAR(MAX) CHECK (ISJSON(cancellation) = 1),
        enqueue_at DATETIMEOFFSET NOT NULL DEFAULT ssf.current_time_fn(),
        first_started_at DATETIMEOFFSET,
        state NVARCHAR(50) NOT NULL CHECK (state IN ('pending', 'running', 'sleeping', 'completed', 'failed', 'cancelled')),
        attempts INT NOT NULL DEFAULT 0,
        last_attempt_run UNIQUEIDENTIFIER,
        completed_payload NVARCHAR(MAX),
        cancelled_at DATETIMEOFFSET,
        idempotency_key NVARCHAR(450),
        
        PRIMARY KEY CLUSTERED (queue_name, task_id)
    );

    -- Enforce uniqueness of idempotency keys per queue
    CREATE UNIQUE NONCLUSTERED INDEX UQ_ssf_tasks_idempotency 
        ON ssf.tasks(queue_name, idempotency_key) WHERE idempotency_key IS NOT NULL;
END
GO

IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'runs' AND schema_id = SCHEMA_ID('ssf'))
BEGIN
    CREATE TABLE ssf.runs (
        queue_name NVARCHAR(57) NOT NULL,
        run_id UNIQUEIDENTIFIER NOT NULL,
        task_id UNIQUEIDENTIFIER NOT NULL,
        attempt INT NOT NULL,
        state NVARCHAR(50) NOT NULL CHECK (state IN ('pending', 'running', 'sleeping', 'completed', 'failed', 'cancelled')),
        claimed_by NVARCHAR(MAX),
        claim_expires_at DATETIMEOFFSET,
        available_at DATETIMEOFFSET NOT NULL,
        wake_event NVARCHAR(MAX),
        event_payload NVARCHAR(MAX),
        started_at DATETIMEOFFSET,
        completed_at DATETIMEOFFSET,
        failed_at DATETIMEOFFSET,
        result NVARCHAR(MAX),
        failure_reason NVARCHAR(MAX) CHECK (ISJSON(failure_reason) = 1),
        created_at DATETIMEOFFSET NOT NULL DEFAULT ssf.current_time_fn(),
        
        PRIMARY KEY CLUSTERED (queue_name, run_id)
    );
        
    CREATE NONCLUSTERED INDEX IX_ssf_runs_sai ON ssf.runs (queue_name, state, available_at);
    CREATE NONCLUSTERED INDEX IX_ssf_runs_cei ON ssf.runs (queue_name, claim_expires_at) WHERE state = 'running' AND claim_expires_at IS NOT NULL;
END
GO

IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'checkpoints' AND schema_id = SCHEMA_ID('ssf'))
BEGIN
    CREATE TABLE ssf.checkpoints (
        queue_name NVARCHAR(57) NOT NULL,
        task_id UNIQUEIDENTIFIER NOT NULL,
        checkpoint_name NVARCHAR(450) NOT NULL,
        state NVARCHAR(MAX),
        status NVARCHAR(50) NOT NULL DEFAULT 'committed',
        owner_run_id UNIQUEIDENTIFIER,
        updated_at DATETIMEOFFSET NOT NULL DEFAULT ssf.current_time_fn(),
        
        PRIMARY KEY CLUSTERED (queue_name, task_id, checkpoint_name)
    );
END
GO

IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'events' AND schema_id = SCHEMA_ID('ssf'))
BEGIN
    CREATE TABLE ssf.events (
        queue_name NVARCHAR(57) NOT NULL,
        event_name NVARCHAR(450) NOT NULL,
        payload NVARCHAR(MAX),
        emitted_at DATETIMEOFFSET NOT NULL DEFAULT ssf.current_time_fn(),
        
        PRIMARY KEY CLUSTERED (queue_name, event_name)
    );
END
GO

IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'waits' AND schema_id = SCHEMA_ID('ssf'))
BEGIN
    CREATE TABLE ssf.waits (
        queue_name NVARCHAR(57) NOT NULL,
        task_id UNIQUEIDENTIFIER NOT NULL,
        run_id UNIQUEIDENTIFIER NOT NULL,
        step_name NVARCHAR(450) NOT NULL,
        event_name NVARCHAR(450) NOT NULL,
        timeout_at DATETIMEOFFSET,
        created_at DATETIMEOFFSET NOT NULL DEFAULT ssf.current_time_fn(),
        
        PRIMARY KEY CLUSTERED (queue_name, run_id, step_name)
    );
END
GO

-- ==========================================
-- STORED PROCEDURES
-- ==========================================

SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
GO

CREATE OR ALTER PROCEDURE ssf.create_queue
    @p_queue_name NVARCHAR(57),
    @p_storage_mode NVARCHAR(50) = 'unpartitioned'
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @v_existing_mode NVARCHAR(50);

    IF ssf.validate_queue_name(@p_queue_name) IS NULL
        THROW 50001, 'Invalid queue name.', 1;

    IF @p_storage_mode NOT IN ('unpartitioned', 'partitioned')
        THROW 50002, 'Unsupported queue storage mode.', 1;

    BEGIN TRY
        INSERT INTO ssf.queues (queue_name, storage_mode)
        VALUES (@p_queue_name, @p_storage_mode);
    END TRY
    BEGIN CATCH
        -- ON CONFLICT DO NOTHING equivalent
        IF ERROR_NUMBER() NOT IN (2601, 2627) 
            THROW;
    END CATCH

    SELECT @v_existing_mode = storage_mode
    FROM ssf.queues
    WHERE queue_name = @p_queue_name;

    IF @v_existing_mode <> @p_storage_mode
        THROW 50003, 'Queue already exists with different storage mode.', 1;
END;
GO

SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
GO

CREATE OR ALTER PROCEDURE ssf.drop_queue
    @p_queue_name NVARCHAR(57)
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @v_existing_queue NVARCHAR(57);

    SELECT @v_existing_queue = queue_name
    FROM ssf.queues
    WHERE queue_name = @p_queue_name;

    IF @v_existing_queue IS NULL
        RETURN;

    DELETE FROM ssf.waits WHERE queue_name = @p_queue_name;
    DELETE FROM ssf.events WHERE queue_name = @p_queue_name;
    DELETE FROM ssf.checkpoints WHERE queue_name = @p_queue_name;
    DELETE FROM ssf.runs WHERE queue_name = @p_queue_name;
    DELETE FROM ssf.tasks WHERE queue_name = @p_queue_name;
    DELETE FROM ssf.queues WHERE queue_name = @p_queue_name;
END;
GO

SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
GO

CREATE OR ALTER PROCEDURE ssf.spawn_task
    @p_queue_name NVARCHAR(57),
    @p_task_name NVARCHAR(MAX),
    @p_params NVARCHAR(MAX),
    @p_options NVARCHAR(MAX) = '{}'
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @v_task_id UNIQUEIDENTIFIER = NEWID(); 
    DECLARE @v_run_id UNIQUEIDENTIFIER = NEWID();
    DECLARE @v_attempt INT = 1;
    DECLARE @v_now DATETIMEOFFSET = ssf.current_time_fn();
    
    DECLARE @v_headers NVARCHAR(MAX) = JSON_QUERY(@p_options, '$.headers');
    DECLARE @v_retry_strategy NVARCHAR(MAX) = JSON_QUERY(@p_options, '$.retry_strategy');
    DECLARE @v_max_attempts INT = CAST(JSON_VALUE(@p_options, '$.max_attempts') AS INT);
    DECLARE @v_cancellation NVARCHAR(MAX) = JSON_QUERY(@p_options, '$.cancellation');
    DECLARE @v_idempotency_key NVARCHAR(450) = JSON_VALUE(@p_options, '$.idempotency_key');
    
    DECLARE @v_existing_task_id UNIQUEIDENTIFIER;
    DECLARE @v_existing_run_id UNIQUEIDENTIFIER;
    DECLARE @v_existing_attempt INT;

    IF @p_task_name IS NULL OR LEN(LTRIM(RTRIM(@p_task_name))) = 0
        THROW 50004, 'task_name must be provided', 1;

    IF @v_idempotency_key IS NOT NULL
    BEGIN
        BEGIN TRY
            INSERT INTO ssf.tasks (queue_name, task_id, task_name, params, headers, retry_strategy, max_attempts, cancellation, enqueue_at, state, attempts, last_attempt_run, idempotency_key)
            VALUES (@p_queue_name, @v_task_id, @p_task_name, @p_params, @v_headers, @v_retry_strategy, @v_max_attempts, @v_cancellation, @v_now, 'pending', @v_attempt, @v_run_id, @v_idempotency_key);
        END TRY
        BEGIN CATCH
            IF ERROR_NUMBER() IN (2601, 2627) 
            BEGIN
                SELECT @v_existing_task_id = task_id, @v_existing_run_id = last_attempt_run, @v_existing_attempt = attempts
                FROM ssf.tasks
                WHERE queue_name = @p_queue_name AND idempotency_key = @v_idempotency_key;
            END
            ELSE THROW;
        END CATCH
            
        IF @v_existing_task_id IS NOT NULL
        BEGIN
            SELECT @v_existing_task_id AS task_id, @v_existing_run_id AS run_id, @v_existing_attempt AS attempt, CAST(0 AS BIT) AS created;
            RETURN;
        END
    END
    ELSE
    BEGIN
        INSERT INTO ssf.tasks (queue_name, task_id, task_name, params, headers, retry_strategy, max_attempts, cancellation, enqueue_at, state, attempts, last_attempt_run)
        VALUES (@p_queue_name, @v_task_id, @p_task_name, @p_params, @v_headers, @v_retry_strategy, @v_max_attempts, @v_cancellation, @v_now, 'pending', @v_attempt, @v_run_id);
    END

    INSERT INTO ssf.runs (queue_name, run_id, task_id, attempt, state, available_at)
    VALUES (@p_queue_name, @v_run_id, @v_task_id, @v_attempt, 'pending', @v_now);
        
    SELECT @v_task_id AS task_id, @v_run_id AS run_id, @v_attempt AS attempt, CAST(1 AS BIT) AS created;
END;
GO

SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
GO

CREATE OR ALTER PROCEDURE ssf.claim_task
    @p_queue_name NVARCHAR(57),
    @p_worker_id NVARCHAR(MAX) = 'worker',
    @p_claim_timeout INT = 30,
    @p_qty INT = 1
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @v_now DATETIMEOFFSET = ssf.current_time_fn();
    DECLARE @v_claim_until DATETIMEOFFSET = DATEADD(SECOND, @p_claim_timeout, @v_now);
    
    DECLARE @Claimed TABLE (run_id UNIQUEIDENTIFIER, task_id UNIQUEIDENTIFIER, attempt INT);

    WITH candidate AS (
        SELECT TOP (@p_qty) r.run_id, r.task_id, r.attempt, r.state, r.claimed_by, r.claim_expires_at, r.started_at, r.available_at
        FROM ssf.runs r WITH (UPDLOCK, ROWLOCK, READPAST)
        JOIN ssf.tasks t WITH (UPDLOCK, ROWLOCK, READPAST) ON t.queue_name = r.queue_name AND t.task_id = r.task_id
        WHERE r.queue_name = @p_queue_name
          AND r.state IN ('pending', 'sleeping')
          AND t.state IN ('pending', 'sleeping', 'running')
          AND r.available_at <= @v_now
        ORDER BY r.available_at, r.run_id
    )
    UPDATE candidate
    SET state = 'running',
        claimed_by = @p_worker_id,
        claim_expires_at = @v_claim_until,
        started_at = @v_now,
        available_at = @v_now
    OUTPUT inserted.run_id, inserted.task_id, inserted.attempt INTO @Claimed;

    UPDATE t
    SET state = 'running',
        attempts = CASE WHEN t.attempts > c.attempt THEN t.attempts ELSE c.attempt END,
        first_started_at = ISNULL(t.first_started_at, @v_now),
        last_attempt_run = c.run_id
    FROM ssf.tasks t
    JOIN @Claimed c ON c.task_id = t.task_id
    WHERE t.queue_name = @p_queue_name;

    SELECT c.run_id, c.task_id, c.attempt, t.task_name, t.params, t.retry_strategy, 
           t.max_attempts, t.headers, r.wake_event, r.event_payload
    FROM @Claimed c
    JOIN ssf.runs r ON r.queue_name = @p_queue_name AND r.run_id = c.run_id
    JOIN ssf.tasks t ON t.queue_name = @p_queue_name AND t.task_id = c.task_id
    ORDER BY r.available_at, c.run_id;
END;
GO

SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
GO

CREATE OR ALTER PROCEDURE ssf.complete_run
    @p_queue_name NVARCHAR(57),
    @p_run_id UNIQUEIDENTIFIER,
    @p_state NVARCHAR(MAX) = NULL 
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @v_now DATETIMEOFFSET = ssf.current_time_fn();
    DECLARE @v_task_id UNIQUEIDENTIFIER, @v_state NVARCHAR(50);
        
    SELECT @v_task_id = task_id, @v_state = state 
    FROM ssf.runs WITH (UPDLOCK, ROWLOCK) 
    WHERE queue_name = @p_queue_name AND run_id = @p_run_id;

    IF @v_task_id IS NULL 
        THROW 50005, 'Run not found.', 1;
            
    IF @v_state <> 'running' 
        THROW 50006, 'Run is not currently running.', 1;

    UPDATE ssf.runs 
    SET state = 'completed', completed_at = @v_now, result = @p_state 
    WHERE queue_name = @p_queue_name AND run_id = @p_run_id;

    UPDATE ssf.tasks 
    SET state = 'completed', completed_payload = @p_state, last_attempt_run = @p_run_id 
    WHERE queue_name = @p_queue_name AND task_id = @v_task_id;

    DELETE FROM ssf.waits 
    WHERE queue_name = @p_queue_name AND run_id = @p_run_id;
END;
GO

SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
GO

CREATE OR ALTER PROCEDURE ssf.schedule_run
    @p_queue_name NVARCHAR(57),
    @p_run_id UNIQUEIDENTIFIER,
    @p_wake_at DATETIMEOFFSET
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @v_task_id UNIQUEIDENTIFIER;
        
    SELECT @v_task_id = task_id 
    FROM ssf.runs WITH (UPDLOCK, ROWLOCK) 
    WHERE queue_name = @p_queue_name AND run_id = @p_run_id AND state = 'running';

    IF @v_task_id IS NULL 
        THROW 50007, 'Run is not currently running or does not exist.', 1;

    UPDATE ssf.runs 
    SET state = 'sleeping', claimed_by = NULL, claim_expires_at = NULL, available_at = @p_wake_at, wake_event = NULL 
    WHERE queue_name = @p_queue_name AND run_id = @p_run_id;

    UPDATE ssf.tasks 
    SET state = 'sleeping' 
    WHERE queue_name = @p_queue_name AND task_id = @v_task_id;
END;
GO

SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
GO

CREATE OR ALTER PROCEDURE ssf.fail_run
    @p_queue_name NVARCHAR(57),
    @p_run_id UNIQUEIDENTIFIER,
    @p_reason NVARCHAR(MAX), 
    @p_retry_at DATETIMEOFFSET = NULL
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @v_now DATETIMEOFFSET = ssf.current_time_fn();
    
    DECLARE @v_task_id UNIQUEIDENTIFIER;
    DECLARE @v_attempt INT;
    DECLARE @v_retry_strategy NVARCHAR(MAX);
    DECLARE @v_max_attempts INT;
    DECLARE @v_first_started DATETIMEOFFSET;
    DECLARE @v_cancellation NVARCHAR(MAX);
    
    DECLARE @v_next_attempt INT;
    DECLARE @v_delay_seconds FLOAT = 0;
    DECLARE @v_next_available DATETIMEOFFSET;
    DECLARE @v_retry_kind NVARCHAR(50);
    DECLARE @v_base FLOAT;
    DECLARE @v_factor FLOAT;
    DECLARE @v_max_seconds FLOAT;
    DECLARE @v_max_duration BIGINT;
    DECLARE @v_task_cancel BIT = 0;
    
    DECLARE @v_new_run_id UNIQUEIDENTIFIER;
    DECLARE @v_task_state_after NVARCHAR(50);
    DECLARE @v_recorded_attempt INT;
    DECLARE @v_last_attempt_run UNIQUEIDENTIFIER = @p_run_id;
    DECLARE @v_cancelled_at DATETIMEOFFSET = NULL;

    SELECT @v_task_id = task_id, @v_attempt = attempt 
    FROM ssf.runs WITH (UPDLOCK, ROWLOCK) 
    WHERE queue_name = @p_queue_name AND run_id = @p_run_id AND state IN ('running', 'sleeping');

    IF @v_task_id IS NULL
        THROW 50008, 'Run cannot be failed.', 1;

    SELECT @v_retry_strategy = retry_strategy, @v_max_attempts = max_attempts, @v_first_started = first_started_at, @v_cancellation = cancellation 
    FROM ssf.tasks WITH (UPDLOCK, ROWLOCK) 
    WHERE queue_name = @p_queue_name AND task_id = @v_task_id;

    SET @v_next_attempt = @v_attempt + 1;
    SET @v_task_state_after = 'failed';
    SET @v_recorded_attempt = @v_attempt;

    IF @v_max_attempts IS NULL OR @v_next_attempt <= @v_max_attempts
    BEGIN
        IF @p_retry_at IS NOT NULL
        BEGIN
            SET @v_next_available = @p_retry_at;
        END
        ELSE
        BEGIN
            SET @v_retry_kind = ISNULL(JSON_VALUE(@v_retry_strategy, '$.kind'), 'none');
            IF @v_retry_kind = 'fixed'
            BEGIN
                SET @v_base = ISNULL(CAST(JSON_VALUE(@v_retry_strategy, '$.base_seconds') AS FLOAT), 60.0);
                SET @v_delay_seconds = @v_base;
            END
            ELSE IF @v_retry_kind = 'exponential'
            BEGIN
                SET @v_base = ISNULL(CAST(JSON_VALUE(@v_retry_strategy, '$.base_seconds') AS FLOAT), 30.0);
                SET @v_factor = ISNULL(CAST(JSON_VALUE(@v_retry_strategy, '$.factor') AS FLOAT), 2.0);
                SET @v_delay_seconds = @v_base * POWER(@v_factor, CASE WHEN @v_attempt - 1 > 0 THEN @v_attempt - 1 ELSE 0 END);
                
                SET @v_max_seconds = CAST(JSON_VALUE(@v_retry_strategy, '$.max_seconds') AS FLOAT);
                IF @v_max_seconds IS NOT NULL AND @v_delay_seconds > @v_max_seconds
                    SET @v_delay_seconds = @v_max_seconds;
            END
            ELSE
            BEGIN
                SET @v_delay_seconds = 0;
            END
            
            SET @v_next_available = DATEADD(SECOND, CAST(@v_delay_seconds AS INT), @v_now);
        END

        IF @v_next_available < @v_now
            SET @v_next_available = @v_now;

        IF @v_cancellation IS NOT NULL
        BEGIN
            SET @v_max_duration = CAST(JSON_VALUE(@v_cancellation, '$.max_duration') AS BIGINT);
            IF @v_max_duration IS NOT NULL AND @v_first_started IS NOT NULL
            BEGIN
                IF DATEDIFF_BIG(SECOND, @v_first_started, @v_next_available) >= @v_max_duration
                    SET @v_task_cancel = 1;
            END
        END

        IF @v_task_cancel = 0
        BEGIN
            SET @v_task_state_after = CASE WHEN @v_next_available > @v_now THEN 'sleeping' ELSE 'pending' END;
            SET @v_new_run_id = NEWID();
            SET @v_recorded_attempt = @v_next_attempt;
            SET @v_last_attempt_run = @v_new_run_id;
        END
    END

    IF @v_task_cancel = 1
    BEGIN
        SET @v_task_state_after = 'cancelled';
        SET @v_cancelled_at = @v_now;
        SET @v_recorded_attempt = CASE WHEN @v_recorded_attempt > @v_attempt THEN @v_recorded_attempt ELSE @v_attempt END;
        SET @v_last_attempt_run = @p_run_id;
    END

    UPDATE ssf.runs 
    SET state = 'failed', wake_event = NULL, failed_at = @v_now, failure_reason = @p_reason 
    WHERE queue_name = @p_queue_name AND run_id = @p_run_id;

    IF @v_new_run_id IS NOT NULL
    BEGIN
        INSERT INTO ssf.runs (queue_name, run_id, task_id, attempt, state, available_at)
        VALUES (@p_queue_name, @v_new_run_id, @v_task_id, @v_next_attempt, @v_task_state_after, @v_next_available);
    END

    UPDATE ssf.tasks 
    SET state = @v_task_state_after, 
        attempts = CASE WHEN attempts > @v_recorded_attempt THEN attempts ELSE @v_recorded_attempt END, 
        last_attempt_run = @v_last_attempt_run, 
        cancelled_at = ISNULL(cancelled_at, @v_cancelled_at) 
    WHERE queue_name = @p_queue_name AND task_id = @v_task_id;

    DELETE FROM ssf.waits WHERE queue_name = @p_queue_name AND run_id = @p_run_id;
END;
GO

SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
GO

CREATE OR ALTER PROCEDURE ssf.set_task_checkpoint_state
    @p_queue_name NVARCHAR(57),
    @p_task_id UNIQUEIDENTIFIER,
    @p_step_name NVARCHAR(MAX),
    @p_state NVARCHAR(MAX),
    @p_owner_run UNIQUEIDENTIFIER,
    @p_extend_claim_by INT = NULL
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @v_now DATETIMEOFFSET = ssf.current_time_fn();

    IF @p_step_name IS NULL OR LEN(LTRIM(RTRIM(@p_step_name))) = 0
        THROW 50009, 'step_name must be provided', 1;

    DECLARE @v_new_attempt INT, @v_run_state NVARCHAR(50), @v_task_state NVARCHAR(50);
        
    SELECT @v_new_attempt = r.attempt, @v_run_state = r.state, @v_task_state = t.state
    FROM ssf.runs r WITH (UPDLOCK, ROWLOCK)
    JOIN ssf.tasks t WITH (UPDLOCK, ROWLOCK) ON t.queue_name = r.queue_name AND t.task_id = r.task_id
    WHERE r.queue_name = @p_queue_name AND r.run_id = @p_owner_run;

    IF @v_new_attempt IS NULL THROW 50010, 'Run not found for checkpoint', 1;
    IF @v_task_state = 'cancelled' THROW 50011, 'Task has been cancelled', 1;
    IF @v_run_state = 'failed' THROW 50012, 'Run has already failed', 1;

    IF @p_extend_claim_by IS NOT NULL AND @p_extend_claim_by > 0
    BEGIN
        UPDATE ssf.runs
        SET claim_expires_at = DATEADD(SECOND, @p_extend_claim_by, @v_now)
        WHERE queue_name = @p_queue_name AND run_id = @p_owner_run AND state = 'running' AND claim_expires_at IS NOT NULL;
    END

    DECLARE @v_existing_owner UNIQUEIDENTIFIER, @v_existing_attempt INT;
    SELECT @v_existing_owner = c.owner_run_id, @v_existing_attempt = r.attempt
    FROM ssf.checkpoints c WITH (UPDLOCK, HOLDLOCK)
    LEFT JOIN ssf.runs r ON r.queue_name = c.queue_name AND r.run_id = c.owner_run_id
    WHERE c.queue_name = @p_queue_name AND c.task_id = @p_task_id AND c.checkpoint_name = @p_step_name;

    IF @v_existing_owner IS NULL OR @v_existing_attempt IS NULL OR @v_new_attempt >= @v_existing_attempt
    BEGIN
        MERGE ssf.checkpoints WITH (UPDLOCK, HOLDLOCK) AS t
        USING (SELECT @p_queue_name AS queue_name, @p_task_id AS task_id, @p_step_name AS checkpoint_name) AS s
        ON t.queue_name = s.queue_name AND t.task_id = s.task_id AND t.checkpoint_name = s.checkpoint_name
        WHEN MATCHED THEN
            UPDATE SET state = @p_state, status = 'committed', owner_run_id = @p_owner_run, updated_at = @v_now
        WHEN NOT MATCHED THEN
            INSERT (queue_name, task_id, checkpoint_name, state, status, owner_run_id, updated_at)
            VALUES (s.queue_name, s.task_id, s.checkpoint_name, @p_state, 'committed', @p_owner_run, @v_now);
    END
END;
GO

SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
GO

CREATE OR ALTER PROCEDURE ssf.extend_claim
    @p_queue_name NVARCHAR(57),
    @p_run_id UNIQUEIDENTIFIER,
    @p_extend_by INT
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @v_now DATETIMEOFFSET = ssf.current_time_fn();

    IF @p_extend_by IS NULL OR @p_extend_by <= 0
        THROW 50013, 'extend_by must be > 0', 1;

    DECLARE @v_run_state NVARCHAR(50), @v_claim_expires_at DATETIMEOFFSET, @v_task_state NVARCHAR(50);
        
    SELECT @v_run_state = r.state, @v_claim_expires_at = r.claim_expires_at, @v_task_state = t.state
    FROM ssf.runs r WITH (UPDLOCK, ROWLOCK)
    JOIN ssf.tasks t WITH (UPDLOCK, ROWLOCK) ON t.queue_name = r.queue_name AND t.task_id = r.task_id
    WHERE r.queue_name = @p_queue_name AND r.run_id = @p_run_id;

    IF @v_run_state IS NULL THROW 50014, 'Run not found', 1;
    IF @v_task_state = 'cancelled' THROW 50011, 'Task cancelled', 1;
    IF @v_run_state <> 'running' THROW 50015, 'Run not running', 1;
    IF @v_claim_expires_at IS NULL THROW 50016, 'No active claim', 1;

    UPDATE ssf.runs
    SET claim_expires_at = DATEADD(SECOND, @p_extend_by, @v_now)
    WHERE queue_name = @p_queue_name AND run_id = @p_run_id;
END;
GO

SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
GO

CREATE OR ALTER PROCEDURE ssf.get_task_checkpoint_state
    @p_queue_name NVARCHAR(57),
    @p_task_id UNIQUEIDENTIFIER,
    @p_step_name NVARCHAR(MAX),
    @p_include_pending BIT = 0
AS
BEGIN
    SET NOCOUNT ON;

    SELECT checkpoint_name, state, status, owner_run_id, updated_at
    FROM ssf.checkpoints
    WHERE queue_name = @p_queue_name 
      AND task_id = @p_task_id 
      AND checkpoint_name = @p_step_name
      AND (@p_include_pending = 1 OR status = 'committed');
END;
GO

SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
GO

CREATE OR ALTER PROCEDURE ssf.get_task_checkpoint_states
    @p_queue_name NVARCHAR(57),
    @p_task_id UNIQUEIDENTIFIER,
    @p_run_id UNIQUEIDENTIFIER
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @v_run_task_id UNIQUEIDENTIFIER, @v_run_attempt INT;
    SELECT @v_run_task_id = task_id, @v_run_attempt = attempt 
    FROM ssf.runs 
    WHERE queue_name = @p_queue_name AND run_id = @p_run_id;

    IF @v_run_task_id IS NULL THROW 50014, 'Run not found', 1;
    IF @v_run_task_id <> @p_task_id THROW 50017, 'Run does not belong to task mismatch', 1;

    SELECT c.checkpoint_name, c.state, c.status, c.owner_run_id, c.updated_at
    FROM ssf.checkpoints c
    LEFT JOIN ssf.runs r ON r.queue_name = c.queue_name AND r.run_id = c.owner_run_id
    WHERE c.queue_name = @p_queue_name 
      AND c.task_id = @p_task_id 
      AND c.status = 'committed'
      AND (r.attempt IS NULL OR r.attempt <= @v_run_attempt)
    ORDER BY c.updated_at ASC;
END;
GO

SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
GO

CREATE OR ALTER PROCEDURE ssf.await_event
    @p_queue_name NVARCHAR(57),
    @p_task_id UNIQUEIDENTIFIER,
    @p_run_id UNIQUEIDENTIFIER,
    @p_step_name NVARCHAR(MAX),
    @p_event_name NVARCHAR(MAX),
    @p_timeout INT = NULL
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @v_now DATETIMEOFFSET = ssf.current_time_fn();
    DECLARE @v_timeout_at DATETIMEOFFSET = NULL;

    IF @p_event_name IS NULL OR LEN(LTRIM(RTRIM(@p_event_name))) = 0
        THROW 50018, 'event_name must be provided', 1;

    IF @p_timeout IS NOT NULL
    BEGIN
        IF @p_timeout < 0 THROW 50019, 'timeout must be non-negative', 1;
        SET @v_timeout_at = DATEADD(SECOND, @p_timeout, @v_now);
    END

    DECLARE @v_run_state NVARCHAR(50), @v_existing_payload NVARCHAR(MAX), @v_wake_event NVARCHAR(MAX), @v_task_state NVARCHAR(50);
    DECLARE @v_checkpoint_payload NVARCHAR(MAX), @v_event_payload NVARCHAR(MAX), @v_resolved_payload NVARCHAR(MAX);

    SELECT @v_checkpoint_payload = state
    FROM ssf.checkpoints
    WHERE queue_name = @p_queue_name AND task_id = @p_task_id AND checkpoint_name = @p_step_name;

    IF @v_checkpoint_payload IS NOT NULL
    BEGIN
        SELECT CAST(0 AS BIT) AS should_suspend, @v_checkpoint_payload AS payload;
        RETURN;
    END

    BEGIN TRY
        INSERT INTO ssf.events (queue_name, event_name, payload, emitted_at)
        VALUES (@p_queue_name, @p_event_name, NULL, '1970-01-01');
    END TRY
    BEGIN CATCH
        IF ERROR_NUMBER() NOT IN (2601, 2627) THROW;
    END CATCH

    DECLARE @dummy INT;
    SELECT @dummy = 1 FROM ssf.events WITH (UPDLOCK, ROWLOCK) WHERE queue_name = @p_queue_name AND event_name = @p_event_name;

    SELECT @v_run_state = r.state, @v_existing_payload = r.event_payload, @v_wake_event = r.wake_event, @v_task_state = t.state
    FROM ssf.runs r WITH (UPDLOCK, ROWLOCK)
    JOIN ssf.tasks t WITH (UPDLOCK, ROWLOCK) ON t.queue_name = r.queue_name AND t.task_id = r.task_id
    WHERE r.queue_name = @p_queue_name AND r.run_id = @p_run_id;

    IF @v_run_state IS NULL THROW 50014, 'Run not found', 1;
    IF @v_task_state = 'cancelled' THROW 50011, 'Task cancelled', 1;

    SELECT @v_event_payload = payload FROM ssf.events WHERE queue_name = @p_queue_name AND event_name = @p_event_name;

    IF @v_existing_payload IS NOT NULL
    BEGIN
        UPDATE ssf.runs SET event_payload = NULL WHERE queue_name = @p_queue_name AND run_id = @p_run_id;
            
        IF @v_event_payload IS NOT NULL AND (CAST(@v_event_payload AS NVARCHAR(MAX)) = CAST(@v_existing_payload AS NVARCHAR(MAX)))
            SET @v_resolved_payload = @v_existing_payload;
    END

    IF @v_run_state <> 'running' THROW 50020, 'Run must be running to await events', 1;

    IF @v_resolved_payload IS NULL AND @v_event_payload IS NOT NULL
        SET @v_resolved_payload = @v_event_payload;

    IF @v_resolved_payload IS NOT NULL
    BEGIN
        MERGE ssf.checkpoints WITH (UPDLOCK, HOLDLOCK) AS c
        USING (SELECT @p_queue_name AS queue_name, @p_task_id AS task_id, @p_step_name AS checkpoint_name) AS s
        ON c.queue_name = s.queue_name AND c.task_id = s.task_id AND c.checkpoint_name = s.checkpoint_name
        WHEN MATCHED THEN UPDATE SET state = @v_resolved_payload, status = 'committed', owner_run_id = @p_run_id, updated_at = @v_now
        WHEN NOT MATCHED THEN INSERT (queue_name, task_id, checkpoint_name, state, status, owner_run_id, updated_at)
        VALUES (s.queue_name, s.task_id, s.checkpoint_name, @v_resolved_payload, 'committed', @p_run_id, @v_now);

        SELECT CAST(0 AS BIT) AS should_suspend, @v_resolved_payload AS payload;
        RETURN;
    END

    IF @v_resolved_payload IS NULL AND @v_wake_event = @p_event_name AND @v_existing_payload IS NULL
    BEGIN
        UPDATE ssf.runs SET wake_event = NULL WHERE queue_name = @p_queue_name AND run_id = @p_run_id;
        SELECT CAST(0 AS BIT) AS should_suspend, NULL AS payload;
        RETURN;
    END

    MERGE ssf.waits WITH (UPDLOCK, HOLDLOCK) AS w
    USING (SELECT @p_queue_name AS queue_name, @p_run_id AS run_id, @p_step_name AS step_name) AS s
    ON w.queue_name = s.queue_name AND w.run_id = s.run_id AND w.step_name = s.step_name
    WHEN MATCHED THEN UPDATE SET event_name = @p_event_name, timeout_at = @v_timeout_at, created_at = @v_now
    WHEN NOT MATCHED THEN INSERT (queue_name, task_id, run_id, step_name, event_name, timeout_at, created_at)
    VALUES (s.queue_name, @p_task_id, s.run_id, s.step_name, @p_event_name, @v_timeout_at, @v_now);

    UPDATE ssf.runs
    SET state = 'sleeping', claimed_by = NULL, claim_expires_at = NULL, available_at = ISNULL(@v_timeout_at, '9999-12-31 23:59:59.9999999 +00:00'), wake_event = @p_event_name, event_payload = NULL
    WHERE queue_name = @p_queue_name AND run_id = @p_run_id;

    UPDATE ssf.tasks SET state = 'sleeping' WHERE queue_name = @p_queue_name AND task_id = @p_task_id;

    SELECT CAST(1 AS BIT) AS should_suspend, NULL AS payload;
END;
GO

SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
GO

CREATE OR ALTER PROCEDURE ssf.emit_event
    @p_queue_name NVARCHAR(57),
    @p_event_name NVARCHAR(MAX),
    @p_payload NVARCHAR(MAX) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @v_now DATETIMEOFFSET = ssf.current_time_fn();
    DECLARE @v_payload NVARCHAR(MAX) = ISNULL(@p_payload, 'null');

    IF @p_event_name IS NULL OR LEN(LTRIM(RTRIM(@p_event_name))) = 0
        THROW 50021, 'event_name must be provided', 1;

    DECLARE @v_emit_applied INT;

    UPDATE ssf.events
    SET payload = @v_payload, emitted_at = @v_now
    WHERE queue_name = @p_queue_name AND event_name = @p_event_name AND payload IS NULL;

    SET @v_emit_applied = @@ROWCOUNT;

    IF @v_emit_applied = 0
    BEGIN
        BEGIN TRY
            INSERT INTO ssf.events (queue_name, event_name, payload, emitted_at)
            VALUES (@p_queue_name, @p_event_name, @v_payload, @v_now);
            SET @v_emit_applied = 1;
        END TRY
        BEGIN CATCH
            IF ERROR_NUMBER() IN (2601, 2627) SET @v_emit_applied = 0; 
            ELSE THROW;
        END CATCH
    END

    IF @v_emit_applied = 0 RETURN;

    DECLARE @AffectedRuns TABLE (run_id UNIQUEIDENTIFIER, task_id UNIQUEIDENTIFIER, step_name NVARCHAR(MAX));

    DELETE FROM ssf.waits
    WHERE queue_name = @p_queue_name AND event_name = @p_event_name AND timeout_at IS NOT NULL AND timeout_at <= @v_now;

    INSERT INTO @AffectedRuns (run_id, task_id, step_name)
    SELECT run_id, task_id, step_name FROM ssf.waits
    WHERE queue_name = @p_queue_name AND event_name = @p_event_name AND (timeout_at IS NULL OR timeout_at > @v_now);

    DECLARE @UpdatedRuns TABLE (run_id UNIQUEIDENTIFIER, task_id UNIQUEIDENTIFIER);

    UPDATE r
    SET state = 'pending', available_at = @v_now, wake_event = NULL, event_payload = @v_payload, claimed_by = NULL, claim_expires_at = NULL
    OUTPUT inserted.run_id, inserted.task_id INTO @UpdatedRuns
    FROM ssf.runs r
    JOIN @AffectedRuns a ON r.run_id = a.run_id
    WHERE r.queue_name = @p_queue_name AND r.state = 'sleeping';

    MERGE ssf.checkpoints WITH (UPDLOCK, HOLDLOCK) AS c
    USING (SELECT a.task_id, a.step_name, u.run_id FROM @AffectedRuns a JOIN @UpdatedRuns u ON a.run_id = u.run_id) AS s
    ON c.queue_name = @p_queue_name AND c.task_id = s.task_id AND c.checkpoint_name = s.step_name
    WHEN MATCHED THEN UPDATE SET state = @v_payload, status = 'committed', owner_run_id = s.run_id, updated_at = @v_now
    WHEN NOT MATCHED THEN INSERT (queue_name, task_id, checkpoint_name, state, status, owner_run_id, updated_at)
    VALUES (@p_queue_name, s.task_id, s.step_name, @v_payload, 'committed', s.run_id, @v_now);

    UPDATE t
    SET state = 'pending'
    FROM ssf.tasks t
    JOIN @UpdatedRuns u ON t.task_id = u.task_id
    WHERE t.queue_name = @p_queue_name;

    DELETE w
    FROM ssf.waits w
    JOIN @UpdatedRuns u ON w.run_id = u.run_id
    WHERE w.queue_name = @p_queue_name AND w.event_name = @p_event_name;
END;
GO

SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
GO

CREATE OR ALTER PROCEDURE ssf.cancel_task
    @p_queue_name NVARCHAR(57),
    @p_task_id UNIQUEIDENTIFIER
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @v_now DATETIMEOFFSET = ssf.current_time_fn();
    DECLARE @v_task_state NVARCHAR(50);
        
    DECLARE @dummy UNIQUEIDENTIFIER;
    SELECT TOP 1 @dummy = run_id
    FROM ssf.runs WITH (UPDLOCK, ROWLOCK)
    WHERE queue_name = @p_queue_name AND task_id = @p_task_id AND state NOT IN ('completed', 'failed', 'cancelled')
    ORDER BY run_id;

    SELECT @v_task_state = state
    FROM ssf.tasks WITH (UPDLOCK, ROWLOCK)
    WHERE queue_name = @p_queue_name AND task_id = @p_task_id;

    IF @v_task_state IS NULL
        THROW 50024, 'Task not found in queue', 1;

    IF @v_task_state IN ('completed', 'failed', 'cancelled')
        RETURN;

    UPDATE ssf.tasks
    SET state = 'cancelled', cancelled_at = ISNULL(cancelled_at, @v_now)
    WHERE queue_name = @p_queue_name AND task_id = @p_task_id;

    UPDATE ssf.runs
    SET state = 'cancelled', claimed_by = NULL, claim_expires_at = NULL
    WHERE queue_name = @p_queue_name AND task_id = @p_task_id AND state NOT IN ('completed', 'failed', 'cancelled');

    DELETE FROM ssf.waits
    WHERE queue_name = @p_queue_name AND task_id = @p_task_id;
END;
GO

SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
GO

CREATE OR ALTER PROCEDURE ssf.cleanup_tasks
    @p_queue_name NVARCHAR(57),
    @p_ttl_seconds INT,
    @p_limit INT = 1000
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @v_now DATETIMEOFFSET = ssf.current_time_fn();
    DECLARE @v_cutoff DATETIMEOFFSET;
    DECLARE @v_deleted_count INT = 0;

    IF @p_ttl_seconds IS NULL OR @p_ttl_seconds < 0
        THROW 50022, 'TTL must be a non-negative number of seconds', 1;

    SET @v_cutoff = DATEADD(SECOND, -@p_ttl_seconds, @v_now);

    DECLARE @ToDelete TABLE (task_id UNIQUEIDENTIFIER);

    INSERT INTO @ToDelete (task_id)
    SELECT TOP (@p_limit) t.task_id
    FROM ssf.tasks t
    LEFT JOIN ssf.runs r ON r.queue_name = t.queue_name AND r.run_id = t.last_attempt_run
    WHERE t.queue_name = @p_queue_name
      AND t.state IN ('completed', 'failed', 'cancelled')
      AND (
        (t.state = 'completed' AND r.completed_at < @v_cutoff) OR
        (t.state = 'failed' AND r.failed_at < @v_cutoff) OR
        (t.state = 'cancelled' AND t.cancelled_at < @v_cutoff)
      );

    DELETE w
    FROM ssf.waits w
    JOIN @ToDelete d ON w.task_id = d.task_id
    WHERE w.queue_name = @p_queue_name;

    DELETE c
    FROM ssf.checkpoints c
    JOIN @ToDelete d ON c.task_id = d.task_id
    WHERE c.queue_name = @p_queue_name;

    DELETE r
    FROM ssf.runs r
    JOIN @ToDelete d ON r.task_id = d.task_id
    WHERE r.queue_name = @p_queue_name;

    DELETE t
    FROM ssf.tasks t
    JOIN @ToDelete d ON t.task_id = d.task_id
    WHERE t.queue_name = @p_queue_name;

    SET @v_deleted_count = @@ROWCOUNT;

    SELECT @v_deleted_count AS deleted_tasks;
END;
GO

SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
GO

CREATE OR ALTER PROCEDURE ssf.cleanup_events
    @p_queue_name NVARCHAR(57),
    @p_ttl_seconds INT,
    @p_limit INT = 1000
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @v_now DATETIMEOFFSET = ssf.current_time_fn();
    DECLARE @v_cutoff DATETIMEOFFSET;
    DECLARE @v_deleted_count INT = 0;

    IF @p_ttl_seconds IS NULL OR @p_ttl_seconds < 0
        THROW 50023, 'TTL must be a non-negative number of seconds', 1;

    SET @v_cutoff = DATEADD(SECOND, -@p_ttl_seconds, @v_now);

    DECLARE @ToDelete TABLE (event_name NVARCHAR(450));

    INSERT INTO @ToDelete (event_name)
    SELECT TOP (@p_limit) event_name
    FROM ssf.events
    WHERE queue_name = @p_queue_name AND emitted_at < @v_cutoff
    ORDER BY emitted_at;

    DELETE e
    FROM ssf.events e
    JOIN @ToDelete d ON e.event_name = d.event_name
    WHERE e.queue_name = @p_queue_name;

    SET @v_deleted_count = @@ROWCOUNT;

    SELECT @v_deleted_count AS deleted_events;
END;
GO


SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
GO

CREATE OR ALTER PROCEDURE ssf.release_worker_claims
    @p_queue_name NVARCHAR(57),
    @p_worker_id NVARCHAR(MAX)
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @v_now DATETIMEOFFSET = ssf.current_time_fn();

    DECLARE @UpdatedTasks TABLE (task_id UNIQUEIDENTIFIER);

    UPDATE ssf.runs
    SET state = 'pending', claimed_by = NULL, claim_expires_at = NULL, available_at = @v_now
    OUTPUT inserted.task_id INTO @UpdatedTasks
    WHERE queue_name = @p_queue_name AND claimed_by = @p_worker_id AND state = 'running';

    UPDATE t
    SET state = 'pending'
    FROM ssf.tasks t
    JOIN @UpdatedTasks u ON t.task_id = u.task_id
    WHERE t.queue_name = @p_queue_name;
END;
GO
