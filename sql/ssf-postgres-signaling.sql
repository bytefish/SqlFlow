-- ==========================================
-- EXTENSIONS: LISTEN / NOTIFY SIGNALING
-- ==========================================
-- Run this script AFTER ssf-postgres-minimal.sql to enable Postgres LISTEN/NOTIFY.
-- It overrides the empty stub function to provide actual signaling.

CREATE OR REPLACE FUNCTION ssf.notify_queue(
    p_queue_name TEXT
)
RETURNS VOID
LANGUAGE plpgsql
AS $$
BEGIN
    IF p_queue_name IS NOT NULL THEN
        PERFORM pg_notify(
            'ssf_work_available',
            p_queue_name
        );
    END IF;
END;
$$;