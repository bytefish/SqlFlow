DO $$
BEGIN
    -- Distribute 'ssf.tasks'
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'ssf' AND table_name = 'tasks')
       AND NOT EXISTS (SELECT 1 FROM pg_dist_partition WHERE logicalrelid = 'ssf.tasks'::regclass) THEN
        PERFORM create_distributed_table('ssf.tasks', 'tenant_id');
    END IF;

    -- Distribute 'ssf.runs'
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'ssf' AND table_name = 'runs')
       AND NOT EXISTS (SELECT 1 FROM pg_dist_partition WHERE logicalrelid = 'ssf.runs'::regclass) THEN
        PERFORM create_distributed_table('ssf.runs', 'tenant_id');
    END IF;

    -- Distribute 'ssf.checkpoints'
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'ssf' AND table_name = 'checkpoints')
       AND NOT EXISTS (SELECT 1 FROM pg_dist_partition WHERE logicalrelid = 'ssf.checkpoints'::regclass) THEN
        PERFORM create_distributed_table('ssf.checkpoints', 'tenant_id');
    END IF;
END $$;