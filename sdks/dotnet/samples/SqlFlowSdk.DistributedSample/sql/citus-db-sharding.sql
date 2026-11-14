-- Coordinator Host and Port Configuration using the Aspire configuration
SELECT citus_set_coordinator_host('coordinator', 5432);

-- Register the worker nodes with the coordinator using the resource names of the worker containers
DO $$
DECLARE
    i INT;
    worker_host TEXT;
BEGIN
    FOR i IN 1..10 LOOP
        worker_host := 'worker-' || i;
        IF NOT EXISTS (SELECT 1 FROM pg_dist_node WHERE nodename = worker_host) THEN
            PERFORM citus_add_node(worker_host, 5432);
        END IF;
    END LOOP;
END $$;

-- Distribute the tables based on tenant_id across the shards
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