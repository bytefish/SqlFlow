# High-Performance Signaling with NATS JetStream

SqlFlow natively uses database features like PostgreSQL `LISTEN/NOTIFY` or SQL Server `Service Broker` to wake up idle workers. 

This is fantastic for small to medium workloads as it requires zero external infrastructure. 

However, at a distributed scale (e.g. hundreds of worker pods in Kubernetes processing thousands of jobs per second), database-native signaling introduces architectural limits.

SqlFlow allows you to cleanly swap the signaling layer to **NATS JetStream**, decoupling the storage plane (the database) from the control plane (the wake-up pings).

## 1. The Limits of Database Signaling

While SqlFlow is highly optimized to use only one `LISTEN` connection per application instance, relying on the database for pub/sub routing at scale introduces several bottlenecks:

* **The Thundering Herd & CPU Fan-Out Tax:** When 1 task is spawned, PostgreSQL must evaluate and broadcast the `NOTIFY` payload to every single connected worker instance. If you have 50 worker pods, PostgreSQL burns CPU cycles sending 50 network pings. 50 workers wake up, but 49 of them will query the database only to find the task is already claimed (`SKIP LOCKED`). NATS offloads 100% of this routing compute from the database.
* **Primary-Node Restrictions:** `LISTEN/NOTIFY` events do not replicate to read-replicas and are stripped from the WAL (Write-Ahead Log). If your architecture spans multiple regions, Postgres signals cannot cross that boundary. NATS clusters natively stretch across regions.
* **PgBouncer / Pooler Incompatibility:** Long-lived `LISTEN` connections are notoriously hostile to transaction-mode connection poolers like PgBouncer. They must be pinned to a dedicated session pool, permanently eating up hard connection slots on the database server. NATS frees up those critical database sessions.

## 2. How NATS Solves the "Thundering Herd"

NATS elegantly solves the Thundering Herd problem using a core JetStream feature called **Consumer Groups**. 

Unlike PostgreSQL, which blindly broadcasts to every listener, NATS acts as a smart load balancer. When multiple SqlFlow workers connect to the same NATS queue, NATS guarantees that a single message is delivered to **exactly one** worker in the group.

If you have 50 idle workers and 1 task is spawned:
1. The Publisher fires 1 `"ping"` to NATS.
2. NATS looks at the pool of 50 workers and pushes the ping to **just Worker 1** (round-robin).
3. **Worker 1** wakes up, queries the database, and claims the task.
4. **Workers 2 through 50** never receive a ping, never wake up, and never execute an empty database query.

By switching to NATS at scale, you achieve a **1:1 ratio** between wake-up pings and database queries. 

## 3. Elegantly Avoiding the Outbox Pattern

In a standard distributed system, writing state to a database and publishing a message to a broker simultaneously is a dangerous "dual-write" problem. If the database commits but the broker is down, the message is lost. The industry standard solution is the **Transactional Outbox Pattern** (writing the event to a DB table and using a background relayer to publish it). 

The Outbox pattern adds massive complexity, doubles database I/O, and introduces publishing latency. SqlFlow bypasses it entirely through two design choices:

1. **Signals are Hints, Not State:** The NATS message payload is just a `"ping"`. It does not contain job parameters or execution state. Because the NATS message carries no business value, it does not strictly matter if it gets lost. The absolute source of truth remains durably committed in PostgreSQL.
2. **The Smart-Polling Fallback:** If NATS is completely offline, the database transaction still succeeds, and the API instantly returns a success response. Meanwhile, idle workers are bound by a fallback timeout (e.g. 60 seconds). If a worker receives no NATS pings during that window, it executes a standard database check anyway, naturally discovering and claiming any tasks that were stranded during the network blip.

You achieve the absolute reliability of an Outbox pattern with zero additional database tables, zero background relayers, and zero added latency to your critical path.

## Example: Enabling NATS in .NET

SqlFlow handles the dependency injection gracefully. You configure your database first, and then chain the NATS extension to silently override the signaling interfaces. This automatically disables the background PostgreSQL `LISTEN` loops so you don't leak database connections.

```csharp
var builder = WebApplication.CreateBuilder(args);

builder.Services
    // 1. The absolute source of truth
    .AddSqlFlowPostgres(builder.Configuration.GetConnectionString("Postgres"))
    
    // 2. Overrides the signaling layer and disables Postgres LISTEN
    .AddNatsSignaling(builder.Configuration.GetConnectionString("Nats")) 
    
    // 3. Worker Configuration
    .AddWorker("ai-agent-queue", worker =>
    {
        worker
            .SetConcurrency(5)
            .SetPollInterval(1); // The Smart-Polling Fallback interval
    });
```