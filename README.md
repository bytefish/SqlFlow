# SqlFlow

SqlFlow is a simple durable execution workflow system for PostgreSQL and SQL Server. It handles scheduling, state checkpointing, and retries without needing heavy external workflow engines like Temporal or Cadence.

The SQL Scripts for creating the SqlFlow Database Schema are available here:
* [PostgreSQL: `sql/ssf-postgres.sql`](https://github.com/bytefish/SqlFlow/blob/main/sql/ssf-postgres.sql)
* [SQL Server: `sql/ssf-sqlserver.sql`](https://github.com/bytefish/SqlFlow/blob/main/sql/ssf-sqlserver.sql)

While SqlFlow took a large deal of inspiration from Absurd, it expands significantly on the core concepts to deliver a production-ready, multi-language ecosystem. It features:
* A much simpler database model optimized for high throughput.
* First-class support for both **PostgreSQL** and **SQL Server**.
* An advanced **Signaling Layer** featuring native database events and **NATS JetStream** support, eliminating polling bottlenecks and allowing infinite horizontal scaling in distributed systems.
* Native SDKs for **.NET, Java, Python, and Go**.

SqlFlow also comes with a Management API and a Control Panel to understand your system's health, monitor task processing, debug slow tasks, inspect event blockades, and search for specific executions:

<a href="https://raw.githubusercontent.com/bytefish/SqlFlow/main/docs/control-panel-event-blockades.jpg">
    <img src="https://raw.githubusercontent.com/bytefish/SqlFlow/main/docs/control-panel-event-blockades.jpg" alt="Screenshot of Event Blockades within the SqlFlow System" width="100%" />
</a>

## Core Concepts & Terminology

Before diving into how the system operates, it helps to understand the vocabulary SqlFlow uses to manage durable execution.

* **Workflow (or Job):** The actual code you write. Unlike traditional scripts, a Workflow is written sequentially but designed to be interrupted. It dictates the business logic (e.g., "Charge a credit card, then send an email").
* **Task (or Run):** A specific, durable execution instance of a Workflow. When you tell SqlFlow to run a workflow, a Task record is created in the database.
* **Step:** The fundamental unit of durability. By wrapping fragile operations (like an HTTP call) in a `Step`, SqlFlow saves the result to the database. If the server crashes and the Task restarts, SqlFlow skips executing the Step again and simply loads the saved result from the database.
* **Suspension & Events:** Workflows often need to wait for external input (e.g., a human approving a document). Instead of blocking a server thread in a `while` loop, workflows `AwaitEvent`. This permanently saves the state to the database, unloads the workflow from memory, and frees up the worker thread until the external system fires the expected event.
* **Signaling:** In SqlFlow, a "Signal" is just a lightweight wake-up call (a ping). **It contains no task data.** It simply tells idle background workers: *"Wake up, the database state has changed, go look for work."*

## How SqlFlow Works (The Big Picture)

At its core, SqlFlow solves the problem of long-running, brittle application logic. If a server crashes while waiting for an API call, local memory is lost. SqlFlow fixes this by separating the **Storage** (the source of truth) from the **Signaling** (the control plane).

Here is how the architecture elegantly fits together from start to finish:

### 1. Spawning and Dispatching
When your application triggers a new workflow (spawns a Task), two things happen instantly:
1. **The DB Commit:** The task's parameters and initial state are durably written to a database table. The database is the absolute source of truth.
2. **The Wake-Up Signal:** The publisher immediately fires a fire-and-forget `"ping"` over a signaling channel. 

This guarantees that even if the signal gets lost due to network issues, the actual work is safely stored in the database.

### 2. Zero-Polling via Smart Signaling
Historically, database-backed queues rely on infinite `WHILE` loops (polling) to check for new tasks, which crushes database CPU. SqlFlow avoids this entirely by suspending idle workers in memory until they receive that wake-up signal. 

SqlFlow adapts this signaling layer natively to your chosen infrastructure:
* **PostgreSQL:** Uses the built-in `LISTEN / NOTIFY` mechanism. A single connection per application instance listens for `NOTIFY` events and instantly wakes up the local worker threads.
* **SQL Server:** Uses **Service Broker**. Instead of locking tables to wait for changes, SqlFlow leverages SQL Server's internal message bus to push wake-up notifications to idle workers, taking the load off the standard query engine.
* **NATS JetStream (Optional):** At massive distributed scale, database signaling can become a bottleneck. SqlFlow allows you to swap the Signal Listener to **NATS**. The database remains the source of truth, but the pub/sub signaling happens entirely over a high-speed broker. *(See [High-Performance Signaling with NATS](docs/nats-signaling.md))*

### 3. Concurrency and Rate Limiting
Because workers are woken up by a generic ping, they don't blindly execute everything at once. When a worker thread wakes up, it executes a transaction to claim a batch of tasks from the database. 

This creates a natural, highly efficient rate limit:
* If you configure a worker with `Concurrency = 5`, it will claim a maximum of 5 tasks from the DB.
* If 1,000 tasks are spawned simultaneously, the workers receive the ping, fill their 5 concurrent slots, and begin processing.
* The remaining 995 tasks safely wait in the database. As soon as a worker finishes a task, it automatically pulls the next one.
* **No wasted DB cycles:** If the queue is empty, the worker goes back to sleep. It only queries the database when it knows work is there (via a signal) or when a fallback safety timeout expires.

### 4. Time and Scheduling
SqlFlow handles time natively in the database. If you schedule a task for tomorrow, or if a task fails and enters an exponential backoff retry state, its `next_execution_time` is pushed to the future. 

Workers simply ignore these rows until the time arrives. Because workers utilize a smart-polling fallback, if they are asleep, they will naturally wake up at intervals to check if any scheduled tasks have become due, ensuring reliable execution without constant database hammering.

## What Does It Look Like in Code?

Instead of writing complex state machines or infinite `while` loops, you write standard, sequential code. You simply wrap fragile operations in a `Step` and replace blocking waits with `AwaitEvent`. 

Here is a conceptual example of a durable workflow:

```csharp
public async Task<Result> ExecuteAsync(TaskContext ctx, Order order)
{
    // 1. Durably execute a fragile API call
    var payment = await ctx.Step("charge-card", async () => 
        await _stripe.ChargeAsync(order.Amount));

    if (!payment.Success) return new Result { Failed = true };

    // 2. Safely suspend the workflow to the DB until a human approves.
    // The worker thread is completely freed during this time!
    var approval = await ctx.AwaitEvent<HumanApproval>("manager-approval", "wait-for-human");

    if (approval.Approved) 
    {
        // 3. Resume and execute the final step
        await ctx.Step("ship-product", async () => 
            await _fulfillment.ShipAsync(order.Id));
    }

    return new Result { Success = true };
}
```
**Why this matters:**
* If the server crashes during `_stripe.ChargeAsync()`, the workflow automatically restarts. 
* If it crashes *after*, SqlFlow skips the Stripe call on reboot and loads the payment result directly from the database. 
* When `AwaitEvent` is called, the state is serialized to the database and the application uses zero CPU/RAM until the `"manager-approval"` event is fired.

## Getting Started

Choose your preferred language to see how to install the SDK and build a real-world durable AI Agent that handles long-running LLM calls and asynchronous human approvals:

* [.NET SDK Tutorial](docs/dotnet.md)
* [Java SDK Tutorial](docs/java.md)
* [Python SDK Tutorial](docs/python.md)
* [Go SDK Tutorial](docs/go.md)