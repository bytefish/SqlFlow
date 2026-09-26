# Getting Started with the Python SDK

Install the SqlFlow SDK from PyPI:

```bash
pip install sqlflow-sdk
```

If you want to use PostgreSQL support, install the PostgreSQL extra:

```bash
pip install "sqlflow-sdk[postgres]"
# or "sqlflow-sdk[sqlserver]" for SQL Server
```

## Building a Durable AI Agent

The classic examples for durable execution are usually e-commerce checkouts. But there's a rapidly growing use case developers are dealing with: Autonomous AI Agents. Building AI agents introduces severe state-management challenges:

1. **Slow API Calls:** LLM API calls are inherently slow, prone to timeouts, and expensive. If a server crashes while waiting 30 seconds for an AI generation, standard `async/await` state is lost forever.
2. **Human-in-the-Loop:** You don't want an AI pushing code to production without human review. Agents need to pause execution, ask a human for permission, and resume only when approved (hours or days later).

Traditional approaches require you to build complex state machines, database polling loops, or heavy external infrastructure. With SqlFlow, we can write our agent as a standard Python asynchronous function. The framework will automatically checkpoint the state, sleep without blocking the event loop, and wake up exactly where it left off.

### 1. The Domain Models

We define our data models using Pydantic.

```python
from pydantic import BaseModel
from typing import Optional

class AgentTask(BaseModel):
    issue_id: str

class Issue(BaseModel):
    stack_trace: str

class Solution(BaseModel):
    patched_code: str

class HumanApproval(BaseModel):
    approved: bool
    reason: Optional[str] = None

class AgentResult(BaseModel):
    success: bool
    pull_request_url: Optional[str] = None
```

### 2. The Autonomous Agent Workflow (The Core Concept)

The workflow is just a normal Python method that takes a `TaskContext` and a parameter dictionary. 

The magic is in the **`ctx.step`** method: every time a step completes, its result is automatically checkpointed to the database. If the process crashes, the framework replays the job, skips the already completed steps, and loads their results directly from the database.

Furthermore, we use **`ctx.await_event`** to wait for human interaction. This instructs the engine to safely suspend the workflow state to the database and free up the worker completely until an external system fires the webhook event.

```python
async def autonomous_agent_workflow(ctx: TaskContext, params: dict) -> dict:
    task = AgentTask(**params)

    # 1. Fetch data safely (results are checkpointed)
    async def fetch_issue():
        return await github_service.get_issue_details(task.issue_id)
        
    bug_report_dict = await ctx.step("fetch-issue-context", fetch_issue)
    bug_report = Issue(**bug_report_dict)

    is_approved = False
    attempt = 0
    last_feedback = "Initial Attempt"

    while not is_approved and attempt < 3:
        attempt += 1
        correlation_id = f"{ctx.task_id}-attempt-{attempt}"

        # 2. Call the slow, expensive LLM safely
        async def generate_code():
            return await llm_service.generate_fix(bug_report.stack_trace, last_feedback)
            
        proposed_fix_dict = await ctx.step(f"generate-code-fix-{attempt}", generate_code)

        async def notify():
            await github_service.request_human_review(task.issue_id, proposed_fix_dict, correlation_id)
            return True 
            
        await ctx.step(f"notify-reviewer-{attempt}", notify)

        # 3. Wait for a human decision without blocking a thread! State is saved to DB here.
        review_data = await ctx.await_event(
            event_name=f"agent-approval:{task.issue_id}:{correlation_id}",
            step_name=f"wait-for-human-review-{attempt}"
        )

        approval = HumanApproval(**review_data)
        is_approved = approval.approved
        last_feedback = approval.reason or "No feedback has been given"

    if is_approved:
        async def create_pr():
            return await github_service.create_pull_request(task.issue_id, "apply-fix")
            
        pr_url = await ctx.step("create-pull-request", create_pr)
        return AgentResult(success=True, pull_request_url=pr_url).model_dump()
        
    return AgentResult(success=False).model_dump()
```

### 3. Application Setup & API (FastAPI)

The FastAPI application serves as the host for our SqlFlow runtime. During application startup, we establish the DB driver, start the worker queue, and map HTTP routes to spawn tasks and emit events.

```python
from fastapi import FastAPI
from contextlib import asynccontextmanager
from sqlflow import SqlFlow, SpawnOptions, EmitEventOptions, WorkerOptions
from sqlflow.drivers.postgres import PostgresDriver

db_driver = None
sqlflow_client = None
worker = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    global db_driver, sqlflow_client, worker
    db_driver = PostgresDriver("postgresql://postgres:password@127.0.0.1:5432/sqlflow_db")
    await db_driver.connect()
    
    sqlflow_client = SqlFlow(db=db_driver)
    await sqlflow_client.create_queue("ai-agent-queue")
    
    # Map the Python method to the string identifier "solve-bug"
    sqlflow_client.register_task("solve-bug", autonomous_agent_workflow, max_attempts=3)
    
    worker = sqlflow_client.create_worker(WorkerOptions(
        worker_id="agent-worker-1",
        queue_name="ai-agent-queue",
        concurrency=5
    ))
    await worker.start()
    
    yield # App runs here
    
    await worker.stop()
    await db_driver.disconnect()

app = FastAPI(lifespan=lifespan)

# Endpoint 1: Spawn the Agent Task
@app.post("/agent/start")
async def start_agent(task: AgentTask):
    result = await sqlflow_client.spawn(
        options=SpawnOptions(queue_name="ai-agent-queue"), 
        task_name="solve-bug", 
        params=task.model_dump()
    )
    return {"run_id": result.run_id, "task_id": result.task_id}

# Endpoint 2: Resume the Agent upon Human Review
@app.post("/agent/review/{issue_id}/{correlation_id}")
async def review_agent(issue_id: str, correlation_id: str, approval: HumanApproval):
    await sqlflow_client.emit_event(
        options=EmitEventOptions(queue_name="ai-agent-queue"), 
        event_name=f"agent-approval:{issue_id}:{correlation_id}", 
        payload=approval.model_dump()
    )
    return {"status": "Event emitted"}
```