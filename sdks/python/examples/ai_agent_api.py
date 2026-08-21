import asyncio
import logging
import random
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI
from pydantic import BaseModel

# Import the SDK
from sqlflow import (
    SqlFlow, 
    TaskContext, 
    WorkerOptions, 
    SpawnOptions, 
    EmitEventOptions
)
from sqlflow.drivers.postgres import PostgresDriver

# Models

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
    reason: Optional[str] = None


# Services

class LlmService:
    def __init__(self):
        self.logger = logging.getLogger("LlmService")

    async def generate_fix(self, log: str, last_feedback: str) -> dict:
        self.logger.info(f"Agent is thinking: 'Learned from feedback: {last_feedback}'")
        
        # Simulate a very expensive LLM call with a delay
        await asyncio.sleep(2.5)
        
        # Change Code based on human feedback
        if "error handling" in last_feedback.lower():
            code = "// AI: Improved Logging & Error-Handling added\nif(data == null) raise ValueError('Null data');"
        else:
            code = "// AI: Simple Fix for the NullReferenceException\nif(data is None): return"
            
        self.logger.info(f"LLM has generated a potential fix: {code}")
        
        # We return a dict to make it easily serializable by SqlFlow
        return Solution(patched_code=code).model_dump()


class GitHubService:
    def __init__(self):
        self.logger = logging.getLogger("GitHubService")

    async def get_issue_details(self, issue_id: str) -> dict:
        self.logger.info(f"GitHub: Gets Ticket #{issue_id} details from the Repository...")
        await asyncio.sleep(0.8)
        return Issue(stack_trace="NullReferenceException at PaymentGateway.cs:42").model_dump()

    async def create_pull_request(self, issue_id: str, code: str) -> str:
        self.logger.info(f"GitHub: PR for Issue #{issue_id} has been created...")
        await asyncio.sleep(1.2)
        return f"https://github.com/company/repo/pull/{random.randint(1000, 9999)}"

    async def escalate_to_senior(self, issue_id: str, reason: str) -> None:
        self.logger.critical(f"ESCALATION to Senior Developer: Issue #{issue_id} - Reason: {reason}")
        await asyncio.sleep(0.5)

    async def request_human_review(self, issue_id: str, proposed_fix: dict, correlation_id: str) -> None:
        patched_code = proposed_fix.get("patched_code", "")
        self.logger.info(f"ACTION REQUIRED: Solution for Issue #{issue_id} with Correlation-ID {correlation_id} has been created: {patched_code}...")
        await asyncio.sleep(1.2)


class LocalNotificationService:
    def __init__(self):
        self.logger = logging.getLogger("LocalNotification")

    async def notify_reviewer(self, issue_id: str, correlation_id: str) -> None:
        self.logger.info(f"Ping! Please review {correlation_id} for issue {issue_id}.")

# Workflow

llm_service = LlmService()
github_service = GitHubService()
notification_service = LocalNotificationService()

async def autonomous_agent_workflow(ctx: TaskContext, params: dict) -> dict:
    logger = logging.getLogger("AutonomousAgentJob")
    task = AgentTask(**params)
    
    logger.info(f"Agent starts researching ticket {task.issue_id}")

    # Helper async functions for steps (since lambda doesn't work well with await in ctx.step)
    async def fetch_issue():
        return await github_service.get_issue_details(task.issue_id)
        
    # Load the Issue Context first, so the LLM has all relevant information
    bug_report_dict = await ctx.step("fetch-issue-context", fetch_issue)
    bug_report = Issue(**bug_report_dict)

    is_approved = False
    attempt = 0
    last_feedback = "Initial Attempt"

    while not is_approved and attempt < 3:
        attempt += 1
        correlation_id = f"attempt-{attempt}"
        
        logger.info(f"Attempt {attempt}/3: Generating a fix based on: {last_feedback}")

        async def generate_code():
            return await llm_service.generate_fix(bug_report.stack_trace, last_feedback)
            
        proposed_fix_dict = await ctx.step(f"generate-code-fix-{attempt}", generate_code)
        proposed_fix = Solution(**proposed_fix_dict)

        async def notify():
            await github_service.request_human_review(task.issue_id, proposed_fix.model_dump(), correlation_id)
            await notification_service.notify_reviewer(task.issue_id, correlation_id)
            return True # Step needs to return something serializable
            
        await ctx.step(f"notify-reviewer-{attempt}", notify)

        logger.info(f"Review for {correlation_id} has been requested. Agent goes idle and waits for the code review...")

        # Wait for a human decision without blocking a thread
        # This will throw SuspendTaskException if the event 
        # hasn't happened yet!
        review_data = await ctx.await_event(
            event_name=f"agent-approval:{task.issue_id}:{correlation_id}",
            step_name=f"wait-for-human-review-{attempt}"
        )

        approval = HumanApproval(**review_data)
        is_approved = approval.approved
        last_feedback = approval.reason or "No feedback has been given"

        if not is_approved:
            logger.warning(f"Attempt {attempt} has been rejected: {last_feedback}")

    if is_approved:
        logger.info("Fix approved. Creating Pull Request...")

        async def create_pr():
            return await github_service.create_pull_request(task.issue_id, "apply-fix")
            
        pr_url = await ctx.step("create-pull-request", create_pr)
        logger.info(f"Mission accomplished, the PR has been created: {pr_url}")
        
        return AgentResult(success=True, pull_request_url=pr_url).model_dump()
    else:
        logger.error(f"Maximum number of attempts reached. Escalates ticket {task.issue_id} to a human.")

        async def escalate():
            await github_service.escalate_to_senior(task.issue_id, "Agent didn't find a solution after 3 attempts.")
            return True
            
        await ctx.step("notify-senior-developer", escalate)
        
        return AgentResult(success=False, reason="Escalated to human supervisor after 3 failures.").model_dump()


# Web API (FastAPI) and Application Setup

logging.basicConfig(level=logging.INFO)

# Global variables to hold our DB driver, Client and Worker

db_driver = None
sqlflow_client = None
worker = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifecycle manager for the FastAPI application."""
    global db_driver, sqlflow_client, worker
    
    connection_string = "postgresql://postgres:password@127.0.0.1:5432/sqlflow_db"
    
    db_driver = PostgresDriver(connection_string)

    await db_driver.connect()
    
    sqlflow_client = SqlFlow(db=db_driver)
    
    await sqlflow_client.create_queue("ai-agent-queue")
    
    # Register Workflow
    sqlflow_client.register_task("solve-bug", autonomous_agent_workflow, max_attempts=3)
    
    # Start Worker
    worker = sqlflow_client.create_worker(WorkerOptions(
        worker_id="agent-worker-1",
        queue_name="ai-agent-queue",
        poll_interval=1.0,
        concurrency=1
    ))
    
    await worker.start()
    
    yield # App runs here
    
    # Cleanup on shutdown
    if worker:
        await worker.stop()
    if db_driver:
        await db_driver.disconnect()

app = FastAPI(lifespan=lifespan)

@app.post("/agent/start")
async def start_agent(task: AgentTask):
    """A Webhook triggers the Agent, such as a new JIRA ticket or GitHub issue."""
    options = SpawnOptions(queue_name="ai-agent-queue")
    
    result = await sqlflow_client.spawn(
        options=options, 
        task_name="solve-bug", 
        params=task.model_dump()
    )
    
    return {
        "run_id": result.run_id, 
        "status": f"Agent dispatched to fix Issue #{task.issue_id}"
    }

@app.post("/agent/review/{issue_id}/{correlation_id}")
async def review_agent(issue_id: str, correlation_id: str, approval: HumanApproval):
    """A Lead-Developer clicks on 'Approve' or 'Reject', with Feedback."""
    
    # Wake up the agent that is working on the ticket
    event_name = f"agent-approval:{issue_id}:{correlation_id}"
    
    options = EmitEventOptions(queue_name="ai-agent-queue")
    await sqlflow_client.emit_event(
        options=options, 
        event_name=event_name, 
        payload=approval.model_dump()
    )
    
    message = (
        f"Fix for {correlation_id} approved. Agent is now completing its work."
        if approval.approved else
        f"Fix for {correlation_id} rejected. Agent tries again with feedback: '{approval.reason}'"
    )
    
    return {"message": message}