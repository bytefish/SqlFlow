import asyncio
import json
import logging
import traceback
from abc import ABC, abstractmethod
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Dict, Optional, List, Tuple
from uuid import UUID

from pydantic import BaseModel

# Configure default logger for the SDK
logger = logging.getLogger("sqlflow")

class SuspendTaskException(Exception):
    """
    Control-flow exception thrown when a task must suspend and wait for an event.
    Caught by the worker to schedule a replay later.
    """
    pass


class SpawnOptions(BaseModel):
    """Configuration for spawning a new task."""
    queue_name: str
    max_attempts: Optional[int] = None
    headers: Optional[Dict[str, Any]] = None
    retry_strategy: Optional[Dict[str, Any]] = None
    cancellation: Optional[Dict[str, Any]] = None
    idempotency_key: Optional[str] = None


class SpawnResult(BaseModel):
    """Result returned after successfully spawning a task."""
    task_id: UUID
    run_id: UUID
    attempt: int
    created: bool


class EmitEventOptions(BaseModel):
    """Configuration for emitting an event to a queue."""
    queue_name: str


class CancelTaskOptions(BaseModel):
    """Configuration for cancelling an existing task."""
    queue_name: str


class WorkerOptions(BaseModel):
    """Configuration for a worker polling a queue."""
    worker_id: str
    queue_name: str
    poll_interval: float
    concurrency: int

class DatabaseDriver(ABC):
    """
    Abstract driver enforcing the exact stored procedure/function signatures
    expected by the SqlFlow database schemas. This allows pluggable support
    for both PostgreSQL and SQL Server.
    """
    
    @abstractmethod
    async def create_queue(self, p_queue_name: str, p_storage_mode: str) -> None:
        """CALL ssf.create_queue(p_queue_name TEXT, p_storage_mode TEXT)"""
        pass

    @abstractmethod
    async def spawn_task(self, p_queue_name: str, p_task_name: str, p_params: Any, p_options: Any) -> SpawnResult:
        """SELECT * FROM ssf.spawn_task(p_queue_name TEXT, p_task_name TEXT, p_params JSONB, p_options JSONB)"""
        pass

    @abstractmethod
    async def claim_task(self, p_queue_name: str, p_worker_id: str, p_claim_timeout: int, p_qty: int) -> List[Dict[str, Any]]:
        """SELECT * FROM ssf.claim_task(p_queue_name TEXT, p_worker_id TEXT, p_claim_timeout INT, p_qty INT)"""
        pass

    @abstractmethod
    async def complete_run(self, p_queue_name: str, p_run_id: UUID, p_state: str) -> None:
        """CALL ssf.complete_run(p_queue_name TEXT, p_run_id UUID, p_state TEXT)"""
        pass

    @abstractmethod
    async def schedule_run(self, p_queue_name: str, p_run_id: UUID, p_wake_at: datetime) -> None:
        """CALL ssf.schedule_run(p_queue_name TEXT, p_run_id UUID, p_wake_at TIMESTAMPTZ)"""
        pass

    @abstractmethod
    async def fail_run(self, p_queue_name: str, p_run_id: UUID, p_reason: Any, p_retry_at: datetime) -> None:
        """CALL ssf.fail_run(p_queue_name TEXT, p_run_id UUID, p_reason JSONB, p_retry_at TIMESTAMPTZ)"""
        pass

    @abstractmethod
    async def set_task_checkpoint_state(self, p_queue_name: str, p_task_id: UUID, p_step_name: str, p_state: str, p_owner_run: UUID, p_extend_claim_by: int) -> None:
        """CALL ssf.set_task_checkpoint_state(p_queue_name TEXT, p_task_id UUID, p_step_name TEXT, p_state TEXT, p_owner_run UUID, p_extend_claim_by INT)"""
        pass

    @abstractmethod
    async def get_task_checkpoint_state(self, p_queue_name: str, p_task_id: UUID, p_step_name: str, p_include_pending: int) -> Optional[Dict[str, Any]]:
        """SELECT * FROM ssf.get_task_checkpoint_state(p_queue_name TEXT, p_task_id UUID, p_step_name TEXT, p_include_pending INT)"""
        pass

    @abstractmethod
    async def await_event(self, p_queue_name: str, p_task_id: UUID, p_run_id: UUID, p_step_name: str, p_event_name: str, p_timeout: Optional[int]) -> Dict[str, Any]:
        """SELECT * FROM ssf.await_event(p_queue_name TEXT, p_task_id UUID, p_run_id UUID, p_step_name TEXT, p_event_name TEXT, p_timeout INT)"""
        pass

    @abstractmethod
    async def emit_event(self, p_queue_name: str, p_event_name: str, p_payload: str) -> None:
        """CALL ssf.emit_event(p_queue_name TEXT, p_event_name TEXT, p_payload TEXT)"""
        pass

    @abstractmethod
    async def cancel_task(self, p_queue_name: str, p_task_id: UUID) -> None:
        """CALL ssf.cancel_task(p_queue_name TEXT, p_task_id UUID)"""
        pass

class TaskContext:
    """
    Injected into workflow handlers. Interacts with the DatabaseDriver to manage
    checkpoints and state, enabling the durable execution model.
    """
    def __init__(self, task_id: UUID, run_id: UUID, attempt: int, queue_name: str, db: DatabaseDriver):
        self.task_id = task_id
        self.run_id = run_id
        self.attempt = attempt
        self._queue_name = queue_name
        self._db = db

    async def step(self, step_name: str, action: Callable) -> Any:
        """
        Executes a workflow step or skips it by retrieving the state from the database checkpoint.
        """
        state_row = await self._db.get_task_checkpoint_state(
            p_queue_name=self._queue_name,
            p_task_id=self.task_id,
            p_step_name=step_name,
            p_include_pending=0
        )
        
        # If already completed in a previous run, return saved state immediately
        if state_row and state_row.get("state") is not None:
            return json.loads(state_row["state"])

        # Execute the action (supports both sync and async functions)
        if asyncio.iscoroutinefunction(action):
            result = await action()
        else:
            result = action()

        # Serialize and checkpoint the result
        serialized_result = json.dumps(result)
        await self._db.set_task_checkpoint_state(
            p_queue_name=self._queue_name,
            p_task_id=self.task_id,
            p_step_name=step_name,
            p_state=serialized_result,
            p_owner_run=self.run_id,
            p_extend_claim_by=0 # Handled by worker heartbeat normally
        )
        return result

    async def await_event(self, event_name: str, step_name: str, timeout: Optional[int] = None) -> Any:
        """
        Attempts to claim an event. Throws a SuspendTaskException if the event is not ready,
        causing the worker to yield execution.
        """
        result = await self._db.await_event(
            p_queue_name=self._queue_name,
            p_task_id=self.task_id,
            p_run_id=self.run_id,
            p_step_name=step_name,
            p_event_name=event_name,
            p_timeout=timeout
        )

        if result.get("should_suspend"):
            raise SuspendTaskException(f"Task suspended waiting for event: {event_name}")
            
        return result.get("payload")

class Worker:
    """
    Background worker that polls for tasks, executes handlers, and manages 
    the run lifecycle (Complete, Fail, Suspend).
    """
    def __init__(
        self, 
        options: WorkerOptions, 
        db: DatabaseDriver, 
        registry: Dict[str, Tuple[Callable[[TaskContext, Any], Any], int]]
    ):
        self._options = options
        self._db = db
        self._registry = registry
        self._is_running = False
        self._worker_task: Optional[asyncio.Task] = None
        # Semaphore limits how many tasks this worker processes concurrently
        self._semaphore = asyncio.Semaphore(options.concurrency)

    async def start(self) -> None:
        """Starts the worker polling loop."""
        if self._is_running:
            return
        self._is_running = True
        self._worker_task = asyncio.create_task(self._poll_loop())
        logger.info(f"Worker {self._options.worker_id} started on queue '{self._options.queue_name}'.")

    async def stop(self) -> None:
        """Stops the worker gracefully."""
        self._is_running = False
        if self._worker_task:
            self._worker_task.cancel()
            try:
                await self._worker_task
            except asyncio.CancelledError:
                pass
        logger.info(f"Worker {self._options.worker_id} stopped.")

    async def _poll_loop(self) -> None:
        """Continuous loop polling for tasks based on poll_interval."""
        while self._is_running:
            try:
                # Wait until we have capacity in our concurrency semaphore
                await self._semaphore.acquire()
                self._semaphore.release()

                # Calculate how many tasks we have capacity to claim right now
                available_capacity = self._options.concurrency
                
                claimed_tasks = await self._db.claim_task(
                    p_queue_name=self._options.queue_name,
                    p_worker_id=self._options.worker_id,
                    p_claim_timeout=300,  # 5 minutes claim lock (configurable as needed)
                    p_qty=available_capacity
                )

                if not claimed_tasks:
                    await asyncio.sleep(self._options.poll_interval)
                    continue

                for task_row in claimed_tasks:
                    # Fire and forget the task execution so we can continue polling
                    asyncio.create_task(self._process_task_with_semaphore(task_row))

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in poll loop: {e}")
                await asyncio.sleep(self._options.poll_interval)

    async def _process_task_with_semaphore(self, task_row: Dict[str, Any]) -> None:
        """Wraps task processing with the concurrency semaphore."""
        async with self._semaphore:
            await self._process_task(task_row)

    async def _process_task(self, task_row: Dict[str, Any]) -> None:
        """Instantiates the context, runs the workflow handler, and updates the database state."""
        task_id = task_row["task_id"]
        run_id = task_row["run_id"]
        attempt = task_row["attempt"]
        task_name = task_row["task_name"]
        
        # Parse params safely
        raw_params = task_row.get("params", "{}")
        params = json.loads(raw_params) if raw_params else {}

        # Ensure task is registered
        if task_name not in self._registry:
            logger.error(f"Task '{task_name}' not found in registry.")
            await self._db.fail_run(
                p_queue_name=self._options.queue_name,
                p_run_id=run_id,
                p_reason=json.dumps({"error": f"Task {task_name} not registered"}),
                p_retry_at=datetime.now(timezone.utc) + timedelta(minutes=5)
            )
            return

        handler, max_attempts = self._registry[task_name]
        context = TaskContext(
            task_id=task_id,
            run_id=run_id,
            attempt=attempt,
            queue_name=self._options.queue_name,
            db=self._db
        )

        try:
            # Execute the user's workflow handler
            if asyncio.iscoroutinefunction(handler):
                await handler(context, params)
            else:
                handler(context, params)

            # Complete the run successfully
            await self._db.complete_run(
                p_queue_name=self._options.queue_name,
                p_run_id=run_id,
                p_state="succeeded"
            )
            logger.info(f"Task {task_id} completed successfully.")

        except SuspendTaskException as e:
            # Suspend Pattern: The DB's await_event procedure already marked the DB state.
            # We just need to mark the run itself as suspended/yielded so the claim lock is 
            # released.
            logger.info(f"Task {task_id} suspended: {e}")
            
            await self._db.complete_run(
                p_queue_name=self._options.queue_name,
                p_run_id=run_id,
                p_state="suspended"
            )

        except Exception as e:
            # Unhandled Exception: Fail the run and schedule a retry
            error_details = {
                "error": str(e),
                "traceback": traceback.format_exc()
            }
            logger.error(f"Task {task_id} failed: {e}")
            
            # Simple backoff logic (e.g., attempt ^ 2 minutes)
            retry_at = datetime.now(timezone.utc) + timedelta(minutes=(attempt ** 2))
            
            await self._db.fail_run(
                p_queue_name=self._options.queue_name,
                p_run_id=run_id,
                p_reason=json.dumps(error_details),
                p_retry_at=retry_at
            )

class SqlFlow:
    """
    Main SDK client for managing queues, spawning tasks, and registering workflows.
    """
    def __init__(self, db: DatabaseDriver):
        self._db = db
        # Internal registry mapping task names to (handler, max_attempts)
        self._registry: Dict[str, Tuple[Callable[[TaskContext, Any], Any], int]] = {}

    async def create_queue(self, queue_name: str, storage_mode: str = 'unpartitioned') -> None:
        """Creates a new durable queue."""
        await self._db.create_queue(
            p_queue_name=queue_name, 
            p_storage_mode=storage_mode
        )

    async def spawn(self, options: SpawnOptions, task_name: str, params: Any) -> SpawnResult:
        """Spawns a new task onto a queue."""
        # Pydantic model_dump handles serialization of options
        return await self._db.spawn_task(
            p_queue_name=options.queue_name,
            p_task_name=task_name,
            p_params=json.dumps(params) if params else "{}",
            p_options=json.dumps(options.model_dump(exclude_none=True))
        )

    async def emit_event(self, options: EmitEventOptions, event_name: str, payload: Any = None) -> None:
        """Emits an event to a queue, potentially waking up suspended tasks."""
        await self._db.emit_event(
            p_queue_name=options.queue_name,
            p_event_name=event_name,
            p_payload=json.dumps(payload) if payload else "{}"
        )

    async def cancel_task(self, options: CancelTaskOptions, task_id: UUID) -> None:
        """Cancels a pending or active task."""
        await self._db.cancel_task(
            p_queue_name=options.queue_name,
            p_task_id=task_id
        )

    def register_task(self, task_name: str, handler: Callable[[TaskContext, Any], Any], max_attempts: int = 3) -> None:
        """
        Registers a workflow task handler locally in the client. 
        Workers created by this client will process these registered tasks.
        """
        self._registry[task_name] = (handler, max_attempts)

    def create_worker(self, options: WorkerOptions) -> Worker:
        """
        Creates a background worker for a specific queue utilizing the tasks 
        registered in this client instance.
        """
        return Worker(
            options=options,
            db=self._db,
            registry=self._registry
        )