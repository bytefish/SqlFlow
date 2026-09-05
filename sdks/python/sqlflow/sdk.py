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
    async def complete_run(self, p_queue_name: str, p_run_id: UUID, p_state: Any) -> None:
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
    async def set_task_checkpoint_state(self, p_queue_name: str, p_task_id: UUID, p_step_name: str, p_state: Any, p_owner_run: UUID, p_extend_claim_by: int) -> None:
        """CALL ssf.set_task_checkpoint_state(p_queue_name TEXT, p_task_id UUID, p_step_name TEXT, p_state ANY, p_owner_run UUID, p_extend_claim_by INT)"""
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
    async def emit_event(self, p_queue_name: str, p_event_name: str, p_payload: Any) -> None:
        """CALL ssf.emit_event(p_queue_name TEXT, p_event_name TEXT, p_payload TEXT)"""
        pass

    @abstractmethod
    async def cancel_task(self, p_queue_name: str, p_task_id: UUID) -> None:
        """CALL ssf.cancel_task(p_queue_name TEXT, p_task_id UUID)"""
        pass

class QueueSignalListener(ABC):

    @abstractmethod
    async def register_queue(
        self,
        queue_name: str
    ) -> None:
        pass

    @abstractmethod
    async def wait_for_signal(
        self,
        queue_name: str,
        timeout_seconds: float
    ) -> bool:
        pass


class AsyncTokenBucket:
    def __init__(
        self,
        permits_per_second: int,
        burst_size: int
    ):
        self._permits_per_second = (
            permits_per_second
        )

        self._burst_size = burst_size

        self._tokens = burst_size

        self._last_refill = (
            time.monotonic()
        )

        self._lock = asyncio.Lock()

    async def acquire(
        self,
        permits: int
    ) -> None:
        while True:
            async with self._lock:
                self._refill()

                if self._tokens >= permits:
                    self._tokens -= permits
                    return

                missing = (
                    permits -
                    self._tokens
                )

            delay = (
                missing /
                self._permits_per_second
            )

            await asyncio.sleep(delay)

    def _refill(self):
        now = time.monotonic()

        elapsed = (
            now -
            self._last_refill
        )

        self._last_refill = now

        self._tokens = min(
            self._burst_size,
            self._tokens +
            elapsed *
            self._permits_per_second
        )

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
            return state_row["state"]

        # Execute the action (supports both sync and async functions)
        if asyncio.iscoroutinefunction(action):
            result = await action()
        else:
            result = action()

        # Serialize and checkpoint the result
        await self._db.set_task_checkpoint_state(
            p_queue_name=self._queue_name,
            p_task_id=self.task_id,
            p_step_name=step_name,
            p_state=result,
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
        registry: Dict[str, Tuple[Callable[[TaskContext, Any], Any], int]],
        signals: QueueSignalListener
    ):
        self._options = options
        self._db = db
        self._registry = registry
        self._is_running = False
        self._worker_task: Optional[asyncio.Task] = None
        self._signals = signals
        self._semaphore = asyncio.Semaphore(options.concurrency)
        self._rate_limiter = None

        if (
            options.max_tasks_per_second and
            options.max_tasks_per_second > 0
        ):
            burst = (
                options.rate_limit_burst_size
                or
                options.max_tasks_per_second
            )

            self._rate_limiter = (
                AsyncTokenBucket(
                    permits_per_second=
                        options.max_tasks_per_second,

                    burst_size=burst
                )
            )

    async def start(self) -> None:
        """Starts the worker polling loop."""
        if self._is_running:
            return
        self._is_running = True
        self._worker_task = asyncio.create_task(self._poll_loop())
        await self._signals.register_queue(self._options.queue_name)
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
        """
        Main worker loop.

        Instead of polling on a fixed interval, the worker waits for a
        queue wake-up signal. A reconciliation timeout ensures that lost
        notifications do not permanently stall processing.

        Rate limiting is applied before claiming tasks so that tasks are
        not unnecessarily held by claim leases.
        """

        reconciliation_timeout = 60.0

        queue_may_contain_work = True

        while self._is_running:
            try:
                #
                # If the previous claim returned no work, wait for either:
                #
                #  1. LISTEN / NOTIFY signal
                #  2. reconciliation timeout
                #
                if not queue_may_contain_work:
                    await self._signals.wait_for_signal(
                        self._options.queue_name,
                        reconciliation_timeout
                    )

                    queue_may_contain_work = True

                #
                # Determine available execution capacity.
                #
                available_capacity = self._semaphore._value

                if available_capacity <= 0:
                    await asyncio.sleep(0.05)
                    continue

                #
                # Batch size is never larger than currently available
                # execution capacity.
                #
                batch_size = min(
                    available_capacity,
                    self._options.concurrency
                )

                #
                # Apply rate limiting BEFORE claiming tasks.
                #
                if (
                    self._rate_limiter is not None and
                    batch_size > 0
                ):
                    await self._rate_limiter.acquire(
                        batch_size
                    )

                #
                # Claim work from the queue.
                #
                claimed_tasks = await self._db.claim_task(
                    p_queue_name=self._options.queue_name,
                    p_worker_id=self._options.worker_id,
                    p_claim_timeout=300,
                    p_qty=batch_size
                )

                #
                # No tasks found.
                #
                if not claimed_tasks:
                    queue_may_contain_work = False
                    continue

                #
                # Schedule execution.
                #
                for task_row in claimed_tasks:
                    asyncio.create_task(
                        self._process_task_with_semaphore(
                            task_row
                        )
                    )

                #
                # Full batch probably means more work is still available.
                #
                queue_may_contain_work = (
                    len(claimed_tasks) == batch_size
                )

            except asyncio.CancelledError:
                break

            except Exception as exception:
                logger.exception(
                    "Worker loop failed: %s",
                    exception
                )

                queue_may_contain_work = False

                await asyncio.sleep(1)

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
        params = task_row.get("params", {})

        # Ensure task is registered
        if task_name not in self._registry:
            logger.error(f"Task '{task_name}' not found in registry.")
            await self._db.fail_run(
                p_queue_name=self._options.queue_name,
                p_run_id=run_id,
                p_reason={"error": f"Task {task_name} not registered"},
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
                p_reason=error_details,
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
            p_params=params if params else {},
            p_options=options.model_dump(exclude_none=True)
        )

    async def emit_event(self, options: EmitEventOptions, event_name: str, payload: Any = None) -> None:
        """Emits an event to a queue, potentially waking up suspended tasks."""
        await self._db.emit_event(
            p_queue_name=options.queue_name,
            p_event_name=event_name,
            p_payload=payload if payload else {}
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