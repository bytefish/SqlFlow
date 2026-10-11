import asyncio
import json
import logging
import time
import traceback
from abc import ABC, abstractmethod
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Dict, Optional, List, Tuple
from uuid import UUID

from pydantic import BaseModel

logger = logging.getLogger("sqlflow")

class SuspendTaskException(Exception):
    pass

class SpawnOptions(BaseModel):
    tenant_id: str
    queue_name: str
    max_attempts: Optional[int] = None
    headers: Optional[Dict[str, Any]] = None
    retry_strategy: Optional[Dict[str, Any]] = None
    cancellation: Optional[Dict[str, Any]] = None
    idempotency_key: Optional[str] = None

class SpawnResult(BaseModel):
    task_id: UUID
    run_id: UUID
    attempt: int
    created: bool

class EmitEventOptions(BaseModel):
    tenant_id: str
    queue_name: str

class CancelTaskOptions(BaseModel):
    tenant_id: str
    queue_name: str

class WorkerOptions(BaseModel):
    tenant_id: str
    worker_id: str
    queue_name: str
    poll_interval: float
    concurrency: int
    max_tasks_per_second: Optional[int] = None
    rate_limit_burst_size: Optional[int] = None

class QueueSignalListener(ABC):
    @abstractmethod
    async def register_queue(self, tenant_id: str, queue_name: str) -> None:
        pass

    @abstractmethod
    async def wait_for_signal(self, tenant_id: str, queue_name: str, timeout_seconds: float) -> bool:
        pass

class DatabaseDriver(ABC):
    @abstractmethod
    async def create_queue(self, p_tenant_id: str, p_queue_name: str, p_storage_mode: str) -> None:
        pass

    @abstractmethod
    async def spawn_task(self, p_tenant_id: str, p_queue_name: str, p_task_name: str, p_params: Any, p_options: Any) -> SpawnResult:
        pass

    @abstractmethod
    async def claim_task(self, p_tenant_id: str, p_queue_name: str, p_worker_id: str, p_claim_timeout: int, p_qty: int) -> List[Dict[str, Any]]:
        pass

    @abstractmethod
    async def complete_run(self, p_tenant_id: str, p_queue_name: str, p_run_id: UUID, p_state: Any) -> None:
        pass

    @abstractmethod
    async def schedule_run(self, p_tenant_id: str, p_queue_name: str, p_run_id: UUID, p_wake_at: datetime) -> None:
        pass

    @abstractmethod
    async def fail_run(self, p_tenant_id: str, p_queue_name: str, p_run_id: UUID, p_reason: Any, p_retry_at: datetime) -> None:
        pass

    @abstractmethod
    async def set_task_checkpoint_state(self, p_tenant_id: str, p_queue_name: str, p_task_id: UUID, p_step_name: str, p_state: Any, p_owner_run: UUID, p_extend_claim_by: int) -> None:
        pass

    @abstractmethod
    async def get_task_checkpoint_state(self, p_tenant_id: str, p_queue_name: str, p_task_id: UUID, p_step_name: str, p_include_pending: int) -> Optional[Dict[str, Any]]:
        pass

    @abstractmethod
    async def await_event(self, p_tenant_id: str, p_queue_name: str, p_task_id: UUID, p_run_id: UUID, p_step_name: str, p_event_name: str, p_timeout: Optional[int]) -> Dict[str, Any]:
        pass

    @abstractmethod
    async def emit_event(self, p_tenant_id: str, p_queue_name: str, p_event_name: str, p_payload: Any) -> None:
        pass

    @abstractmethod
    async def cancel_task(self, p_tenant_id: str, p_queue_name: str, p_task_id: UUID) -> None:
        pass
        
    @abstractmethod
    async def get_next_available_at(self, p_tenant_id: str, p_queue_name: str) -> Optional[datetime]:
        pass

    @abstractmethod
    def create_queue_signal_listener(self) -> QueueSignalListener:
        pass

class AsyncTokenBucket:
    def __init__(self, permits_per_second: int, burst_size: int):
        self._permits_per_second = permits_per_second
        self._burst_size = burst_size
        self._tokens = burst_size
        self._last_refill = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self, permits: int) -> None:
        while True:
            async with self._lock:
                self._refill()
                if self._tokens >= permits:
                    self._tokens -= permits
                    return
                missing = permits - self._tokens
            delay = missing / self._permits_per_second
            await asyncio.sleep(delay)

    def _refill(self):
        now = time.monotonic()
        elapsed = now - self._last_refill
        self._last_refill = now
        self._tokens = min(self._burst_size, self._tokens + elapsed * self._permits_per_second)

class TaskContext:
    def __init__(self, tenant_id: str, task_id: UUID, run_id: UUID, attempt: int, queue_name: str, db: DatabaseDriver):
        self.tenant_id = tenant_id
        self.task_id = task_id
        self.run_id = run_id
        self.attempt = attempt
        self._queue_name = queue_name
        self._db = db

    async def step(self, step_name: str, action: Callable) -> Any:
        state_row = await self._db.get_task_checkpoint_state(
            p_tenant_id=self.tenant_id,
            p_queue_name=self._queue_name,
            p_task_id=self.task_id,
            p_step_name=step_name,
            p_include_pending=0
        )
        
        if state_row and state_row.get("state") is not None:
            return state_row["state"]

        if asyncio.iscoroutinefunction(action):
            result = await action()
        else:
            result = action()

        await self._db.set_task_checkpoint_state(
            p_tenant_id=self.tenant_id,
            p_queue_name=self._queue_name,
            p_task_id=self.task_id,
            p_step_name=step_name,
            p_state=result,
            p_owner_run=self.run_id,
            p_extend_claim_by=0
        )
        return result

    async def await_event(self, event_name: str, step_name: str, timeout: Optional[int] = None) -> Any:
        result = await self._db.await_event(
            p_tenant_id=self.tenant_id,
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
    def __init__(self, options: WorkerOptions, db: DatabaseDriver, registry: Dict[str, Tuple[Callable[[TaskContext, Any], Any], int]], signals: QueueSignalListener):
        self._options = options
        self._db = db
        self._registry = registry
        self._is_running = False
        self._worker_task: Optional[asyncio.Task] = None
        self._signals = signals
        self._semaphore = asyncio.Semaphore(options.concurrency)
        self._rate_limiter = None

        if options.max_tasks_per_second and options.max_tasks_per_second > 0:
            burst = options.rate_limit_burst_size or options.max_tasks_per_second
            self._rate_limiter = AsyncTokenBucket(permits_per_second=options.max_tasks_per_second, burst_size=burst)

    async def start(self) -> None:
        if self._is_running:
            return
        self._is_running = True
        self._worker_task = asyncio.create_task(self._poll_loop())
        await self._signals.register_queue(self._options.tenant_id, self._options.queue_name)
        logger.info(f"Worker {self._options.worker_id} started for tenant '{self._options.tenant_id}' on queue '{self._options.queue_name}'.")

    async def stop(self) -> None:
        self._is_running = False
        if self._worker_task:
            self._worker_task.cancel()
            try:
                await self._worker_task
            except asyncio.CancelledError:
                pass
        logger.info(f"Worker {self._options.worker_id} stopped.")

    async def _poll_loop(self) -> None:
        reconciliation_timeout = 60.0
        queue_may_contain_work = True

        while self._is_running:
            try:
                if not queue_may_contain_work:
                    delay = reconciliation_timeout
                    skip_wait = False

                    try:
                        next_available_at = await self._db.get_next_available_at(
                            p_tenant_id=self._options.tenant_id,
                            p_queue_name=self._options.queue_name
                        )

                        if next_available_at is not None:
                            if next_available_at.tzinfo is None:
                                next_available_at = next_available_at.replace(tzinfo=timezone.utc)
                                
                            time_until_next_job = (next_available_at - datetime.now(timezone.utc)).total_seconds()

                            if time_until_next_job <= 0:
                                skip_wait = True
                            elif time_until_next_job < delay:
                                delay = time_until_next_job
                    except Exception as ex:
                        logger.warning("Failed to retrieve next available time. Falling back to default.", exc_info=ex)

                    if not skip_wait and delay > 0:
                        await self._signals.wait_for_signal(self._options.tenant_id, self._options.queue_name, delay)

                    queue_may_contain_work = True

                available_capacity = self._semaphore._value
                if available_capacity <= 0:
                    await asyncio.sleep(0.05)
                    continue

                batch_size = min(available_capacity, self._options.concurrency)

                if self._rate_limiter is not None and batch_size > 0:
                    await self._rate_limiter.acquire(batch_size)

                claimed_tasks = await self._db.claim_task(
                    p_tenant_id=self._options.tenant_id,
                    p_queue_name=self._options.queue_name,
                    p_worker_id=self._options.worker_id,
                    p_claim_timeout=300,
                    p_qty=batch_size
                )

                if not claimed_tasks:
                    queue_may_contain_work = False
                    continue

                for task_row in claimed_tasks:
                    asyncio.create_task(self._process_task_with_semaphore(task_row))

                queue_may_contain_work = (len(claimed_tasks) == batch_size)

            except asyncio.CancelledError:
                break
            except Exception as exception:
                logger.exception("Worker loop failed: %s", exception)
                queue_may_contain_work = False
                await asyncio.sleep(1)

    async def _process_task_with_semaphore(self, task_row: Dict[str, Any]) -> None:
        async with self._semaphore:
            await self._process_task(task_row)

    async def _process_task(self, task_row: Dict[str, Any]) -> None:
        task_id = task_row["task_id"]
        run_id = task_row["run_id"]
        attempt = task_row["attempt"]
        task_name = task_row["task_name"]
        params = task_row.get("params", {})

        if task_name not in self._registry:
            logger.error(f"Task '{task_name}' not found in registry.")
            await self._db.fail_run(
                p_tenant_id=self._options.tenant_id,
                p_queue_name=self._options.queue_name,
                p_run_id=run_id,
                p_reason={"error": f"Task {task_name} not registered"},
                p_retry_at=datetime.now(timezone.utc) + timedelta(minutes=5)
            )
            return

        handler, max_attempts = self._registry[task_name]
        context = TaskContext(
            tenant_id=self._options.tenant_id,
            task_id=task_id,
            run_id=run_id,
            attempt=attempt,
            queue_name=self._options.queue_name,
            db=self._db
        )

        try:
            if asyncio.iscoroutinefunction(handler):
                await handler(context, params)
            else:
                handler(context, params)

            await self._db.complete_run(
                p_tenant_id=self._options.tenant_id,
                p_queue_name=self._options.queue_name,
                p_run_id=run_id,
                p_state="succeeded"
            )
            logger.info(f"Task {task_id} completed successfully.")

        except SuspendTaskException as e:
            logger.info(f"Task {task_id} suspended: {e}")
        except Exception as e:
            error_details = {"error": str(e), "traceback": traceback.format_exc()}
            logger.error(f"Task {task_id} failed: {e}")
            retry_at = datetime.now(timezone.utc) + timedelta(minutes=(attempt ** 2))
            
            await self._db.fail_run(
                p_tenant_id=self._options.tenant_id,
                p_queue_name=self._options.queue_name,
                p_run_id=run_id,
                p_reason=error_details,
                p_retry_at=retry_at
            )

class SqlFlow:
    def __init__(self, db: DatabaseDriver):
        self._db = db
        self._listener = None
        self._registry: Dict[str, Tuple[Callable[[TaskContext, Any], Any], int]] = {}

    async def create_queue(self, tenant_id: str, queue_name: str, storage_mode: str = 'unpartitioned') -> None:
        await self._db.create_queue(
            p_tenant_id=tenant_id,
            p_queue_name=queue_name, 
            p_storage_mode=storage_mode
        )

    async def spawn(self, options: SpawnOptions, task_name: str, params: Any) -> SpawnResult:
        return await self._db.spawn_task(
            p_tenant_id=options.tenant_id,
            p_queue_name=options.queue_name,
            p_task_name=task_name,
            p_params=params if params else {},
            p_options=options.model_dump(exclude_none=True)
        )

    async def emit_event(self, options: EmitEventOptions, event_name: str, payload: Any = None) -> None:
        await self._db.emit_event(
            p_tenant_id=options.tenant_id,
            p_queue_name=options.queue_name,
            p_event_name=event_name,
            p_payload=payload if payload else {}
        )

    async def cancel_task(self, options: CancelTaskOptions, task_id: UUID) -> None:
        await self._db.cancel_task(
            p_tenant_id=options.tenant_id,
            p_queue_name=options.queue_name,
            p_task_id=task_id
        )

    def register_task(self, task_name: str, handler: Callable[[TaskContext, Any], Any], max_attempts: int = 3) -> None:
        self._registry[task_name] = (handler, max_attempts)

    async def create_worker(self, options: WorkerOptions) -> Worker:
        if self._listener is None:
            self._listener = (await self._db.create_queue_signal_listener())

        return Worker(
            options=options,
            db=self._db,
            registry=self._registry,
            signals=self._listener
        )