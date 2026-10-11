import asyncio
import logging
from typing import Any, Dict, List, Optional
from datetime import datetime
from uuid import UUID

from nats.js.client import JetStreamContext
from nats.js.api import ConsumerConfig, DeliverPolicy

from sqlflow.sdk import QueueSignalListener, DatabaseDriver, SpawnResult

logger = logging.getLogger("sqlflow.nats")

class NatsQueueSignalListener(QueueSignalListener):
    def __init__(self, js: JetStreamContext):
        self._js = js
        self._queue_channels: Dict[str, asyncio.Queue] = {}
        self._tasks: List[asyncio.Task] = []

    async def register_queue(self, tenant_id: str, queue_name: str) -> None:
        key = f"{tenant_id}:{queue_name}"
        if key not in self._queue_channels:
            self._queue_channels[key] = asyncio.Queue(maxsize=1)
            task = asyncio.create_task(self._start_consuming(tenant_id, queue_name))
            self._tasks.append(task)

    async def wait_for_signal(self, tenant_id: str, queue_name: str, timeout_seconds: float) -> bool:
        key = f"{tenant_id}:{queue_name}"
        queue = self._queue_channels.get(key)
        if not queue:
            await asyncio.sleep(timeout_seconds)
            return False
        
        try:
            await asyncio.wait_for(queue.get(), timeout=timeout_seconds)
            return True
        except asyncio.TimeoutError:
            return False

    async def _start_consuming(self, tenant_id: str, queue_name: str) -> None:
        subject = f"ssf.tenants.{tenant_id}.queues.{queue_name}"
        key = f"{tenant_id}:{queue_name}"
        
        try:
            await self._js.add_stream(
                name="SSF_QUEUES",
                subjects=["ssf.tenants.*.queues.>"]
            )
            
            sub = await self._js.subscribe(
                subject,
                durable=f"ssf-worker-{tenant_id}-{queue_name}",
                config=ConsumerConfig(
                    deliver_policy=DeliverPolicy.NEW
                )
            )
            
            async for msg in sub.messages:
                await msg.ack()
                
                queue = self._queue_channels.get(key)
                if queue:
                    try:
                        queue.put_nowait(True)
                    except asyncio.QueueFull:
                        pass
                        
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.error(f"Error consuming NATS JetStream for tenant {tenant_id} queue {queue_name}: {e}")

    async def close(self) -> None:
        for task in self._tasks:
            task.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)


class NatsDatabaseDriverDecorator(DatabaseDriver):
    def __init__(self, inner: DatabaseDriver, js: JetStreamContext):
        self._inner = inner
        self._js = js
        self._listener: Optional[NatsQueueSignalListener] = None

    async def emit_event(self, p_tenant_id: str, p_queue_name: str, p_event_name: str, p_payload: Any) -> None:
        await self._inner.emit_event(p_tenant_id, p_queue_name, p_event_name, p_payload)

        try:
            subject = f"ssf.tenants.{p_tenant_id}.queues.{p_queue_name}"
            await self._js.publish(subject, b"ping")
        except Exception as e:
            logger.warning(f"Failed to publish NATS wake-up signal for queue '{p_queue_name}' of tenant '{p_tenant_id}': {e}")

    async def create_queue_signal_listener(self) -> QueueSignalListener:
        if not self._listener:
            self._listener = NatsQueueSignalListener(self._js)
        return self._listener

    async def create_queue(self, p_tenant_id: str, p_queue_name: str, p_storage_mode: str) -> None:
        return await self._inner.create_queue(p_tenant_id, p_queue_name, p_storage_mode)

    async def spawn_task(self, p_tenant_id: str, p_queue_name: str, p_task_name: str, p_params: Any, p_options: Any) -> SpawnResult:
        return await self._inner.spawn_task(p_tenant_id, p_queue_name, p_task_name, p_params, p_options)

    async def claim_task(self, p_tenant_id: str, p_queue_name: str, p_worker_id: str, p_claim_timeout: int, p_qty: int) -> List[Dict[str, Any]]:
        return await self._inner.claim_task(p_tenant_id, p_queue_name, p_worker_id, p_claim_timeout, p_qty)

    async def complete_run(self, p_tenant_id: str, p_queue_name: str, p_run_id: UUID, p_state: Any) -> None:
        return await self._inner.complete_run(p_tenant_id, p_queue_name, p_run_id, p_state)

    async def schedule_run(self, p_tenant_id: str, p_queue_name: str, p_run_id: UUID, p_wake_at: datetime) -> None:
        return await self._inner.schedule_run(p_tenant_id, p_queue_name, p_run_id, p_wake_at)

    async def fail_run(self, p_tenant_id: str, p_queue_name: str, p_run_id: UUID, p_reason: Any, p_retry_at: datetime) -> None:
        return await self._inner.fail_run(p_tenant_id, p_queue_name, p_run_id, p_reason, p_retry_at)

    async def set_task_checkpoint_state(self, p_tenant_id: str, p_queue_name: str, p_task_id: UUID, p_step_name: str, p_state: Any, p_owner_run: UUID, p_extend_claim_by: int) -> None:
        return await self._inner.set_task_checkpoint_state(p_tenant_id, p_queue_name, p_task_id, p_step_name, p_state, p_owner_run, p_extend_claim_by)

    async def get_task_checkpoint_state(self, p_tenant_id: str, p_queue_name: str, p_task_id: UUID, p_step_name: str, p_include_pending: int) -> Optional[Dict[str, Any]]:
        return await self._inner.get_task_checkpoint_state(p_tenant_id, p_queue_name, p_task_id, p_step_name, p_include_pending)

    async def await_event(self, p_tenant_id: str, p_queue_name: str, p_task_id: UUID, p_run_id: UUID, p_step_name: str, p_event_name: str, p_timeout: Optional[int]) -> Dict[str, Any]:
        return await self._inner.await_event(p_tenant_id, p_queue_name, p_task_id, p_run_id, p_step_name, p_event_name, p_timeout)

    async def cancel_task(self, p_tenant_id: str, p_queue_name: str, p_task_id: UUID) -> None:
        return await self._inner.cancel_task(p_tenant_id, p_queue_name, p_task_id)

    async def get_next_available_at(self, p_tenant_id: str, p_queue_name: str) -> Optional[datetime]:
        return await self._inner.get_next_available_at(p_tenant_id, p_queue_name)