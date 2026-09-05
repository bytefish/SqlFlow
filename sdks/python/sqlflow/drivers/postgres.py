import json
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional
from uuid import UUID

try:
    import asyncpg
except ImportError:
    raise ImportError("The 'asyncpg' package is required for PostgreSQL support. Install with: pip install sqlflow-sdk[postgres]")

from sqlflow.sdk import DatabaseDriver, SpawnResult

logger = logging.getLogger("sqlflow.postgres")

async def _init_connection(conn):
    await conn.set_type_codec(
        "jsonb",
        schema="pg_catalog",
        encoder=json.dumps,
        decoder=json.loads,
        format="text"
    )


import asyncio
import asyncpg

class PostgresQueueSignalListener(
    QueueSignalListener
):
    def __init__(self, connection_string: str):
        self._connection_string = (
            connection_string
        )

        self._connection = None

        self._queues = {}

    async def start(self):
        self._connection = (
            await asyncpg.connect(
                self._connection_string
            )
        )

    async def register_queue(
        self,
        queue_name: str
    ) -> None:
        if queue_name in self._queues:
            return

        event = asyncio.Event()

        self._queues[queue_name] = event

        async def callback(
            connection,
            pid,
            channel,
            payload
        ):
            event.set()

        await self._connection.add_listener(
            f"ssf_{queue_name}",
            callback
        )

    async def wait_for_signal(
        self,
        queue_name: str,
        timeout_seconds: float
    ) -> bool:
        event = self._queues[queue_name]

        try:
            await asyncio.wait_for(
                event.wait(),
                timeout_seconds
            )

            event.clear()

            return True

        except asyncio.TimeoutError:
            return False

class PostgresDriver(DatabaseDriver):
    """
    PostgreSQL implementation of the SqlFlow DatabaseDriver using asyncpg.
    Maps exactly to the 'ssf' schema stored procedures and functions.
    """
    
    def __init__(self, dsn: str):
        """
        Initializes the Postgres driver.
        :param dsn: The connection string (e.g., postgresql://user:pass@localhost:5432/mydb)
        """
        self.dsn = dsn
        self._pool: Optional[asyncpg.Pool] = None

    async def connect(self) -> None:
        """Initializes the async connection pool."""
        if not self._pool:
            self._pool = await asyncpg.create_pool(self.dsn, init=_init_connection)
            logger.info("PostgreSQL connection pool created.")

    async def disconnect(self) -> None:
        """Closes the connection pool."""
        if self._pool:
            await self._pool.close()
            self._pool = None
            logger.info("PostgreSQL connection pool closed.")

    def _ensure_pool(self) -> asyncpg.Pool:
        if not self._pool:
            raise RuntimeError("Database connection pool is not initialized. Call connect() first.")
        return self._pool

    async def create_queue(self, p_queue_name: str, p_storage_mode: str) -> None:
        pool = self._ensure_pool()
        await pool.execute(
            "CALL ssf.create_queue($1, $2)", 
            p_queue_name, p_storage_mode
        )

    async def spawn_task(self, p_queue_name: str, p_task_name: str, p_params: Any, p_options: Any) -> SpawnResult:
        pool = self._ensure_pool()

        row = await pool.fetchrow(
            """
            SELECT task_id, run_id, attempt, created 
            FROM ssf.spawn_task($1, $2, $3::jsonb, $4::jsonb)
            """,
            p_queue_name, p_task_name, p_params, p_options
        )
        
        if not row:
            raise RuntimeError("Failed to spawn task: Database returned no result.")
            
        return SpawnResult(
            task_id=row['task_id'],
            run_id=row['run_id'],
            attempt=row['attempt'],
            created=row['created']
        )

    async def claim_task(self, p_queue_name: str, p_worker_id: str, p_claim_timeout: int, p_qty: int) -> List[Dict[str, Any]]:
        pool = self._ensure_pool()
        rows = await pool.fetch(
            "SELECT * FROM ssf.claim_task($1, $2, $3, $4)",
            p_queue_name, p_worker_id, p_claim_timeout, p_qty
        )
        return [dict(row) for row in rows]

    async def complete_run(self, p_queue_name: str, p_run_id: UUID, p_state: Any) -> None:
        pool = self._ensure_pool()
        await pool.execute(
            "CALL ssf.complete_run($1, $2, $3::jsonb)",
            p_queue_name, p_run_id, p_state
        )

    async def schedule_run(self, p_queue_name: str, p_run_id: UUID, p_wake_at: datetime) -> None:
        pool = self._ensure_pool()
        await pool.execute(
            "CALL ssf.schedule_run($1, $2, $3)",
            p_queue_name, p_run_id, p_wake_at
        )

    async def fail_run(self, p_queue_name: str, p_run_id: UUID, p_reason: Any, p_retry_at: datetime) -> None:
        pool = self._ensure_pool()
        await pool.execute(
            "CALL ssf.fail_run($1, $2, $3::jsonb, $4)",
            p_queue_name, p_run_id, p_reason, p_retry_at
        )

    async def set_task_checkpoint_state(self, p_queue_name: str, p_task_id: UUID, p_step_name: str, p_state: Any, p_owner_run: UUID, p_extend_claim_by: int) -> None:
        pool = self._ensure_pool()
        await pool.execute(
            "CALL ssf.set_task_checkpoint_state($1, $2, $3, $4, $5, $6)",
            p_queue_name, p_task_id, p_step_name, p_state, p_owner_run, p_extend_claim_by
        )

    async def get_task_checkpoint_state(self, p_queue_name: str, p_task_id: UUID, p_step_name: str, p_include_pending: int) -> Optional[Dict[str, Any]]:
        pool = self._ensure_pool()
        row = await pool.fetchrow(
            "SELECT * FROM ssf.get_task_checkpoint_state($1, $2, $3, $4)",
            p_queue_name, p_task_id, p_step_name, p_include_pending
        )
        return dict(row) if row else None

    async def await_event(self, p_queue_name: str, p_task_id: UUID, p_run_id: UUID, p_step_name: str, p_event_name: str, p_timeout: Optional[int]) -> Dict[str, Any]:
        pool = self._ensure_pool()
        row = await pool.fetchrow(
            "SELECT * FROM ssf.await_event($1, $2, $3, $4, $5, $6)",
            p_queue_name, p_task_id, p_run_id, p_step_name, p_event_name, p_timeout
        )
        if not row:
            raise RuntimeError("Database error: await_event returned no state.")
        return dict(row)

    async def emit_event(self, p_queue_name: str, p_event_name: str, p_payload: Any) -> None:
        pool = self._ensure_pool()           
        await pool.execute(
            "CALL ssf.emit_event($1, $2, $3::jsonb)",
            p_queue_name, p_event_name, p_payload
        )

    async def cancel_task(self, p_queue_name: str, p_task_id: UUID) -> None:
        pool = self._ensure_pool()
        await pool.execute(
            "CALL ssf.cancel_task($1, $2)",
            p_queue_name, p_task_id
        )
            
