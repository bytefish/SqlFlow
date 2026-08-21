import json
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional
from uuid import UUID

try:
    import aioodbc
except ImportError:
    raise ImportError("The 'aioodbc' package is required for SQL Server support. Install with: pip install sqlflow-sdk[sqlserver]")

from sqlflow.sdk import DatabaseDriver, SpawnResult

logger = logging.getLogger("sqlflow.sqlserver")

class SqlServerDriver(DatabaseDriver):
    """
    SQL Server implementation of the SqlFlow DatabaseDriver using aioodbc.
    Maps exactly to the 'ssf' schema stored procedures and functions.
    Note: ODBC parameter binding uses '?' exclusively.
    """
    
    def __init__(self, dsn: str):
        """
        Initializes the SQL Server driver.
        :param dsn: The ODBC connection string (e.g., 'Driver={ODBC Driver 17 for SQL Server};Server=localhost;Database=mydb;UID=user;PWD=pass;')
        """
        self.dsn = dsn
        self._pool: Optional[aioodbc.Pool] = None

    async def connect(self) -> None:
        """Initializes the async ODBC connection pool."""
        if not self._pool:
            self._pool = await aioodbc.create_pool(dsn=self.dsn, autocommit=True)
            logger.info("SQL Server connection pool created.")

    async def disconnect(self) -> None:
        """Closes the connection pool."""
        if self._pool:
            self._pool.close()
            await self._pool.wait_closed()
            self._pool = None
            logger.info("SQL Server connection pool closed.")

    def _ensure_pool(self) -> aioodbc.Pool:
        if not self._pool:
            raise RuntimeError("Database connection pool is not initialized. Call connect() first.")
        return self._pool

    async def create_queue(self, p_queue_name: str, p_storage_mode: str) -> None:
        pool = self._ensure_pool()
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "{CALL ssf.create_queue (?, ?)}", 
                    (p_queue_name, p_storage_mode)
                )

    async def spawn_task(self, p_queue_name: str, p_task_name: str, p_params: Any, p_options: Any) -> SpawnResult:
        pool = self._ensure_pool()
        params_str = p_params if isinstance(p_params, str) else json.dumps(p_params)
        options_str = p_options if isinstance(p_options, str) else json.dumps(p_options)
        
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "SELECT task_id, run_id, attempt, created FROM ssf.spawn_task(?, ?, ?, ?)",
                    (p_queue_name, p_task_name, params_str, options_str)
                )
                row = await cur.fetchone()
                
                if not row:
                    raise RuntimeError("Failed to spawn task: Database returned no result.")
                    
                # aioodbc returns pyodbc.Row, access by index or column name
                return SpawnResult(
                    task_id=row.task_id,
                    run_id=row.run_id,
                    attempt=row.attempt,
                    created=bool(row.created)
                )

    async def claim_task(self, p_queue_name: str, p_worker_id: str, p_claim_timeout: int, p_qty: int) -> List[Dict[str, Any]]:
        pool = self._ensure_pool()
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "SELECT * FROM ssf.claim_task(?, ?, ?, ?)",
                    (p_queue_name, p_worker_id, p_claim_timeout, p_qty)
                )
                
                rows = await cur.fetchall()
                if not rows:
                    return []
                    
                columns = [column[0] for column in cur.description]
                return [dict(zip(columns, row)) for row in rows]

    async def complete_run(self, p_queue_name: str, p_run_id: UUID, p_state: str) -> None:
        pool = self._ensure_pool()
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "{CALL ssf.complete_run (?, ?, ?)}",
                    (p_queue_name, str(p_run_id), p_state)
                )

    async def schedule_run(self, p_queue_name: str, p_run_id: UUID, p_wake_at: datetime) -> None:
        pool = self._ensure_pool()
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "{CALL ssf.schedule_run (?, ?, ?)}",
                    (p_queue_name, str(p_run_id), p_wake_at)
                )

    async def fail_run(self, p_queue_name: str, p_run_id: UUID, p_reason: Any, p_retry_at: datetime) -> None:
        pool = self._ensure_pool()
        reason_str = p_reason if isinstance(p_reason, str) else json.dumps(p_reason)
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "{CALL ssf.fail_run (?, ?, ?, ?)}",
                    (p_queue_name, str(p_run_id), reason_str, p_retry_at)
                )

    async def set_task_checkpoint_state(self, p_queue_name: str, p_task_id: UUID, p_step_name: str, p_state: str, p_owner_run: UUID, p_extend_claim_by: int) -> None:
        pool = self._ensure_pool()
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "{CALL ssf.set_task_checkpoint_state (?, ?, ?, ?, ?, ?)}",
                    (p_queue_name, str(p_task_id), p_step_name, p_state, str(p_owner_run), p_extend_claim_by)
                )

    async def get_task_checkpoint_state(self, p_queue_name: str, p_task_id: UUID, p_step_name: str, p_include_pending: int) -> Optional[Dict[str, Any]]:
        pool = self._ensure_pool()
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "SELECT * FROM ssf.get_task_checkpoint_state(?, ?, ?, ?)",
                    (p_queue_name, str(p_task_id), p_step_name, p_include_pending)
                )
                row = await cur.fetchone()
                if not row:
                    return None
                
                columns = [column[0] for column in cur.description]
                return dict(zip(columns, row))

    async def await_event(self, p_queue_name: str, p_task_id: UUID, p_run_id: UUID, p_step_name: str, p_event_name: str, p_timeout: Optional[int]) -> Dict[str, Any]:
        pool = self._ensure_pool()
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "SELECT * FROM ssf.await_event(?, ?, ?, ?, ?, ?)",
                    (p_queue_name, str(p_task_id), str(p_run_id), p_step_name, p_event_name, p_timeout)
                )
                row = await cur.fetchone()
                if not row:
                    raise RuntimeError("Database error: await_event returned no state.")
                
                columns = [column[0] for column in cur.description]
                return dict(zip(columns, row))

    async def emit_event(self, p_queue_name: str, p_event_name: str, p_payload: str) -> None:
        pool = self._ensure_pool()
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "{CALL ssf.emit_event (?, ?, ?)}",
                    (p_queue_name, p_event_name, p_payload)
                )

    async def cancel_task(self, p_queue_name: str, p_task_id: UUID) -> None:
        pool = self._ensure_pool()
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "{CALL ssf.cancel_task (?, ?)}",
                    (p_queue_name, str(p_task_id))
                )