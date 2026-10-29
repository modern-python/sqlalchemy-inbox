import contextlib
import dataclasses
import datetime as dt
import logging
from collections.abc import AsyncGenerator

from sqlalchemy import Table, delete, func, select, tuple_
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from sqlalchemy_inbox.schema import validate_table_identifiers


_LOGGER = logging.getLogger(__name__)


@dataclasses.dataclass(kw_only=True, frozen=True, slots=True)
class CleanupResult:
    cutoff: dt.datetime
    deleted_count: int


class InboxCleaner:
    """Deletes markers older than *retention*, one transaction per batch.

    Opens its own connections from *engine* but never disposes it: the caller owns the engine.
    """

    def __init__(self, *, engine: AsyncEngine, table: Table, retention: dt.timedelta, batch_size: int = 1000) -> None:
        if not isinstance(engine, AsyncEngine):
            msg = "InboxCleaner requires an sqlalchemy.ext.asyncio.AsyncEngine"
            raise TypeError(msg)
        validate_table_identifiers(table.name)
        if retention <= dt.timedelta(0):
            msg = f"retention must be positive (got {retention})"
            raise ValueError(msg)
        if batch_size < 1:
            msg = f"batch_size must be positive (got {batch_size})"
            raise ValueError(msg)
        self._engine = engine
        self._table = table
        self._retention = retention
        self._batch_size = batch_size

    async def cleanup(self) -> CleanupResult:
        """Take one cutoff from the database clock, then delete the markers older than it until a batch deletes nothing.

        Each batch commits on its own and skips markers other transactions hold. If a batch fails or the task
        is cancelled, that batch rolls back, earlier batches stay deleted, and the error propagates.
        """
        async with self._transaction() as connection:
            cutoff = (await connection.execute(select(func.now() - self._retention))).scalar_one()
        deleted_count = 0
        while deleted := await self._delete_batch(cutoff):
            deleted_count += deleted
            _LOGGER.debug("Inbox cleanup batch deleted %d markers (%d so far)", deleted, deleted_count)
        _LOGGER.info("Inbox cleanup deleted %d markers older than %s", deleted_count, cutoff.isoformat())
        return CleanupResult(cutoff=cutoff, deleted_count=deleted_count)

    async def _delete_batch(self, cutoff: dt.datetime) -> int:
        key = tuple_(self._table.c.scope, self._table.c.message_id)
        expired = (
            select(self._table.c.scope, self._table.c.message_id)
            .where(self._table.c.processed_at < cutoff)
            .limit(self._batch_size)
            .with_for_update(skip_locked=True)
        )
        async with self._transaction() as connection:
            result = await connection.execute(
                delete(self._table).where(key.in_(expired)).returning(self._table.c.scope)
            )
            return len(result.all())

    @contextlib.asynccontextmanager
    async def _transaction(self) -> AsyncGenerator[AsyncConnection]:
        async with self._engine.connect() as connection:
            await connection.execution_options(isolation_level="READ COMMITTED")
            async with connection.begin():
                yield connection
