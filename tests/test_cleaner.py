import asyncio
import contextlib
import datetime as dt
import logging
import typing
import uuid
from collections.abc import AsyncGenerator, AsyncIterator

import pytest
from sqlalchemy import MetaData, Table, event, func, insert, select, text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine, AsyncSession, async_sessionmaker
from sqlalchemy.pool import QueuePool

from sqlalchemy_inbox import CleanupResult, Inbox, InboxCleaner, make_inbox_table
from tests.conftest import Tables, wait_until_lock_wait


HOUR = dt.timedelta(hours=1)
_ADVISORY_KEY = 7_331


class _BatchError(Exception):
    pass


async def _add_markers(engine: AsyncEngine, table: Table, *message_ids: str, age: dt.timedelta = 2 * HOUR) -> None:
    async with engine.begin() as conn:
        for message_id in message_ids:
            await conn.execute(insert(table).values(scope="s", message_id=message_id, processed_at=func.now() - age))


async def _message_ids(engine: AsyncEngine, table: Table) -> list[str]:
    async with engine.connect() as conn:
        return list((await conn.execute(select(table.c.message_id).order_by(table.c.message_id))).scalars())


def _cleaner(engine: AsyncEngine, table: Table, *, batch_size: int = 100) -> InboxCleaner:
    return InboxCleaner(engine=engine, table=table, retention=HOUR, batch_size=batch_size)


@contextlib.asynccontextmanager
async def _delete_trigger(engine: AsyncEngine, table: Table, body: str) -> AsyncGenerator[None]:
    function = f"{table.name}_on_delete"
    async with engine.begin() as conn:
        await conn.execute(
            text(
                f"CREATE FUNCTION {function}() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN {body} RETURN NULL; END $$"
            )
        )
        await conn.execute(
            text(f"CREATE TRIGGER {table.name}_on_delete AFTER DELETE ON {table.name} EXECUTE FUNCTION {function}()")
        )
    yield
    async with engine.begin() as conn:
        await conn.execute(text(f"DROP FUNCTION {function}() CASCADE"))


@pytest.fixture
async def delete_gate(pg_engine: AsyncEngine, tables: Tables) -> AsyncIterator[AsyncConnection]:
    """Make every cleanup batch wait after its delete until the test unlocks the yielded connection's lock."""
    async with (
        _delete_trigger(pg_engine, tables.inbox, f"PERFORM pg_advisory_xact_lock_shared({_ADVISORY_KEY});"),
        pg_engine.connect() as holder,
    ):
        await holder.execute(select(func.pg_advisory_lock(_ADVISORY_KEY)))
        try:
            yield holder
        finally:
            await holder.execute(select(func.pg_advisory_unlock_all()))


async def _open_gate(holder: AsyncConnection) -> None:
    await holder.execute(select(func.pg_advisory_unlock(_ADVISORY_KEY)))


async def _wait_for_blocked_delete(engine: AsyncEngine) -> None:
    async with engine.connect() as conn:
        for _ in range(200):
            blocked = (
                await conn.execute(
                    text(
                        "SELECT count(*) FROM pg_stat_activity WHERE wait_event_type = 'Lock' "
                        "AND query LIKE 'DELETE%' AND pid <> pg_backend_pid()"
                    )
                )
            ).scalar_one()
            await conn.commit()
            if blocked:
                return
            await asyncio.sleep(0.025)  # pragma: no cover - only when the first poll runs before the lock wait
    msg = "the cleanup batch never waited on the advisory lock"  # pragma: no cover - only when nothing blocked
    raise AssertionError(msg)  # pragma: no cover - only when nothing blocked


async def test_cleanup_deletes_only_markers_older_than_the_cutoff(pg_engine: AsyncEngine, tables: Tables) -> None:
    await _add_markers(pg_engine, tables.inbox, "old-1", "old-2")
    await _add_markers(pg_engine, tables.inbox, "recent", age=dt.timedelta(minutes=10))

    result = await _cleaner(pg_engine, tables.inbox).cleanup()

    assert result.deleted_count == 2
    assert await _message_ids(pg_engine, tables.inbox) == ["recent"]


async def test_cutoff_comes_from_the_database_clock(pg_engine: AsyncEngine, tables: Tables) -> None:
    async with pg_engine.connect() as conn:
        before = (await conn.execute(select(func.now()))).scalar_one()

    result = await _cleaner(pg_engine, tables.inbox).cleanup()

    async with pg_engine.connect() as conn:
        after = (await conn.execute(select(func.now()))).scalar_one()
    assert before - HOUR <= result.cutoff <= after - HOUR


async def test_cleanup_deletes_in_batches_until_one_deletes_nothing(
    pg_engine: AsyncEngine, tables: Tables, caplog: pytest.LogCaptureFixture
) -> None:
    await _add_markers(pg_engine, tables.inbox, *(f"m-{index}" for index in range(5)))

    with caplog.at_level(logging.DEBUG, logger="sqlalchemy_inbox"):
        result = await _cleaner(pg_engine, tables.inbox, batch_size=2).cleanup()

    assert result == CleanupResult(cutoff=result.cutoff, deleted_count=5)
    assert await _message_ids(pg_engine, tables.inbox) == []
    levels = [record.levelno for record in caplog.records if record.name.startswith("sqlalchemy_inbox")]
    assert levels == [logging.DEBUG] * 3 + [logging.INFO]


async def test_every_batch_uses_the_cutoff_taken_at_the_start(pg_engine: AsyncEngine, tables: Tables) -> None:
    await _add_markers(pg_engine, tables.inbox, "m-1", "m-2", "m-3")
    cutoffs: list[dt.datetime] = []

    @event.listens_for(pg_engine.sync_engine, "before_cursor_execute")
    def record_cutoff(
        _conn: object, _cursor: object, statement: str, parameters: tuple[object, ...], *_: object
    ) -> None:
        if statement.startswith("DELETE"):
            cutoffs.extend(value for value in parameters if isinstance(value, dt.datetime))

    result = await _cleaner(pg_engine, tables.inbox, batch_size=1).cleanup()

    assert cutoffs == [result.cutoff] * 4


async def test_committed_batches_survive_a_failed_batch(pg_engine: AsyncEngine, tables: Tables) -> None:
    await _add_markers(pg_engine, tables.inbox, "m-1", "m-2", "m-3", "m-4")
    deletes = 0

    @event.listens_for(pg_engine.sync_engine, "before_cursor_execute")
    def fail_second_delete(_conn: object, _cursor: object, statement: str, *_: object) -> None:
        nonlocal deletes
        if statement.startswith("DELETE"):
            deletes += 1
            if deletes == 2:
                raise _BatchError

    with pytest.raises(_BatchError):
        await _cleaner(pg_engine, tables.inbox, batch_size=2).cleanup()

    assert len(await _message_ids(pg_engine, tables.inbox)) == 2


async def test_cancelled_cleanup_rolls_back_the_batch_in_progress(
    pg_engine: AsyncEngine, tables: Tables, delete_gate: AsyncConnection
) -> None:
    await _add_markers(pg_engine, tables.inbox, "m-1", "m-2")
    cleanup = asyncio.create_task(_cleaner(pg_engine, tables.inbox).cleanup())
    await _wait_for_blocked_delete(pg_engine)

    cleanup.cancel()
    with pytest.raises(asyncio.CancelledError):
        await cleanup
    await _open_gate(delete_gate)

    assert isinstance(pg_engine.pool, QueuePool)
    assert pg_engine.pool.checkedout() == 1
    assert await _message_ids(pg_engine, tables.inbox) == ["m-1", "m-2"]


async def test_cleanup_skips_markers_locked_by_another_transaction(
    pg_engine: AsyncEngine, tables: Tables, session_maker: async_sessionmaker[AsyncSession]
) -> None:
    await _add_markers(pg_engine, tables.inbox, "locked", "free")

    async with session_maker() as holder, holder.begin():
        await holder.execute(select(tables.inbox).where(tables.inbox.c.message_id == "locked").with_for_update())
        result = await asyncio.wait_for(_cleaner(pg_engine, tables.inbox).cleanup(), timeout=5)

    assert result.deleted_count == 1
    assert await _message_ids(pg_engine, tables.inbox) == ["locked"]


async def test_concurrent_runs_delete_every_marker_once(pg_engine: AsyncEngine, tables: Tables) -> None:
    await _add_markers(pg_engine, tables.inbox, *(f"m-{index:02}" for index in range(20)))

    results = await asyncio.gather(*(_cleaner(pg_engine, tables.inbox, batch_size=1).cleanup() for _ in range(3)))

    assert sum(result.deleted_count for result in results) == 20
    assert await _message_ids(pg_engine, tables.inbox) == []


async def test_cleanup_runs_at_read_committed_whatever_the_engine_default(
    pg_engine: AsyncEngine, tables: Tables
) -> None:
    await _add_markers(pg_engine, tables.inbox, "m-1")
    async with _delete_trigger(
        pg_engine,
        tables.inbox,
        "IF current_setting('transaction_isolation') <> 'read committed' THEN RAISE EXCEPTION 'isolation'; END IF;",
    ):
        repeatable_read = pg_engine.execution_options(isolation_level="REPEATABLE READ")
        result = await _cleaner(repeatable_read, tables.inbox).cleanup()

    assert result.deleted_count == 1


async def test_an_expired_message_is_processed_again(
    pg_engine: AsyncEngine, tables: Tables, session_maker: async_sessionmaker[AsyncSession]
) -> None:
    await _add_markers(pg_engine, tables.inbox, "m-1")
    async with session_maker() as session, session.begin():
        assert not await Inbox(session, tables.inbox).claim(scope="s", message_id="m-1")

    await _cleaner(pg_engine, tables.inbox).cleanup()

    async with session_maker() as session, session.begin():
        assert await Inbox(session, tables.inbox).claim(scope="s", message_id="m-1")


async def test_claim_during_uncommitted_cleanup_waits_then_claims(
    pg_engine: AsyncEngine,
    tables: Tables,
    session_maker: async_sessionmaker[AsyncSession],
    delete_gate: AsyncConnection,
) -> None:
    await _add_markers(pg_engine, tables.inbox, "m-1")
    cleanup = asyncio.create_task(_cleaner(pg_engine, tables.inbox).cleanup())
    await _wait_for_blocked_delete(pg_engine)

    async with session_maker() as consumer, consumer.begin():
        pid = (await consumer.execute(text("SELECT pg_backend_pid()"))).scalar_one()
        competing = asyncio.create_task(Inbox(consumer, tables.inbox).claim(scope="s", message_id="m-1"))
        await wait_until_lock_wait(pg_engine, pid)
        assert not competing.done()

        await _open_gate(delete_gate)
        assert (await cleanup).deleted_count == 1
        assert await competing

    assert await _message_ids(pg_engine, tables.inbox) == ["m-1"]


async def test_cleanup_errors_reach_the_caller(pg_engine: AsyncEngine) -> None:
    missing = make_inbox_table(MetaData(), table_name=f"missing_{uuid.uuid4().hex[:12]}")

    with pytest.raises(DBAPIError):
        await _cleaner(pg_engine, missing).cleanup()


@pytest.mark.parametrize(
    ("overrides", "error", "match"),
    [
        ({"retention": dt.timedelta(0)}, ValueError, "retention"),
        ({"batch_size": 0}, ValueError, "batch_size"),
        ({"table": Table("t" * 50, MetaData())}, ValueError, "too long"),
        ({"engine": object()}, TypeError, "AsyncEngine"),
    ],
)
async def test_constructor_rejects_invalid_arguments(
    pg_engine: AsyncEngine, tables: Tables, overrides: dict[str, typing.Any], error: type[Exception], match: str
) -> None:
    arguments: dict[str, typing.Any] = {"engine": pg_engine, "table": tables.inbox, "retention": HOUR}

    with pytest.raises(error, match=match):
        InboxCleaner(**(arguments | overrides))


async def test_constructor_takes_only_named_arguments(pg_engine: AsyncEngine, tables: Tables) -> None:
    with pytest.raises(TypeError):
        InboxCleaner(pg_engine, tables.inbox, HOUR)  # ty: ignore[missing-argument, too-many-positional-arguments]
