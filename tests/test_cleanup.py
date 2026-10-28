import asyncio
import datetime as dt
import uuid

import pytest
from sqlalchemy import MetaData, Table, func, insert, select, text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from sqlalchemy_inbox import Inbox, make_inbox_table
from tests.conftest import Tables, wait_until_lock_wait


HOUR = dt.timedelta(hours=1)


class _BatchError(Exception):
    pass


async def _add_marker(engine: AsyncEngine, table: Table, message_id: str, age: dt.timedelta) -> None:
    async with engine.begin() as conn:
        await conn.execute(insert(table).values(scope="s", message_id=message_id, processed_at=func.now() - age))


async def _message_ids(engine: AsyncEngine, table: Table) -> list[str]:
    async with engine.connect() as conn:
        return list((await conn.execute(select(table.c.message_id).order_by(table.c.message_id))).scalars())


async def _cutoff(
    session_maker: async_sessionmaker[AsyncSession], table: Table, retention: dt.timedelta
) -> dt.datetime:
    async with session_maker() as session, session.begin():
        return await Inbox(session, table).cleanup_cutoff(retention=retention)


async def _delete_batch(
    session_maker: async_sessionmaker[AsyncSession], table: Table, cutoff: dt.datetime, limit: int
) -> int:
    async with session_maker() as session, session.begin():
        return await Inbox(session, table).delete_expired_batch(cutoff=cutoff, limit=limit)


async def test_cutoff_comes_from_the_database_clock(
    tables: Tables, session_maker: async_sessionmaker[AsyncSession]
) -> None:
    async with session_maker() as session, session.begin():
        cutoff = await Inbox(session, tables.inbox).cleanup_cutoff(retention=HOUR)
        database_now = (await session.execute(select(func.now()))).scalar_one()

    assert cutoff == database_now - HOUR


async def test_only_markers_older_than_the_cutoff_are_deleted(
    pg_engine: AsyncEngine, tables: Tables, session_maker: async_sessionmaker[AsyncSession]
) -> None:
    await _add_marker(pg_engine, tables.inbox, "old", 2 * HOUR)
    await _add_marker(pg_engine, tables.inbox, "recent", dt.timedelta(minutes=10))
    cutoff = await _cutoff(session_maker, tables.inbox, HOUR)

    assert await _delete_batch(session_maker, tables.inbox, cutoff, 100) == 1
    assert await _message_ids(pg_engine, tables.inbox) == ["recent"]


async def test_each_batch_respects_its_limit(
    pg_engine: AsyncEngine, tables: Tables, session_maker: async_sessionmaker[AsyncSession]
) -> None:
    for index in range(5):
        await _add_marker(pg_engine, tables.inbox, f"m-{index}", 2 * HOUR)
    cutoff = await _cutoff(session_maker, tables.inbox, HOUR)

    deleted = [await _delete_batch(session_maker, tables.inbox, cutoff, 2) for _ in range(4)]

    assert deleted == [2, 2, 1, 0]


async def test_a_run_keeps_its_cutoff_for_markers_added_later(
    pg_engine: AsyncEngine, tables: Tables, session_maker: async_sessionmaker[AsyncSession]
) -> None:
    await _add_marker(pg_engine, tables.inbox, "old", 2 * HOUR)
    cutoff = await _cutoff(session_maker, tables.inbox, HOUR)
    async with pg_engine.begin() as conn:
        await conn.execute(
            insert(tables.inbox).values(
                scope="s", message_id="after-cutoff", processed_at=cutoff + dt.timedelta(seconds=1)
            )
        )

    assert await _delete_batch(session_maker, tables.inbox, cutoff, 100) == 1
    assert await _message_ids(pg_engine, tables.inbox) == ["after-cutoff"]


async def test_committed_batches_survive_a_failed_batch(
    pg_engine: AsyncEngine, tables: Tables, session_maker: async_sessionmaker[AsyncSession]
) -> None:
    for index in range(4):
        await _add_marker(pg_engine, tables.inbox, f"m-{index}", 2 * HOUR)
    cutoff = await _cutoff(session_maker, tables.inbox, HOUR)

    assert await _delete_batch(session_maker, tables.inbox, cutoff, 2) == 2

    async def fail_after_delete(session: AsyncSession) -> None:
        async with session.begin():
            assert await Inbox(session, tables.inbox).delete_expired_batch(cutoff=cutoff, limit=2) == 2
            raise _BatchError

    async with session_maker() as session:
        with pytest.raises(_BatchError):
            await fail_after_delete(session)

    assert len(await _message_ids(pg_engine, tables.inbox)) == 2
    assert await _delete_batch(session_maker, tables.inbox, cutoff, 2) == 2
    assert await _message_ids(pg_engine, tables.inbox) == []


async def test_cleanup_skips_markers_locked_by_another_transaction(
    pg_engine: AsyncEngine, tables: Tables, session_maker: async_sessionmaker[AsyncSession]
) -> None:
    await _add_marker(pg_engine, tables.inbox, "locked", 2 * HOUR)
    await _add_marker(pg_engine, tables.inbox, "free", 2 * HOUR)
    cutoff = await _cutoff(session_maker, tables.inbox, HOUR)

    async with session_maker() as holder, holder.begin():
        await holder.execute(select(tables.inbox).where(tables.inbox.c.message_id == "locked").with_for_update())
        deleted = await asyncio.wait_for(_delete_batch(session_maker, tables.inbox, cutoff, 100), timeout=5)

    assert deleted == 1
    assert await _message_ids(pg_engine, tables.inbox) == ["locked"]


async def test_cleanup_errors_reach_the_caller(
    session_maker: async_sessionmaker[AsyncSession],
) -> None:
    missing = make_inbox_table(MetaData(), table_name=f"missing_{uuid.uuid4().hex[:12]}")
    async with session_maker() as session:
        with pytest.raises(DBAPIError):
            async with session.begin():
                await Inbox(session, missing).delete_expired_batch(cutoff=dt.datetime.now(tz=dt.UTC), limit=10)


async def test_an_expired_message_is_processed_again(
    pg_engine: AsyncEngine, tables: Tables, session_maker: async_sessionmaker[AsyncSession]
) -> None:
    await _add_marker(pg_engine, tables.inbox, "m-1", 2 * HOUR)
    async with session_maker() as session, session.begin():
        assert not await Inbox(session, tables.inbox).claim(scope="s", message_id="m-1")

    cutoff = await _cutoff(session_maker, tables.inbox, HOUR)
    assert await _delete_batch(session_maker, tables.inbox, cutoff, 100) == 1

    async with session_maker() as session, session.begin():
        assert await Inbox(session, tables.inbox).claim(scope="s", message_id="m-1")


async def test_claim_during_uncommitted_cleanup_waits_then_claims(
    pg_engine: AsyncEngine, tables: Tables, session_maker: async_sessionmaker[AsyncSession]
) -> None:
    await _add_marker(pg_engine, tables.inbox, "m-1", 2 * HOUR)
    cutoff = await _cutoff(session_maker, tables.inbox, HOUR)

    async with session_maker() as cleaner, session_maker() as consumer:
        await cleaner.begin()
        assert await Inbox(cleaner, tables.inbox).delete_expired_batch(cutoff=cutoff, limit=100) == 1

        await consumer.begin()
        pid = (await consumer.execute(text("SELECT pg_backend_pid()"))).scalar_one()
        competing = asyncio.create_task(Inbox(consumer, tables.inbox).claim(scope="s", message_id="m-1"))
        await wait_until_lock_wait(pg_engine, pid)
        assert not competing.done()

        await cleaner.commit()
        assert await competing
        await consumer.commit()

    assert await _message_ids(pg_engine, tables.inbox) == ["m-1"]


async def test_cutoff_rejects_a_non_positive_retention(
    tables: Tables, session_maker: async_sessionmaker[AsyncSession]
) -> None:
    async with session_maker() as session, session.begin():
        with pytest.raises(ValueError, match="retention"):
            await Inbox(session, tables.inbox).cleanup_cutoff(retention=dt.timedelta(0))


async def test_delete_rejects_a_non_positive_limit(
    tables: Tables, session_maker: async_sessionmaker[AsyncSession]
) -> None:
    async with session_maker() as session, session.begin():
        with pytest.raises(ValueError, match="limit"):
            await Inbox(session, tables.inbox).delete_expired_batch(cutoff=dt.datetime.now(tz=dt.UTC), limit=0)


async def test_cleanup_without_an_open_transaction_raises(
    tables: Tables, session_maker: async_sessionmaker[AsyncSession]
) -> None:
    async with session_maker() as session:
        inbox = Inbox(session, tables.inbox)
        with pytest.raises(RuntimeError, match="open transaction"):
            await inbox.cleanup_cutoff(retention=HOUR)
        with pytest.raises(RuntimeError, match="open transaction"):
            await inbox.delete_expired_batch(cutoff=dt.datetime.now(tz=dt.UTC), limit=10)
        assert not session.in_transaction()
