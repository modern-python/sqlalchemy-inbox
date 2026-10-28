import asyncio
import uuid

import pytest
from db_retry import Transaction, postgres_retry
from db_retry.retriable import is_retriable
from sqlalchemy import BigInteger, Column, MetaData, String, Table, UniqueConstraint, insert, text
from sqlalchemy.exc import DBAPIError, IntegrityError
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from sqlalchemy_inbox import Inbox, make_inbox_table
from tests.conftest import Tables, count_rows, wait_until_lock_wait


class _AckLostError(Exception):
    pass


async def _backend_pid(session: AsyncSession) -> int:
    return (await session.execute(text("SELECT pg_backend_pid()"))).scalar_one()


async def _claim_and_write(session: AsyncSession, tables: Tables, message_id: str) -> bool:
    async with session.begin():
        if not await Inbox(session, tables.inbox).claim(scope="s", message_id=message_id):
            return False
        await session.execute(insert(tables.business).values(message_id=message_id))
    return True


async def _blocked_claim_after(
    engine: AsyncEngine,
    tables: Tables,
    session_maker: async_sessionmaker[AsyncSession],
    *,
    first_commits: bool,
) -> bool:
    async with session_maker() as first, session_maker() as second:
        await first.begin()
        assert await Inbox(first, tables.inbox).claim(scope="s", message_id="m-1")
        await first.execute(insert(tables.business).values(message_id="m-1"))

        await second.begin()
        pid = await _backend_pid(second)
        competing = asyncio.create_task(Inbox(second, tables.inbox).claim(scope="s", message_id="m-1"))
        await wait_until_lock_wait(engine, pid)
        assert not competing.done()

        if first_commits:
            await first.commit()
        else:
            await first.rollback()
        acquired = await competing
        if acquired:
            await second.execute(insert(tables.business).values(message_id="m-1"))
        await second.commit()
    return acquired


async def test_competing_claim_waits_and_skips_when_first_commits(
    pg_engine: AsyncEngine, tables: Tables, session_maker: async_sessionmaker[AsyncSession]
) -> None:
    assert not await _blocked_claim_after(pg_engine, tables, session_maker, first_commits=True)

    assert await count_rows(pg_engine, tables.inbox) == 1
    assert await count_rows(pg_engine, tables.business) == 1


async def test_competing_claim_proceeds_when_first_rolls_back(
    pg_engine: AsyncEngine, tables: Tables, session_maker: async_sessionmaker[AsyncSession]
) -> None:
    assert await _blocked_claim_after(pg_engine, tables, session_maker, first_commits=False)

    assert await count_rows(pg_engine, tables.inbox) == 1
    assert await count_rows(pg_engine, tables.business) == 1


async def test_many_concurrent_deliveries_commit_one_effect(
    pg_engine: AsyncEngine, tables: Tables, session_maker: async_sessionmaker[AsyncSession]
) -> None:
    async def deliver() -> bool:
        async with session_maker() as session:
            return await _claim_and_write(session, tables, "m-1")

    results = await asyncio.gather(*(deliver() for _ in range(20)))

    assert results.count(True) == 1
    assert await count_rows(pg_engine, tables.inbox) == 1
    assert await count_rows(pg_engine, tables.business) == 1


async def test_repeatable_read_conflict_is_a_retriable_serialization_failure(
    pg_engine: AsyncEngine, tables: Tables, session_maker: async_sessionmaker[AsyncSession]
) -> None:
    attempts: list[bool] = []

    @postgres_retry(retries=2)
    async def deliver() -> bool:
        async with Transaction(session=session_maker(), isolation_level="REPEATABLE READ") as transaction:
            await transaction.session.execute(text("SELECT 1"))
            if not attempts:
                async with session_maker() as other:
                    assert await _claim_and_write(other, tables, "m-1")
            attempts.append(True)
            return await Inbox(transaction.session, tables.inbox).claim(scope="s", message_id="m-1")

    async with Transaction(session=session_maker(), isolation_level="REPEATABLE READ") as transaction:
        await transaction.session.execute(text("SELECT 1"))
        async with session_maker() as other:
            assert await _claim_and_write(other, tables, "m-0")
        with pytest.raises(DBAPIError) as raised:
            await Inbox(transaction.session, tables.inbox).claim(scope="s", message_id="m-0")
        assert is_retriable(raised.value)

    assert not await deliver()
    assert len(attempts) == 2
    assert await count_rows(pg_engine, tables.business) == 2


async def test_cancelled_delivery_rolls_back_and_can_be_retried(
    pg_engine: AsyncEngine, tables: Tables, session_maker: async_sessionmaker[AsyncSession]
) -> None:
    claimed = asyncio.Event()

    async def deliver() -> None:
        async with Transaction(session=session_maker()) as transaction:
            assert await Inbox(transaction.session, tables.inbox).claim(scope="s", message_id="m-1")
            await transaction.session.execute(insert(tables.business).values(message_id="m-1"))
            claimed.set()
            await asyncio.Event().wait()

    task = asyncio.create_task(deliver())
    await claimed.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert await count_rows(pg_engine, tables.inbox) == 0
    assert await count_rows(pg_engine, tables.business) == 0
    async with session_maker() as session:
        assert await _claim_and_write(session, tables, "m-1")


async def test_failed_commit_persists_nothing_and_retry_redoes_the_work(
    pg_engine: AsyncEngine, session_maker: async_sessionmaker[AsyncSession]
) -> None:
    metadata = MetaData()
    suffix = uuid.uuid4().hex[:12]
    inbox_table = make_inbox_table(metadata, table_name=f"test_inbox_{suffix}")
    business = Table(
        f"test_deferred_{suffix}",
        metadata,
        Column("id", BigInteger, primary_key=True, autoincrement=True),
        Column("value", String(255), nullable=False),
        UniqueConstraint("value", deferrable=True, initially="DEFERRED"),
    )
    async with pg_engine.begin() as conn:
        await conn.run_sync(metadata.create_all)
        await conn.execute(insert(business).values(value="taken"))
    try:

        async def commit_a_duplicate(session: AsyncSession) -> None:
            async with session.begin():
                assert await Inbox(session, inbox_table).claim(scope="s", message_id="m-1")
                await session.execute(insert(business).values(value="taken"))

        async with session_maker() as session:
            with pytest.raises(IntegrityError):
                await commit_a_duplicate(session)

        assert await count_rows(pg_engine, inbox_table) == 0
        async with session_maker() as session, session.begin():
            assert await Inbox(session, inbox_table).claim(scope="s", message_id="m-1")
            await session.execute(insert(business).values(value="free"))
        assert await count_rows(pg_engine, inbox_table) == 1
        assert await count_rows(pg_engine, business) == 2
    finally:
        async with pg_engine.begin() as conn:
            await conn.run_sync(metadata.drop_all)


async def test_redelivery_after_commit_with_lost_ack_is_skipped(
    pg_engine: AsyncEngine, tables: Tables, session_maker: async_sessionmaker[AsyncSession]
) -> None:
    async def deliver() -> bool:
        async with session_maker() as session:
            acquired = await _claim_and_write(session, tables, "m-1")
        if acquired:
            raise _AckLostError
        return acquired

    with pytest.raises(_AckLostError):
        await deliver()
    assert not await deliver()

    assert await count_rows(pg_engine, tables.inbox) == 1
    assert await count_rows(pg_engine, tables.business) == 1


async def test_claim_works_on_an_async_connection(pg_engine: AsyncEngine, tables: Tables) -> None:
    async with pg_engine.connect() as conn:
        async with conn.begin():
            assert await Inbox(conn, tables.inbox).claim(scope="s", message_id="m-1")
        async with conn.begin():
            assert not await Inbox(conn, tables.inbox).claim(scope="s", message_id="m-1")
