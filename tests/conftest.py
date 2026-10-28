import asyncio
import dataclasses
import os
import uuid
from collections.abc import AsyncIterator

import pytest
from sqlalchemy import BigInteger, Column, MetaData, String, Table, func, select, text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine

from sqlalchemy_inbox import make_inbox_table


PG_DSN = os.environ.get("POSTGRES_DSN", "postgresql+asyncpg://inbox:inbox@localhost:5432/inbox")


@pytest.fixture
async def pg_engine() -> AsyncIterator[AsyncEngine]:
    engine = create_async_engine(PG_DSN)
    try:
        async with engine.connect() as conn:
            await conn.exec_driver_sql("SELECT 1")
    except Exception as exc:  # noqa: BLE001  # pragma: no cover - only when Postgres is unreachable; the test then skips
        await engine.dispose()
        pytest.skip(f"Postgres not available at {PG_DSN}: {exc}")
    yield engine
    await engine.dispose()


@dataclasses.dataclass(frozen=True, slots=True)
class Tables:
    inbox: Table
    business: Table


@pytest.fixture
async def tables(pg_engine: AsyncEngine) -> AsyncIterator[Tables]:
    """Per-test inbox table plus a ``business`` table that stands in for a service's own writes."""
    metadata = MetaData()
    suffix = uuid.uuid4().hex[:12]
    business = Table(
        f"test_business_{suffix}",
        metadata,
        Column("id", BigInteger, primary_key=True, autoincrement=True),
        Column("message_id", String(255), nullable=False),
    )
    created = Tables(inbox=make_inbox_table(metadata, table_name=f"test_inbox_{suffix}"), business=business)
    async with pg_engine.begin() as conn:
        await conn.run_sync(metadata.create_all)
    yield created
    async with pg_engine.begin() as conn:
        await conn.run_sync(metadata.drop_all)


@pytest.fixture
def session_maker(pg_engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(pg_engine, expire_on_commit=False)


async def count_rows(engine: AsyncEngine, table: Table) -> int:
    async with engine.connect() as conn:
        return (await conn.execute(select(func.count()).select_from(table))).scalar_one()


async def wait_until_lock_wait(engine: AsyncEngine, pid: int) -> None:
    async with engine.connect() as conn:
        for _ in range(200):
            waiting = (
                await conn.execute(
                    text("SELECT wait_event_type = 'Lock' FROM pg_stat_activity WHERE pid = :pid"), {"pid": pid}
                )
            ).scalar_one()
            if waiting:
                return
            await asyncio.sleep(0.025)  # pragma: no cover - only when the first poll runs before the lock wait
    msg = f"backend {pid} never waited on a lock"  # pragma: no cover - only when the claim did not block
    raise AssertionError(msg)  # pragma: no cover - only when the claim did not block
