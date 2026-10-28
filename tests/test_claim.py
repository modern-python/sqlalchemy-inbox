import pytest
from sqlalchemy import MetaData, Table, insert
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from sqlalchemy_inbox import Inbox
from tests.conftest import Tables, count_rows


class _BusinessError(Exception):
    pass


async def _process(session: AsyncSession, tables: Tables, *, scope: str, message_id: str) -> bool:
    async with session.begin():
        if not await Inbox(session, tables.inbox).claim(scope=scope, message_id=message_id):
            return False
        await session.execute(insert(tables.business).values(message_id=message_id))
    return True


async def test_first_delivery_commits_marker_and_business_write(
    pg_engine: AsyncEngine, tables: Tables, session_maker: async_sessionmaker[AsyncSession]
) -> None:
    async with session_maker() as session:
        assert await _process(session, tables, scope="s", message_id="m-1")

    assert await count_rows(pg_engine, tables.inbox) == 1
    assert await count_rows(pg_engine, tables.business) == 1


async def test_sequential_redelivery_is_skipped(
    pg_engine: AsyncEngine, tables: Tables, session_maker: async_sessionmaker[AsyncSession]
) -> None:
    async with session_maker() as session:
        assert await _process(session, tables, scope="s", message_id="m-1")
    async with session_maker() as session:
        assert not await _process(session, tables, scope="s", message_id="m-1")

    assert await count_rows(pg_engine, tables.inbox) == 1
    assert await count_rows(pg_engine, tables.business) == 1


async def test_different_scopes_process_the_same_message_independently(
    pg_engine: AsyncEngine, tables: Tables, session_maker: async_sessionmaker[AsyncSession]
) -> None:
    async with session_maker() as session:
        assert await _process(session, tables, scope="first", message_id="m-1")
    async with session_maker() as session:
        assert await _process(session, tables, scope="second", message_id="m-1")

    assert await count_rows(pg_engine, tables.inbox) == 2
    assert await count_rows(pg_engine, tables.business) == 2


async def test_failure_after_claim_rolls_back_marker_and_business_write(
    pg_engine: AsyncEngine, tables: Tables, session_maker: async_sessionmaker[AsyncSession]
) -> None:
    async def fail_after_claim(session: AsyncSession) -> None:
        async with session.begin():
            assert await Inbox(session, tables.inbox).claim(scope="s", message_id="m-1")
            await session.execute(insert(tables.business).values(message_id="m-1"))
            raise _BusinessError

    async with session_maker() as session:
        with pytest.raises(_BusinessError):
            await fail_after_claim(session)

    assert await count_rows(pg_engine, tables.inbox) == 0
    assert await count_rows(pg_engine, tables.business) == 0
    async with session_maker() as session:
        assert await _process(session, tables, scope="s", message_id="m-1")


async def test_inbox_rejects_a_non_session(tables: Tables) -> None:
    with pytest.raises(TypeError, match="AsyncSession or AsyncConnection"):
        Inbox(object(), tables.inbox)  # ty: ignore[invalid-argument-type]


def test_inbox_rejects_an_over_long_table_name() -> None:
    with pytest.raises(ValueError, match="63 bytes"):
        Inbox(AsyncSession(), Table("t" * 50, MetaData()))


async def test_claim_without_an_open_transaction_raises(
    pg_engine: AsyncEngine, tables: Tables, session_maker: async_sessionmaker[AsyncSession]
) -> None:
    async with session_maker() as session:
        with pytest.raises(RuntimeError, match="open transaction"):
            await Inbox(session, tables.inbox).claim(scope="s", message_id="m-1")
        assert not session.in_transaction()

    assert await count_rows(pg_engine, tables.inbox) == 0


@pytest.mark.parametrize(
    ("scope", "message_id"),
    [("", "m-1"), ("s", ""), ("s" * 256, "m-1"), ("s", "m" * 256)],
)
async def test_claim_rejects_empty_or_over_long_keys(
    pg_engine: AsyncEngine,
    tables: Tables,
    session_maker: async_sessionmaker[AsyncSession],
    scope: str,
    message_id: str,
) -> None:
    async with session_maker() as session, session.begin():
        with pytest.raises(ValueError, match=r"scope|message_id"):
            await Inbox(session, tables.inbox).claim(scope=scope, message_id=message_id)

    assert await count_rows(pg_engine, tables.inbox) == 0


async def test_claim_on_a_wrongly_wired_idle_session_raises(
    pg_engine: AsyncEngine, tables: Tables, session_maker: async_sessionmaker[AsyncSession]
) -> None:
    async def write_with_wrong_inbox_session(business_session: AsyncSession, inbox_session: AsyncSession) -> None:
        async with business_session.begin():
            await business_session.execute(insert(tables.business).values(message_id="m-1"))
            await Inbox(inbox_session, tables.inbox).claim(scope="s", message_id="m-1")

    async with session_maker() as business_session, session_maker() as inbox_session:
        with pytest.raises(RuntimeError, match="open transaction"):
            await write_with_wrong_inbox_session(business_session, inbox_session)

    assert await count_rows(pg_engine, tables.inbox) == 0
    assert await count_rows(pg_engine, tables.business) == 0
