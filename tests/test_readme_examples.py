import dataclasses
import datetime as dt

from db_retry import Transaction, postgres_retry
from sqlalchemy import Table, func, insert, select, text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from sqlalchemy_inbox import Inbox
from tests.conftest import Tables, count_rows


@dataclasses.dataclass(kw_only=True, frozen=True, slots=True)
class CreateChat:
    idempotency_id: str


class ChatsRepository:
    def __init__(self, session: AsyncSession, table: Table, *, failures: int = 0) -> None:
        self._session = session
        self._table = table
        self._failures = failures
        self.calls = 0

    async def create(self, command: CreateChat) -> None:
        self.calls += 1
        await self._session.execute(insert(self._table).values(message_id=command.idempotency_id))
        if self.calls <= self._failures:
            await self._session.execute(
                text("DO $$ BEGIN RAISE EXCEPTION 'injected' USING ERRCODE = 'serialization_failure'; END $$")
            )


@dataclasses.dataclass(kw_only=True, frozen=True, slots=True)
class CreateChatUseCase:
    transaction: Transaction
    inbox: Inbox
    chats_repository: ChatsRepository

    @postgres_retry
    async def __call__(self, *, command: CreateChat) -> None:
        async with self.transaction:
            if not await self.inbox.claim(scope="chats.create-chat.v1", message_id=command.idempotency_id):
                return
            await self.chats_repository.create(command)
            await self.transaction.commit()


def _create_chat_use_case(
    session_maker: async_sessionmaker[AsyncSession], tables: Tables, *, failures: int = 0
) -> CreateChatUseCase:
    session = session_maker()
    return CreateChatUseCase(
        transaction=Transaction(session=session),
        inbox=Inbox(session, tables.inbox),
        chats_repository=ChatsRepository(session, tables.business, failures=failures),
    )


async def test_readme_processing_use_case_retries_on_the_same_session(
    pg_engine: AsyncEngine, tables: Tables, session_maker: async_sessionmaker[AsyncSession]
) -> None:
    use_case = _create_chat_use_case(session_maker, tables, failures=1)

    await use_case(command=CreateChat(idempotency_id="m-1"))

    assert use_case.chats_repository.calls == 2
    assert await count_rows(pg_engine, tables.inbox) == 1
    assert await count_rows(pg_engine, tables.business) == 1


async def test_readme_processing_use_case_skips_a_duplicate_and_handles_the_next_message(
    pg_engine: AsyncEngine, tables: Tables, session_maker: async_sessionmaker[AsyncSession]
) -> None:
    use_case = _create_chat_use_case(session_maker, tables)

    await use_case(command=CreateChat(idempotency_id="m-1"))
    await use_case(command=CreateChat(idempotency_id="m-1"))
    await use_case(command=CreateChat(idempotency_id="m-2"))

    assert use_case.chats_repository.calls == 2
    assert not use_case.transaction.session.in_transaction()
    assert await count_rows(pg_engine, tables.inbox) == 2
    assert await count_rows(pg_engine, tables.business) == 2


@dataclasses.dataclass(kw_only=True, frozen=True, slots=True)
class CleanupInboxUseCase:
    transaction: Transaction
    inbox: Inbox

    async def __call__(self, *, retention: dt.timedelta, batch_size: int) -> None:
        async with self.transaction:
            cutoff = await self.inbox.cleanup_cutoff(retention=retention)
        while await self._delete_batch(cutoff=cutoff, limit=batch_size):
            pass

    @postgres_retry
    async def _delete_batch(self, *, cutoff: dt.datetime, limit: int) -> int:
        async with self.transaction:
            deleted = await self.inbox.delete_expired_batch(cutoff=cutoff, limit=limit)
            await self.transaction.commit()
        return deleted


async def test_readme_cleanup_use_case_deletes_every_expired_marker(
    pg_engine: AsyncEngine, tables: Tables, session_maker: async_sessionmaker[AsyncSession]
) -> None:
    async with pg_engine.begin() as conn:
        for index in range(5):
            await conn.execute(
                insert(tables.inbox).values(
                    scope="s", message_id=f"old-{index}", processed_at=func.now() - dt.timedelta(hours=2)
                )
            )
        await conn.execute(insert(tables.inbox).values(scope="s", message_id="recent"))

    session = session_maker()
    use_case = CleanupInboxUseCase(transaction=Transaction(session=session), inbox=Inbox(session, tables.inbox))
    await use_case(retention=dt.timedelta(hours=1), batch_size=2)

    async with pg_engine.connect() as conn:
        remaining = list((await conn.execute(select(tables.inbox.c.message_id))).scalars())
    assert remaining == ["recent"]
