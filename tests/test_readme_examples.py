import dataclasses
import datetime as dt

from db_retry import Transaction, postgres_retry
from sqlalchemy import func, insert, select
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from sqlalchemy_inbox import Inbox
from tests.conftest import Tables


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
