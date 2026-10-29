# sqlalchemy-inbox

[![CI](https://github.com/modern-python/sqlalchemy-inbox/actions/workflows/ci.yml/badge.svg)](https://github.com/modern-python/sqlalchemy-inbox/actions/workflows/ci.yml)
[![License](https://img.shields.io/github/license/modern-python/sqlalchemy-inbox.svg)](https://github.com/modern-python/sqlalchemy-inbox/blob/main/LICENSE)

`sqlalchemy-inbox` records which messages a consumer has processed, in the same PostgreSQL
transaction as the consumer's own writes. A redelivered message finds its marker and is skipped. A
failed transaction rolls the marker back, so a retry processes the message again.

It works with SQLAlchemy's asyncio extension on PostgreSQL. It stores no payload and runs no worker.
`Inbox` never begins, commits or rolls back a transaction for you. Only `InboxCleaner` does that, on
its own connections.

```bash
pip install sqlalchemy-inbox
```

You also need an async PostgreSQL driver, such as asyncpg.

## The table

Attach the table to your own `MetaData` and write the Alembic migration yourself. The package owns
no migrations.

```python
from sqlalchemy import MetaData
from sqlalchemy_inbox import make_inbox_table

metadata = MetaData()
INBOX_TABLE = make_inbox_table(metadata, table_name="inbox")
```

The table has three columns: `scope` and `message_id` (both `varchar(255)`, together the primary
key) and `processed_at` (`timestamptz`, defaulting to the database's `now()`), with an index on
`processed_at` for cleanup.

## Registering with modern-di

`Inbox` takes a session and the table, so it registers like a repository. The session provider must
be REQUEST-scoped and cached, so that `Inbox` and your `Transaction` get the same session object:

```python
from db_retry import Transaction
from modern_di import Group, Scope, providers
from sqlalchemy_inbox import Inbox


class Database(Group):
    session = providers.Factory(
        scope=Scope.REQUEST,
        creator=create_session,
        cache=providers.CacheSettings(finalizer=close_session),
        kwargs={"engine": database_engine},
    )
    transaction = providers.Factory(scope=Scope.REQUEST, creator=Transaction, kwargs={"session": session})


class Repositories(Group, scope=Scope.REQUEST):
    inbox = providers.Factory(creator=Inbox, kwargs={"session": Database.session, "table": INBOX_TABLE})
```

If a service wires an uncached session by mistake, `Inbox` gets a second session that has no open
transaction, and `claim` raises `RuntimeError` instead of writing a marker that would never commit.

## Processing a message

Claim the message inside your transaction, before the business writes:

```python
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
```

`claim` returns `True` when this transaction inserted the marker, and `False` when the message is
already committed. If another transaction holds an uncommitted marker for the same key, `claim`
waits for it to finish. Retrying the whole block with `postgres_retry` is safe: if an earlier attempt
committed, the claim on the retry returns `False`.

`claim` does not accept `None`. If a message can arrive without an id, decide in the handler what to
do with it: reject it, or skip the inbox in a branch you can see. The package does not hash payloads
or fall back to Kafka offsets.

## Cleaning up

Markers are kept until you delete them. Run `InboxCleaner` from a cron job:

```python
import asyncio
import datetime

from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy_inbox import InboxCleaner


async def main() -> None:
    engine = create_async_engine(DATABASE_DSN)
    try:
        cleaner = InboxCleaner(engine=engine, table=INBOX_TABLE, retention=datetime.timedelta(days=7))
        await cleaner.cleanup()
    finally:
        await engine.dispose()


asyncio.run(main())
```

A cleanup run takes one cutoff from the database clock, `now()` minus `retention`, and deletes the
markers older than it in batches of `batch_size` (1000 by default), one transaction per batch, until
a batch deletes nothing. `cleanup()` returns a `CleanupResult` with the `cutoff` and the
`deleted_count`. It logs each batch at DEBUG and the total at INFO to the `sqlalchemy_inbox` logger.

Batches that committed stay deleted if a later batch fails or the task is cancelled, and the error
reaches your cron job. The cleaner doesn't retry: the next run deletes what is left. It skips markers
that another transaction holds, so it never waits on a consumer, and two runs at once share the
work. Each batch runs at READ COMMITTED whatever the engine's default. The engine stays yours to
dispose.

## Guarantees

What the package guarantees, given a PostgreSQL transaction that you own:

- The marker and your writes commit together or not at all. That includes a failure in your code,
  a database error, a cancelled task and a failed `COMMIT`.
- Two transactions processing the same `(scope, message_id)` at once commit the database effects at
  most once. The second waits for the first and returns `False` if the first committed, `True` if it
  rolled back.
- A redelivery after a successful commit returns `False`, including when the process died after the
  commit but before the broker was acknowledged.
- If you can't tell whether a commit landed, run the transaction again: `claim` returns `False` if it
  did and `True` if it didn't.

What it does not guarantee:

- Exactly-once delivery from end to end. The guarantee covers database effects for one key while
  its marker is retained.
- Effects outside PostgreSQL. An HTTP call made in a transaction that later rolls back has already
  happened, and the retry makes it again. Send those through a transactional outbox, or make the
  receiving side idempotent.
- Anything after the marker is deleted. Once cleanup removes it, the same key counts as a new
  message.
- Under REPEATABLE READ or SERIALIZABLE, a conflicting `claim` raises a serialization failure
  instead of returning `False`. db-retry's `postgres_retry` retries it, and the retry returns
  `False`.

## Choosing a scope

A scope names one logical consumer of a message. Write it as a constant, such as
`"chats.create-chat.v1"`.

Don't build it from a function name, module path, consumer group or topic. Those change when code
is refactored or a deployment is renamed, and a changed scope makes every retained message look new.

Two consumers that each need to process the same message use different scopes. Change the version
suffix only when you want old messages to be processed again.

## Choosing a retention

Retention must cover the longest time a message can come back. That includes:

- the Kafka topic's retention, which defaults to 7 days,
- DLQ retention plus the time it takes to replay from it,
- manual replays and backfills.

A day of retention is enough only if nothing can be redelivered after a day. Each marker is dated by
the start of the transaction that wrote it, so a marker from a long transaction expires that much
earlier. At retention measured in days, that makes no practical difference.

## Kafka offsets for failed handlers

The inbox makes redelivery safe. It cannot cause a redelivery. With
[faststream-concurrent-aiokafka](https://github.com/modern-python/faststream-concurrent-aiokafka),
a handler that raises still has its offset committed. Only a cancelled handler holds the offset
back. So when your transaction fails, the marker rolls back as it should, but Kafka will not send
the message again.

If a failed message has to be processed later, retry inside the handler (for example with
`postgres_retry`) or send it to a dead-letter queue before the handler returns. The same applies to
errors db-retry deliberately doesn't retry, such as `StatementCompletionUnknownError`, where you
can't tell whether the commit landed.

## Links

- [License](https://github.com/modern-python/sqlalchemy-inbox/blob/main/LICENSE)
