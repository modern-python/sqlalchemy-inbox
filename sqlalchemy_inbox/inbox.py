from sqlalchemy import Table
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncSession

from sqlalchemy_inbox.schema import MESSAGE_ID_MAX_LENGTH, SCOPE_MAX_LENGTH, validate_table_identifiers


SessionOrConnection = AsyncSession | AsyncConnection


def _validate_key_part(name: str, value: str, max_length: int) -> None:
    if not value:
        msg = f"{name} must not be empty"
        raise ValueError(msg)
    if len(value) > max_length:
        msg = f"{name} must be at most {max_length} characters (got {len(value)})"
        raise ValueError(msg)


class Inbox:
    """Processed-message ledger bound to the caller's session.

    Never commits, rolls back, begins, or opens a session or connection: the caller owns the transaction.
    """

    def __init__(self, session: SessionOrConnection, table: Table) -> None:
        if not isinstance(session, SessionOrConnection):
            msg = "Inbox requires an sqlalchemy.ext.asyncio.AsyncSession or AsyncConnection"
            raise TypeError(msg)
        validate_table_identifiers(table.name)
        self._session = session
        self._table = table

    async def claim(self, *, scope: str, message_id: str) -> bool:
        """Insert the marker for ``(scope, message_id)`` in the caller's open transaction.

        Returns ``True`` if this transaction inserted it and the caller should do the work, ``False`` if the
        key is already committed. While another transaction holds an uncommitted marker for the key, waits for
        it to finish. Raises ``RuntimeError`` if the session has no open transaction.
        """
        _validate_key_part("scope", scope, SCOPE_MAX_LENGTH)
        _validate_key_part("message_id", message_id, MESSAGE_ID_MAX_LENGTH)
        self._require_transaction("claim")
        statement = (
            pg_insert(self._table)
            .values(scope=scope, message_id=message_id)
            .on_conflict_do_nothing(index_elements=["scope", "message_id"])
            .returning(self._table.c.scope)
        )
        result = await self._session.execute(statement)
        return result.first() is not None

    def _require_transaction(self, method: str) -> None:
        if not self._session.in_transaction():
            msg = f"Inbox.{method} requires an open transaction on its session"
            raise RuntimeError(msg)
