from sqlalchemy import Column, DateTime, Index, MetaData, PrimaryKeyConstraint, String, Table, func


SCOPE_MAX_LENGTH = 255
MESSAGE_ID_MAX_LENGTH = 255

_POSTGRES_IDENT_MAX_BYTES = 63
_PKEY_SUFFIX = "_pkey"
_PROCESSED_AT_IDX_SUFFIX = "_processed_at_idx"


def validate_table_identifiers(table_name: str) -> None:
    """Raise ``ValueError`` if an identifier derived from *table_name* exceeds Postgres' 63-byte limit."""
    longest = max(
        (f"{table_name}{suffix}".encode() for suffix in (_PKEY_SUFFIX, _PROCESSED_AT_IDX_SUFFIX)),
        key=len,
    )
    if len(longest) > _POSTGRES_IDENT_MAX_BYTES:
        msg = (
            f"table_name {table_name!r} too long: the derived identifier {longest.decode()!r} "
            f"must fit in {_POSTGRES_IDENT_MAX_BYTES} bytes (got {len(longest)})"
        )
        raise ValueError(msg)


def make_inbox_table(metadata: MetaData, table_name: str = "inbox") -> Table:
    """Build the inbox ``Table`` and attach it to *metadata*.

    The package does not own the schema: wire the table into your own ``MetaData`` and write the
    Alembic migration yourself.
    """
    validate_table_identifiers(table_name)
    return Table(
        table_name,
        metadata,
        Column("scope", String(SCOPE_MAX_LENGTH), nullable=False),
        Column("message_id", String(MESSAGE_ID_MAX_LENGTH), nullable=False),
        Column("processed_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
        PrimaryKeyConstraint("scope", "message_id", name=f"{table_name}{_PKEY_SUFFIX}"),
        Index(f"{table_name}{_PROCESSED_AT_IDX_SUFFIX}", "processed_at"),
    )
