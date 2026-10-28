import uuid

import pytest
from sqlalchemy import MetaData, text
from sqlalchemy.ext.asyncio import AsyncEngine

from sqlalchemy_inbox import make_inbox_table


async def test_table_has_scope_message_id_primary_key_and_processed_at_index(pg_engine: AsyncEngine) -> None:
    metadata = MetaData()
    table_name = f"test_inbox_{uuid.uuid4().hex[:12]}"
    make_inbox_table(metadata, table_name=table_name)
    async with pg_engine.begin() as conn:
        await conn.run_sync(metadata.create_all)
    try:
        async with pg_engine.connect() as conn:
            primary_key = (
                await conn.execute(
                    text(
                        "SELECT array_agg(a.attname ORDER BY k.ord) FROM pg_index i "
                        "CROSS JOIN LATERAL unnest(i.indkey) WITH ORDINALITY AS k(attnum, ord) "
                        "JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = k.attnum "
                        "WHERE i.indrelid = CAST(:t AS regclass) AND i.indisprimary"
                    ),
                    {"t": table_name},
                )
            ).scalar_one()
            processed_at_default = (
                await conn.execute(
                    text(
                        "SELECT pg_get_expr(d.adbin, d.adrelid) FROM pg_attrdef d "
                        "JOIN pg_attribute a ON a.attrelid = d.adrelid AND a.attnum = d.adnum "
                        "WHERE d.adrelid = CAST(:t AS regclass) AND a.attname = 'processed_at'"
                    ),
                    {"t": table_name},
                )
            ).scalar_one()
            processed_at_index = (
                await conn.execute(
                    text("SELECT indexdef FROM pg_indexes WHERE tablename = :t AND indexname = :i"),
                    {"t": table_name, "i": f"{table_name}_processed_at_idx"},
                )
            ).scalar_one()
    finally:
        async with pg_engine.begin() as conn:
            await conn.run_sync(metadata.drop_all)

    assert primary_key == ["scope", "message_id"]
    assert processed_at_default == "now()"
    assert processed_at_index.endswith("(processed_at)")


def test_table_name_whose_index_name_exceeds_63_bytes_is_rejected() -> None:
    with pytest.raises(ValueError, match="63 bytes"):
        make_inbox_table(MetaData(), table_name="t" * 50)
