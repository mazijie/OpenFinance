"""Safe pgvector schema dimension checks."""

from __future__ import annotations

from typing import Any, Callable


def ensure_vector_dimension(
    cur: Any,
    *,
    schema: str,
    table: str,
    dimension: int,
    create_table_sql: Callable[[], str],
    error_type: type[Exception],
) -> None:
    """Rebuild an empty vector table when its embedding dimension changed."""

    fqtn = f"{schema}.{table}"
    cur.execute(
        """
        SELECT format_type(a.atttypid, a.atttypmod)
        FROM pg_attribute a
        JOIN pg_class c ON c.oid = a.attrelid
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = %s AND c.relname = %s AND a.attname = 'embedding' AND NOT a.attisdropped
        """,
        (schema, table),
    )
    row = cur.fetchone()
    if row is None:
        return
    actual = str(row[0])
    expected = f"vector({dimension})"
    if actual == expected:
        return
    cur.execute(f"SELECT EXISTS (SELECT 1 FROM {fqtn} LIMIT 1)")
    has_rows = bool(cur.fetchone()[0])
    if has_rows:
        raise error_type(
            f"{fqtn} uses {actual}, configured embedding is {expected}; "
            "clear and rebuild the vector index before changing embedding models"
        )
    cur.execute(f"DROP TABLE {fqtn}")
    cur.execute(create_table_sql())
