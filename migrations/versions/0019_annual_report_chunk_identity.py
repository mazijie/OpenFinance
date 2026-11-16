"""Allow distinct annual-report chunks to share a content hash."""
from alembic import op
from sqlalchemy import text

from backend.config import settings

revision = "0019_report_chunk_identity"
down_revision = "0018_quote_capture_only"
branch_labels = None
depends_on = None


def upgrade() -> None:
    connection = op.get_bind()
    schema = settings.rag_schema
    table = settings.RAG_ANNUAL_REPORT_VECTOR_TABLE
    constraints = connection.execute(text("""
        SELECT c.conname
        FROM pg_constraint c
        JOIN pg_class t ON t.oid = c.conrelid
        JOIN pg_namespace n ON n.oid = t.relnamespace
        WHERE n.nspname = :schema AND t.relname = :table AND c.contype = 'u'
          AND ARRAY(SELECT a.attname::text FROM unnest(c.conkey) WITH ORDINALITY AS k(attnum, ord)
                    JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = k.attnum
                    ORDER BY k.ord) = ARRAY['generation_id', 'report_id', 'content_hash']::text[]
    """), {"schema": schema, "table": table}).scalars()
    for name in constraints:
        op.drop_constraint(name, table, schema=schema, type_="unique")


def downgrade() -> None:
    op.create_unique_constraint(
        "annual_report_chunks_generation_report_hash_key",
        settings.RAG_ANNUAL_REPORT_VECTOR_TABLE,
        ["generation_id", "report_id", "content_hash"],
        schema=settings.rag_schema,
    )
