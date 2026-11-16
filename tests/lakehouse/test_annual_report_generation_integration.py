"""Run only against a disposable database through CONSISTENCY_TEST_DATABASE_URL."""
import os
from uuid import uuid4

import psycopg
import pytest
from backend.pipelines.annual_report_pipeline_state import AnnualReportPipelineStateStore, AnnualReportPipelineStateError


@pytest.mark.skipif(not os.getenv("CONSISTENCY_TEST_DATABASE_URL"), reason="requires isolated PostgreSQL")
def test_failed_activation_keeps_previous_generation_and_expired_list_is_readable():
    dsn = os.environ["CONSISTENCY_TEST_DATABASE_URL"]
    store = AnnualReportPipelineStateStore(dsn=dsn)
    first, second = uuid4().hex, uuid4().hex
    try:
        for generation in (first, second):
            store.prepare_generation(generation_id=generation, embedding_model="test",
                                     embedding_dimension=2, chunker_version="test", expected_chunks=1)
        store.mark_generation_ready(first, vector_chunks=1, lexical_chunks=1)
        store.activate_generation(first)
        with pytest.raises(AnnualReportPipelineStateError):
            store.activate_generation(second)
        assert store.get_active_generation()["generation_id"] == first
        store.mark_generation_ready(second, vector_chunks=1, lexical_chunks=1)
        store.activate_generation(second)
        with psycopg.connect(dsn) as conn:
            conn.execute("UPDATE openfinance.annual_report_index_generations SET retain_until=now()-interval '1 minute' WHERE generation_id=%s", (first,))
        assert first in store.expired_generations()
        store.delete_generation_record(second)
        assert store.get_active_generation()["generation_id"] == second
    finally:
        with psycopg.connect(dsn) as conn:
            conn.execute("DELETE FROM openfinance.annual_report_index_generations WHERE generation_id=ANY(%s)", ([first, second],))


@pytest.mark.skipif(not os.getenv("CONSISTENCY_TEST_DATABASE_URL"), reason="requires isolated PostgreSQL")
def test_migration_preserves_repeated_chunks_by_identity(monkeypatch):
    import importlib.util
    from pathlib import Path
    from alembic.migration import MigrationContext
    from alembic.operations import Operations
    from sqlalchemy import create_engine, text
    from backend.config import settings

    schema = "test_chunk_" + uuid4().hex
    table = "annual_report_chunks"
    engine = create_engine(os.environ["CONSISTENCY_TEST_DATABASE_URL"].replace("postgresql://", "postgresql+psycopg://", 1))
    spec = importlib.util.spec_from_file_location("chunk_migration", Path(__file__).parents[2] / "migrations/versions/0019_annual_report_chunk_identity.py")
    migration = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(migration)
    monkeypatch.setattr(settings, "RAG_ANNUAL_REPORT_VECTOR_TABLE", table)
    monkeypatch.setattr(type(settings), "rag_schema", property(lambda self: schema))
    try:
        with engine.begin() as connection:
            connection.execute(text(f'CREATE SCHEMA "{schema}"'))
            connection.execute(text(f'CREATE TABLE "{schema}"."{table}" (generation_id text, chunk_id text, report_id text, content_hash text, PRIMARY KEY (generation_id, chunk_id), UNIQUE (generation_id, report_id, content_hash))'))
            monkeypatch.setattr(migration, "op", Operations(MigrationContext.configure(connection)))
            migration.upgrade()
            connection.execute(text(f'INSERT INTO "{schema}"."{table}" VALUES (\'g\',\'a\',\'r\',\'h\'), (\'g\',\'b\',\'r\',\'h\')'))
            assert connection.execute(text(f'SELECT count(*) FROM "{schema}"."{table}"')).scalar_one() == 2
            migration.upgrade()  # Running against an already migrated table is harmless.
    finally:
        with engine.begin() as connection:
            connection.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
        engine.dispose()
