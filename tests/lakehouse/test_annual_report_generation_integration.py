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
