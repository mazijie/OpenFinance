from contextlib import contextmanager
from datetime import UTC, datetime
from types import SimpleNamespace

from backend.rag.annual_report_indexer import AnnualReportRAGIndexer
from backend.rag.annual_report_store import AnnualReportVectorStore
from backend.schemas.annual_report import AnnualReportChunkRecord

NOW = datetime(2026, 1, 1, tzinfo=UTC)


def chunk(chunk_id, page_number, run='run-1'):
    return AnnualReportChunkRecord(chunk_id=chunk_id, report_id='report', parse_run_id=run, section_path='正文', content_type='paragraph', page_start=page_number, page_end=page_number, region_ids=[f'region-{page_number}'], chunk_text='经营情况正常', content_hash=chunk_id)


class Writer:
    def scan_parse_runs(self):
        return [SimpleNamespace(report_id='report', parse_run_id='run-1', started_at=NOW, finished_at=NOW)]

    def scan_reports(self):
        return [SimpleNamespace(report_id='report', parse_run_id='run-1', extraction_status='needs_review', stock_code='600519.SH', report_year=2024, company_name=None, announcement_time=NOW, ingested_at=NOW)]

    def scan_pages(self):
        return [SimpleNamespace(report_id='report', parse_run_id='run-1', page_number=number, extraction_status=status) for number, status in [(1, 'ok'), (2, 'needs_review')]]

    def scan_chunks(self):
        return [chunk('current', 1), chunk('review', 2), chunk('old', 1, 'old-run')]


class Store:
    def ensure_schema(self):
        pass

    def existing_hashes(self, hashes):
        return set(hashes)  # Simulate a successful pgvector write on the previous attempt.

    def retain_chunks(self, ids):
        self.retained = ids

    def upsert(self, chunks):
        raise AssertionError('vectors already exist')


class Lexical:
    def ensure_index(self):
        pass

    def retain_chunks(self, ids):
        self.retained = ids

    def upsert(self, chunks):
        self.written = chunks
        return len(chunks)


class Embeddings:
    def embed_documents(self, texts):
        assert texts == []
        return []


def test_only_current_ok_pages_are_indexed_and_lexical_retry_is_repaired():
    vector, lexical = Store(), Lexical()
    result = AnnualReportRAGIndexer(writer=Writer(), store=vector, lexical_store=lexical, embedding_service=Embeddings()).run()
    assert result['skipped_review_pages'] == 1
    assert vector.retained == ['current']
    assert lexical.retained == ['current']
    assert [item['chunk_id'] for item in lexical.written] == ['current']
    assert result['written_chunks'] == 0
    assert result['written_lexical_chunks'] == 1


def test_failed_source_read_does_not_clear_existing_indexes():
    import pytest

    class FailedWriter(Writer):
        def scan_chunks(self):
            raise RuntimeError("source unavailable")

    class ExistingStore(Store):
        def clear(self):
            raise AssertionError("must not clear before source read")

    class ExistingLexical(Lexical):
        def clear(self):
            raise AssertionError("must not clear before source read")

    indexer = AnnualReportRAGIndexer(writer=FailedWriter(), store=ExistingStore(),
                                     lexical_store=ExistingLexical(), embedding_service=Embeddings())
    with pytest.raises(RuntimeError, match="source unavailable"):
        indexer.run(rebuild_all=True)


def test_vector_search_binds_embedding_before_filter_parameters():
    class Cursor:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def execute(self, sql, params):
            self.params = params

        def fetchall(self):
            return []

    cursor = Cursor()
    connection = SimpleNamespace(cursor=lambda **kwargs: cursor)
    store = object.__new__(AnnualReportVectorStore)
    store.schema = 'gold'
    store.table = 'annual_report_chunks'

    @contextmanager
    def connect():
        yield connection

    store._connection = connect
    assert store.search(embedding=[0.1, 0.2], stock_code='600519.SH', report_year=2024, as_of=None, published_after=None, limit=2) == []
    assert cursor.params[1:3] == ('600519.SH', 2024)
    assert cursor.params[-1] == 2
    assert cursor.params[0] is cursor.params[-2]


def test_versioned_index_writes_completed_embedding_batches_before_later_failure(monkeypatch):
    from backend.config import settings

    monkeypatch.setattr(settings, "EMBEDDING_BATCH_SIZE", 1)
    class VersionedWriter:
        def scan_reports(self):
            return [SimpleNamespace(report_id="report", parse_run_id="run-1", pdf_sha256="a" * 64, stock_code="600519.SH", report_year=2024, company_name=None, announcement_time=NOW, ingested_at=NOW)]

        def scan_pages(self):
            return [SimpleNamespace(report_id="report", parse_run_id="run-1", page_number=index, extraction_status="ok") for index in (1, 2)]

        def scan_parse_runs(self):
            return [SimpleNamespace(report_id="report", parse_run_id="run-1", started_at=NOW, finished_at=NOW)]

        def scan_chunks(self):
            return [chunk("one", 1), chunk("two", 2)]

    class VersionedStore:
        def __init__(self):
            self.rows = {}

        def ensure_schema(self):
            pass

        def existing_hashes(self, hashes, generation_id=None):
            return {row["content_hash"] for row in self.rows.values() if row.get("generation_id") == generation_id and row["content_hash"] in hashes}

        def existing_chunk_ids(self, chunk_ids, generation_id):
            return {chunk_id for gen, chunk_id in self.rows if gen == generation_id and chunk_id in chunk_ids}

        def seed_generation(self, **kwargs):
            return 0

        def upsert(self, rows, generation_id=None):
            for row in rows:
                self.rows[(generation_id, row["chunk_id"])] = {**row, "generation_id": generation_id}
            return len(rows)

        def count_generation(self, generation_id):
            return sum(1 for row in self.rows.values() if row.get("generation_id") == generation_id)

        def contains_chunk_ids(self, generation_id, chunk_ids):
            return all((generation_id, chunk_id) in self.rows for chunk_id in chunk_ids)

        def create_generation_index(self, generation_id):
            self.indexed_generation = generation_id

        def embedding_for(self, generation_id, chunk_id):
            return self.rows[(generation_id, chunk_id)]["embedding"]

        def search(self, **kwargs):
            return [SimpleNamespace(chunk_id=kwargs.get("report_id") and "one" or "one")]

        def delete_generation(self, generation_id):
            return None

        def delete_unversioned_documents(self):
            return None

        def delete_unversioned_documents(self):
            return None

    class VersionedLexical:
        enabled = False

        def ensure_index(self):
            pass

        def upsert(self, rows, generation_id=None):
            return len(rows)

        def count_generation(self, generation_id):
            return 0

        def delete_generation(self, generation_id):
            return None

        def delete_unversioned_documents(self):
            return None

    class State:
        def __init__(self):
            self.generations = {}
            self.leases = []

        def ensure_schema(self):
            pass

        def get_active_generation(self):
            return None

        def prepare_generation(self, **kwargs):
            self.generations[kwargs["generation_id"]] = {**kwargs, "status": "building"}

        def claim_stage(self, **kwargs):
            lease = SimpleNamespace(**kwargs, lease_token="token")
            self.leases.append(lease)
            return lease

        def mark_generation_ready(self, generation_id, **kwargs):
            self.generations[generation_id].update(kwargs, status="ready")

        def activate_generation(self, generation_id):
            self.generations[generation_id]["status"] = "active"

        def complete_stage(self, lease, **kwargs):
            return None

        def fail_generation(self, generation_id, error):
            self.generations[generation_id]["status"] = "failed"

        def fail_stage(self, lease, error):
            return None

        def expired_generations(self):
            return []

        def delete_generation_record(self, generation_id):
            return None

    class FlakyEmbedding:
        model_name = "bge-m3"
        dimension = 2

        def __init__(self):
            self.calls = 0

        def embed_documents(self, texts):
            self.calls += 1
            if self.calls == 2:
                raise RuntimeError("embedding unavailable")
            return [[0.1, 0.2] for _ in texts]

        def close(self):
            pass

    vector = VersionedStore()
    state = State()
    embeddings = FlakyEmbedding()
    indexer = AnnualReportRAGIndexer(
        writer=VersionedWriter(),
        store=vector,
        embedding_service=embeddings,
        lexical_store=VersionedLexical(),
        state_store=state,
    )
    import pytest
    with pytest.raises(RuntimeError, match="embedding unavailable"):
        indexer.run()
    assert len(vector.rows) == 1
    embeddings.calls = 0
    result = indexer.run()
    assert len(vector.rows) == 2
    assert result["skipped_existing"] == 1
