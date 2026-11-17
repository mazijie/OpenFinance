from contextlib import contextmanager
from datetime import date

import pytest

from backend.rag.news.vector_store import PgVectorNewsStore, RagStoreError
from backend.repositories.rag_repository import RagRepository


class Cursor:
    def __init__(self, state):
        self.state = state

    def __enter__(self): return self
    def __exit__(self, *args): pass
    def execute(self, sql, params=None): pass
    def fetchone(self): return self.state


class Lexical:
    enabled = True
    def search_chunks(self, **kwargs): return []


class Embedding:
    model_name = "qwen"
    def __init__(self): self.called = False
    def embed_query(self, question): self.called = True; return [1.0]


class Reranker:
    def rerank(self, *, question, candidates): return candidates


@pytest.mark.parametrize("state", [("running", "qwen"), ("failed", "qwen"), ("success", "bge"), None])
def test_news_search_never_queries_unready_or_wrong_model(state):
    vector = object.__new__(PgVectorNewsStore)
    vector.search_chunks = lambda **kwargs: pytest.fail("must not query incompatible vectors")
    @contextmanager
    def connection():
        class Conn:
            def cursor(self): return Cursor(state)
        yield Conn()
    vector._connection = connection
    embedding = Embedding()
    repo = RagRepository(vector_store=vector, embedding_service=embedding, lexical_store=Lexical(), reranker=Reranker())
    hits, warnings = repo.search_with_warnings(question="收入", symbol=None, start_date=date(2026, 1, 1), end_date=date(2026, 1, 2))
    assert hits == []
    assert warnings
    assert not embedding.called


def test_model_snapshot_checksum_detects_changes(tmp_path):
    import hashlib
    from backend.rag.common.model_integrity import verify_model_files
    model_file = tmp_path / "weights"
    model_file.write_bytes(b"original")
    expected = {"weights": hashlib.sha256(b"original").hexdigest()}
    verify_model_files(str(tmp_path), expected)
    model_file.write_bytes(b"different")
    with pytest.raises(RuntimeError, match="checksum mismatch"):
        verify_model_files(str(tmp_path), expected)
