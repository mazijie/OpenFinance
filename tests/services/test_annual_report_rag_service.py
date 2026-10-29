from __future__ import annotations

from backend.services.annual_report_rag_service import AnnualReportRagService


def test_query_embedding_matches_the_active_generation_model(monkeypatch):
    import backend.services.annual_report_rag_service as module

    class CurrentEmbeddings:
        model_name = "new-model"
        dimension = 2

        def embed_query(self, question):
            raise AssertionError("active generation model should be selected")

        def close(self):
            pass

    class ActiveEmbeddings:
        def __init__(self, *, model_name, dimension):
            assert model_name == "active-model"
            assert dimension == 3
            self.closed = False

        def embed_query(self, question):
            assert question == "年报查询"
            return [0.1, 0.2, 0.3]

        def close(self):
            self.closed = True

    class Store:
        def ensure_schema(self):
            pass

        def search(self, **kwargs):
            assert kwargs["embedding"] == [0.1, 0.2, 0.3]
            assert kwargs["generation_id"] == "a" * 32
            return []

    class Lexical:
        def ensure_index(self):
            pass

        def search(self, **kwargs):
            return []

        def close(self):
            pass

    class State:
        def ensure_schema(self):
            pass

        def get_active_generation(self):
            return {
                "generation_id": "a" * 32,
                "embedding_model": "active-model",
                "embedding_dimension": 3,
            }

    monkeypatch.setattr(module, "EmbeddingService", ActiveEmbeddings)
    service = AnnualReportRagService(
        store=Store(),
        embeddings=CurrentEmbeddings(),
        lexical=Lexical(),
        state_store=State(),
    )
    assert service.search(
        question="年报查询",
        stock_code=None,
        report_year=None,
        limit=5,
    ) == []
