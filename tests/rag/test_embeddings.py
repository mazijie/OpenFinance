from __future__ import annotations

from backend.config import settings
from backend.rag.embeddings import EmbeddingService


class FakeResponse:
    def __init__(self, count: int):
        self._count = count

    def raise_for_status(self):
        return None

    def json(self):
        return {
            "data": [
                {"index": index, "embedding": [0.25] * 1024}
                for index in range(self._count)
            ]
        }


class FakeClient:
    def __init__(self):
        self.calls = []
        self.closed = False

    def post(self, url, **kwargs):
        self.calls.append((url, kwargs))
        return FakeResponse(len(kwargs["json"]["input"]))

    def close(self):
        self.closed = True


def test_local_openai_embedding_is_batched_without_api_key(monkeypatch):
    monkeypatch.setattr(settings, "EMBEDDING_API_KEY", "")
    monkeypatch.setattr(settings, "EMBEDDING_BASE_URL", "http://embedding:11434/v1")
    monkeypatch.setattr(settings, "EMBEDDING_MODEL", "bge-m3")
    monkeypatch.setattr(settings, "EMBEDDING_PATH", "/embeddings")
    monkeypatch.setattr(settings, "EMBEDDING_DIMENSION", 1024)
    monkeypatch.setattr(settings, "EMBEDDING_BATCH_SIZE", 2)
    client = FakeClient()
    monkeypatch.setattr("backend.rag.embeddings.httpx.Client", lambda **kwargs: client)

    service = EmbeddingService()
    vectors = service.embed_documents(["新闻一", "新闻二", "新闻三"])

    assert len(vectors) == 3
    assert all(len(vector) == 1024 for vector in vectors)
    assert [call[1]["json"]["input"] for call in client.calls] == [
        ["新闻一", "新闻二"],
        ["新闻三"],
    ]
    assert all(call[0] == "http://embedding:11434/v1/embeddings" for call in client.calls)
    assert all("Authorization" not in call[1]["headers"] for call in client.calls)
    assert all(call[1]["json"]["model"] == "bge-m3" for call in client.calls)


def test_remote_embedding_uses_optional_bearer_token(monkeypatch):
    monkeypatch.setattr(settings, "EMBEDDING_API_KEY", "local-secret")
    monkeypatch.setattr(settings, "EMBEDDING_BASE_URL", "https://embedding.example/v1")
    monkeypatch.setattr(settings, "EMBEDDING_MODEL", "bge-m3")
    monkeypatch.setattr(settings, "EMBEDDING_PATH", "/embeddings")
    monkeypatch.setattr(settings, "EMBEDDING_DIMENSION", 1024)
    monkeypatch.setattr(settings, "EMBEDDING_BATCH_SIZE", 16)
    client = FakeClient()
    monkeypatch.setattr("backend.rag.embeddings.httpx.Client", lambda **kwargs: client)

    service = EmbeddingService()
    service.embed_query("查询")

    assert client.calls[0][1]["headers"]["Authorization"] == "Bearer local-secret"
