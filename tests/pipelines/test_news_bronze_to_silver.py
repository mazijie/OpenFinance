from __future__ import annotations

from datetime import UTC, datetime

from backend.pipelines.news_bronze_to_silver import NewsBronzeToSilverPipeline
from backend.schemas.news import NewsArticleRecord


class FakeBronzeWriter:
    def __init__(self, payload: dict) -> None:
        self.payload = payload

    def read_json(self, object_path: str) -> dict:
        return self.payload

    def list_objects(self) -> list[str]:
        return ["news/provider=juhe/ingest_date=2026-07-14/hour=00/sample.json"]


class FakeNewsWriter:
    def __init__(self) -> None:
        self.records: list[NewsArticleRecord] = []

    def existing_content_hashes(self, content_hashes):
        wanted = set(content_hashes)
        return {record.content_hash for record in self.records if record.content_hash in wanted}

    def append_records(self, records: list[NewsArticleRecord]) -> int:
        self.records.extend(records)
        return len(records)

    def scan_records(self) -> list[NewsArticleRecord]:
        return list(self.records)


def _bronze_payload() -> dict:
    return {
        "provider": "juhe",
        "ingested_at": "2026-07-14T00:00:00Z",
        "query": "NVIDIA",
        "limit": 10,
        "raw_response": {
            "articles": [
                {
                    "title": " NVIDIA launches new GPU ",
                    "url": "https://example.com/news?id=1&utm_source=test",
                    "sourcecommonname": "Example",
                    "language": "en",
                    "seendate": "20260714T010203Z",
                    "snippet": "NVIDIA announced a new product for NVDA investors.",
                    "content": "NVIDIA announced a new product for NVDA investors.",
                },
                {
                    "title": "NVIDIA launches new GPU",
                    "url": "https://example.com/news?id=1",
                    "sourcecommonname": "Example",
                    "language": "en",
                    "seendate": "20260714T010203Z",
                    "snippet": "Duplicate row",
                    "content": "NVIDIA announced a new product for NVDA investors.",
                },
                {
                    "title": "",
                    "url": "https://example.com/empty-title",
                },
                {
                    "title": "Bad url",
                    "url": "not-a-url",
                },
                {
                    "title": "Missing content",
                    "url": "https://example.com/missing-content",
                    "content": "",
                },
                {
                    "title": "Whitespace content",
                    "url": "https://example.com/whitespace-content",
                    "content": "   ",
                },
            ]
        },
    }


def test_pipeline_cleans_and_deduplicates() -> None:
    writer = FakeNewsWriter()
    pipeline = NewsBronzeToSilverPipeline(bronze_writer=FakeBronzeWriter(_bronze_payload()), news_writer=writer)
    pipeline.symbol_matcher = pipeline.symbol_matcher.__class__({"NVDA": ["NVIDIA", "英伟达"]})

    stats = pipeline.process_object("news/provider=juhe/ingest_date=2026-07-14/hour=00/sample.json")

    assert stats.read_count == 6
    assert stats.filtered_count == 4
    assert stats.duplicate_count == 1
    assert stats.written_count == 1
    assert writer.records[0].symbols == ["NVDA"]


def test_pipeline_is_idempotent_for_same_bronze_file() -> None:
    writer = FakeNewsWriter()
    pipeline = NewsBronzeToSilverPipeline(bronze_writer=FakeBronzeWriter(_bronze_payload()), news_writer=writer)
    pipeline.symbol_matcher = pipeline.symbol_matcher.__class__({"NVDA": ["NVIDIA"]})

    first = pipeline.process_object("news/provider=juhe/ingest_date=2026-07-14/hour=00/sample.json")
    second = pipeline.process_object("news/provider=juhe/ingest_date=2026-07-14/hour=00/sample.json")

    assert first.written_count == 1
    assert second.written_count == 0
    assert second.duplicate_count >= 1
