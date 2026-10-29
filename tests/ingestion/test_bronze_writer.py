from __future__ import annotations

import io
import json
from datetime import UTC, datetime

import pytest

from backend.ingestion.writers.bronze_writer import BronzeWriter, BronzeWriterError


class FakeReadResponse:
    def __init__(self, payload: bytes) -> None:
        self.payload = payload

    def read(self) -> bytes:
        return self.payload

    def close(self) -> None:
        return None

    def release_conn(self) -> None:
        return None


class FakeMinioClient:
    def __init__(self, fail_put: bool = False) -> None:
        self.fail_put = fail_put
        self.objects: dict[str, bytes] = {}
        self.created_bucket = False

    def bucket_exists(self, bucket_name: str) -> bool:
        return self.created_bucket

    def make_bucket(self, bucket_name: str) -> None:
        self.created_bucket = True

    def put_object(self, bucket_name: str, object_name: str, data: io.BytesIO, length: int, content_type: str) -> None:
        if self.fail_put:
            raise RuntimeError("put failed")
        self.objects[object_name] = data.read()

    def get_object(self, bucket_name: str, object_name: str) -> FakeReadResponse:
        return FakeReadResponse(self.objects[object_name])

    def stat_object(self, bucket_name: str, object_name: str):
        if object_name not in self.objects:
            raise KeyError(object_name)
        return object()

    def list_objects(self, bucket_name: str, prefix: str, recursive: bool):
        return [type("Obj", (), {"object_name": key}) for key in self.objects if key.startswith(prefix)]


def test_bronze_writer_success() -> None:
    client = FakeMinioClient()
    writer = BronzeWriter(client=client)

    object_path = writer.write_news_raw(
        provider="juhe",
        query="NVDA",
        raw_response={"articles": []},
        start_time=None,
        end_time=None,
        limit=10,
        ingested_at=datetime(2026, 7, 14, 1, 0, tzinfo=UTC),
    )

    assert object_path.startswith("news/provider=juhe/ingest_date=2026-07-14/hour=01/")
    saved = writer.read_json(object_path)
    assert saved["query"] == "NVDA"


def test_bronze_writer_failure() -> None:
    client = FakeMinioClient(fail_put=True)
    writer = BronzeWriter(client=client)

    with pytest.raises(BronzeWriterError):
        writer.write_news_raw(
            provider="juhe",
            query="NVDA",
            raw_response={"articles": []},
            start_time=None,
            end_time=None,
            limit=10,
        )


def test_finance_bronze_writer_uses_shared_layout() -> None:
    client = FakeMinioClient()
    writer = BronzeWriter(client=client)

    object_path = writer.write_finance_raw(
        provider="baostock",
        raw_response={"daily_bars": []},
        request_params={"symbols": ["600519.SH"], "adjustment_type": "qfq"},
        ingested_at=datetime(2026, 7, 14, 9, 0, tzinfo=UTC),
        source_version="snapshot-1",
    )

    assert object_path.startswith("finance/provider=baostock/ingest_date=2026-07-14/hour=09/")
    saved = writer.read_json(object_path)
    assert saved["provider"] == "baostock"
    assert saved["request_params"]["adjustment_type"] == "qfq"


def test_finance_bronze_writer_reuses_object_path_for_same_idempotency_key() -> None:
    client = FakeMinioClient()
    writer = BronzeWriter(client=client)
    common = {
        "provider": "baostock",
        "raw_response": {"daily_bars": []},
        "request_params": {"symbols": ["600519.SH"], "adjustment_type": "qfq"},
        "ingested_at": datetime(2026, 7, 14, 9, 0, tzinfo=UTC),
        "object_key": "job-123",
    }

    first = writer.write_finance_raw(**common)
    second = writer.write_finance_raw(
        **{
            **common,
            "ingested_at": datetime(2026, 7, 14, 11, 0, tzinfo=UTC),
        }
    )

    assert first == second
    assert first.startswith("finance/provider=baostock/requests/")
    assert len(client.objects) == 1
