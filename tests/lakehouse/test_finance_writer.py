from __future__ import annotations


def test_new_daily_schema_accepts_unknown_provider_fields():
    import pytest
    pytest.importorskip("pyarrow")
    from backend.lakehouse.finance_tables import DAILY_BARS_ARROW_SCHEMA, DAILY_BARS_SCHEMA
    for name in ("is_st", "is_suspended", "adjustment_factor"):
        assert DAILY_BARS_ARROW_SCHEMA.field(name).nullable
        assert not DAILY_BARS_SCHEMA.find_field(name).required

from datetime import UTC, date, datetime
from types import SimpleNamespace
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from threading import Lock

from backend.lakehouse.finance_writer import FinanceWriter
from backend.schemas.finance import DailyBarRecord


def _daily_bar(*, close: float = 10.0) -> DailyBarRecord:
    observed_at = datetime(2026, 10, 24, 9, tzinfo=UTC)
    return DailyBarRecord(
        security_id="600519.SH",
        symbol="600519",
        exchange="SH",
        trading_date=date(2026, 10, 23),
        open=9.8,
        high=10.2,
        low=9.7,
        close=close,
        volume=1000,
        adjustment_type="qfq",
        provider="baostock",
        source_version="2026-10-24",
        payload_hash="same-payload",
        raw_object_path="finance/requests/example.json",
        event_time=datetime(2026, 10, 23, tzinfo=UTC),
        available_at=observed_at,
        ingested_at=observed_at,
    )


def _writer(
    monkeypatch,
    versions: dict | None = None,
) -> tuple[FinanceWriter, dict[str, int]]:
    writer = FinanceWriter.__new__(FinanceWriter)
    writer._get_arrow_module = lambda: SimpleNamespace(
        Table=SimpleNamespace(
            from_pylist=lambda rows, schema=None: rows,
        )
    )
    writer._existing_daily_versions = lambda keys: versions or {}
    calls = {"append": 0, "upsert": 0}

    def fake_append(arrow_table):
        calls["append"] += 1
        return SimpleNamespace()

    def fake_upsert(arrow_table, *, join_cols, snapshot_properties):
        calls["upsert"] += 1
        return SimpleNamespace(rows_inserted=0, rows_updated=len(arrow_table))

    writer._reload_daily_bars = lambda: SimpleNamespace(append=fake_append, upsert=fake_upsert)
    monkeypatch.setattr("backend.lakehouse.finance_writer.coordinated_write_with_retry", lambda operation, **kwargs: operation())
    return writer, calls


def test_daily_bars_use_append_fast_path_for_new_keys(monkeypatch) -> None:
    writer, calls = _writer(monkeypatch)

    written = writer.append_daily_bars([_daily_bar()])

    assert written == 1
    assert calls == {"append": 1, "upsert": 0}


def test_daily_bars_skip_unchanged_replayed_rows(monkeypatch) -> None:
    record = _daily_bar()
    key = (
        record.provider,
        record.adjustment_type,
        record.security_id,
        record.trading_date,
    )
    expected_version = FinanceWriter._daily_record_version(record)
    writer, calls = _writer(monkeypatch, {key: expected_version})

    written = writer.append_daily_bars([record])

    assert written == 0
    assert calls == {"append": 0, "upsert": 0}


def test_daily_bars_use_upsert_for_changed_existing_keys(monkeypatch) -> None:
    record = _daily_bar()
    key = (
        record.provider,
        record.adjustment_type,
        record.security_id,
        record.trading_date,
    )
    old_version = (
        record.available_at,
        record.ingested_at,
        record.source_version,
        "old-payload",
    )
    writer, calls = _writer(monkeypatch, {key: old_version})

    written = writer.append_daily_bars([record])

    assert written == 1
    assert calls == {"append": 0, "upsert": 1}


def test_concurrent_daily_bar_writes_check_and_append_under_same_lock(monkeypatch) -> None:
    record = _daily_bar()
    writer = FinanceWriter.__new__(FinanceWriter)
    versions = {}
    inserted = []
    lock = Lock()

    @contextmanager
    def fake_lock():
        with lock:
            yield

    def append(rows):
        inserted.extend(rows)
        key = (record.provider, record.adjustment_type, record.security_id, record.trading_date)
        versions[key] = FinanceWriter._daily_record_version(record)

    writer._existing_daily_versions = lambda keys: dict(versions)
    writer._get_arrow_module = lambda: SimpleNamespace(Table=SimpleNamespace(from_pylist=lambda rows, schema=None: rows))
    writer._reload_daily_bars = lambda: SimpleNamespace(append=append)
    monkeypatch.setattr("backend.lakehouse.write_coordination.iceberg_write_lock", fake_lock)
    with ThreadPoolExecutor(max_workers=2) as pool:
        counts = list(pool.map(lambda _: writer.append_daily_bars([record]), range(2)))
    assert sorted(counts) == [0, 1]
    assert len(inserted) == 1
