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
from backend.schemas.finance import DailyBarRecord, TradingCalendarRecord


def test_calendar_upserts_in_bounded_batches_and_counts_all_rows(monkeypatch) -> None:
    from datetime import timedelta

    writer = FinanceWriter.__new__(FinanceWriter)
    writer._get_arrow_module = lambda: SimpleNamespace(
        Table=SimpleNamespace(from_pylist=lambda rows, schema=None: rows)
    )
    batches = []

    def fake_upsert(rows, *, join_cols, snapshot_properties):
        assert join_cols == ["exchange", "trading_date"]
        batches.append(rows)
        return SimpleNamespace(rows_inserted=len(rows), rows_updated=0)

    writer._reload_trading_calendar = lambda: SimpleNamespace(
        scan=lambda: SimpleNamespace(select=lambda *args: SimpleNamespace(to_arrow=lambda: SimpleNamespace(to_pylist=lambda: []))),
        upsert=fake_upsert,
    )
    monkeypatch.setattr("backend.lakehouse.finance_writer.coordinated_write_with_retry", lambda operation, **kwargs: operation())
    records = [TradingCalendarRecord(exchange="SH", trading_date=date(2000, 1, 1) + timedelta(days=i), is_open=True)
               for i in range(600)]
    assert writer.append_trading_calendar(records) == 600
    assert [len(batch) for batch in batches] == [256, 256, 88]
    assert [row["trading_date"] for batch in batches for row in batch] == [r.trading_date for r in records]


def test_calendar_replay_updates_only_changes_and_serializes_writers(monkeypatch) -> None:
    writer = FinanceWriter.__new__(FinanceWriter)
    writer._get_arrow_module = lambda: SimpleNamespace(Table=SimpleNamespace(from_pylist=lambda rows, schema=None: rows))
    data = {}
    lock = Lock()
    writes = []

    @contextmanager
    def fake_lock():
        with lock:
            yield

    class Table:
        def scan(self):
            return SimpleNamespace(select=lambda *args: SimpleNamespace(to_arrow=lambda: SimpleNamespace(to_pylist=lambda: list(data.values()))))

        def upsert(self, rows, **kwargs):
            writes.append(len(rows))
            for row in rows:
                data[(row["exchange"], row["trading_date"])] = dict(row)
            return SimpleNamespace(rows_inserted=len(rows), rows_updated=0)

    writer._reload_trading_calendar = Table
    monkeypatch.setattr("backend.lakehouse.write_coordination.iceberg_write_lock", fake_lock)
    original = TradingCalendarRecord(exchange="SH", trading_date=date(2026, 1, 1), is_open=True)
    with ThreadPoolExecutor(max_workers=2) as pool:
        assert sorted(pool.map(lambda _: writer.append_trading_calendar([original]), range(2))) == [0, 1]
    changed = original.model_copy(update={"is_open": False})
    extra = TradingCalendarRecord(exchange="SZ", trading_date=date(2026, 1, 1), is_open=True)
    assert writer.append_trading_calendar([changed, extra]) == 2
    assert writer.append_trading_calendar([changed, extra]) == 0
    assert writer.append_trading_calendar([]) == 0
    assert writes == [1, 2]
    assert data[("SH", date(2026, 1, 1))]["is_open"] is False


def test_calendar_retry_after_partial_commit_only_writes_remaining_rows(monkeypatch) -> None:
    from datetime import timedelta
    from backend.lakehouse import write_coordination

    writer = FinanceWriter.__new__(FinanceWriter)
    writer._get_arrow_module = lambda: SimpleNamespace(Table=SimpleNamespace(from_pylist=lambda rows, schema=None: rows))
    data, batches = {}, []
    lock = Lock()

    @contextmanager
    def fake_lock():
        with lock:
            yield

    class Table:
        def scan(self):
            return SimpleNamespace(select=lambda *args: SimpleNamespace(to_arrow=lambda: SimpleNamespace(to_pylist=lambda: list(data.values()))))

        def upsert(self, rows, **kwargs):
            batches.append(len(rows))
            if len(batches) == 2:
                raise write_coordination.CommitFailedException("retry test")
            for row in rows:
                data[(row["exchange"], row["trading_date"])] = dict(row)
            return SimpleNamespace(rows_inserted=len(rows), rows_updated=0)

    writer._reload_trading_calendar = Table
    monkeypatch.setattr(write_coordination, "iceberg_write_lock", fake_lock)
    monkeypatch.setattr(write_coordination.time, "sleep", lambda _: None)
    records = [TradingCalendarRecord(exchange="SH", trading_date=date(2000, 1, 1) + timedelta(days=i), is_open=True) for i in range(300)]
    assert writer.append_trading_calendar(records) == 300
    assert batches == [256, 44, 44]
    assert len(data) == 300


def test_calendar_updates_existing_iceberg_table(tmp_path, monkeypatch) -> None:
    import pytest
    pytest.importorskip("pyiceberg")
    pytest.importorskip("pyarrow")
    from pyiceberg.catalog.sql import SqlCatalog
    from backend.lakehouse.finance_tables import TRADING_CALENDAR_SCHEMA, TRADING_CALENDAR_PARTITION_SPEC

    warehouse = tmp_path / "warehouse"
    warehouse.mkdir()
    catalog = SqlCatalog("test", uri=f"sqlite:///{tmp_path / 'catalog.db'}", warehouse=warehouse.as_uri())
    catalog.create_namespace("test")
    table = catalog.create_table("test.calendar", schema=TRADING_CALENDAR_SCHEMA,
                                 partition_spec=TRADING_CALENDAR_PARTITION_SPEC)
    writer = FinanceWriter.__new__(FinanceWriter)
    writer._reload_trading_calendar = lambda: catalog.load_table("test.calendar")
    monkeypatch.setattr("backend.lakehouse.finance_writer.coordinated_write_with_retry", lambda operation, **kwargs: operation())
    day = TradingCalendarRecord(exchange="SH", trading_date=date(2026, 1, 1), is_open=True)
    assert writer.append_trading_calendar([day]) == 1
    assert writer.append_trading_calendar([day]) == 0
    assert writer.append_trading_calendar([day.model_copy(update={"is_open": False})]) == 1
    assert catalog.load_table("test.calendar").scan().to_arrow().to_pylist()[0]["is_open"] is False


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
