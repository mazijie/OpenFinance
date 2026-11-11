from datetime import UTC, date, datetime, timedelta, timezone

import pytest

from backend.lakehouse.starrocks_daily_reader import StarRocksDailyReader, _identifier


def test_query_pins_snapshot_filters_in_utc_and_preserves_nulls(monkeypatch):
    reader = StarRocksDailyReader()
    calls = []
    timestamp = datetime(2026, 1, 2, 3, 4, 5)
    row = dict(security_id="600519.SH", symbol="600519.SH", exchange="SH",
               trading_date=date(2026, 1, 2), open=1, high=2, low=1, close=2,
               volume=10, amount=None, turnover=None, is_suspended=None, is_st=None,
               adjustment_factor=None, adjustment_type="none", provider="tencent",
               source_record_id=None, source_version=None, payload_hash=None,
               raw_object_path="sample", event_time=timestamp, available_at=timestamp,
               ingested_at=timestamp, schema_version=1)
    monkeypatch.setattr(reader, "_query", lambda sql, args: calls.append((sql, args)) or [row])
    bars = reader.scan_daily_bars_for_query(
        symbol="600519.SH", security_id=None, start_date=date(2026, 1, 1),
        end_date=date(2026, 1, 3), adjustment_type="none",
        as_of=datetime(2026, 1, 2, 12, tzinfo=timezone(timedelta(hours=8))),
        limit=500, snapshot_id=123,
    )
    assert "FOR VERSION AS OF 123" in calls[0][0]
    assert calls[0][1][-1] == datetime(2026, 1, 2, 4)
    assert bars[0].available_at == timestamp.replace(tzinfo=UTC)
    assert bars[0].is_st is None and bars[0].amount is None


def test_empty_snapshot_never_queries_latest(monkeypatch):
    reader = StarRocksDailyReader()
    monkeypatch.setattr(reader, "_query", lambda *args: pytest.fail("unexpected query"))
    assert reader.scan_daily_bars_for_query(symbol="x", security_id=None, start_date=date.min,
        end_date=date.max, adjustment_type="none", as_of=datetime.now(UTC), limit=1) == []
    assert not reader.has_daily_bars_before(symbol="x", adjustment_type="none", before=date.today(),
                                            as_of=datetime.now(UTC))


def test_rejects_unsafe_identifier_and_snapshot():
    with pytest.raises(ValueError):
        _identifier("daily_bars; DROP TABLE x")
    with pytest.raises(ValueError):
        StarRocksDailyReader()._table("1 OR 1=1")


def test_older_query_uses_same_snapshot_and_cutoffs(monkeypatch):
    reader = StarRocksDailyReader()
    calls = []
    monkeypatch.setattr(reader, "_query", lambda sql, args: calls.append((sql, args)) or [{"1": 1}])
    assert reader.has_daily_bars_before(symbol="x", adjustment_type="none", before=date(2026, 1, 1),
                                        as_of=datetime(2026, 2, 1, tzinfo=UTC), snapshot_id=88)
    assert "FOR VERSION AS OF 88" in calls[0][0]
    assert calls[0][1][-2:] == (datetime(2026, 2, 1),) * 2
