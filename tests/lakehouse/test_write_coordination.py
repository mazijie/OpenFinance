from __future__ import annotations

import threading
import time

import pytest

from backend.lakehouse.write_coordination import CommitFailedException, append_with_retry, upsert_with_retry


class FakeTable:
    def __init__(self, failures: int = 0) -> None:
        self.failures = failures
        self.append_calls = 0

    def append(self, arrow_table) -> None:
        self.append_calls += 1
        if self.append_calls <= self.failures:
            raise CommitFailedException("snapshot changed")


def test_append_reloads_and_retries_after_commit_conflict(monkeypatch) -> None:
    table = FakeTable(failures=1)
    loads = 0

    def load_table():
        nonlocal loads
        loads += 1
        return table

    monkeypatch.setattr("backend.lakehouse.write_coordination.settings.ICEBERG_WRITE_MAX_RETRIES", 2)
    monkeypatch.setattr("backend.lakehouse.write_coordination.time.sleep", lambda _: None)

    append_with_retry(load_table, object(), table_name="test_table")

    assert table.append_calls == 2
    assert loads >= 3


def test_append_raises_after_configured_retries(monkeypatch) -> None:
    table = FakeTable(failures=10)
    monkeypatch.setattr("backend.lakehouse.write_coordination.settings.ICEBERG_WRITE_MAX_RETRIES", 2)
    monkeypatch.setattr("backend.lakehouse.write_coordination.time.sleep", lambda _: None)

    with pytest.raises(CommitFailedException):
        append_with_retry(lambda: table, object(), table_name="test_table")

    assert table.append_calls == 3


def test_fallback_lock_serializes_concurrent_appends(monkeypatch) -> None:
    active = 0
    peak_active = 0
    guard = threading.Lock()

    class SlowTable:
        def append(self, arrow_table) -> None:
            nonlocal active, peak_active
            with guard:
                active += 1
                peak_active = max(peak_active, active)
            time.sleep(0.05)
            with guard:
                active -= 1

    monkeypatch.setattr("backend.lakehouse.write_coordination.psycopg", None)
    table = SlowTable()
    threads = [threading.Thread(target=append_with_retry, args=(lambda: table, object()), kwargs={"table_name": "test_table"}) for _ in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert peak_active == 1


def test_upsert_retries_and_passes_business_keys(monkeypatch) -> None:
    class FakeUpsertTable:
        def __init__(self) -> None:
            self.calls = 0

        def upsert(self, arrow_table, *, join_cols, snapshot_properties):
            self.calls += 1
            if self.calls == 1:
                raise CommitFailedException("snapshot changed")
            assert join_cols == ["security_id", "trading_date"]
            assert snapshot_properties == {"openfinance.write-mode": "upsert"}
            return type("Result", (), {"rows_inserted": 2, "rows_updated": 1})()

    monkeypatch.setattr("backend.lakehouse.write_coordination.settings.ICEBERG_WRITE_MAX_RETRIES", 2)
    monkeypatch.setattr("backend.lakehouse.write_coordination.time.sleep", lambda _: None)
    table = FakeUpsertTable()

    result = upsert_with_retry(
        lambda: table,
        object(),
        table_name="test_table",
        join_cols=["security_id", "trading_date"],
    )

    assert table.calls == 2
    assert result.rows_inserted == 2
    assert result.rows_updated == 1
