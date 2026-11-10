from datetime import date, datetime, timezone
from types import SimpleNamespace

import backend.lakehouse.market_history_index as index


def test_index_uses_matching_generation_and_visibility_window(monkeypatch):
    class Cursor:
        def __enter__(self): return self
        def __exit__(self, *args): pass
        def execute(self, sql, params):
            self.sql, self.params = sql, params
        def fetchone(self): return (date(2001, 8, 27),)
    cursor = Cursor()
    class Connection:
        def __enter__(self): return self
        def __exit__(self, *args): pass
        def cursor(self): return cursor
    monkeypatch.setattr(index, "settings", SimpleNamespace(DATABASE_URL="test"))
    monkeypatch.setattr(index.psycopg, "connect", lambda *args, **kwargs: Connection())
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)
    kwargs = dict(table_uuid="uuid", snapshot_id=7, as_of=now, symbol="600519.SH", adjustment_type="none")
    assert index.has_earlier(before=date(2025, 1, 1), **kwargs) is True
    assert index.has_earlier(before=date(2001, 8, 27), **kwargs) is False
    assert "s.snapshot_id = %s" in cursor.sql and "s.valid_from <= %s" in cursor.sql
    assert "s.valid_until IS NULL OR %s < s.valid_until" in cursor.sql
    assert cursor.params == ("600519.SH", "none", "uuid", 7, now, now)


def test_index_missing_generation_and_db_failure_are_unknown(monkeypatch):
    monkeypatch.setattr(index, "settings", SimpleNamespace(DATABASE_URL=""))
    assert index.has_earlier(table_uuid="uuid", snapshot_id=7, as_of=datetime.now(timezone.utc),
                             symbol="x", adjustment_type="none", before=date.today()) is None
