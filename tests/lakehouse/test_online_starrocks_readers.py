from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, date, datetime
from types import SimpleNamespace

import pytest

from backend.lakehouse import catalog
from backend.lakehouse.starrocks_daily_reader import StarRocksDailyReader, StarRocksUnavailable
from backend.lakehouse.starrocks_readers import StarRocksReader


def test_catalog_created_once_across_threads_and_disposed(monkeypatch):
    catalog.close_catalog()
    created = []
    disposed = []
    def load():
        item = SimpleNamespace(engine=SimpleNamespace(dispose=lambda: disposed.append(True)))
        created.append(item)
        return item
    monkeypatch.setattr(catalog, '_load_catalog', load)
    with ThreadPoolExecutor(max_workers=16) as pool:
        results = list(pool.map(lambda _: catalog.get_catalog(), range(100)))
    assert len(created) == 1
    assert all(item is created[0] for item in results)
    catalog.close_catalog()
    assert disposed == [True]


def test_calendar_and_security_readers_use_parameterized_starrocks(monkeypatch):
    reader = StarRocksReader()
    queries = []
    def query(sql, args):
        queries.append((sql, args))
        if 'trading_calendar' in sql:
            return [{'exchange': 'SH', 'trading_date': date(2026, 11, 12), 'is_open': True}]
        return [{'symbol': '600519.SH', 'name': 'old', 'exchange': 'sh', 'ingested_at': datetime(2025, 1, 1)},
                {'symbol': '600519.SH', 'name': 'new', 'exchange': 'sh', 'ingested_at': datetime(2026, 1, 1)}]
    monkeypatch.setattr(reader, '_query', query)
    assert reader.scan_trading_calendar_for_date(date(2026, 11, 12))[0].is_open
    assert queries[0][1] == (date(2026, 11, 12),)
    assert '%s' in queries[0][0]
    assert reader.scan_security_options() == [{'symbol': '600519', 'exchange': 'SH', 'name': 'new'}]


def test_latest_query_does_not_require_snapshot(monkeypatch):
    reader = StarRocksReader()
    queries = []
    monkeypatch.setattr(reader, '_query', lambda sql, args: queries.append(sql) or [])
    reader.scan_daily_bars_for_query(symbol='X', security_id=None, start_date=date(2026, 1, 1),
        end_date=date(2026, 2, 1), adjustment_type='none', as_of=datetime.now(UTC), limit=10)
    assert len(queries) == 1 and 'FOR VERSION AS OF' not in queries[0]


def test_query_closes_connection_on_error(monkeypatch):
    import pymysql
    closed = []
    class Cursor:
        def __enter__(self): return self
        def __exit__(self, *args): pass
        def execute(self, *args): raise OSError('failed')
    class Connection:
        def cursor(self): return Cursor()
        def close(self): closed.append(True)
    monkeypatch.setattr(pymysql, 'connect', lambda **kwargs: Connection())
    with pytest.raises(StarRocksUnavailable):
        StarRocksDailyReader()._query('SELECT 1', ())
    assert closed == [True]


def test_annual_report_rows_are_filtered_and_decoded(monkeypatch):
    reader = StarRocksReader()
    calls = []
    monkeypatch.setattr(reader, '_query', lambda sql, args: calls.append((sql, args)) or [{
        'report_id': 'r', 'parse_run_id': 'p', 'region_id': 'region', 'page_number': 1,
        'region_type': 'paragraph', 'bbox': '[0,1,2,3]', 'reading_order': 1,
        'text': 'hello', 'content_ref': None,
    }])
    assert reader.scan_regions('r', 'p')[0].bbox == [0, 1, 2, 3]
    assert calls[0][1] == ('r', 'p')
    assert '`report_id` = %s' in calls[0][0]


def test_news_decodes_arrays_and_normalizes_utc(monkeypatch):
    reader = StarRocksReader()
    monkeypatch.setattr(reader, '_query', lambda sql, args: [{
        'news_id': 'n', 'provider': 'p', 'title': 'title', 'url': 'https://example.com',
        'published_at': datetime(2026, 1, 1), 'ingested_at': datetime(2026, 1, 2),
        'symbols': '["A"]', 'entities': '[]', 'content_hash': 'h', 'raw_object_path': 'x',
    }])
    record = reader.scan_records()[0]
    assert record.symbols == ['A'] and record.entities == []
    assert record.published_at.tzinfo == UTC


def test_news_page_pushes_pagination_and_preserves_empty_page_total(monkeypatch):
    reader = StarRocksReader()
    calls = []
    monkeypatch.setattr(reader, '_query', lambda sql, args: calls.append((sql, args)) or [{'total': 3, 'news_id': None}])
    rows, total = reader.news_page(page=4, page_size=2)
    assert rows == [] and total == 3
    assert calls[0][1] == (2, 6)
    assert 'COUNT(*)' in calls[0][0] and 'LIMIT %s OFFSET %s' in calls[0][0]


def test_news_search_dates_and_report_id_are_bound(monkeypatch):
    reader = StarRocksReader()
    calls = []
    monkeypatch.setattr(reader, '_query', lambda sql, args: calls.append((sql, args)) or [])
    start = datetime(2026, 1, 1, tzinfo=UTC)
    end = datetime(2026, 1, 2, tzinfo=UTC)
    reader.scan_records(start_at=start, end_at=end)
    reader.scan_reports("report'1")
    assert calls[0][1] == (start.replace(tzinfo=None), end.replace(tzinfo=None))
    assert '`published_at` >= %s' in calls[0][0]
    assert calls[1][1] == ("report'1",) and '`report_id` = %s' in calls[1][0]


def test_online_repositories_have_no_iceberg_reader_switch():
    from backend.config import settings
    from backend.repositories.finance_repository import FinanceRepository
    from backend.repositories.news_repository import NewsRepository

    assert not hasattr(settings, 'MARKET_DAILY_READER')
    assert isinstance(FinanceRepository().writer, StarRocksReader)
    assert isinstance(NewsRepository().writer, StarRocksReader)
