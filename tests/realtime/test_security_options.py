from backend.lakehouse.starrocks_readers import StarRocksReader


def test_security_options_deduplicates_by_exchange_and_keeps_latest_name(monkeypatch):
    rows = [
        {'symbol': '600519.SH', 'exchange': 'SH', 'name': '旧名', 'ingested_at': '2026-11-05'},
        {'symbol': '600519', 'exchange': 'SH', 'name': '贵州茅台', 'ingested_at': '2026-11-06'},
        {'symbol': '600519', 'exchange': 'SZ', 'name': '另一证券', 'ingested_at': '2026-11-05'},
    ]
    reader = StarRocksReader()
    monkeypatch.setattr(reader, '_query', lambda sql, args: rows)
    assert reader.scan_security_options() == [
        {'symbol': '600519', 'exchange': 'SH', 'name': '贵州茅台'},
        {'symbol': '600519', 'exchange': 'SZ', 'name': '另一证券'},
    ]
