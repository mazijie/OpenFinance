from backend.lakehouse.finance_writer import FinanceWriter


def test_security_options_deduplicates_by_exchange_and_prefers_tencent():
    rows = [
        {'symbol': '600519.SH', 'exchange': 'SH', 'name': '旧名', 'provider': 'baostock', 'ingested_at': '2026-11-06'},
        {'symbol': '600519', 'exchange': 'SH', 'name': '贵州茅台', 'provider': 'tencent', 'ingested_at': '2026-11-05'},
        {'symbol': '600519', 'exchange': 'SZ', 'name': '另一证券', 'provider': 'tencent', 'ingested_at': '2026-11-05'},
    ]
    class Scan:
        def select(self, *args): return self
        def to_arrow(self): return self
        def to_pylist(self): return rows
    class Table:
        def scan(self): return Scan()
    writer = object.__new__(FinanceWriter)
    writer.security_master_table = Table()
    assert writer.scan_security_options() == [
        {'symbol': '600519', 'exchange': 'SH', 'name': '贵州茅台'},
        {'symbol': '600519', 'exchange': 'SZ', 'name': '另一证券'},
    ]
