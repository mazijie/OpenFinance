from datetime import UTC, datetime
from types import SimpleNamespace

from backend.rag.annual_reports.annual_report_index_reader import StarRocksAnnualReportIndexReader
from backend.maintenance.compare_annual_report_index_readers import _hashes


class Table:
    def __init__(self, snapshot_id):
        self.snapshot_id = snapshot_id
        self.metadata = SimpleNamespace(table_uuid="table-id")
        self.read_snapshot = None

    def current_snapshot(self):
        return SimpleNamespace(snapshot_id=self.snapshot_id) if self.snapshot_id is not None else None

    def scan(self, *, snapshot_id):
        self.read_snapshot = snapshot_id
        return self

    def select(self, *fields):
        self.fields = fields
        return self

    def to_arrow(self):
        return SimpleNamespace(to_pylist=lambda: [{"report_id": "r", "parse_run_id": "p",
                                                  "started_at": datetime(2026, 1, 1, tzinfo=UTC), "finished_at": None}])


class Sql:
    def __init__(self):
        self.calls = []

    def _table(self, snapshot_id, name):
        return f"{name} FOR VERSION AS OF {snapshot_id}"

    def _query(self, sql, args):
        self.calls.append((sql, args))
        return [{"report_id": "r", "parse_run_id": "p", "started_at": datetime(2026, 1, 1), "finished_at": None}]


def test_readers_use_same_pinned_snapshot_and_normalize_timestamps():
    tables = {}
    class Catalog:
        def load_table(self, name):
            table = Table(123)
            tables[name] = table
            return table
    sql = Sql()
    reader = StarRocksAnnualReportIndexReader(catalog=Catalog(), sql_reader=sql)
    expected = reader.read_rows("parse_runs", engine="pyiceberg")
    actual = reader.read_rows("parse_runs")
    assert _hashes(actual) == _hashes(expected)
    assert reader.tables["parse_runs"].read_snapshot == 123
    assert "FOR VERSION AS OF 123" in sql.calls[0][0]
    assert "SELECT *" not in sql.calls[0][0]


def test_empty_snapshot_is_not_read_as_latest():
    catalog = SimpleNamespace(load_table=lambda name: Table(None))
    sql = Sql()
    reader = StarRocksAnnualReportIndexReader(catalog=catalog, sql_reader=sql)
    assert reader.scan_chunks() == []
    assert sql.calls == []


def test_comparison_detects_duplicate_counts():
    assert _hashes([{"id": "a"}, {"id": "a"}]) != _hashes([{"id": "a"}])
