from __future__ import annotations

from datetime import UTC, datetime

from backend.lakehouse.news_writer import NewsWriter
from backend.schemas.news import NewsArticleRecord


class FakeScanResult:
    def __init__(self, rows):
        self._rows = rows

    def to_arrow(self):
        class FakeArrowTable:
            def __init__(self, rows):
                self._rows = rows

            def to_pylist(self):
                return list(self._rows)

            @property
            def num_rows(self):
                return len(self._rows)

            def column(self, name):
                class FakeColumn:
                    def __init__(self, rows, key):
                        self.rows = rows
                        self.key = key

                    def to_pylist(self):
                        return [row.get(self.key) for row in self.rows]

                return FakeColumn(self._rows, name)

        return FakeArrowTable(self._rows)


class FakeTable:
    def __init__(self) -> None:
        self.rows: list[dict] = []

    def append(self, arrow_table) -> None:
        self.rows.extend(arrow_table.to_pylist())

    def scan(self):
        return FakeScanResult(self.rows)


class FakeCatalog:
    def __init__(self) -> None:
        self.namespace_created = False
        self.table = FakeTable()
        self.table_created = False

    def load_namespace_properties(self, namespace):
        if not self.namespace_created:
            raise KeyError("missing")
        return {}

    def create_namespace(self, namespace):
        self.namespace_created = True

    def load_table(self, identifier):
        if not self.table_created:
            raise KeyError("missing")
        return self.table

    def create_table(self, identifier, schema, partition_spec):
        self.table_created = True
        return self.table


def test_iceberg_table_auto_create_and_read_write(monkeypatch) -> None:
    fake_catalog = FakeCatalog()
    monkeypatch.setattr("backend.lakehouse.tables.get_catalog", lambda: fake_catalog)

    writer = NewsWriter()
    written = writer.append_records(
        [
            NewsArticleRecord(
                news_id="1",
                provider="juhe",
                source_name="Example",
                title="NVIDIA launches new GPU",
                summary="Summary",
                url="https://example.com/news",
                published_at=datetime(2026, 7, 14, 1, 0, tzinfo=UTC),
                ingested_at=datetime(2026, 7, 14, 2, 0, tzinfo=UTC),
                language="en",
                symbols=["NVDA"],
                entities=["nvidia"],
                content_hash="hash-1",
                raw_object_path="news/provider=juhe/sample.json",
            )
        ]
    )

    rows = writer.scan_records()
    assert written == 1
    assert fake_catalog.namespace_created is True
    assert fake_catalog.table_created is True
    assert rows[0].title == "NVIDIA launches new GPU"
