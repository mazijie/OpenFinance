"""Silver writer for news articles."""

from __future__ import annotations

from collections.abc import Iterable

from backend.lakehouse.tables import NEWS_TABLE_ARROW_SCHEMA, ensure_news_table
from backend.logging_utils import get_logger
from backend.lakehouse.write_coordination import append_with_retry
from backend.schemas.news import NewsArticleRecord

logger = get_logger(__name__)


class _SimpleColumn:
    def __init__(self, values: list[object]) -> None:
        self._values = values

    def to_pylist(self) -> list[object]:
        return list(self._values)


class _SimpleArrowTable:
    def __init__(self, rows: list[dict]) -> None:
        self._rows = rows

    @classmethod
    def from_pylist(cls, rows: list[dict], schema=None):  # noqa: ANN001
        return cls(list(rows))

    @property
    def num_rows(self) -> int:
        return len(self._rows)

    def to_pylist(self) -> list[dict]:
        return list(self._rows)

    def column(self, name: str) -> _SimpleColumn:
        return _SimpleColumn([row.get(name) for row in self._rows])


class NewsWriter:
    """Write and read Silver news records from Iceberg."""

    def __init__(self) -> None:
        self.table = ensure_news_table()

    def existing_content_hashes(self, content_hashes: Iterable[str]) -> set[str]:
        """Return content hashes already present in the table."""

        wanted = {value for value in content_hashes if value}
        if not wanted:
            return set()

        arrow_table = self._scan_arrow_table()
        if arrow_table.num_rows == 0:
            return set()

        existing = set()
        hash_column = arrow_table.column("content_hash").to_pylist()
        for content_hash in hash_column:
            if content_hash in wanted:
                existing.add(content_hash)
        return existing

    def append_records(self, records: list[NewsArticleRecord]) -> int:
        """Append records to Iceberg table."""

        if not records:
            return 0

        arrow_module = self._get_arrow_module()
        arrow_table = arrow_module.Table.from_pylist(
            [
                {
                    "news_id": record.news_id,
                    "external_news_id": record.external_news_id,
                    "provider": record.provider,
                    "category": record.category,
                    "source_name": record.source_name,
                    "title": record.title,
                    "summary": record.summary,
                    "content": record.content,
                    "url": str(record.url),
                    "cover_image_url": record.cover_image_url,
                    "published_at": record.published_at,
                    "ingested_at": record.ingested_at,
                    "language": record.language,
                    "symbols": record.symbols,
                    "entities": record.entities,
                    "content_hash": record.content_hash,
                    "raw_object_path": record.raw_object_path,
                }
                for record in records
            ],
            schema=NEWS_TABLE_ARROW_SCHEMA,
        )
        append_with_retry(self._reload_table, arrow_table, table_name="news_articles")
        logger.info(
            "Silver 新闻写入完成",
            event="silver_news_written",
            extra_fields={"written_count": len(records)},
        )
        return len(records)

    def scan_records(self) -> list[NewsArticleRecord]:
        """Return all news records from Iceberg."""

        arrow_table = self._scan_arrow_table()
        return [NewsArticleRecord.model_validate(row) for row in arrow_table.to_pylist()]

    def existing_external_ids(self, external_ids: Iterable[str]) -> set[str]:
        """Return external news ids already present in the table."""

        wanted = {value for value in external_ids if value}
        if not wanted:
            return set()

        arrow_table = self._scan_arrow_table()
        if arrow_table.num_rows == 0:
            return set()

        existing = set()
        external_id_column = arrow_table.column("external_news_id").to_pylist()
        for external_id in external_id_column:
            if external_id in wanted:
                existing.add(external_id)
        return existing

    def all_external_ids(self) -> set[str]:
        """Return all external news ids stored in the table."""

        arrow_table = self._scan_arrow_table()
        if arrow_table.num_rows == 0:
            return set()
        return {value for value in arrow_table.column("external_news_id").to_pylist() if value}

    def _scan_arrow_table(self):
        return self.table.scan().to_arrow()

    def _reload_table(self):
        self.table = ensure_news_table()
        return self.table

    def _get_arrow_module(self):
        try:  # pragma: no cover - exercised in real environment
            import pyarrow as pa  # type: ignore
        except ImportError:  # pragma: no cover - test fallback
            class _ArrowModule:
                Table = _SimpleArrowTable

            return _ArrowModule()
        return pa
