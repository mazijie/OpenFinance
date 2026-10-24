"""Pipeline to normalize Bronze news into Silver Iceberg table."""

from __future__ import annotations

import argparse
import json
from collections.abc import Iterable
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse
from uuid import NAMESPACE_URL, uuid5

from backend.config import settings
from backend.ingestion.writers.bronze_writer import BronzeWriter
from backend.lakehouse.news_writer import NewsWriter
from backend.logging_utils import configure_logging, get_logger
from backend.schemas.news import BronzeNewsObject, NewsArticleRecord
from backend.services.security_alias_service import build_security_matcher
from backend.utils.hash import sha256_text
from backend.utils.symbols import SymbolMatcher
from backend.utils.time import ensure_utc, parse_possible_datetime

logger = get_logger(__name__)


@dataclass(slots=True)
class PipelineStats:
    read_count: int = 0
    filtered_count: int = 0
    duplicate_count: int = 0
    failed_count: int = 0
    written_count: int = 0
    processed_objects: int = 0

    def merge(self, other: "PipelineStats") -> None:
        """Merge stats from another run."""

        self.read_count += other.read_count
        self.filtered_count += other.filtered_count
        self.duplicate_count += other.duplicate_count
        self.failed_count += other.failed_count
        self.written_count += other.written_count
        self.processed_objects += other.processed_objects

    def to_dict(self) -> dict[str, int]:
        """Return JSON-serializable stats."""

        return asdict(self)


class NewsBronzeToSilverPipeline:
    """Normalize Bronze JSON files into the Silver Iceberg table."""

    def __init__(self, bronze_writer: BronzeWriter | None = None, news_writer: NewsWriter | None = None) -> None:
        self.bronze_writer = bronze_writer or BronzeWriter()
        self.news_writer = news_writer or NewsWriter()
        if news_writer is None:
            try:
                self.symbol_matcher = build_security_matcher(base_aliases=settings.news_symbol_aliases)
            except Exception:
                self.symbol_matcher = SymbolMatcher(settings.news_symbol_aliases)
        else:
            self.symbol_matcher = SymbolMatcher(settings.news_symbol_aliases)

    def run(
        self,
        object_paths: Iterable[str] | None = None,
        *,
        prefix: str | None = None,
    ) -> PipelineStats:
        """Run normalization for one or many Bronze objects."""

        paths = list(object_paths) if object_paths is not None else self.bronze_writer.list_objects(prefix=prefix or "news/provider=juhe/")
        stats = PipelineStats()
        for object_path in paths:
            object_stats = self.process_object(object_path)
            stats.merge(object_stats)

        logger.info(
            "Bronze 到 Silver 处理完成",
            event="news_bronze_to_silver_completed",
            extra_fields=stats.to_dict(),
        )
        return stats

    def process_object(self, object_path: str) -> PipelineStats:
        """Process a single Bronze JSON object with idempotency."""

        payload = self.bronze_writer.read_json(object_path)
        bronze_object = BronzeNewsObject.model_validate(payload)
        stats = PipelineStats(processed_objects=1)
        raw_articles = list(bronze_object.raw_response.get("articles") or [])
        stats.read_count = len(raw_articles)

        prepared_records: list[NewsArticleRecord] = []
        batch_hashes: set[str] = set()

        for article in raw_articles:
            try:
                record = self._normalize_article(
                    provider=bronze_object.provider,
                    article=article,
                    raw_object_path=object_path,
                    ingested_at=bronze_object.ingested_at,
                )
            except ValueError:
                stats.filtered_count += 1
                continue
            except Exception:
                stats.failed_count += 1
                continue

            if record.content_hash in batch_hashes:
                stats.duplicate_count += 1
                continue

            batch_hashes.add(record.content_hash)
            prepared_records.append(record)

        existing_hashes = self.news_writer.existing_content_hashes(record.content_hash for record in prepared_records)
        to_write = []
        for record in prepared_records:
            if record.content_hash in existing_hashes:
                stats.duplicate_count += 1
            else:
                to_write.append(record)

        stats.written_count = self.news_writer.append_records(to_write)
        return stats

    def _normalize_article(
        self,
        *,
        provider: str,
        article: dict[str, Any],
        raw_object_path: str,
        ingested_at: datetime,
    ) -> NewsArticleRecord:
        title = self._normalize_title(str(article.get("title") or ""))
        if not title:
            raise ValueError("empty title")

        url = self._normalize_url(str(article.get("url") or ""))
        if not url:
            raise ValueError("invalid url")

        published_at = parse_possible_datetime(article.get("date")) or parse_possible_datetime(article.get("published")) or parse_possible_datetime(article.get("datetime")) or ensure_utc(ingested_at)
        assert published_at is not None

        external_news_id = self._normalize_optional_text(article.get("uniquekey") or article.get("external_news_id"))
        content = self._normalize_optional_text(article.get("content"))
        if not content:
            raise ValueError("empty content")

        summary = self._normalize_optional_text(article.get("summary") or article.get("abstract") or article.get("title"))
        source_name = self._normalize_optional_text(article.get("author_name") or article.get("source_name") or article.get("source"))
        category = self._normalize_optional_text(article.get("category"))
        cover_image_url = self._normalize_optional_text(article.get("thumbnail_pic_s") or article.get("cover_image_url"))
        language = self._normalize_optional_text(article.get("language")) or "zh-cn"
        symbols, entities = self.symbol_matcher.extract(title, summary, content)
        content_hash = sha256_text(f"{title.casefold()}|{url.casefold()}")

        return NewsArticleRecord(
            news_id=str(uuid5(NAMESPACE_URL, content_hash)),
            external_news_id=external_news_id,
            provider=provider,
            category=category,
            source_name=source_name,
            title=title,
            summary=summary,
            content=content,
            url=url,
            cover_image_url=cover_image_url,
            published_at=published_at,
            ingested_at=ensure_utc(ingested_at) or datetime.now(UTC),
            language=language,
            symbols=symbols,
            entities=entities,
            content_hash=content_hash,
            raw_object_path=raw_object_path,
        )

    def _normalize_title(self, value: str) -> str:
        return " ".join(value.split()).strip()

    def _normalize_optional_text(self, value: Any) -> str | None:
        if value is None:
            return None
        normalized = " ".join(str(value).split()).strip()
        return normalized or None

    def _normalize_url(self, value: str) -> str | None:
        parsed = urlparse(value.strip())
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            return None

        filtered_query = [
            (k, v) for k, v in parse_qsl(parsed.query, keep_blank_values=False) if not k.lower().startswith("utm_")
        ]
        normalized = parsed._replace(
            scheme=parsed.scheme.lower(),
            netloc=parsed.netloc.lower(),
            fragment="",
            query=urlencode(filtered_query, doseq=True),
        )
        return urlunparse(normalized)


def build_parser() -> argparse.ArgumentParser:
    """Build CLI parser."""

    parser = argparse.ArgumentParser(description="Normalize Bronze news into Silver")
    parser.add_argument("--object-path", action="append", dest="object_paths", help="Optional Bronze object path to process")
    parser.add_argument("--object-paths-json", help="JSON array of Bronze object paths to process")
    parser.add_argument("--prefix", help="Optional Bronze object prefix to scan")
    return parser


def _resolve_object_paths(object_paths: list[str] | None, object_paths_json: str | None) -> list[str] | None:
    if object_paths_json:
        return [str(item).strip() for item in json.loads(object_paths_json) if str(item).strip()]
    if object_paths:
        return object_paths
    return None


def main() -> None:
    """CLI entry point."""

    configure_logging()
    args = build_parser().parse_args()
    pipeline = NewsBronzeToSilverPipeline()
    resolved_object_paths = _resolve_object_paths(args.object_paths, args.object_paths_json)
    stats = pipeline.run(object_paths=resolved_object_paths, prefix=args.prefix)
    print(json.dumps(stats.to_dict(), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
