"""CLI job to ingest finance news into Bronze."""

from __future__ import annotations

import argparse
import asyncio
import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta

from backend.config import settings
from backend.ingestion.collectors.caixin import CaixinCollector
from backend.ingestion.writers.bronze_writer import BronzeWriter
from backend.logging_utils import configure_logging, get_logger
from backend.lakehouse.news_writer import NewsWriter
from backend.utils.time import ensure_utc, parse_possible_datetime

logger = get_logger(__name__)


@dataclass(slots=True)
class IngestNewsResult:
    """Result returned by one ingestion execution."""

    category: str
    object_path: str | None
    article_count: int
    start_time: datetime | None
    end_time: datetime | None
    limit: int | None

    def to_dict(self) -> dict[str, object]:
        """Serialize result for logging or Airflow XCom."""

        payload = asdict(self)
        for key in ("start_time", "end_time"):
            value = payload[key]
            if isinstance(value, datetime):
                payload[key] = value.isoformat()
        return payload


async def ingest_finance_news(
    *,
    start_time: datetime | None = None,
    end_time: datetime | None = None,
    limit: int | None = None,
) -> IngestNewsResult:
    """Collect Caixin summaries and persist the feed response to Bronze."""

    collector = CaixinCollector()
    writer = BronzeWriter()
    # Deduplicate against both layers; a failed Silver job can replay its Bronze object.
    bronze_ids = writer.all_external_ids(prefix="news/provider=caixin/")
    silver_writer = NewsWriter()
    resolved_start = ensure_utc(start_time)
    resolved_end = ensure_utc(end_time)
    response = await collector.collect_news(
        existing_external_ids=bronze_ids | silver_writer.all_external_ids(),
    )
    object_path: str | None = None
    if response.articles:
        object_path = writer.write_news_raw(
            provider="caixin",
            query="latest",
            raw_response=response.raw_response,
            start_time=resolved_start,
            end_time=resolved_end,
            limit=None,
        )
    result = IngestNewsResult(
        category="caijing",
        object_path=object_path,
        article_count=len(response.articles),
        start_time=resolved_start,
        end_time=resolved_end,
        limit=None,
    )
    logger.info(
        "财经新闻采集任务完成" if object_path else "没有新的有效新闻摘要，未写入 Bronze",
        event="ingest_finance_news_completed",
        extra_fields=result.to_dict(),
    )
    return result


async def ingest_news_batch(
    *,
    start_time: datetime | None = None,
    end_time: datetime | None = None,
    limit: int | None = None,
) -> list[IngestNewsResult]:
    """Run one finance-news ingestion batch."""

    return [
        await ingest_finance_news(
            start_time=start_time,
            end_time=end_time,
            limit=limit,
        )
    ]


def build_default_window(end_time: datetime | None = None, lookback_hours: int | None = None) -> tuple[datetime, datetime]:
    """Build default incremental window for scheduled ingestion."""

    resolved_end = ensure_utc(end_time) or datetime.now(UTC)
    resolved_lookback = lookback_hours or settings.NEWS_INGEST_LOOKBACK_HOURS
    resolved_start = resolved_end - timedelta(hours=resolved_lookback)
    return resolved_start, resolved_end


def build_parser() -> argparse.ArgumentParser:
    """Build CLI argument parser."""

    parser = argparse.ArgumentParser(description="Ingest Caixin news summaries into Bronze")
    parser.add_argument("--start-time", help="Start time in ISO-8601 format")
    parser.add_argument("--end-time", help="End time in ISO-8601 format")
    parser.add_argument("--limit", type=int, help="Deprecated; accepted for compatibility and ignored")
    parser.add_argument(
        "--use-default-window",
        action="store_true",
        help="Use configured lookback window when start/end time are not explicitly provided",
    )
    return parser


def main() -> None:
    """Entry point for CLI execution."""

    configure_logging()
    parser = build_parser()
    args = parser.parse_args()

    start_time = parse_possible_datetime(args.start_time)
    end_time = parse_possible_datetime(args.end_time)
    if args.use_default_window and start_time is None and end_time is None:
        start_time, end_time = build_default_window()

    results = asyncio.run(
        ingest_news_batch(
            start_time=start_time,
            end_time=end_time,
            limit=args.limit,
        )
    )
    print(json.dumps([result.to_dict() for result in results], ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
