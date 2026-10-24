"""Point-in-time reader for the Gold security metrics table."""

from __future__ import annotations

from datetime import UTC, date, datetime

from backend.config import settings
from backend.lakehouse.catalog import get_catalog
from backend.schemas.finance import GoldMetricRecord


class GoldMetricsRepository:
    """Read Gold rows without exposing future feature availability."""

    def __init__(self, table=None) -> None:
        self.table = table or get_catalog().load_table(
            f"{settings.ICEBERG_NAMESPACE}.{settings.ICEBERG_GOLD_METRICS_TABLE}"
        )

    def scan(
        self,
        *,
        symbol: str,
        trading_date: date,
        cutoff_at: datetime,
        history_rows: int = 25,
    ) -> list[GoldMetricRecord]:
        try:
            from pyiceberg.expressions import And, EqualTo, LessThanOrEqual

            rows = (
                self.table.scan(
                    row_filter=And(
                        EqualTo("symbol", symbol.upper()),
                        LessThanOrEqual("trading_date", trading_date),
                        LessThanOrEqual("feature_available_at", cutoff_at.astimezone(UTC)),
                    )
                )
                .to_arrow()
                .to_pylist()
            )
        except ImportError:  # pragma: no cover - local fallback
            rows = self.table.scan().to_arrow().to_pylist()
        records = [GoldMetricRecord.model_validate(row) for row in rows]
        records.sort(key=lambda row: row.trading_date)
        return records[-history_rows:]

    def scan_window(
        self,
        *,
        symbol: str,
        start_date: date,
        end_date: date,
        limit: int = 30,
    ) -> list[GoldMetricRecord]:
        """Read a bounded forward window for post-run evaluation."""

        try:
            from pyiceberg.expressions import And, EqualTo, GreaterThanOrEqual, LessThanOrEqual

            rows = (
                self.table.scan(
                    row_filter=And(
                        EqualTo("symbol", symbol.upper()),
                        GreaterThanOrEqual("trading_date", start_date),
                        LessThanOrEqual("trading_date", end_date),
                    )
                )
                .to_arrow()
                .to_pylist()
            )
        except ImportError:  # pragma: no cover - local fallback
            rows = self.table.scan().to_arrow().to_pylist()
        records = [
            GoldMetricRecord.model_validate(row)
            for row in rows
            if start_date <= row.get("trading_date", start_date - date.resolution) <= end_date
        ]
        records.sort(key=lambda row: row.trading_date)
        return records[:limit]
