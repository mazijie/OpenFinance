"""Gold-market feature snapshots for market analysis."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime

from backend.lakehouse.starrocks_daily_reader import StarRocksUnavailable
from backend.lakehouse.starrocks_readers import StarRocksReader
from backend.schemas.finance import GoldMetricRecord


class GoldMetricsError(Exception):
    """Raised when Gold metrics cannot be read."""


@dataclass(slots=True)
class GoldMetricSnapshot:
    rows: list[GoldMetricRecord]
    latest: GoldMetricRecord | None
    requested_trading_date: date
    complete: bool
    missing_reason: str | None = None


class GoldMetricsService:
    def __init__(self, repository: StarRocksReader | None = None) -> None:
        self.repository = repository

    def snapshot(
        self,
        *,
        symbol: str,
        trading_date: date,
        cutoff_at: datetime,
    ) -> GoldMetricSnapshot:
        try:
            repository = self.repository or StarRocksReader()
            rows = repository.scan(
                symbol=symbol,
                trading_date=trading_date,
                cutoff_at=cutoff_at,
            )
        except StarRocksUnavailable:
            raise
        except Exception as exc:
            return GoldMetricSnapshot(
                rows=[],
                latest=None,
                requested_trading_date=trading_date,
                complete=False,
                missing_reason=f"Gold metrics unavailable: {exc}",
            )
        if not rows:
            return GoldMetricSnapshot(
                rows=[],
                latest=None,
                requested_trading_date=trading_date,
                complete=False,
                missing_reason=f"No visible Gold metrics for {trading_date.isoformat()}",
            )
        latest = rows[-1]
        if latest.trading_date != trading_date:
            return GoldMetricSnapshot(
                rows=rows,
                latest=latest,
                requested_trading_date=trading_date,
                complete=False,
                missing_reason=(
                    f"Latest visible Gold trading date is {latest.trading_date.isoformat()}, "
                    f"not requested {trading_date.isoformat()}"
                ),
            )
        if latest.metric_status != "complete":
            return GoldMetricSnapshot(
                rows=rows,
                latest=latest,
                requested_trading_date=trading_date,
                complete=False,
                missing_reason=f"Latest Gold metric status is {latest.metric_status}",
            )
        if latest.feature_available_at > cutoff_at.astimezone(UTC):
            return GoldMetricSnapshot(
                rows=rows,
                latest=latest,
                requested_trading_date=trading_date,
                complete=False,
                missing_reason="Latest Gold feature was not available at the requested cutoff",
            )
        return GoldMetricSnapshot(
            rows=rows,
            latest=latest,
            requested_trading_date=trading_date,
            complete=True,
        )
