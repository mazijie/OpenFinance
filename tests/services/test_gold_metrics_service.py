from __future__ import annotations

from datetime import UTC, date, datetime

from backend.schemas.finance import GoldMetricRecord
from backend.services.gold_metrics_service import GoldMetricsService


def _row(*, feature_available_at: datetime, trading_date: date) -> GoldMetricRecord:
    return GoldMetricRecord(
        security_id="600519.SH",
        symbol="600519.SH",
        exchange="SH",
        trading_date=trading_date,
        provider="baostock",
        adjustment_type="qfq",
        open=10,
        high=11,
        low=9,
        close=10,
        volume=100,
        amount=1000,
        turnover=0.1,
        is_suspended=False,
        is_st=False,
        adjustment_factor=1,
        daily_return_1d=0.01,
        return_5d=0.02,
        return_20d=0.03,
        ma_5d=10,
        ma_20d=9.9,
        volume_ma_5d=100,
        volatility_20d_annualized=0.2,
        observed_days_20d=20,
        metric_status="complete",
        source_available_at=feature_available_at,
        source_ingested_at=feature_available_at,
        feature_available_at=feature_available_at,
        gold_built_at=feature_available_at,
        schema_version=1,
    )


class FakeRepository:
    def __init__(self, rows):
        self.rows = rows

    def scan(self, **kwargs):
        return self.rows


def test_complete_snapshot_requires_requested_trading_date() -> None:
    day = date(2026, 10, 23)
    available = datetime(2026, 10, 23, 8, tzinfo=UTC)
    service = GoldMetricsService(FakeRepository([_row(feature_available_at=available, trading_date=day)]))

    snapshot = service.snapshot(
        symbol="600519.SH",
        trading_date=day,
        cutoff_at=datetime(2026, 10, 23, 12, tzinfo=UTC),
    )

    assert snapshot.complete is True
    assert snapshot.latest is not None


def test_snapshot_degrades_when_feature_is_not_yet_available() -> None:
    day = date(2026, 10, 23)
    service = GoldMetricsService(
        FakeRepository(
            [
                _row(
                    feature_available_at=datetime(2026, 10, 24, 8, tzinfo=UTC),
                    trading_date=day,
                )
            ]
        )
    )

    snapshot = service.snapshot(
        symbol="600519.SH",
        trading_date=day,
        cutoff_at=datetime(2026, 10, 23, 12, tzinfo=UTC),
    )

    assert snapshot.complete is False
    assert "not available" in (snapshot.missing_reason or "")
