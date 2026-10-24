from __future__ import annotations

from datetime import UTC, date, datetime

from backend.monitoring.events import detect_market_events
from backend.schemas.finance import GoldMetricRecord
from backend.schemas.monitor import WatchlistThresholds


def _gold(
    *,
    trading_date: date,
    close: float,
    daily_return: float,
    volume: float,
    volume_ma: float,
    ma5: float,
    ma20: float,
) -> GoldMetricRecord:
    available = datetime.combine(trading_date, datetime.min.time(), tzinfo=UTC)
    return GoldMetricRecord(
        security_id="600519.SH",
        symbol="600519.SH",
        exchange="SH",
        trading_date=trading_date,
        provider="baostock",
        adjustment_type="qfq",
        open=close - 0.1,
        high=close + 0.2,
        low=close - 0.2,
        close=close,
        volume=volume,
        amount=close * volume,
        turnover=0.01,
        is_suspended=False,
        is_st=False,
        adjustment_factor=1.0,
        daily_return_1d=daily_return,
        return_5d=0.02,
        return_20d=0.03,
        ma_5d=ma5,
        ma_20d=ma20,
        volume_ma_5d=volume_ma,
        volatility_20d_annualized=0.2,
        observed_days_20d=20,
        metric_status="complete",
        source_available_at=available,
        source_ingested_at=available,
        feature_available_at=available,
        gold_built_at=available,
        schema_version=1,
    )


def test_detects_price_volume_and_ma_cross() -> None:
    rows = [
        _gold(
            trading_date=date(2026, 10, 22),
            close=10.0,
            daily_return=-0.01,
            volume=100,
            volume_ma=100,
            ma5=9.9,
            ma20=10.0,
        ),
        _gold(
            trading_date=date(2026, 10, 23),
            close=10.5,
            daily_return=0.05,
            volume=250,
            volume_ma=100,
            ma5=10.1,
            ma20=10.0,
        ),
    ]

    events = detect_market_events(
        symbol="600519.SH",
        rows=rows,
        thresholds=WatchlistThresholds(price_move=0.03, volume_ratio=1.8),
        observed_at=datetime(2026, 10, 23, 12, tzinfo=UTC),
    )

    assert {event.event_type for event in events} == {"price_move", "volume_surge", "ma_cross"}


def test_does_not_trigger_below_threshold() -> None:
    rows = [
        _gold(
            trading_date=date(2026, 10, 23),
            close=10.1,
            daily_return=0.01,
            volume=100,
            volume_ma=100,
            ma5=10.0,
            ma20=9.9,
        )
    ]

    assert detect_market_events(
        symbol="600519.SH",
        rows=rows,
        thresholds=WatchlistThresholds(),
        observed_at=datetime(2026, 10, 23, 12, tzinfo=UTC),
    ) == []
