from __future__ import annotations

from datetime import UTC, date, datetime, timedelta

from backend.monitoring.evaluation import evaluate_reports
from backend.schemas.finance import GoldMetricRecord
from backend.schemas.monitor import HorizonRecommendation, MonitorReport


def _gold(day: date, close: float) -> GoldMetricRecord:
    now = datetime.combine(day, datetime.min.time(), tzinfo=UTC)
    return GoldMetricRecord(
        security_id="000001.SZ",
        symbol="000001.SZ",
        exchange="SZ",
        trading_date=day,
        provider="baostock",
        adjustment_type="qfq",
        open=close,
        high=close,
        low=close,
        close=close,
        volume=100,
        amount=100 * close,
        turnover=0.01,
        is_suspended=False,
        is_st=False,
        adjustment_factor=1,
        daily_return_1d=0.01,
        return_5d=0.02,
        return_20d=0.03,
        ma_5d=close,
        ma_20d=close,
        volume_ma_5d=100,
        volatility_20d_annualized=0.2,
        observed_days_20d=20,
        metric_status="complete",
        source_available_at=now,
        source_ingested_at=now,
        feature_available_at=now,
        gold_built_at=now,
        schema_version=1,
    )


class FakeGoldRepository:
    def scan_window(self, *, symbol, start_date, end_date, limit):
        rows = [_gold(start_date + timedelta(days=index), 10 + index * 0.1) for index in range(21)]
        return rows[:limit]


def _report(report_date: date) -> MonitorReport:
    now = datetime(2026, 10, 23, 12, tzinfo=UTC)
    bullish = HorizonRecommendation(
        horizon="5d",
        stance="bullish",
        confidence=0.6,
        actionable=True,
        rationale="测试。",
        citation_ids=[1],
    )
    bullish_20d = bullish.model_copy(update={"horizon": "20d"})
    return MonitorReport(
        report_id="00000000-0000-0000-0000-000000000002",
        run_id="00000000-0000-0000-0000-000000000001",
        symbol="000001.SZ",
        name="平安银行",
        trading_date=report_date,
        data_cutoff_at=now,
        data_status="complete",
        status="ready",
        recommendation_5d=bullish,
        recommendation_20d=bullish_20d,
        generated_at=now,
    )


def test_evaluates_direction_after_horizon_matures() -> None:
    results = evaluate_reports(
        [_report(date(2026, 10, 1))],
        FakeGoldRepository(),
    )

    assert results["5d"]["directional_hit_rate"] == 1.0
    assert results["20d"]["directional_hit_rate"] == 1.0
    assert results["20d"]["mean_net_return"] is not None
