from __future__ import annotations

from datetime import UTC, date, datetime

from backend.agent.monitor_workflow import InvestmentMonitorAgent
from backend.schemas.finance import GoldMetricRecord
from backend.schemas.monitor import (
    HorizonRecommendation,
    MonitorJudgement,
    WatchlistItem,
    WatchlistThresholds,
)
from backend.services.gold_metrics_service import GoldMetricSnapshot


def _item() -> WatchlistItem:
    now = datetime(2026, 10, 23, 12, tzinfo=UTC)
    return WatchlistItem(
        symbol="600519.SH",
        security_id="600519.SH",
        name="贵州茅台",
        exchange="SH",
        thresholds=WatchlistThresholds(),
        created_at=now,
        updated_at=now,
    )


def _row(*, daily_return: float = 0.01) -> GoldMetricRecord:
    now = datetime(2026, 10, 23, 8, tzinfo=UTC)
    return GoldMetricRecord(
        security_id="600519.SH",
        symbol="600519.SH",
        exchange="SH",
        trading_date=date(2026, 10, 23),
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
        daily_return_1d=daily_return,
        return_5d=0.02,
        return_20d=0.03,
        ma_5d=10,
        ma_20d=9.9,
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


class FakeGoldService:
    def __init__(self, snapshot: GoldMetricSnapshot) -> None:
        self.value = snapshot

    def snapshot(self, **kwargs):
        return self.value


class FakeLLM:
    model_name = "fake-model"

    def __init__(self, outputs=None) -> None:
        self.outputs = list(outputs or [])
        self.calls = 0

    def structured_chat(self, messages, schema):
        self.calls += 1
        if self.outputs:
            return self.outputs.pop(0)
        return schema.model_validate(
            {
                "summary": "价格和成交量同时异动。",
                "recommendation_5d": {
                    "horizon": "5d",
                    "stance": "bullish",
                    "confidence": 0.6,
                    "actionable": True,
                    "entry_condition": "缩量回踩后重新放量",
                    "invalidation_condition": "跌破近期低点",
                    "rationale": "量价同步改善。",
                    "citation_ids": [1],
                },
                "recommendation_20d": {
                    "horizon": "20d",
                    "stance": "neutral",
                    "confidence": 0.5,
                    "actionable": False,
                    "entry_condition": None,
                    "invalidation_condition": None,
                    "rationale": "中期证据仍有限。",
                    "citation_ids": [1],
                },
                "risk_notes": ["波动风险"],
            }
        )


def _agent(gold: GoldMetricSnapshot, llm=None):
    return InvestmentMonitorAgent(
        gold_service=FakeGoldService(gold),
        rag_tool=lambda **kwargs: {"success": True, "data": {"articles": []}, "error": None},
        annual_tool=lambda **kwargs: {"success": True, "data": {"articles": []}, "error": None},
        llm_factory=lambda: llm or FakeLLM(),
    )


def test_no_event_generates_deterministic_neutral_report() -> None:
    row = _row()
    result = _agent(GoldMetricSnapshot([row], row, row.trading_date, True, None)).run(
        item=_item(),
        run_id="00000000-0000-0000-0000-000000000001",
        trading_date=row.trading_date,
        cutoff_at=datetime(2026, 10, 23, 12, tzinfo=UTC),
        previous_cutoff_at=None,
    )

    assert result.report.status == "ready"
    assert result.report.recommendation_5d.stance == "neutral"
    assert result.report.model_metadata["generation"] == "deterministic_neutral"


def test_degraded_data_never_contains_recommendation() -> None:
    result = _agent(
        GoldMetricSnapshot([], None, date(2026, 10, 23), False, "Gold stale")
    ).run(
        item=_item(),
        run_id="00000000-0000-0000-0000-000000000001",
        trading_date=date(2026, 10, 23),
        cutoff_at=datetime(2026, 10, 23, 12, tzinfo=UTC),
        previous_cutoff_at=None,
    )

    assert result.report.status == "degraded"
    assert result.report.recommendation_5d is None
    assert result.report.recommendation_20d is None


def test_triggered_event_uses_structured_judgement() -> None:
    row = _row(daily_return=0.05)
    llm = FakeLLM()
    result = _agent(
        GoldMetricSnapshot([row], row, row.trading_date, True, None),
        llm=llm,
    ).run(
        item=_item(),
        run_id="00000000-0000-0000-0000-000000000001",
        trading_date=row.trading_date,
        cutoff_at=datetime(2026, 10, 23, 12, tzinfo=UTC),
        previous_cutoff_at=None,
    )

    assert llm.calls == 1
    assert result.report.recommendation_5d.stance == "bullish"
    assert result.report.model_metadata["generation"] == "structured"


def test_invalid_citation_gets_one_repair_then_degrades() -> None:
    row = _row(daily_return=0.05)
    invalid = MonitorJudgement(
        summary="证据不足但尝试输出。",
        recommendation_5d=HorizonRecommendation(
            horizon="5d",
            stance="bullish",
            confidence=0.5,
            actionable=True,
            rationale="错误引用。",
            citation_ids=[99],
        ),
        recommendation_20d=HorizonRecommendation(
            horizon="20d",
            stance="neutral",
            confidence=0.5,
            actionable=False,
            rationale="错误引用。",
            citation_ids=[99],
        ),
    )
    llm = FakeLLM([invalid, invalid])
    result = _agent(
        GoldMetricSnapshot([row], row, row.trading_date, True, None),
        llm=llm,
    ).run(
        item=_item(),
        run_id="00000000-0000-0000-0000-000000000001",
        trading_date=row.trading_date,
        cutoff_at=datetime(2026, 10, 23, 12, tzinfo=UTC),
        previous_cutoff_at=None,
    )

    assert llm.calls == 2
    assert result.report.status == "degraded"
    assert result.report.recommendation_5d is None

