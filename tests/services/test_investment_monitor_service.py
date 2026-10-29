from __future__ import annotations

from datetime import UTC, date, datetime

from backend.agent.monitor_workflow import MonitorAgentResult
from backend.schemas.monitor import (
    HorizonRecommendation,
    MonitorReport,
    MonitorRun,
    WatchlistItem,
)
from backend.schemas.harness import AgentThread
from backend.services.investment_monitor_service import InvestmentMonitorService


def _now() -> datetime:
    return datetime(2026, 10, 23, 12, tzinfo=UTC)


def _item() -> WatchlistItem:
    return WatchlistItem(
        symbol="000001.SZ",
        security_id="000001.SZ",
        name="平安银行",
        exchange="SZ",
        created_at=_now(),
        updated_at=_now(),
    )


def _report() -> MonitorReport:
    recommendation = HorizonRecommendation(
        horizon="5d",
        stance="bullish",
        confidence=0.6,
        actionable=True,
        rationale="测试。",
        citation_ids=[1],
    )
    return MonitorReport(
        report_id="00000000-0000-0000-0000-000000000002",
        run_id="00000000-0000-0000-0000-000000000001",
        symbol="000001.SZ",
        name="平安银行",
        trading_date=date(2026, 10, 23),
        data_cutoff_at=_now(),
        data_status="complete",
        status="ready",
        recommendation_5d=recommendation,
        recommendation_20d=recommendation.model_copy(update={"horizon": "20d"}),
        generated_at=_now(),
    )


class FakeStore:
    def __init__(self) -> None:
        self.reports = []
        self.finished = None

    def list_watchlist(self, *, enabled_only):
        return [_item()]

    def watchlist_version(self, items):
        return "version"

    def latest_successful_cutoff(self):
        return None

    def create_run(self, **kwargs):
        run = MonitorRun(
            run_id="00000000-0000-0000-0000-000000000001",
            run_key="key",
            trading_date=kwargs["trading_date"],
            cutoff_at=kwargs["cutoff_at"],
            status="queued",
            watchlist_version=kwargs["watchlist_version"],
            config=kwargs["config"],
            created_at=_now(),
        )
        return run, False

    def upsert_report(self, report):
        self.reports.append(report)
        return report

    def save_trace(self, **kwargs):
        return None

    def finish_run(self, run_id, *, status, stats, error=None):
        self.finished = (status, stats, error)


class FakeAgent:
    def run(self, **kwargs):
        return MonitorAgentResult(report=_report(), tool_calls=[], latency_ms=1)


class FakeHarnessStore:
    def get_or_create_system_thread(self, *, system_key, title):
        now = _now()
        return AgentThread(
            thread_id="00000000-0000-0000-0000-000000000003",
            title=title,
            status="active",
            kind="system",
            created_at=now,
            updated_at=now,
        )

    def append_monitor_report_message(self, *, thread_id, report):
        return None


def test_shadow_mode_persists_recommendations_for_evaluation() -> None:
    store = FakeStore()
    service = InvestmentMonitorService(
        store=store,
        agent_factory=FakeAgent,
        harness_store=FakeHarnessStore(),
    )
    from backend.schemas.monitor import MonitorRunRequest

    accepted = service.create_run(MonitorRunRequest(trading_date=date(2026, 10, 23)))
    run = store.create_run(
        trading_date=date(2026, 10, 23),
        cutoff_at=_now(),
        watchlist_version="version",
        config={
            "watchlist_items": [_item().model_dump(mode="json")],
            "previous_cutoff_at": None,
            "recommendations_enabled": False,
            "concurrency": 1,
        },
    )[0]
    service.execute(run)

    assert accepted.status == "queued"
    assert accepted.thread_id == "00000000-0000-0000-0000-000000000003"
    assert store.reports[0].recommendation_5d is not None
    assert store.reports[0].model_metadata["shadow_mode"] is True
    assert store.finished[0] == "completed"
