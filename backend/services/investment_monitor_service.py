"""Application service for durable after-close investment monitoring."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime
from zoneinfo import ZoneInfo
from typing import Any
from uuid import uuid4

from backend.agent.monitor_workflow import InvestmentMonitorAgent
from backend.config import settings
from backend.harness.evidence import EvidenceService
from backend.harness.store import HarnessStore
from backend.monitoring.store import MonitorStore
from backend.schemas.monitor import (
    MonitorReport,
    MonitorRun,
    MonitorRunAccepted,
    MonitorRunRequest,
    WatchlistItem,
)


class InvestmentMonitorService:
    def __init__(
        self,
        *,
        store: MonitorStore | None = None,
        agent_factory=None,
        harness_store: HarnessStore | None = None,
    ) -> None:
        self.store = store or MonitorStore()
        self.agent_factory = agent_factory or (
            lambda: InvestmentMonitorAgent(evidence_service=EvidenceService())
        )
        self.harness_store = harness_store

    def create_run(self, request: MonitorRunRequest) -> MonitorRunAccepted:
        items = self.store.list_watchlist(enabled_only=True)
        if not items:
            raise ValueError("No enabled watchlist items")
        if len(items) > settings.INVESTMENT_MONITOR_MAX_SYMBOLS:
            raise ValueError(
                f"Watchlist has {len(items)} items; maximum is {settings.INVESTMENT_MONITOR_MAX_SYMBOLS}"
            )

        now = datetime.now(UTC)
        cutoff = request.cutoff_at or now
        cutoff = cutoff.replace(tzinfo=UTC) if cutoff.tzinfo is None else cutoff.astimezone(UTC)
        trading_date = request.trading_date or now.astimezone(ZoneInfo("Asia/Shanghai")).date()
        watchlist_version = self.store.watchlist_version(items)
        previous_cutoff = self.store.latest_successful_cutoff()
        thread_id = request.thread_id
        if not thread_id:
            harness_store = self.harness_store or HarnessStore()
            thread = harness_store.get_or_create_system_thread(
                system_key=f"monitor:{trading_date.isoformat()}",
                title=f"{trading_date.isoformat()} 收盘监测",
            )
            thread_id = thread.thread_id
        config = {
            "watchlist_items": [item.model_dump(mode="json") for item in items],
            "previous_cutoff_at": previous_cutoff.isoformat() if previous_cutoff else None,
            "recommendations_enabled": settings.INVESTMENT_MONITOR_RECOMMENDATIONS_ENABLED,
            "max_symbols": settings.INVESTMENT_MONITOR_MAX_SYMBOLS,
            "concurrency": settings.INVESTMENT_MONITOR_CONCURRENCY,
            "origin_thread_id": thread_id,
        }
        run, deduplicated = self.store.create_run(
            trading_date=trading_date,
            cutoff_at=cutoff,
            watchlist_version=watchlist_version,
            config=config,
            origin_thread_id=thread_id,
        )
        return MonitorRunAccepted(
            run_id=run.run_id,
            status=run.status,
            thread_id=thread_id,
            deduplicated=deduplicated,
        )

    def execute(self, run: MonitorRun) -> None:
        items = [WatchlistItem.model_validate(item) for item in run.config.get("watchlist_items", [])]
        previous_raw = run.config.get("previous_cutoff_at")
        previous_cutoff = (
            datetime.fromisoformat(str(previous_raw).replace("Z", "+00:00"))
            if previous_raw
            else None
        )
        agent = self.agent_factory()
        concurrency = max(1, min(int(run.config.get("concurrency", 4)), 8))
        reports: list[MonitorReport] = []
        errors: list[str] = []
        harness_store = self.harness_store or HarnessStore()
        with ThreadPoolExecutor(max_workers=concurrency, thread_name_prefix="monitor-agent") as pool:
            futures = {
                pool.submit(
                    agent.run,
                    item=item,
                    run_id=run.run_id,
                    trading_date=run.trading_date,
                    cutoff_at=run.cutoff_at,
                    previous_cutoff_at=previous_cutoff,
                ): item
                for item in items
            }
            for future in as_completed(futures):
                item = futures[future]
                try:
                    result = future.result()
                    report = result.report
                    if not run.config.get("recommendations_enabled", False):
                        report = report.model_copy(
                            update={
                                "model_metadata": {
                                    **report.model_metadata,
                                    "shadow_mode": True,
                                },
                            }
                        )
                    self.store.upsert_report(report)
                    if run.origin_thread_id:
                        harness_store.append_monitor_report_message(
                            thread_id=run.origin_thread_id,
                            report=report.model_dump(mode="json"),
                        )
                    self.store.save_trace(
                        run_id=run.run_id,
                        symbol=item.symbol,
                        status=report.status,
                        tool_calls=result.tool_calls,
                        latency_ms=result.latency_ms,
                    )
                    reports.append(report)
                except Exception as exc:
                    errors.append(f"{item.symbol}: {exc}")
                    failed = MonitorReport(
                        report_id=str(uuid4()),
                        run_id=run.run_id,
                        symbol=item.symbol,
                        name=item.name,
                        trading_date=run.trading_date,
                        data_cutoff_at=run.cutoff_at,
                        data_status="degraded",
                        status="failed",
                        analysis="监测任务失败，未生成交易建议。",
                        missing_data=[str(exc)],
                        generated_at=datetime.now(UTC),
                    )
                    self.store.upsert_report(failed)
                    if run.origin_thread_id:
                        harness_store.append_monitor_report_message(
                            thread_id=run.origin_thread_id,
                            report=failed.model_dump(mode="json"),
                        )
                    self.store.save_trace(
                        run_id=run.run_id,
                        symbol=item.symbol,
                        status="failed",
                        tool_calls=[],
                        latency_ms=0,
                        error_stage="agent",
                        error=str(exc),
                    )
                    reports.append(failed)

        ready_count = sum(report.status == "ready" for report in reports)
        degraded_count = sum(report.status == "degraded" for report in reports)
        failed_count = sum(report.status == "failed" for report in reports)
        stats: dict[str, Any] = {
            "watchlist_count": len(items),
            "ready_count": ready_count,
            "degraded_count": degraded_count,
            "failed_count": failed_count,
        }
        if reports and failed_count == len(reports):
            status = "failed"
        elif degraded_count or failed_count:
            status = "degraded"
        else:
            status = "completed"
        self.store.finish_run(
            run.run_id,
            status=status,
            stats=stats,
            error="; ".join(errors) if errors else None,
        )
