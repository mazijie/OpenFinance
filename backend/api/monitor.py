"""Investment-monitor runs and report inbox API."""

from __future__ import annotations

from datetime import date
from functools import lru_cache

from fastapi import APIRouter, Depends, HTTPException, Query, status

from backend.config import settings
from backend.monitoring.store import MonitorStore
from backend.schemas.monitor import (
    MonitorReport,
    MonitorReportPage,
    MonitorReportStatus,
    MonitorRun,
    MonitorRunAccepted,
    MonitorRunRequest,
)
from backend.services.investment_monitor_service import InvestmentMonitorService

router = APIRouter(prefix="/api/monitor", tags=["monitor"])


@lru_cache(maxsize=1)
def get_monitor_store() -> MonitorStore:
    return MonitorStore()


@lru_cache(maxsize=1)
def get_monitor_service() -> InvestmentMonitorService:
    return InvestmentMonitorService(store=get_monitor_store())


@router.post("/runs", response_model=MonitorRunAccepted, status_code=status.HTTP_202_ACCEPTED)
def create_monitor_run(
    request: MonitorRunRequest,
    service: InvestmentMonitorService = Depends(get_monitor_service),
) -> MonitorRunAccepted:
    try:
        return service.create_run(request)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


@router.get("/runs/{run_id}", response_model=MonitorRun)
def get_monitor_run(
    run_id: str,
    store: MonitorStore = Depends(get_monitor_store),
) -> MonitorRun:
    try:
        run = store.get_run(run_id)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail="Invalid run id") from exc
    if run is None:
        raise HTTPException(status_code=404, detail="Monitor run not found")
    return run


@router.get("/reports", response_model=MonitorReportPage)
def list_monitor_reports(
    trading_date: date | None = Query(default=None),
    symbol: str | None = Query(default=None, max_length=16),
    report_status: MonitorReportStatus | None = Query(default=None, alias="status"),
    limit: int = Query(default=50, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
    store: MonitorStore = Depends(get_monitor_store),
) -> MonitorReportPage:
    reports, total = store.list_reports(
        trading_date=trading_date,
        symbol=symbol,
        status=report_status,
        limit=limit,
        offset=offset,
    )
    return MonitorReportPage(
        items=[_public_report(report) for report in reports],
        total=total,
        limit=limit,
        offset=offset,
    )


@router.get("/reports/{report_id}", response_model=MonitorReport)
def get_monitor_report(
    report_id: str,
    store: MonitorStore = Depends(get_monitor_store),
) -> MonitorReport:
    try:
        report = store.get_report(report_id)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail="Invalid report id") from exc
    if report is None:
        raise HTTPException(status_code=404, detail="Monitor report not found")
    return _public_report(report)


def _public_report(report: MonitorReport) -> MonitorReport:
    if settings.INVESTMENT_MONITOR_RECOMMENDATIONS_ENABLED:
        return report
    return report.model_copy(
        update={
            "recommendation_5d": None,
            "recommendation_20d": None,
            "model_metadata": {**report.model_metadata, "shadow_mode": True},
        }
    )
