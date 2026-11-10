"""Read-only market data API routes."""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta, timezone
from functools import lru_cache
from time import monotonic
from typing import Literal

from fastapi import APIRouter, Depends, HTTPException, Query, Request, status

from backend.schemas.finance import AdjustmentType, MarketSearchResult, SecurityOptionsResult
from backend.services.finance_service import FinanceService
from backend.services.market_query_gate import market_query_gate
from backend.lakehouse.market_history_index import has_earlier
from backend.logging_utils import get_logger

router = APIRouter(prefix="/api/market", tags=["market"])
logger = get_logger(__name__)


@lru_cache(maxsize=1)
def get_finance_service() -> FinanceService:
    """Return the process-wide read-only finance service."""

    return FinanceService()


@router.get("/securities", response_model=SecurityOptionsResult, status_code=status.HTTP_200_OK)
def list_securities(
    finance_service: FinanceService = Depends(get_finance_service),
) -> SecurityOptionsResult:
    """List securities available in the local lakehouse."""

    return finance_service.list_security_options()


@router.get("/history")
def market_history(
    request: Request,
    symbol: str = Query(min_length=1, max_length=16),
    period: Literal["daily", "weekly", "monthly"] = "daily",
    before: date | None = None,
    adjustment_type: AdjustmentType = "none",
    finance_service: FinanceService = Depends(get_finance_service),
):
    if "as_of" in request.query_params:
        raise HTTPException(status_code=422, detail="as_of is no longer supported")
    now = datetime.now(UTC)
    today = now.astimezone(timezone(timedelta(hours=8))).date()
    if before and (before > today or before <= date(1901, 1, 1)):
        raise HTTPException(status_code=422, detail="before is outside the supported date range")
    if before and ((period == "weekly" and before.weekday() != 0) or (period == "monthly" and before.day != 1)):
        raise HTTPException(status_code=422, detail="before must align with the period boundary")
    end = before - timedelta(days=1) if before else today
    start = end - timedelta(days={"daily": 365, "weekly": 1095, "monthly": 1826}[period] - 1)
    if period == "weekly":
        start -= timedelta(days=start.weekday())
    elif period == "monthly":
        start = start.replace(day=1)
    symbol = symbol.strip().upper()
    try:
        started = monotonic()
        with market_query_gate.acquire(page=True):
            table = finance_service.history_table()
            snapshot = table.current_snapshot()
            snapshot_id = snapshot.snapshot_id if snapshot else None
            table_uuid = str(table.metadata.table_uuid)
            result = finance_service.search_market(symbol=symbol, start_date=start, end_date=end,
                                                   adjustment_type=adjustment_type, as_of=now,
                                                   table=table, snapshot_id=snapshot_id)
        query_seconds = monotonic() - started
        has_more = False if snapshot_id is None else has_earlier(
            table_uuid=table_uuid, snapshot_id=snapshot_id, as_of=now,
            symbol=symbol, adjustment_type=adjustment_type, before=start,
        )
        logger.info("Market history page loaded", event="market_history_page_loaded",
                    extra_fields={"period": period, "query_seconds": round(query_seconds, 3),
                                  "total_seconds": round(monotonic() - started, 3),
                                  "index_hit": has_more is not None})
        return {"items": result.items, "start_date": start, "end_date": end,
                "next_before": start, "has_more": has_more,
                "snapshot_id": str(snapshot_id) if snapshot_id is not None else None,
                "table_uuid": table_uuid, "query_time": now}
    except TimeoutError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to query market history: {exc}") from exc


@router.get("/history/availability")
def history_availability(
    symbol: str = Query(min_length=1, max_length=16),
    before: date = Query(),
    adjustment_type: AdjustmentType = "none",
    snapshot_id: int = Query(),
    table_uuid: str = Query(min_length=1, max_length=36),
    query_time: datetime = Query(),
    finance_service: FinanceService = Depends(get_finance_service),
):
    now = datetime.now(UTC)
    if query_time.tzinfo is None or query_time > now or before <= date(1901, 1, 1):
        raise HTTPException(status_code=422, detail="Invalid query time or history boundary")
    symbol = symbol.strip().upper()
    try:
        started = monotonic()
        table = finance_service.history_table()
        if str(table.metadata.table_uuid) != table_uuid or table.snapshot_by_id(snapshot_id) is None:
            raise HTTPException(status_code=410, detail="History snapshot is no longer available; reload the chart")
        answer = has_earlier(table_uuid=table_uuid, snapshot_id=snapshot_id, as_of=query_time,
                             symbol=symbol, adjustment_type=adjustment_type, before=before)
        if answer is None:
            with market_query_gate.acquire(page=False):
                answer = finance_service.has_history_before(symbol, adjustment_type, before, query_time,
                                                             table=table, snapshot_id=snapshot_id)
            source = "iceberg"
        else:
            source = "index"
        logger.info("Market history availability checked", event="market_history_availability_checked",
                    extra_fields={"source": source, "seconds": round(monotonic() - started, 3)})
        return {"has_more": answer}
    except HTTPException:
        raise
    except TimeoutError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to check earlier history: {exc}") from exc


@router.get("", response_model=MarketSearchResult, status_code=status.HTTP_200_OK)
def get_market(
    request: Request,
    symbol: str | None = Query(default=None, min_length=1, max_length=16),
    security_id: str | None = Query(default=None, min_length=1, max_length=64),
    start_date: date | None = Query(default=None),
    end_date: date | None = Query(default=None),
    adjustment_type: AdjustmentType = Query(default="none"),
    finance_service: FinanceService = Depends(get_finance_service),
) -> MarketSearchResult:
    """Query Silver/Gold market data without contacting an external provider."""

    if "as_of" in request.query_params:
        raise HTTPException(status_code=422, detail="as_of is no longer supported")

    if not symbol and not security_id:
        raise HTTPException(status_code=422, detail="symbol or security_id is required")

    resolved_end = end_date or datetime.now(UTC).date()
    resolved_start = start_date or (resolved_end - timedelta(days=90))
    if resolved_start > resolved_end:
        raise HTTPException(status_code=422, detail="start_date must be on or before end_date")

    try:
        return finance_service.search_market(
            symbol=symbol.strip().upper() if symbol else None,
            security_id=security_id.strip().upper() if security_id else None,
            start_date=resolved_start,
            end_date=resolved_end,
            adjustment_type=adjustment_type,
        )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to query market data from lakehouse: {exc}",
        ) from exc
