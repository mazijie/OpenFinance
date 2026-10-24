"""Read-only market data API routes."""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from functools import lru_cache

from fastapi import APIRouter, Depends, HTTPException, Query, status

from backend.schemas.finance import AdjustmentType, MarketSearchResult, SecurityOptionsResult
from backend.services.finance_service import FinanceService

router = APIRouter(prefix="/api/market", tags=["market"])


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


@router.get("", response_model=MarketSearchResult, status_code=status.HTTP_200_OK)
def get_market(
    symbol: str | None = Query(default=None, min_length=1, max_length=16),
    security_id: str | None = Query(default=None, min_length=1, max_length=64),
    start_date: date | None = Query(default=None),
    end_date: date | None = Query(default=None),
    adjustment_type: AdjustmentType = Query(default="qfq"),
    as_of: datetime | None = Query(default=None),
    finance_service: FinanceService = Depends(get_finance_service),
) -> MarketSearchResult:
    """Query Silver/Gold market data without contacting an external provider."""

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
            as_of=as_of,
        )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to query market data from lakehouse: {exc}",
        ) from exc
