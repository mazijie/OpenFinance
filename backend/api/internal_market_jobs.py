"""Internal job trigger APIs used by orchestrators."""

from __future__ import annotations

from datetime import date

from fastapi import APIRouter, Header, HTTPException, Query, status

from backend.config import settings
from backend.schemas.jobs import AShareGapPlanRequest, AShareUniverseCoverageRequest, IngestAshareDailyBarsRequest

from backend.api.internal_auth import _require_internal_token

router = APIRouter(prefix="/internal/jobs", tags=["internal-jobs"])


@router.post("/trading-calendar-sync", status_code=status.HTTP_200_OK)
def sync_trading_calendar(x_openfinance_token: str | None = Header(default=None)) -> dict[str, object]:
    """Refresh the local calendar from BaoStock."""
    _require_internal_token(x_openfinance_token)
    from backend.ingestion.jobs.sync_trading_calendar import run
    return run()


@router.post("/ingest-a-share-daily-bars/submit", status_code=status.HTTP_202_ACCEPTED)
def submit_ingest_a_share_daily_bars(
    request: IngestAshareDailyBarsRequest,
    x_openfinance_token: str | None = Header(default=None),
) -> dict[str, str]:
    """Queue an A-share ingestion job and return immediately."""

    from backend.job_store.a_share_ingestion_jobs import AShareIngestionJobStore

    _require_internal_token(x_openfinance_token)
    payload = request.model_dump(mode="json", exclude={"idempotency_key"})
    payload["daily_provider_name"] = request.daily_provider_name or settings.FINANCE_DAILY_PROVIDER
    job_id = AShareIngestionJobStore().create(
        payload,
        idempotency_key=request.idempotency_key,
    )
    return {"job_id": job_id, "status": "queued"}


@router.get("/ingest-a-share-daily-bars/jobs/{job_id}", status_code=status.HTTP_200_OK)
def get_ingest_a_share_daily_bars_job(
    job_id: str,
    x_openfinance_token: str | None = Header(default=None),
) -> dict[str, object]:
    """Return the durable status/result of an A-share ingestion job."""

    from backend.job_store.a_share_ingestion_jobs import AShareIngestionJobStore

    _require_internal_token(x_openfinance_token)
    try:
        job = AShareIngestionJobStore().get(job_id)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid job id") from exc
    if job is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Job not found")
    return job


@router.get("/list-a-share-symbols", status_code=status.HTTP_200_OK)
def list_a_share_symbols(
    adjustment_type: str = Query(default=settings.FINANCE_DEFAULT_ADJUSTMENT_TYPE),
    ignore_coverage: bool = Query(default=False),
    x_openfinance_token: str | None = Header(default=None),
) -> dict[str, object]:
    """Return the current ordinary A-share universe for Airflow batch planning."""

    from backend.providers.tencent_securities import TencentSecurityProvider

    _require_internal_token(x_openfinance_token)
    options = {"adjustment_type": adjustment_type}
    if ignore_coverage:
        options["ignore_coverage"] = True
    provider = TencentSecurityProvider()
    try:
        result = provider.list_a_share_symbols(**options)
    finally:
        provider.close()
    return {
        "status": result.status,
        "provider": settings.FINANCE_DAILY_PROVIDER,
        "as_of_date": result.as_of_date.isoformat(),
        "symbols": result.symbols,
        "symbol_count": len(result.symbols),
        "historical_universe_complete": False,
    }


@router.get("/realtime-universe")
def realtime_universe(x_openfinance_token: str | None = Header(default=None)) -> dict[str, object]:
    """Refresh today's Tencent universe, retaining the last valid snapshot on failure."""
    from datetime import datetime
    from zoneinfo import ZoneInfo
    from backend.job_store.a_share_universe_cache import AShareUniverseCacheStore
    from backend.providers.tencent_securities import TencentSecurityProvider

    _require_internal_token(x_openfinance_token)
    today = datetime.now(ZoneInfo("Asia/Shanghai")).date()
    cache = AShareUniverseCacheStore()
    latest = cache.latest(provider="tencent")
    if latest and latest[0] == today:
        return {"symbols": latest[1], "as_of_date": latest[0].isoformat(), "stale": False}
    provider = None
    try:
        provider = TencentSecurityProvider()
        symbols = provider.fetch_a_share_symbols_for_day(day=today)
        if not symbols:
            raise ValueError("Tencent returned an empty universe")
        cache.put(provider="tencent", as_of_date=today, symbols=symbols)
        return {"symbols": symbols, "as_of_date": today.isoformat(), "stale": False}
    except Exception as exc:
        if latest:
            return {"symbols": latest[1], "as_of_date": latest[0].isoformat(), "stale": True}
        raise HTTPException(status_code=503, detail="Tencent security universe unavailable") from exc
    finally:
        if provider is not None:
            provider.close()


@router.post("/a-share-universe-coverage", status_code=status.HTTP_200_OK)
def record_a_share_universe_coverage(
    request: AShareUniverseCoverageRequest,
    x_openfinance_token: str | None = Header(default=None),
) -> dict[str, object]:
    """Persist a full-market completion watermark after all batches reach Silver."""

    from backend.job_store.a_share_coverage import AShareCoverageStore

    _require_internal_token(x_openfinance_token)
    created = AShareCoverageStore().record_coverage(**request.model_dump())
    return {"created": created}


@router.post("/a-share-missing-bars-plan", status_code=status.HTTP_200_OK)
def plan_missing_a_share_daily_bars(
    request: AShareGapPlanRequest,
    x_openfinance_token: str | None = Header(default=None),
) -> dict[str, object]:
    """Discover missing bars without spending a provider request per date."""

    from backend.services.a_share_gap_service import AShareGapService

    _require_internal_token(x_openfinance_token)
    return AShareGapService().plan(
        adjustment_type=request.adjustment_type,
        lookback_days=request.lookback_days,
        end_date=request.end_date,
        start_date=request.start_date,
        symbols=request.symbols,
        mode=request.mode,
    )


@router.post("/ingest-a-share-daily-bars", status_code=status.HTTP_200_OK)
def trigger_ingest_a_share_daily_bars(
    request: IngestAshareDailyBarsRequest,
    x_openfinance_token: str | None = Header(default=None),
) -> dict[str, object]:
    """Run one A-share daily bars ingestion cycle inside the backend runtime."""

    from backend.ingestion.jobs.ingest_a_share_daily_bars import ingest_a_share_daily_bars

    _require_internal_token(x_openfinance_token)
    result = ingest_a_share_daily_bars(
        symbols=request.symbols,
        start_date=request.start_date.date(),
        end_date=request.end_date.date(),
        adjustment_type=request.adjustment_type,
        idempotency_key=request.idempotency_key,
        include_security_master=request.include_security_master,
        include_trading_calendar=request.include_trading_calendar,
        daily_provider_name=request.daily_provider_name or settings.FINANCE_DAILY_PROVIDER,
    )
    return result.to_dict()


@router.get("/trading-day", status_code=status.HTTP_200_OK)
def get_trading_day(
    trading_date: date = Query(...),
    x_openfinance_token: str | None = Header(default=None),
) -> dict[str, object]:
    """Return calendar readiness so Airflow can skip exchange holidays."""

    from backend.services.finance_service import FinanceService

    _require_internal_token(x_openfinance_token)
    is_open = FinanceService().is_trading_day(trading_date)
    return {"trading_date": trading_date.isoformat(), "is_open": is_open}
