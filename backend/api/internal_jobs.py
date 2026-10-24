"""Internal job trigger APIs used by orchestrators."""

from __future__ import annotations

import asyncio
from datetime import date

from fastapi import APIRouter, Header, HTTPException, Query, status

from backend.config import settings
from backend.schemas.jobs import AShareGapPlanRequest, AShareUniverseCoverageRequest, BronzeToSilverRequest, IngestAnnualReportsRequest, IngestAshareDailyBarsRequest, IngestFinanceNewsRequest, RagIndexRequest
from backend.schemas.monitor import MonitorRunAccepted, MonitorRunRequest

router = APIRouter(prefix="/internal/jobs", tags=["internal-jobs"])


@router.post("/ingest-a-share-daily-bars/submit", status_code=status.HTTP_202_ACCEPTED)
def submit_ingest_a_share_daily_bars(
    request: IngestAshareDailyBarsRequest,
    x_openfinance_token: str | None = Header(default=None),
) -> dict[str, str]:
    """Queue an A-share ingestion job and return immediately."""

    from backend.lakehouse.a_share_ingestion_jobs import AShareIngestionJobStore

    _require_internal_token(x_openfinance_token)
    payload = request.model_dump(mode="json", exclude={"idempotency_key"})
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

    from backend.lakehouse.a_share_ingestion_jobs import AShareIngestionJobStore

    _require_internal_token(x_openfinance_token)
    try:
        job = AShareIngestionJobStore().get(job_id)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid job id") from exc
    if job is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Job not found")
    return job


def _require_internal_token(x_openfinance_token: str | None) -> None:
    expected_token = settings.OPENFINANCE_INTERNAL_API_TOKEN.strip()
    provided_token = (x_openfinance_token or "").strip()
    if not expected_token or provided_token != expected_token:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Forbidden")


@router.get("/list-a-share-symbols", status_code=status.HTTP_200_OK)
def list_a_share_symbols(
    adjustment_type: str = Query(default=settings.FINANCE_DEFAULT_ADJUSTMENT_TYPE),
    x_openfinance_token: str | None = Header(default=None),
) -> dict[str, object]:
    """Return the current ordinary A-share universe for Airflow batch planning."""

    from backend.providers.finance import BaoStockFinanceProvider

    _require_internal_token(x_openfinance_token)
    result = BaoStockFinanceProvider().list_a_share_symbols(
        adjustment_type=adjustment_type,
    )
    return {
        "status": result.status,
        "as_of_date": result.as_of_date.isoformat(),
        "symbols": result.symbols,
        "symbol_count": len(result.symbols),
    }


@router.post("/a-share-universe-coverage", status_code=status.HTTP_200_OK)
def record_a_share_universe_coverage(
    request: AShareUniverseCoverageRequest,
    x_openfinance_token: str | None = Header(default=None),
) -> dict[str, object]:
    """Persist a full-market completion watermark after all batches reach Silver."""

    from backend.lakehouse.a_share_coverage import AShareCoverageStore

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
    )


@router.post("/ingest-finance-news", status_code=status.HTTP_200_OK)
def trigger_ingest_finance_news(
    request: IngestFinanceNewsRequest,
    x_openfinance_token: str | None = Header(default=None),
) -> dict[str, object]:
    """Run one ingestion cycle inside the backend runtime."""

    from backend.ingestion.jobs.ingest_news import ingest_finance_news

    _require_internal_token(x_openfinance_token)
    result = asyncio.run(
        ingest_finance_news(
            start_time=request.start_time,
            end_time=request.end_time,
            limit=request.limit,
        )
    )
    return result.to_dict()


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
    )
    return result.to_dict()


@router.post("/news-bronze-to-silver", status_code=status.HTTP_200_OK)
def trigger_news_bronze_to_silver(
    request: BronzeToSilverRequest,
    x_openfinance_token: str | None = Header(default=None),
) -> dict[str, int]:
    """Run Bronze-to-Silver normalization inside the backend runtime."""

    from backend.pipelines.news_bronze_to_silver import NewsBronzeToSilverPipeline

    _require_internal_token(x_openfinance_token)
    pipeline = NewsBronzeToSilverPipeline()
    stats = pipeline.run(object_paths=request.object_paths)
    return stats.to_dict()


@router.post("/finance-bronze-to-silver", status_code=status.HTTP_200_OK)
def trigger_finance_bronze_to_silver(
    request: BronzeToSilverRequest,
    x_openfinance_token: str | None = Header(default=None),
) -> dict[str, int]:
    """Run finance Bronze-to-Silver normalization inside the backend runtime."""

    from backend.pipelines.finance_bronze_to_silver import FinanceBronzeToSilverPipeline

    _require_internal_token(x_openfinance_token)
    pipeline = FinanceBronzeToSilverPipeline()
    aggregate = {
        "read_count": 0,
        "filtered_count": 0,
        "duplicate_count": 0,
        "failed_count": 0,
        "written_security_master": 0,
        "written_trading_calendar": 0,
        "written_daily_bars": 0,
        "processed_objects": 0,
    }
    for object_path in request.object_paths:
        stats = pipeline.process_object(object_path)
        for key, value in stats.to_dict().items():
            aggregate[key] = aggregate.get(key, 0) + int(value)
    return aggregate

@router.post("/finance-bronze-to-silver/submit", status_code=status.HTTP_202_ACCEPTED)
def submit_finance_bronze_to_silver(request: BronzeToSilverRequest, x_openfinance_token: str | None = Header(default=None)) -> dict[str, str]:
    from backend.lakehouse.finance_normalization_jobs import FinanceNormalizationJobStore
    _require_internal_token(x_openfinance_token)
    return {"job_id": FinanceNormalizationJobStore().create(request.model_dump(mode="json")), "status": "queued"}

@router.get("/finance-bronze-to-silver/jobs/{job_id}")
def get_finance_bronze_to_silver_job(job_id: str, x_openfinance_token: str | None = Header(default=None)) -> dict[str, object]:
    from backend.lakehouse.finance_normalization_jobs import FinanceNormalizationJobStore
    _require_internal_token(x_openfinance_token)
    try: job = FinanceNormalizationJobStore().get(job_id)
    except ValueError as exc: raise HTTPException(status_code=400, detail="Invalid job id") from exc
    if job is None: raise HTTPException(status_code=404, detail="Job not found")
    return job


@router.post("/news-rag-index", status_code=status.HTTP_200_OK)
def trigger_news_rag_index(
    request: RagIndexRequest,
    x_openfinance_token: str | None = Header(default=None),
) -> dict[str, object]:
    """Build or refresh the semantic news index."""

    from backend.rag.indexer import NewsRAGIndexer

    _require_internal_token(x_openfinance_token)
    indexer = NewsRAGIndexer()
    try:
        try:
            stats = indexer.run(rebuild_all=request.rebuild_all)
        except TypeError:
            stats = indexer.run()
    finally:
        close = getattr(indexer, "close", None)
        if callable(close):
            close()
    return stats.model_dump()


@router.post("/ingest-annual-reports", status_code=status.HTTP_200_OK)
def trigger_ingest_annual_reports(request: IngestAnnualReportsRequest, x_openfinance_token: str | None = Header(default=None)) -> list[dict[str, object]]:
    """Collect configured CNInfo annual-report PDFs into Bronze."""

    from backend.ingestion.jobs.ingest_annual_reports import ingest_annual_reports

    _require_internal_token(x_openfinance_token)
    return [item.to_dict() for item in ingest_annual_reports(stocks=request.stocks or None, years=request.years or None)]


@router.post("/annual-reports-bronze-to-silver", status_code=status.HTTP_200_OK)
def trigger_annual_reports_bronze_to_silver(request: BronzeToSilverRequest, x_openfinance_token: str | None = Header(default=None)) -> dict[str, int]:
    """Extract annual-report PDF text and write Silver tables."""

    from backend.pipelines.annual_reports_bronze_to_silver import AnnualReportsBronzeToSilverPipeline

    _require_internal_token(x_openfinance_token)
    return AnnualReportsBronzeToSilverPipeline().run(request.object_paths or None).to_dict()


@router.post("/annual-reports-rag-index", status_code=status.HTTP_200_OK)
def trigger_annual_reports_rag_index(request: RagIndexRequest, x_openfinance_token: str | None = Header(default=None)) -> dict[str, int]:
    """Build the annual-report pgvector index."""

    from backend.rag.annual_report_indexer import AnnualReportRAGIndexer

    _require_internal_token(x_openfinance_token)
    return AnnualReportRAGIndexer().run()


@router.post(
    "/investment-monitor/run",
    response_model=MonitorRunAccepted,
    status_code=status.HTTP_202_ACCEPTED,
)
def submit_investment_monitor_run(
    request: MonitorRunRequest,
    x_openfinance_token: str | None = Header(default=None),
) -> MonitorRunAccepted:
    """Queue one idempotent after-close research run."""

    from backend.services.investment_monitor_service import InvestmentMonitorService

    _require_internal_token(x_openfinance_token)
    try:
        return InvestmentMonitorService().create_run(request)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


@router.get("/investment-monitor/runs/{run_id}")
def get_investment_monitor_run(
    run_id: str,
    x_openfinance_token: str | None = Header(default=None),
) -> dict[str, object]:
    """Return durable monitor progress for Airflow."""

    from backend.monitoring.store import MonitorStore

    _require_internal_token(x_openfinance_token)
    try:
        run = MonitorStore().get_run(run_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="Invalid run id") from exc
    if run is None:
        raise HTTPException(status_code=404, detail="Monitor run not found")
    return run.model_dump(mode="json")


@router.get("/investment-monitor/trading-day", status_code=status.HTTP_200_OK)
def get_investment_monitor_trading_day(
    trading_date: date = Query(...),
    x_openfinance_token: str | None = Header(default=None),
) -> dict[str, object]:
    """Return calendar readiness so Airflow can skip exchange holidays."""

    from backend.services.finance_service import FinanceService

    _require_internal_token(x_openfinance_token)
    is_open = FinanceService().is_trading_day(trading_date)
    return {"trading_date": trading_date.isoformat(), "is_open": is_open}
