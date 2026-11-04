"""Internal job trigger APIs used by orchestrators."""

from __future__ import annotations

import asyncio
from datetime import date
from uuid import NAMESPACE_URL, uuid5

from fastapi import APIRouter, Header, HTTPException, Query, status

from backend.config import settings
from backend.schemas.jobs import (
    AShareGapPlanRequest,
    AShareUniverseCoverageRequest,
    AnnualReportBronzeManifestRequest,
    AnnualReportIRToSilverRequest,
    AnnualReportParseRequest,
    AnnualReportRaySubmitRequest,
    AnnualReportRecoveryPlanRequest,
    AnnualReportRecoverOneRequest,
    BronzeToSilverRequest,
    IngestAnnualReportsRequest,
    IngestAshareDailyBarsRequest,
    IngestFinanceNewsRequest,
    RagIndexRequest,
)
from backend.schemas.monitor import MonitorRunAccepted, MonitorRunRequest

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

    from backend.lakehouse.a_share_ingestion_jobs import AShareIngestionJobStore

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
    ignore_coverage: bool = Query(default=False),
    x_openfinance_token: str | None = Header(default=None),
) -> dict[str, object]:
    """Return the current ordinary A-share universe for Airflow batch planning."""

    from backend.providers.finance import BaoStockFinanceProvider

    _require_internal_token(x_openfinance_token)
    options = {"adjustment_type": adjustment_type}
    if ignore_coverage:
        options["ignore_coverage"] = True
    result = BaoStockFinanceProvider().list_a_share_symbols(**options)
    return {
        "status": result.status,
        "provider": settings.FINANCE_DAILY_PROVIDER,
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
        start_date=request.start_date,
        symbols=request.symbols,
        mode=request.mode,
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
        daily_provider_name=request.daily_provider_name or settings.FINANCE_DAILY_PROVIDER,
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
    return FinanceBronzeToSilverPipeline().process_objects(request.object_paths).to_dict()

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


@router.post("/annual-reports-bronze-manifests", status_code=status.HTTP_200_OK)
def list_annual_report_bronze_manifests(
    request: AnnualReportBronzeManifestRequest,
    x_openfinance_token: str | None = Header(default=None),
) -> list[str]:
    """List complete existing Bronze manifests for an offline Silver replay."""

    from backend.ingestion.writers.bronze_writer import BronzeWriter, BronzeWriterError
    from backend.schemas.annual_report import AnnualReportBronzeObject

    _require_internal_token(x_openfinance_token)
    stock_filters = {str(stock).strip().upper().split(".", 1)[0] for stock in request.stocks}
    year_filters = {int(year) for year in request.years}
    writer = BronzeWriter()
    paths: list[str] = []
    for path in writer.list_objects(prefix="reports/provider=cninfo/"):
        if not path.endswith("/metadata.json"):
            continue
        try:
            manifest = AnnualReportBronzeObject.model_validate(writer.read_json(path))
            announcement = manifest.announcement
            if announcement.report_year not in year_filters:
                continue
            if stock_filters and announcement.stock_code.split(".", 1)[0] not in stock_filters:
                continue
            if not writer.object_exists(manifest.pdf_object_path):
                continue
        except (BronzeWriterError, ValueError):
            continue
        paths.append(path)
    return sorted(set(paths))


@router.post("/annual-reports-bronze-to-silver", status_code=status.HTTP_200_OK)
def trigger_annual_reports_bronze_to_silver(request: BronzeToSilverRequest, x_openfinance_token: str | None = Header(default=None)) -> dict[str, int]:
    """Compatibility entrypoint that now uses parser service and minimal Silver."""

    _require_internal_token(x_openfinance_token)
    if not request.object_paths:
        return {"processed_runs": 0, "written_chunks": 0}
    from backend.pdf_parser.client import PdfParserClient
    from backend.pipelines.annual_reports_ir_to_silver import AnnualReportsIRToSilverPipeline

    parse_results = [PdfParserClient().parse(path) for path in request.object_paths]
    return AnnualReportsIRToSilverPipeline().run(parse_results).to_dict()


@router.post("/annual-reports-parse", status_code=status.HTTP_200_OK)
def trigger_annual_reports_parse(request: AnnualReportParseRequest, x_openfinance_token: str | None = Header(default=None)) -> list[dict[str, object]]:
    """Parse Bronze manifests through the dedicated parser service."""

    _require_internal_token(x_openfinance_token)
    return _parse_annual_report_manifests(request.manifest_paths, force=request.force)


@router.post("/annual-reports-ray", status_code=status.HTTP_202_ACCEPTED)
def submit_annual_reports_ray(
    request: AnnualReportRaySubmitRequest,
    x_openfinance_token: str | None = Header(default=None),
) -> dict[str, str]:
    """Submit a bounded annual-report batch to the shared Ray cluster."""

    from backend.services.annual_report_ray_service import AnnualReportRayError, AnnualReportRayService

    _require_internal_token(x_openfinance_token)
    try:
        return AnnualReportRayService().submit(request.manifest_paths, force=request.force)
    except AnnualReportRayError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc


@router.get("/annual-reports-ray/{job_id}", status_code=status.HTTP_200_OK)
def get_annual_reports_ray(
    job_id: str,
    x_openfinance_token: str | None = Header(default=None),
) -> dict[str, str]:
    """Return the current Ray Job status for Airflow polling."""

    from backend.services.annual_report_ray_service import AnnualReportRayError, AnnualReportRayService

    _require_internal_token(x_openfinance_token)
    try:
        return AnnualReportRayService().status(job_id)
    except AnnualReportRayError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc


@router.post("/annual-reports-ray/{job_id}/cancel", status_code=status.HTTP_200_OK)
def cancel_annual_reports_ray(
    job_id: str,
    x_openfinance_token: str | None = Header(default=None),
) -> dict[str, str]:
    """Stop a Ray Job without deleting any Bronze or Derived artifacts."""

    from backend.services.annual_report_ray_service import AnnualReportRayError, AnnualReportRayService

    _require_internal_token(x_openfinance_token)
    try:
        return AnnualReportRayService().stop(job_id)
    except AnnualReportRayError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc


def _parse_annual_report_manifests(manifest_paths: list[str], *, force: bool) -> list[dict[str, object]]:
    from backend.ingestion.writers.bronze_writer import BronzeWriter
    from backend.pdf_parser.client import PdfParserClient
    from backend.pipelines.annual_report_pipeline_state import AnnualReportPipelineStateStore
    from backend.schemas.annual_report import AnnualReportBronzeObject

    bronze = BronzeWriter()
    state = AnnualReportPipelineStateStore()
    state.ensure_schema()
    client = PdfParserClient()
    results: list[dict[str, object]] = []
    failures: list[dict[str, str]] = []
    for manifest_path in manifest_paths:
        lease = None
        try:
            manifest = AnnualReportBronzeObject.model_validate(bronze.read_json(manifest_path))
            report_id = str(uuid5(NAMESPACE_URL, f"cninfo:{manifest.announcement.announcement_id}:{manifest.pdf_sha256}"))
            lease = state.claim_stage(
                report_id=report_id,
                pdf_sha256=manifest.pdf_sha256,
                stage="parse",
                stage_version=settings.PDF_PARSER_VERSION,
            )
            result = client.parse(manifest_path, force=force)
            state.complete_stage(lease, output_ref=str(result["ir_object_path"]))
            results.append(result)
        except Exception as exc:
            if lease is not None:
                state.fail_stage(lease, exc)
            failures.append({"manifest_path": manifest_path, "error": str(exc)[:1000]})
    if failures:
        raise HTTPException(status_code=500, detail={"parse_results": results, "failed": failures})
    return results


@router.post("/annual-reports-ir-to-silver", status_code=status.HTTP_200_OK)
def trigger_annual_reports_ir_to_silver(request: AnnualReportIRToSilverRequest, x_openfinance_token: str | None = Header(default=None)) -> dict[str, int]:
    """Materialize parser-derived Document IR into minimal Silver tables."""

    from backend.pipelines.annual_reports_ir_to_silver import AnnualReportsIRToSilverPipeline

    _require_internal_token(x_openfinance_token)
    stats = AnnualReportsIRToSilverPipeline().run([item.model_dump() for item in request.parse_results]).to_dict()
    if stats["failed_count"]:
        raise HTTPException(status_code=500, detail=stats)
    return stats


@router.post("/annual-reports-recovery-plan", status_code=status.HTTP_200_OK)
def annual_reports_recovery_plan(
    request: AnnualReportRecoveryPlanRequest,
    x_openfinance_token: str | None = Header(default=None),
) -> list[dict[str, object]]:
    """List failed or lease-expired report stages for the recovery DAG."""

    from backend.ingestion.writers.bronze_writer import BronzeWriter, BronzeWriterError
    from backend.ingestion.writers.derived_writer import DerivedWriter
    from backend.lakehouse.annual_report_writer import AnnualReportWriter
    from backend.pipelines.annual_report_pipeline_state import AnnualReportPipelineStateStore
    from backend.schemas.annual_report import AnnualReportBronzeObject

    _require_internal_token(x_openfinance_token)
    state = AnnualReportPipelineStateStore()
    state.ensure_schema()
    report_filter = set(request.report_ids)
    pending = state.pending_stages()
    all_states = state.list_stages()
    grouped: dict[tuple[str, str], list[str]] = {}
    for item in pending:
        report_id = str(item["report_id"])
        if report_filter and report_id not in report_filter:
            continue
        grouped.setdefault((report_id, str(item["pdf_sha256"])), []).append(str(item["stage"]))
    writer = BronzeWriter()
    derived = DerivedWriter()
    silver = AnnualReportWriter()
    silver_snapshot = silver.projection_snapshot()
    results: list[dict[str, object]] = []
    seen: set[tuple[str, str]] = set()
    seen_manifest_paths: set[str] = set()
    for object_path in writer.list_objects(prefix="reports/provider=cninfo/"):
        if not object_path.endswith("/metadata.json"):
            continue
        try:
            manifest = AnnualReportBronzeObject.model_validate(writer.read_json(object_path))
            seen_manifest_paths.add(object_path)
            report_id = str(uuid5(NAMESPACE_URL, f"cninfo:{manifest.announcement.announcement_id}:{manifest.pdf_sha256}"))
            key = (report_id, manifest.pdf_sha256)
            if report_filter and report_id not in report_filter:
                continue
            seen.add(key)
            state_rows = [item for item in all_states if item["report_id"] == report_id and item["pdf_sha256"] == manifest.pdf_sha256]
            failed_stages = {str(item["stage"]) for item in pending if item["report_id"] == report_id and item["pdf_sha256"] == manifest.pdf_sha256}
            bronze_row = next((item for item in state_rows if item["stage"] == "bronze" and item["stage_version"] == "bronze-v1"), None)
            verify_hash = bool(report_filter) or bronze_row is None or bronze_row["status"] != "succeeded"
            try:
                bronze_ok = writer.object_exists(manifest.pdf_object_path)
                if bronze_ok and verify_hash:
                    bronze_ok = writer.object_sha256(manifest.pdf_object_path) == manifest.pdf_sha256
            except BronzeWriterError:
                bronze_ok = False
            if not bronze_ok:
                if state.enqueue_stage(report_id=report_id, pdf_sha256=manifest.pdf_sha256, stage="bronze", stage_version="bronze-v1", error="Bronze PDF is missing or sha256 does not match manifest"):
                    failed_stages.add("bronze")
            elif bronze_row is None and verify_hash:
                state.record_verified_stage(report_id=report_id, pdf_sha256=manifest.pdf_sha256, stage="bronze", stage_version="bronze-v1", output_ref=object_path)
                failed_stages.discard("bronze")
            elif bronze_row is not None and bronze_ok:
                state.record_verified_stage(report_id=report_id, pdf_sha256=manifest.pdf_sha256, stage="bronze", stage_version="bronze-v1", output_ref=object_path)
                failed_stages.discard("bronze")
            parse_row = next((item for item in state_rows if item["stage"] == "parse" and item["stage_version"] == settings.PDF_PARSER_VERSION), None)
            parse_ok = False
            ir: dict[str, object] | None = None
            expected_parse_run_id = str(uuid5(NAMESPACE_URL, f"{report_id}:{settings.PDF_PARSER_VERSION}:{manifest.pdf_sha256}"))
            expected_prefix = (
                f"reports/provider={manifest.announcement.provider}/stock={manifest.announcement.stock_code}/"
                f"report_year={manifest.announcement.report_year}/report_id={report_id}/"
                f"parser_version={settings.PDF_PARSER_VERSION}/run={expected_parse_run_id}"
            )
            ir_path = str(parse_row.get("output_ref")) if parse_row and parse_row.get("output_ref") else f"{expected_prefix}/document.json"
            if bronze_ok and "bronze" not in failed_stages:
                parse_ok, ir = _annual_report_ir_is_valid(derived, ir_path, report_id, manifest.pdf_sha256)
                if not parse_ok:
                    if state.enqueue_stage(report_id=report_id, pdf_sha256=manifest.pdf_sha256, stage="parse", stage_version=settings.PDF_PARSER_VERSION, error="Derived Document IR or referenced page artifact is missing"):
                        failed_stages.add("parse")
                elif parse_row is None:
                    state.record_verified_stage(report_id=report_id, pdf_sha256=manifest.pdf_sha256, stage="parse", stage_version=settings.PDF_PARSER_VERSION, output_ref=ir_path)
                    failed_stages.discard("parse")
                elif parse_ok:
                    state.record_verified_stage(report_id=report_id, pdf_sha256=manifest.pdf_sha256, stage="parse", stage_version=settings.PDF_PARSER_VERSION, output_ref=ir_path)
                    failed_stages.discard("parse")
            silver_row = next((item for item in state_rows if item["stage"] == "silver" and item["stage_version"] == "annual-report-silver-v1"), None)
            if bronze_ok and "bronze" not in failed_stages and parse_ok and ir is not None:
                if not _annual_report_silver_is_valid(silver_snapshot, ir, report_id, str(ir.get("parse_run_id") or "")):
                    if state.enqueue_stage(report_id=report_id, pdf_sha256=manifest.pdf_sha256, stage="silver", stage_version="annual-report-silver-v1", error="Silver projection or parse-run commit marker is missing"):
                        failed_stages.add("silver")
                elif silver_row is None:
                    state.record_verified_stage(report_id=report_id, pdf_sha256=manifest.pdf_sha256, stage="silver", stage_version="annual-report-silver-v1", output_ref=expected_parse_run_id)
                    failed_stages.discard("silver")
                else:
                    state.record_verified_stage(report_id=report_id, pdf_sha256=manifest.pdf_sha256, stage="silver", stage_version="annual-report-silver-v1", output_ref=expected_parse_run_id)
                    failed_stages.discard("silver")
            if not failed_stages:
                continue
        except (BronzeWriterError, ValueError):
            continue
        results.append({
            "report_id": report_id,
            "pdf_sha256": manifest.pdf_sha256,
            "manifest_path": object_path,
            "pending_stages": _ordered_annual_report_stages(failed_stages),
        })
    # Preserve failed state rows whose manifest is temporarily unavailable so
    # the next Bronze ingestion can repair that one report.
    for key, stages in grouped.items():
        if key in seen:
            continue
        report_id, pdf_sha256 = key
        if report_filter and report_id not in report_filter:
            continue
        output_ref = next((item.get("output_ref") for item in all_states if item["report_id"] == report_id and item["pdf_sha256"] == pdf_sha256 and item["stage"] == "bronze"), None)
        if output_ref:
            stages = set(stages) | {"bronze"}
            if state.enqueue_stage(report_id=report_id, pdf_sha256=pdf_sha256, stage="bronze", stage_version="bronze-v1", error="Bronze manifest is missing or invalid"):
                results.append({"report_id": report_id, "pdf_sha256": pdf_sha256, "manifest_path": str(output_ref), "pending_stages": _ordered_annual_report_stages(stages)})
                seen.add(key)
    for item in all_states:
        if item["stage"] != "bronze" or not item.get("output_ref"):
            continue
        path = str(item["output_ref"])
        key = (str(item["report_id"]), str(item["pdf_sha256"]))
        if path in seen_manifest_paths or key in seen or (report_filter and key[0] not in report_filter):
            continue
        if state.enqueue_stage(report_id=key[0], pdf_sha256=key[1], stage="bronze", stage_version="bronze-v1", error="Bronze manifest is missing or invalid"):
            results.append({"report_id": key[0], "pdf_sha256": key[1], "manifest_path": path, "pending_stages": ["bronze"]})
            seen.add(key)
    return results


def _ordered_annual_report_stages(stages: set[str] | list[str]) -> list[str]:
    order = {"bronze": 0, "parse": 1, "silver": 2, "index": 3}
    return sorted(set(stages), key=lambda item: (order.get(item, 99), item))


def _annual_report_ir_is_valid(derived, ir_path: str, report_id: str, pdf_sha256: str) -> tuple[bool, dict[str, object] | None]:
    quality_path = f"{ir_path.rsplit('/', 1)[0]}/quality.json"
    try:
        ir = derived.read_json(ir_path)
        quality = derived.read_json(quality_path)
    except Exception:
        return False, None
    pages = ir.get("pages") if isinstance(ir, dict) else None
    if not isinstance(ir, dict) or not isinstance(pages, list) or not pages or not isinstance(quality, dict):
        return False, None
    if ir.get("report_id") != report_id or ir.get("pdf_sha256") != pdf_sha256 or quality != ir.get("quality"):
        return False, None
    if not ir.get("parser_version") or not ir.get("raw_page_manifest_path"):
        return False, None
    try:
        page_manifest = derived.read_json(str(ir["raw_page_manifest_path"]))
    except Exception:
        return False, None
    if not isinstance(page_manifest, dict):
        return False, None
    if (
        page_manifest.get("report_id") != report_id
        or page_manifest.get("parse_run_id") != ir.get("parse_run_id")
        or page_manifest.get("pdf_sha256") != pdf_sha256
        or page_manifest.get("parser_version") != ir.get("parser_version")
        or page_manifest.get("page_count") != len(pages)
        or page_manifest.get("complete") is not True
    ):
        return False, None
    page_numbers: set[int] = set()
    for page in pages:
        if not isinstance(page, dict) or not page.get("ir_page_path"):
            return False, None
        page_number = page.get("page_number")
        if not isinstance(page_number, int) or page_number < 1 or page_number in page_numbers:
            return False, None
        page_numbers.add(page_number)
        try:
            page_artifact = derived.read_json(str(page["ir_page_path"]))
            if not isinstance(page_artifact, dict):
                return False, None
            if page_artifact.get("page_number") != page_number:
                return False, None
        except Exception:
            return False, None
        for region in page.get("regions") or []:
            if not isinstance(region, dict):
                return False, None
            content_ref = region.get("content_ref")
            if content_ref:
                try:
                    if not derived.object_exists(str(content_ref)):
                        return False, None
                except Exception:
                    return False, None
    return True, ir


def _annual_report_silver_is_valid(snapshot: dict[str, set[tuple[str, ...]]], ir: dict[str, object], report_id: str, parse_run_id: str) -> bool:
    from backend.rag.annual_report_chunking import build_ir_chunks

    pages = [item for item in ir.get("pages") or [] if isinstance(item, dict)]
    sections = [item for item in ir.get("section_tree") or [] if isinstance(item, dict)]
    try:
        chunks = build_ir_chunks(ir, report_id=report_id, parse_run_id=parse_run_id)
        key = (report_id, parse_run_id)
        return (
            key in snapshot["reports"]
            and key in snapshot["parse_runs"]
            and {(report_id, parse_run_id, str(int(item["page_number"]))) for item in pages}
            == {item for item in snapshot["pages"] if item[:2] == key}
            and {(report_id, parse_run_id, str(region["region_id"])) for page in pages for region in page.get("regions") or []}
            == {item for item in snapshot["regions"] if item[:2] == key}
            and {(report_id, parse_run_id, str(item["section_id"])) for item in sections}
            == {item for item in snapshot["sections"] if item[:2] == key}
            and {(report_id, parse_run_id, item.chunk_id) for item in chunks}
            == {item for item in snapshot["chunks"] if item[:2] == key}
        )
    except Exception:
        return False


@router.post("/annual-reports-recover-one", status_code=status.HTTP_200_OK)
def recover_one_annual_report(
    request: AnnualReportRecoverOneRequest,
    x_openfinance_token: str | None = Header(default=None),
) -> dict[str, object]:
    """Resume one report from its earliest recorded failed stage."""

    _require_internal_token(x_openfinance_token)
    parse_results: list[dict[str, object]] = []
    if set(request.pending_stages) & {"bronze", "parse", "silver"}:
        manifest_path = request.manifest_path
        if "bronze" in request.pending_stages:
            from backend.ingestion.jobs.ingest_annual_reports import repair_annual_report_bronze
            from backend.ingestion.writers.bronze_writer import BronzeWriter
            from backend.pipelines.annual_report_pipeline_state import AnnualReportPipelineStateStore
            from backend.schemas.annual_report import AnnualReportBronzeObject

            state = AnnualReportPipelineStateStore()
            state.ensure_schema()
            bronze_lease = state.claim_stage(
                report_id=request.report_id,
                pdf_sha256=request.pdf_sha256,
                stage="bronze",
                stage_version="bronze-v1",
            )
            try:
                manifest_path = repair_annual_report_bronze(
                    manifest_path,
                    report_id=request.report_id,
                    pdf_sha256=request.pdf_sha256,
                )
                repaired_manifest = AnnualReportBronzeObject.model_validate(BronzeWriter().read_json(manifest_path))
                state.complete_stage(bronze_lease, output_ref=manifest_path)
                repaired_report_id = str(uuid5(NAMESPACE_URL, f"cninfo:{repaired_manifest.announcement.announcement_id}:{repaired_manifest.pdf_sha256}"))
                if (repaired_report_id, repaired_manifest.pdf_sha256) != (request.report_id, request.pdf_sha256):
                    state.record_verified_stage(
                        report_id=repaired_report_id,
                        pdf_sha256=repaired_manifest.pdf_sha256,
                        stage="bronze",
                        stage_version="bronze-v1",
                        output_ref=manifest_path,
                    )
            except Exception as exc:
                try:
                    state.fail_stage(bronze_lease, exc)
                except Exception:
                    pass
                raise HTTPException(status_code=500, detail=f"Bronze repair failed: {exc}") from exc
        # Recovery uses the same Ray entrypoint as replay/backfill. This keeps
        # the stopped legacy parser out of the recovery path and lets the Ray
        # driver reuse page checkpoints before rebuilding Silver.
        from backend.services.annual_report_ray_service import AnnualReportRayError, AnnualReportRayService

        try:
            ray_result = AnnualReportRayService().submit_and_wait([manifest_path], force=False)
        except AnnualReportRayError as exc:
            raise HTTPException(status_code=503, detail=str(exc)) from exc
        stats = {"processed_runs": 1, "written_reports": 0, "failed_count": 0}
        return {"silver": stats, "parse_results": 1, "ray_job": ray_result}
    else:
        stats = {"processed_runs": 0, "written_reports": 0, "failed_count": 0}
    return {"silver": stats, "parse_results": len(parse_results)}


@router.post("/annual-reports-rag-index", status_code=status.HTTP_200_OK)
def trigger_annual_reports_rag_index(request: RagIndexRequest, x_openfinance_token: str | None = Header(default=None)) -> dict[str, int | str]:
    """Build the annual-report pgvector index."""

    from backend.rag.annual_report_indexer import AnnualReportRAGIndexer

    _require_internal_token(x_openfinance_token)
    indexer = AnnualReportRAGIndexer()
    try:
        return indexer.run(rebuild_all=request.rebuild_all)
    finally:
        indexer.close()


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


@router.post("/prune-harness-checkpoints", status_code=status.HTTP_200_OK)
def prune_harness_checkpoints(
    x_openfinance_token: str | None = Header(default=None),
) -> dict[str, int]:
    """Prune checkpoints for archived research threads."""

    from backend.harness.store import HarnessStore

    _require_internal_token(x_openfinance_token)
    removed = HarnessStore().prune_archived_checkpoints(
        older_than_days=settings.HARNESS_CHECKPOINT_RETENTION_DAYS,
    )
    return {"threads": removed}
