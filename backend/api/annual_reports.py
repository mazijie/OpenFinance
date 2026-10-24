"""Annual-report serving API."""

from __future__ import annotations

from functools import lru_cache

from fastapi import APIRouter, Depends, HTTPException, Query, Response, status

from backend.ingestion.writers.bronze_writer import BronzeWriterError
from backend.schemas.annual_report import AnnualReportPage, AnnualReportPageResult
from backend.services.annual_report_service import AnnualReportService

router = APIRouter(prefix="/api/reports", tags=["annual-reports"])


@lru_cache(maxsize=1)
def get_annual_report_service() -> AnnualReportService:
    return AnnualReportService()


@router.get("", response_model=AnnualReportPageResult, status_code=status.HTTP_200_OK)
def list_reports(
    stock_code: str | None = Query(default=None, min_length=1, max_length=16),
    report_year: int | None = Query(default=None, ge=2000, le=2100),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=20, ge=1, le=100),
    service: AnnualReportService = Depends(get_annual_report_service),
) -> AnnualReportPageResult:
    try:
        return service.list_reports(stock_code=stock_code, report_year=report_year, page=page, page_size=page_size)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to list annual reports: {exc}") from exc


@router.get("/{report_id}/pages/{page_number}", response_model=AnnualReportPage)
def get_report_page(report_id: str, page_number: int, service: AnnualReportService = Depends(get_annual_report_service)) -> AnnualReportPage:
    try:
        return service.get_page(report_id, page_number)
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get("/{report_id}/download")
def download_report(report_id: str, service: AnnualReportService = Depends(get_annual_report_service)) -> Response:
    try:
        pdf_bytes = service.read_pdf(report_id)
        return Response(
            content=pdf_bytes,
            media_type="application/pdf",
            headers={"Content-Disposition": 'inline; filename="annual-report.pdf"'},
        )
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except BronzeWriterError as exc:
        raise HTTPException(status_code=502, detail="Failed to read annual report from object storage") from exc
