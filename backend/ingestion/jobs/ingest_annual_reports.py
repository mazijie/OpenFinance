"""Jobs for downloading configured CNInfo annual reports into Bronze."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
from dataclasses import asdict, dataclass
from datetime import UTC, datetime

from backend.config import settings
from backend.providers.cninfo import CNInfoCollector
from backend.ingestion.writers.bronze_writer import BronzeWriter, BronzeWriterError
from backend.pipelines.annual_report_pipeline_state import AnnualReportPipelineStateStore
from backend.schemas.annual_report import AnnualReportBronzeObject, AnnualReportAnnouncement
from uuid import NAMESPACE_URL, uuid5


@dataclass(slots=True)
class IngestAnnualReportsResult:
    stock_code: str
    report_year: int
    discovered_count: int = 0
    written_count: int = 0
    failed_count: int = 0
    manifest_paths: list[str] | None = None

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


def ingest_annual_reports(*, stocks: list[str] | None = None, years: list[int] | None = None) -> list[IngestAnnualReportsResult]:
    collector = CNInfoCollector()
    writer = BronzeWriter()
    state_store = AnnualReportPipelineStateStore()
    state_store.ensure_schema()
    results: list[IngestAnnualReportsResult] = []
    try:
        for stock_code in stocks or settings.cninfo_report_stocks:
            for report_year in years or settings.cninfo_report_years:
                result = IngestAnnualReportsResult(stock_code=stock_code, report_year=report_year, manifest_paths=[])
                try:
                    reports = collector.list_annual_reports(stock_code=stock_code, report_year=report_year)
                except Exception:
                    result.failed_count += 1
                    results.append(result)
                    continue
                result.discovered_count = len(reports)
                for report in reports:
                    try:
                        existing_path = _find_existing_manifest(writer, report)
                        if existing_path is not None:
                            manifest_path = existing_path
                        else:
                            pdf = collector.download_pdf(report)
                            manifest_path = writer.write_annual_report(
                                announcement=report,
                                pdf_bytes=pdf,
                                query_params={"stock_code": stock_code, "report_year": report_year},
                                ingested_at=datetime.now(UTC),
                            )
                        manifest = AnnualReportBronzeObject.model_validate(writer.read_json(manifest_path))
                        report_id = str(uuid5(NAMESPACE_URL, f"cninfo:{manifest.announcement.announcement_id}:{manifest.pdf_sha256}"))
                        lease = state_store.claim_stage(
                            report_id=report_id,
                            pdf_sha256=manifest.pdf_sha256,
                            stage="bronze",
                            stage_version="bronze-v1",
                        )
                        state_store.complete_stage(lease, output_ref=manifest_path)
                        result.manifest_paths.append(manifest_path)
                        result.written_count += 1
                    except Exception:
                        result.failed_count += 1
                results.append(result)
    finally:
        collector.close()
    return results


def _find_existing_manifest(writer: BronzeWriter, report: AnnualReportAnnouncement) -> str | None:
    """Reuse a complete deterministic Bronze object before calling CNInfo again."""

    prefix = (
        f"reports/provider=cninfo/stock={report.stock_code}/"
        f"report_year={report.report_year}/announcement_id={report.announcement_id}"
    )
    manifest_path = f"{prefix}/metadata.json"
    try:
        manifest = AnnualReportBronzeObject.model_validate(writer.read_json(manifest_path))
        if (
            manifest.announcement.announcement_id != report.announcement_id
            or manifest.announcement.stock_code != report.stock_code
            or manifest.announcement.report_year != report.report_year
        ):
            return None
        # Validate the immutable Bronze object before reusing it. A present but
        # corrupted object must be eligible for a fresh download.
        if not writer.object_exists(manifest.pdf_object_path):
            return None
        if writer.object_sha256(manifest.pdf_object_path) != manifest.pdf_sha256:
            return None
    except (BronzeWriterError, ValueError):
        return None
    return manifest_path


def repair_annual_report_bronze(manifest_path: str, *, report_id: str, pdf_sha256: str) -> str:
    """Repair one CNInfo Bronze object without scanning or downloading siblings."""
    writer = BronzeWriter()
    manifest = None
    try:
        manifest = AnnualReportBronzeObject.model_validate(writer.read_json(manifest_path))
        announcement = manifest.announcement
        expected_path = (
            f"reports/provider={announcement.provider}/stock={announcement.stock_code}/"
            f"report_year={announcement.report_year}/announcement_id={announcement.announcement_id}/metadata.json"
        )
        if expected_path != manifest_path:
            raise ValueError("Manifest identity does not match its Bronze object path")
        if writer.object_exists(manifest.pdf_object_path) and writer.object_sha256(manifest.pdf_object_path) == manifest.pdf_sha256:
            return manifest_path
    except (BronzeWriterError, ValueError):
        match = re.fullmatch(
            r"reports/provider=cninfo/stock=([^/]+)/report_year=(20\d{2})/announcement_id=([^/]+)/metadata\.json",
            manifest_path,
        )
        if match is None:
            raise ValueError("Cannot identify the CNInfo announcement from the Bronze path")
        stock_code, report_year, announcement_id = match.groups()
        announcement = None
    if len(report_id) != 36 or len(pdf_sha256) != 64:
        raise ValueError("Recovery report identity is invalid")
    collector = CNInfoCollector()
    try:
        if manifest is None:
            stock_code, report_year, announcement_id = match.groups()
        else:
            stock_code, report_year, announcement_id = (
                announcement.stock_code,
                str(announcement.report_year),
                announcement.announcement_id,
            )
        current = next(
            (
                item
                for item in collector.list_annual_reports(stock_code=stock_code, report_year=int(report_year))
                if item.announcement_id == announcement_id
            ),
            None,
        )
        if current is None:
            raise ValueError("CNInfo announcement is no longer available for Bronze repair")
        pdf_bytes = collector.download_pdf(current)
        if hashlib.sha256(pdf_bytes).hexdigest() != pdf_sha256:
            raise ValueError("Replacement PDF does not match the failed Bronze object's expected SHA-256")
        repaired_report_id = str(uuid5(NAMESPACE_URL, f"cninfo:{current.announcement_id}:{pdf_sha256}"))
        if repaired_report_id != report_id:
            raise ValueError("Replacement PDF does not match the failed Bronze report identity")
        repaired_path = writer.write_annual_report(
            announcement=current,
            pdf_bytes=pdf_bytes,
            query_params={"stock_code": current.stock_code, "report_year": current.report_year},
            ingested_at=datetime.now(UTC),
        )
        repaired = AnnualReportBronzeObject.model_validate(writer.read_json(repaired_path))
        if writer.object_sha256(repaired.pdf_object_path) != repaired.pdf_sha256:
            raise BronzeWriterError("Repaired Bronze PDF failed its SHA-256 verification")
        return repaired_path
    finally:
        collector.close()




def main() -> None:
    parser = argparse.ArgumentParser(description="Ingest CNInfo annual-report PDFs into Bronze")
    parser.add_argument("--stocks", nargs="*")
    parser.add_argument("--years", nargs="*", type=int)
    args = parser.parse_args()
    print(json.dumps([item.to_dict() for item in ingest_annual_reports(stocks=args.stocks, years=args.years)], ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
