"""Jobs for downloading configured CNInfo annual reports into Bronze."""

from __future__ import annotations

import argparse
import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime

from backend.config import settings
from backend.ingestion.collectors.cninfo import CNInfoCollector
from backend.ingestion.writers.bronze_writer import BronzeWriter


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
    results: list[IngestAnnualReportsResult] = []
    try:
        for stock_code in stocks or settings.cninfo_report_stocks:
            for report_year in years or settings.cninfo_report_years:
                result = IngestAnnualReportsResult(stock_code=stock_code, report_year=report_year, manifest_paths=[])
                try:
                    reports = collector.list_annual_reports(stock_code=stock_code, report_year=report_year)
                    result.discovered_count = len(reports)
                    for report in reports:
                        pdf = collector.download_pdf(report)
                        result.manifest_paths.append(writer.write_annual_report(
                            announcement=report,
                            pdf_bytes=pdf,
                            query_params={"stock_code": stock_code, "report_year": report_year},
                            ingested_at=datetime.now(UTC),
                        ))
                        result.written_count += 1
                except Exception:
                    result.failed_count += 1
                results.append(result)
    finally:
        collector.close()
    return results


def main() -> None:
    parser = argparse.ArgumentParser(description="Ingest CNInfo annual-report PDFs into Bronze")
    parser.add_argument("--stocks", nargs="*")
    parser.add_argument("--years", nargs="*", type=int)
    args = parser.parse_args()
    print(json.dumps([item.to_dict() for item in ingest_annual_reports(stocks=args.stocks, years=args.years)], ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
