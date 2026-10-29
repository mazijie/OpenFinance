"""Strict entrypoint for rebuilding annual-report Silver from Bronze.

The parser service and IR materializer are the only implementation of the
annual-report pipeline. This module remains as a callable entrypoint for
operators that still invoke the old job name; it does not write the old
report/page/section shape.
"""

from __future__ import annotations

from dataclasses import dataclass

from backend.pdf_parser.client import PdfParserClient
from backend.pipelines.annual_reports_ir_to_silver import AnnualReportsIRToSilverPipeline


@dataclass(slots=True)
class AnnualReportPipelineStats:
    processed_objects: int = 0
    written_reports: int = 0
    written_pages: int = 0
    written_sections: int = 0
    written_chunks: int = 0
    failed_count: int = 0

    def to_dict(self) -> dict[str, int]:
        return {
            "processed_objects": self.processed_objects,
            "written_reports": self.written_reports,
            "written_pages": self.written_pages,
            "written_sections": self.written_sections,
            "written_chunks": self.written_chunks,
            "failed_count": self.failed_count,
        }


class AnnualReportsBronzeToSilverPipeline:
    """Run the current parser service followed by strict IR materialization."""

    def __init__(
        self,
        *,
        parser_client: PdfParserClient | None = None,
        ir_pipeline: AnnualReportsIRToSilverPipeline | None = None,
    ) -> None:
        self.parser_client = parser_client
        self.ir_pipeline = ir_pipeline

    def run(self, object_paths: list[str] | None = None) -> AnnualReportPipelineStats:
        paths = list(object_paths or [])
        if not paths:
            return AnnualReportPipelineStats()
        stats = AnnualReportPipelineStats(processed_objects=len(paths))
        try:
            parser_client = self.parser_client or PdfParserClient()
            ir_pipeline = self.ir_pipeline or AnnualReportsIRToSilverPipeline()
            parse_results = [parser_client.parse(path) for path in paths]
            materialized = ir_pipeline.run(parse_results)
            stats.written_reports = materialized.written_reports
            stats.written_pages = materialized.written_pages
            stats.written_sections = materialized.written_sections
            stats.written_chunks = materialized.written_chunks
            stats.failed_count = materialized.failed_count
        except Exception:
            stats.failed_count = len(paths)
        return stats
