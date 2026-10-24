"""PDF text and section extraction for annual reports."""

from __future__ import annotations

import re
from uuid import NAMESPACE_URL, uuid5

from backend.schemas.annual_report import AnnualReportPageRecord, AnnualReportSectionRecord
from backend.utils.hash import sha256_text

try:  # pragma: no cover - exercised in the runtime image
    import fitz
except ImportError:  # pragma: no cover
    fitz = None


class AnnualReportParseError(Exception):
    """Raised when a PDF cannot be parsed."""


SECTION_PATTERN = re.compile(r"(第[一二三四五六七八九十百]+章\s+.{2,80}|[一二三四五六七八九十百]+、\s*.{2,80})")


def extract_report_pages(pdf_bytes: bytes, *, report_id: str, stock_code: str, report_year: int) -> list[AnnualReportPageRecord]:
    """Extract normalized text while retaining physical page numbers."""

    if fitz is None:
        raise AnnualReportParseError("PyMuPDF is not installed")
    try:
        document = fitz.open(stream=pdf_bytes, filetype="pdf")
        pages = []
        for index, page in enumerate(document):
            text = re.sub(r"\s+", " ", page.get_text("text") or "").strip()
            pages.append(
                AnnualReportPageRecord(
                    report_id=report_id,
                    stock_code=stock_code,
                    report_year=report_year,
                    page_number=index + 1,
                    text=text,
                    text_sha256=sha256_text(text),
                    char_count=len(text),
                    extraction_status="ok" if text else "needs_ocr",
                )
            )
        document.close()
        return pages
    except Exception as exc:  # pragma: no cover - depends on malformed external PDFs
        raise AnnualReportParseError(f"PDF extraction failed: {exc}") from exc


def build_report_sections(pages: list[AnnualReportPageRecord]) -> list[AnnualReportSectionRecord]:
    """Group pages by conservative chapter-heading detection."""

    if not pages:
        return []
    sections: list[AnnualReportSectionRecord] = []
    current_title = "报告正文"
    current_start = pages[0].page_number
    current_text: list[str] = []
    order = 0

    def flush(end_page: int) -> None:
        nonlocal order, current_text
        text = "\n".join(part for part in current_text if part).strip()
        if not text:
            return
        order += 1
        section_path = current_title
        section_id = str(uuid5(NAMESPACE_URL, f"{pages[0].report_id}:{section_path}:{current_start}:{end_page}"))
        sections.append(AnnualReportSectionRecord(
            section_id=section_id,
            report_id=pages[0].report_id,
            stock_code=pages[0].stock_code,
            report_year=pages[0].report_year,
            section_title=current_title,
            section_path=section_path,
            start_page=current_start,
            end_page=end_page,
            text=text,
            section_order=order,
        ))
        current_text = []

    for page in pages:
        heading_match = SECTION_PATTERN.search(page.text)
        heading = heading_match.group(1).strip() if heading_match else None
        if heading and current_text:
            flush(page.page_number - 1)
            current_title = heading
            current_start = page.page_number
        current_text.append(page.text)
    flush(pages[-1].page_number)
    return sections
