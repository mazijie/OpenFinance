"""Structure-aware chunk generation for annual-report Document IR."""

from __future__ import annotations

from uuid import NAMESPACE_URL, uuid5

from backend.config import settings
from backend.pipelines.annual_report_structure import heading_level
from backend.schemas.annual_report import AnnualReportChunkRecord
from backend.utils.hash import sha256_text


def build_ir_chunks(ir: dict, *, report_id: str, parse_run_id: str) -> list[AnnualReportChunkRecord]:
    """Coalesce prose, preserve section boundaries, and repeat table headers."""
    pages = ir["pages"]
    if not isinstance(pages, list):
        raise ValueError("Document IR pages must be a list")
    size = settings.RAG_CHUNK_SIZE
    overlap = settings.RAG_CHUNK_OVERLAP
    if size <= 0 or not 0 <= overlap < size:
        raise ValueError("Chunk size must be positive and overlap smaller than chunk size")
    chunks: list[AnnualReportChunkRecord] = []
    section_path = "报告正文"
    headings: list[tuple[int, str]] = []
    paragraph_parts: list[tuple[str, str, int]] = []

    def emit(text: str, content_type: str, region_ids: list[str], first_page: int, last_page: int) -> None:
        if not text.strip():
            return
        unique_ids = list(dict.fromkeys(region_ids))
        content_hash = sha256_text(f"{parse_run_id}|{content_type}|{section_path}|{first_page}|{last_page}|{unique_ids}|{text}")
        chunks.append(AnnualReportChunkRecord(
            chunk_id=str(uuid5(NAMESPACE_URL, content_hash)), report_id=report_id,
            parse_run_id=parse_run_id, section_path=section_path, content_type=content_type,
            page_start=first_page, page_end=last_page, region_ids=unique_ids,
            chunk_text=text, content_hash=content_hash,
        ))

    def flush_paragraphs() -> None:
        if not paragraph_parts:
            return
        text = "\n".join(part[0] for part in paragraph_parts)
        spans: list[tuple[int, int, str, int]] = []
        position = 0
        for part, region_id, page_number in paragraph_parts:
            spans.append((position, position + len(part), region_id, page_number))
            position += len(part) + 1
        start = 0
        while start < len(text):
            end = min(start + size, len(text))
            if end < len(text):
                lower = start + min(500, size // 2)
                candidates = [text.rfind(mark, lower, end) for mark in "。！？；\n"]
                boundary = max(candidates)
                if boundary >= lower:
                    end = boundary + 1
            sources = [span for span in spans if span[0] < end and span[1] > start]
            emit(text[start:end].strip(), "paragraph", [span[2] for span in sources], min(span[3] for span in sources), max(span[3] for span in sources))
            if end == len(text):
                break
            start = max(start + 1, end - overlap)
        paragraph_parts.clear()

    for page in pages:
        if "extraction_status" in page and page["extraction_status"] != "ok":
            flush_paragraphs()
        page_number = page["page_number"]
        for region in page["regions"]:
            region_id = region["region_id"]
            region_type = region["region_type"]
            if not region_id:
                raise ValueError("Document IR region_id cannot be empty")
            text_value = region["clean_text"] if "clean_text" in region else region["text"]
            if not isinstance(text_value, str):
                raise ValueError(f"Document IR region {region_id} text must be a string")
            text = text_value.strip()
            if region_type == "heading":
                flush_paragraphs()
                if text:
                    level = heading_level(text)
                    while headings and headings[-1][0] >= level:
                        headings.pop()
                    headings.append((level, text))
                    section_path = region.get("section_path", "/".join(title for _, title in headings))
                continue
            target_path = region.get("section_path", section_path)
            if target_path != section_path:
                flush_paragraphs()
                section_path = target_path
            if not text:
                continue
            if region_type == "paragraph" and region.get("semantic_role") not in {"figure_title", "table_title"}:
                paragraph_parts.append((text, region_id, page_number))
                continue
            flush_paragraphs()
            if region_type == "table":
                for piece in _table_chunks(region, text, size):
                    emit(piece, "table", [region_id], page_number, page_number)
            else:
                emit(text, region_type, [region_id], page_number, page_number)
        if "extraction_status" in page and page["extraction_status"] != "ok":
            flush_paragraphs()
    flush_paragraphs()
    return chunks


def _table_chunks(region: dict, text: str, size: int) -> list[str]:
    rows = (region.get("table") or {}).get("rows")
    if rows:
        lines = ["| " + " | ".join(str(cell).replace("\n", " ").replace("|", "\\|") for cell in row) + " |" for row in rows]
        header = [lines[0], "| " + " | ".join("---" for _ in rows[0]) + " |"]
        data = lines[1:]
    else:
        lines = text.splitlines()
        if len(lines) < 3 or not lines[0].startswith("|"):
            return [text]
        header, data = lines[:2], lines[2:]
    title = (region.get("table") or {}).get("title")
    prefix = ([str(title)] if title else []) + header
    result: list[str] = []
    current = list(prefix)
    for row in data:
        if len("\n".join([*current, row])) > size and len(current) > len(prefix):
            result.append("\n".join(current))
            current = list(prefix)
        current.append(row)
    if len(current) > len(prefix) or not result:
        result.append("\n".join(current))
    return result
