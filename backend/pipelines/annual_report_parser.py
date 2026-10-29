"""PDF text and section extraction for annual reports."""

from __future__ import annotations

import io
import json
import unicodedata
from collections.abc import Callable, Iterable, Mapping
import math
import re
from collections import Counter
from typing import Any
from uuid import NAMESPACE_URL, uuid5

from backend.config import settings
from backend.pipelines.annual_report_structure import annotate_sections

try:  # pragma: no cover - exercised in the runtime image
    import fitz
except ImportError:  # pragma: no cover
    fitz = None

try:  # pragma: no cover - optional parser dependency
    import pdfplumber
except ImportError:  # pragma: no cover
    pdfplumber = None


FORMULA_PATTERN = re.compile(r"[∑∏√∫≤≥≠≈±]|\\(?:frac|sum|sqrt|begin|end)\b")
WHITESPACE_PATTERN = re.compile(r"\s+")


class AnnualReportParseError(Exception):
    """Raised when a PDF cannot be parsed."""


class AnnualReportParserUnavailable(AnnualReportParseError):
    """Raised when a configured parser cannot be initialized."""


def extract_document_ir(
    pdf_bytes: bytes,
    *,
    report_id: str,
    stock_code: str,
    report_year: int,
    parse_run_id: str | None = None,
    parser_version: str | None = None,
    cached_raw_pages: Mapping[int, dict[str, Any]] | None = None,
    on_raw_page: Callable[[dict[str, Any]], None] | None = None,
) -> dict[str, Any]:
    """Return a versioned Document IR while preserving page and region locators."""

    if fitz is None:
        raise AnnualReportParseError("PyMuPDF is not installed")
    parser_version = parser_version or settings.PDF_PARSER_VERSION
    parse_run_id = parse_run_id or str(uuid5(NAMESPACE_URL, f"{report_id}:{parser_version}"))
    document = None
    plumber_document = None
    try:
        document = fitz.open(stream=pdf_bytes, filetype="pdf")
        if pdfplumber is not None:
            try:
                plumber_document = pdfplumber.open(io.BytesIO(pdf_bytes))
            except Exception:
                plumber_document = None

        if len(document) == 0:
            raise AnnualReportParseError("PDF contains no pages")
        raw_pages: list[dict[str, Any]] = []
        for index, page in enumerate(document):
            page_number = index + 1
            cached_page = (cached_raw_pages or {}).get(page_number)
            if cached_page is not None:
                if int(cached_page.get("page_number", 0)) != page_number:
                    raise AnnualReportParseError(f"Cached page number mismatch at page {page_number}")
                raw_pages.append(cached_page)
                continue
            raw_regions, metrics = _extract_native_regions(page, page_number)
            page_type = _classify_page(metrics, raw_regions)
            extraction_method = "pymupdf"
            diagnostics: list[dict[str, Any]] = []
            extraction_status = "ok" if any(region.get("text") for region in raw_regions) else "needs_review"
            if page_type in {"scan", "mixed", "complex"}:
                try:
                    # On mixed pages OCR only image regions. The native text
                    # outside those rectangles is preserved byte for byte.
                    clips = [region["bbox"] for region in raw_regions if region["region_type"] == "figure"] if page_type == "mixed" else [None]
                    ocr_regions: list[dict[str, Any]] = []
                    for clip in clips:
                        if settings.PDF_LAYOUT_ENGINE == "mineru":
                            ocr_regions.extend(_extract_mineru_regions(page, page_number, clip=clip))
                        elif settings.PDF_LAYOUT_ENGINE == "paddle":
                            ocr_regions.extend(_extract_paddle_regions(page, page_number, clip=clip))
                        else:
                            raise AnnualReportParseError(f"Unknown layout engine: {settings.PDF_LAYOUT_ENGINE}")
                    if not ocr_regions:
                        raise AnnualReportParseError("PP-StructureV3 produced no regions")
                    if page_type == "scan" or (page_type == "complex" and settings.PDF_LAYOUT_ENGINE == "mineru"):
                        raw_regions = ocr_regions
                    else:
                        raw_regions = _merge_regions(raw_regions, ocr_regions)
                    engine_name = "mineru" if settings.PDF_LAYOUT_ENGINE == "mineru" else "paddleocr_pp_structurev3"
                    extraction_method = f"pymupdf+{engine_name}" if page_type == "mixed" else engine_name
                    extraction_status = "ok"
                    low_confidence = [region for region in ocr_regions if region.get("text") and region.get("confidence") is not None and region["confidence"] < settings.PDF_OCR_CONFIDENCE_THRESHOLD]
                    if low_confidence:
                        extraction_status = "needs_review"
                        diagnostics.append({"code": "low_ocr_confidence", "region_count": len(low_confidence)})
                except AnnualReportParserUnavailable:
                    raise
                except Exception as exc:
                    extraction_status = "needs_review"
                    diagnostics.append({"code": "layout_ocr_failed", "error": str(exc)})
            if plumber_document is not None and page_number <= len(plumber_document.pages):
                try:
                    table_regions = _extract_pdfplumber_tables(plumber_document.pages[page_number - 1], page_number)
                except Exception as exc:
                    table_regions = []
                    extraction_status = "needs_review"
                    diagnostics.append({"code": "native_table_extraction_failed", "error": str(exc)})
                if table_regions:
                    raw_regions = _merge_regions(raw_regions, table_regions)
                    page_type = "complex"
                    if extraction_method == "pymupdf":
                        extraction_method = "pymupdf+pdfplumber"
            raw_page = {
                "page_number": page_number,
                "width": float(page.rect.width),
                "height": float(page.rect.height),
                "page_type": page_type,
                "extraction_method": extraction_method,
                "extraction_status": extraction_status,
                "metrics": metrics,
                "diagnostics": diagnostics,
                "regions": raw_regions,
            }
            if on_raw_page is not None:
                on_raw_page(raw_page)
            raw_pages.append(raw_page)

        repeated_lines = _find_repeated_headers_footers(raw_pages)
        pages: list[dict[str, Any]] = []
        for page in raw_pages:
            regions: list[dict[str, Any]] = []
            ordered = _ordered_regions(page["regions"], page["width"])
            for order, region in enumerate(ordered, start=1):
                raw_text = str(region.get("text") or "")
                bbox = region.get("bbox", [])
                is_margin = len(bbox) == 4 and (bbox[1] <= page["height"] * 0.08 or bbox[3] >= page["height"] * 0.92)
                clean_text = _clean_region_text(raw_text, repeated_lines if is_margin else set())
                region_id = str(uuid5(NAMESPACE_URL, f"{parse_run_id}:region:{page['page_number']}:{order}:{clean_text}"))
                regions.append(
                    {
                        "region_id": region_id,
                        "region_type": region.get("region_type", "paragraph"),
                        "bbox": [float(value) for value in (region.get("bbox") or [])],
                        "reading_order": order,
                        "raw_text": raw_text,
                        "clean_text": clean_text,
                        "text": clean_text,
                        "content_ref": region.get("content_ref"),
                        "confidence": region.get("confidence"),
                        "table": region.get("table"),
                        "formula_latex": region.get("formula_latex"),
                        "semantic_role": region.get("semantic_role"),
                        "parser_details": region.get("parser_details"),
                        "layout_order": region.get("layout_order"),
                    }
                )
            text = "\n".join(region["clean_text"] for region in regions if region["clean_text"]).strip()
            _attach_captions(regions)
            page["regions"] = regions
            page["text"] = text
            if not text:
                page["extraction_status"] = "needs_review"
                page["diagnostics"].append({"code": "empty_clean_text"})
            page["quality"] = _page_quality(page)
            pages.append(dict(page))

        section_tree = annotate_sections(pages, report_id=report_id, parse_run_id=parse_run_id)
        return {
            "schema_version": "document-ir/v1",
            "report_id": report_id,
            "stock_code": stock_code,
            "report_year": report_year,
            "parse_run_id": parse_run_id,
            "parser_version": parser_version,
            "pages": pages,
            "section_tree": section_tree,
            "quality": {
                "page_count": len(pages),
                "needs_review_pages": [page["page_number"] for page in pages if page["extraction_status"] != "ok"],
                "average_page_quality": round(sum(page["quality"] for page in pages) / max(1, len(pages)), 4),
                "page_diagnostics": [{"page_number": page["page_number"], "diagnostics": page["diagnostics"]} for page in pages if page["diagnostics"]],
            },
        }
    except AnnualReportParseError:
        raise
    except Exception as exc:  # pragma: no cover - depends on malformed external PDFs
        raise AnnualReportParseError(f"PDF extraction failed: {exc}") from exc
    finally:
        if plumber_document is not None:
            plumber_document.close()
        if document is not None:
            document.close()


def _extract_native_regions(page: Any, page_number: int) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    try:
        blocks = page.get_text("dict").get("blocks", [])
    except Exception:
        text = str(page.get_text("text") or "").strip()
        blocks = [{"type": 0, "bbox": list(page.rect), "lines": [{"spans": [{"text": text, "size": 0}]}]}] if text else []
    regions: list[dict[str, Any]] = []
    text_chars = 0
    image_area = 0.0
    page_area = max(1.0, float(page.rect.width * page.rect.height))
    font_sizes: list[float] = []
    for block in blocks:
        bbox = list(block.get("bbox") or [])
        if block.get("type") == 1:
            area = max(0.0, (bbox[2] - bbox[0]) * (bbox[3] - bbox[1])) if len(bbox) == 4 else 0.0
            image_area += area
            regions.append({"region_type": "figure", "bbox": bbox, "text": ""})
            continue
        parts: list[str] = []
        max_size = 0.0
        for line in block.get("lines") or []:
            line_text = "".join(str(span.get("text") or "") for span in line.get("spans") or [])
            if line_text:
                parts.append(line_text)
            for span in line.get("spans") or []:
                size = float(span.get("size") or 0)
                max_size = max(max_size, size)
                if size:
                    font_sizes.append(size)
        text = "\n".join(parts).strip()
        if not text:
            continue
        text_chars += len(text)
        region_type = "formula" if FORMULA_PATTERN.search(text) else "heading" if _looks_like_heading(text, max_size, font_sizes) else "paragraph"
        regions.append({"region_type": region_type, "bbox": bbox, "text": text, "semantic_role": "figure_title" if re.match(r"^图\s*[0-9一二三四五六七八九十]", text) else "table_title" if re.match(r"^表\s*[0-9一二三四五六七八九十]", text) else None})
    metrics = {
        "text_chars": text_chars,
        "image_area_ratio": round(min(1.0, image_area / page_area), 4),
        "block_count": len(regions),
        "column_count": _estimate_columns(regions, float(page.rect.width)),
        "page_number": page_number,
    }
    return regions, metrics


def _classify_page(metrics: dict[str, Any], regions: list[dict[str, Any]]) -> str:
    text_chars = int(metrics.get("text_chars") or 0)
    image_ratio = float(metrics.get("image_area_ratio") or 0)
    columns = int(metrics.get("column_count") or 1)
    has_formula = any(region.get("region_type") == "formula" for region in regions)
    has_figure = any(region.get("region_type") == "figure" for region in regions)
    if text_chars < 40 and image_ratio >= 0.5:
        return "scan"
    if image_ratio >= 0.2 and text_chars >= 40 and columns < 2:
        return "mixed"
    if columns >= 2 or has_formula or (has_figure and text_chars > 0):
        return "complex"
    return "text"


def _mineru_text(value: Any) -> str:
    if isinstance(value, str):
        return value
    if isinstance(value, list):
        return " ".join(filter(None, (_mineru_text(item) for item in value)))
    if isinstance(value, dict):
        return _mineru_text(value.get("content", ""))
    return ""


def _normalize_mineru_result(payload: dict[str, Any], page_number: int, rect: Any) -> list[dict[str, Any]]:
    """Map MinerU 4 middle JSON normalized coordinates into PDF points."""
    if payload.get("schema") != "docvortex.middle" or payload.get("schema_version") != "2.0":
        raise AnnualReportParseError("Unsupported MinerU middle JSON contract")
    pages = payload.get("pages")
    if not isinstance(pages, list) or len(pages) != 1:
        raise AnnualReportParseError("MinerU image result must contain exactly one page")
    regions = []
    for order, block in enumerate(pages[0].get("blocks", [])):
        label = str(block.get("type", ""))
        if label in {"header", "footer", "page_number"}:
            continue
        coords = block.get("bbox")
        if not isinstance(coords, list) or len(coords) != 4:
            raise AnnualReportParseError(f"MinerU block {label} lacks a bounding box")
        coords = [float(v) for v in coords]
        if any(not math.isfinite(v) or v < 0 or v > 1 for v in coords) or coords[0] >= coords[2] or coords[1] >= coords[3]:
            raise AnnualReportParseError(f"Invalid MinerU bounding box for {label}")
        bbox = [rect.x0 + coords[0] * rect.width, rect.y0 + coords[1] * rect.height,
                rect.x0 + coords[2] * rect.width, rect.y0 + coords[3] * rect.height]
        kind = ("table" if label == "table" else "figure" if label in {"image", "chart", "figure"}
                else "formula" if label in {"equation", "formula"} else "heading" if label in {"doc_title", "paragraph_title", "title"}
                else "footnote" if label in {"footnote", "page_footnote"} else "paragraph")
        text = _mineru_text(block.get("content", "")).strip()
        region = {"region_type": kind, "bbox": bbox, "text": text, "confidence": None,
                  "layout_order": order, "semantic_role": label, "parser_details": {"engine": "mineru", "block_type": label}}
        if kind == "table":
            children = block.get("content", [])
            html = next((str(child.get("content", "")) for child in children if isinstance(child, dict) and child.get("type") == "table_body"), "")
            if not html:
                raise AnnualReportParseError("MinerU table has no HTML body")
            region["table"] = _table_from_html(html, [], page_number, scale=1.0, offset=(0.0, 0.0))
            region["text"] = region["table"]["markdown"]
        elif kind == "formula":
            region["formula_latex"] = text.strip("$ ")
        if text or kind in {"table", "figure", "formula"}:
            regions.append(region)
    return regions


def _extract_mineru_regions(page: Any, page_number: int, *, clip: list[float] | None = None) -> list[dict[str, Any]]:
    import tempfile
    from importlib.metadata import version
    from pathlib import Path
    try:
        from mineru.parser import MinerUParser
        if version("mineru") != "4.0.12":
            raise ValueError("MinerU adapter requires mineru==4.0.12")
    except Exception as exc:
        raise AnnualReportParserUnavailable(f"MinerU dependency unavailable: {exc}") from exc
    rect = fitz.Rect(*clip) if clip is not None else page.rect
    scale = max(1.0, float(settings.PDF_RENDER_DPI) / 72.0)
    pixmap = page.get_pixmap(matrix=fitz.Matrix(scale, scale), clip=rect, alpha=False, colorspace=fitz.csRGB)
    parser = getattr(_extract_mineru_regions, "_parser", None)
    if parser is None:
        try:
            parser = MinerUParser(tier=settings.PDF_MINERU_TIER, parse_mode="ocr")
        except Exception as exc:
            raise AnnualReportParserUnavailable(f"MinerU initialization failed: {exc}") from exc
        setattr(_extract_mineru_regions, "_parser", parser)
    with tempfile.TemporaryDirectory(prefix="openfinance-mineru-") as folder:
        path = Path(folder) / "page.png"
        pixmap.save(str(path))
        payload = parser.parse(path).to_dict()
    return _normalize_mineru_result(payload, page_number, rect)


def _extract_paddle_regions(page: Any, page_number: int, *, clip: list[float] | None = None) -> list[dict[str, Any]]:
    """Run the pinned PP-StructureV3 contract with image-space coordinates."""

    import numpy as np
    from paddleocr import PPStructureV3

    scale = max(1.0, float(settings.PDF_RENDER_DPI) / 72.0)
    rect = fitz.Rect(*clip) if clip is not None else page.rect
    pixmap = page.get_pixmap(matrix=fitz.Matrix(scale, scale), clip=rect, alpha=False, colorspace=fitz.csRGB)
    # PP-Structure accepts a BGR ndarray or a file path, not PNG bytes.
    image = np.frombuffer(pixmap.samples, dtype=np.uint8).reshape(pixmap.height, pixmap.width, 3)[:, :, ::-1].copy()
    pipeline = getattr(_extract_paddle_regions, "_pipeline", None)
    if pipeline is None:
        pipeline = PPStructureV3(
            device="gpu:0", use_doc_orientation_classify=False,
            use_doc_unwarping=False, use_textline_orientation=False,
            use_seal_recognition=False,
        )
        setattr(_extract_paddle_regions, "_pipeline", pipeline)
    return _normalize_paddle_results(pipeline.predict(input=image), page_number, scale, offset=(rect.x0, rect.y0))


def _normalize_paddle_results(results: Iterable[Any], page_number: int, scale: float, *, offset: tuple[float, float] = (0.0, 0.0)) -> list[dict[str, Any]]:
    """Normalize PaddleOCR 3.x output; all diagnostics remain in Document IR."""

    regions: list[dict[str, Any]] = []
    for item in results:
        payload = item if isinstance(item, dict) else item.json
        if callable(payload):
            payload = payload()
        if isinstance(payload, str):
            payload = json.loads(payload)
        payload = payload.get("res", payload)
        if not isinstance(payload, dict) or "parsing_res_list" not in payload:
            raise AnnualReportParseError("PP-StructureV3 result is missing parsing_res_list")
        ocr = payload.get("overall_ocr_res", {})
        tables = iter(payload.get("table_res_list", []))
        for layout_index, candidate in enumerate(payload["parsing_res_list"], start=1):
            pixel_bbox = [float(value) for value in _paddle_field(candidate, "block_bbox", [])]
            bbox = [pixel_bbox[0] / scale + offset[0], pixel_bbox[1] / scale + offset[1], pixel_bbox[2] / scale + offset[0], pixel_bbox[3] / scale + offset[1]]
            text = str(_paddle_field(candidate, "block_content", "")).strip()
            label = str(_paddle_field(candidate, "block_label", "paragraph")).lower()
            region_type = "table" if "table" == label else "figure" if label in {"image", "figure", "chart"} else "formula" if "formula" in label else "heading" if label in {"doc_title", "paragraph_title"} else "footnote" if label in {"footnote", "footer", "header"} else "paragraph"
            scores = [float(score) for box, score in zip(_paddle_field(ocr, "rec_boxes", []), _paddle_field(ocr, "rec_scores", []), strict=True) if _overlap_fraction([float(v) for v in box], pixel_bbox) >= 0.5]
            region: dict[str, Any] = {
                "region_type": region_type, "bbox": bbox, "text": text,
                "confidence": min(scores) if scores else None,
                "layout_order": layout_index,
                "semantic_role": label,
                "parser_details": {"block_id": _paddle_field(candidate, "block_id"), "block_order": _paddle_field(candidate, "block_order"), "ocr_scores": scores},
            }
            if region_type == "table":
                table = next(tables, {})
                html = str(_paddle_field(table, "pred_html", text))
                region["table"] = _table_from_html(html, _paddle_field(table, "cell_box_list", []), page_number, scale=scale, offset=offset)
                region["text"] = region["table"]["markdown"]
            elif region_type == "formula":
                region["formula_latex"] = text.strip("$ ")
            if text or region_type in {"table", "figure", "formula"}:
                regions.append(region)
    return regions


def _paddle_field(value: Any, name: str, default: Any = None) -> Any:
    """Read a PP-Structure field from either its dict or typed result form."""

    aliases = {
        "block_bbox": ("block_bbox", "bbox"),
        "block_label": ("block_label", "label", "region_label"),
        "block_content": ("block_content", "content"),
        "block_id": ("block_id", "index"),
        "block_order": ("block_order", "order", "index"),
    }.get(name, (name,))
    if isinstance(value, Mapping):
        for alias in aliases:
            if alias in value:
                return value[alias]
        return default
    for alias in aliases:
        result = getattr(value, alias, None)
        if result is not None:
            return result
    return default


def _table_from_html(html: str, boxes: Any, page_number: int, *, scale: float, offset: tuple[float, float]) -> dict[str, Any]:
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(html, "html.parser")
    cells: list[dict[str, Any]] = []
    rows: list[list[str]] = []
    for row_number, tr in enumerate(soup.find_all("tr")):
        row: list[str] = []
        for column_number, td in enumerate(tr.find_all(["td", "th"])):
            text = td.get_text(" ", strip=True)
            row.append(text)
            cell = {"row": row_number, "column": column_number, "text": text, "rowspan": int(td.get("rowspan", 1)), "colspan": int(td.get("colspan", 1))}
            if len(boxes) > len(cells):
                cell["bbox"] = _json_value(boxes[len(cells)])
            cells.append(cell)
        if row:
            rows.append(row)
    return {"page_number": page_number, "html": html, "markdown": _table_markdown(rows) if rows else "", "rows": rows, "cells": cells, "cell_coordinate_scale": scale, "cell_coordinate_offset": list(offset)}


def _json_value(value: Any) -> Any:
    if hasattr(value, "tolist"):
        return value.tolist()
    return value


def _extract_pdfplumber_tables(page: Any, page_number: int) -> list[dict[str, Any]]:
    regions: list[dict[str, Any]] = []
    for index, table in enumerate(page.find_tables()):
        rows = [[str(cell or "").strip() for cell in row] for row in table.extract() if row]
        if not rows or not any(cell for row in rows for cell in row):
            continue
        markdown = _table_markdown(rows)
        html = "<table>" + "".join("<tr>" + "".join(f"<td>{_escape_html(cell)}</td>" for cell in row) + "</tr>" for row in rows) + "</table>"
        cells = [{"row": row_index, "column": col_index, "bbox": list(cell_bbox) if cell_bbox else None, "text": rows[row_index][col_index] if col_index < len(rows[row_index]) else ""} for row_index, row in enumerate(table.rows) for col_index, cell_bbox in enumerate(row.cells)]
        regions.append({"region_type": "table", "bbox": list(table.bbox), "text": markdown, "table": {"page_number": page_number, "table_index": index, "rows": rows, "cells": cells, "markdown": markdown, "html": html}})
    return regions


def _overlap_fraction(box: list[float], other: list[float]) -> float:
    if len(box) != 4 or len(other) != 4:
        return 0.0
    area = max(0.0, box[2] - box[0]) * max(0.0, box[3] - box[1])
    intersection = max(0.0, min(box[2], other[2]) - max(box[0], other[0])) * max(0.0, min(box[3], other[3]) - max(box[1], other[1]))
    return intersection / area if area else 0.0


def _merge_regions(primary: list[dict[str, Any]], secondary: list[dict[str, Any]]) -> list[dict[str, Any]]:
    merged = list(primary)
    for candidate in secondary:
        bbox = candidate["bbox"]
        matching = [region for region in merged if _overlap_fraction(region["bbox"], bbox) >= 0.65]
        if candidate["region_type"] in {"table", "formula"}:
            merged = [region for region in merged if region not in matching]
        elif candidate["region_type"] in {"paragraph", "heading", "footnote"}:
            native = next((region for region in matching if region.get("text") and _normalize_line(region["text"]).replace(" ", "") == _normalize_line(candidate["text"]).replace(" ", "")), None)
            if native is not None:
                native["layout_order"] = candidate.get("layout_order")
                if candidate["region_type"] == "heading":
                    native["region_type"] = "heading"
                continue
        elif matching and any(region["region_type"] == "figure" for region in matching):
            continue
        merged.append(candidate)
    return merged


def _ordered_regions(regions: list[dict[str, Any]], page_width: float) -> list[dict[str, Any]]:
    if not regions:
        return []
    if all(region.get("layout_order") is not None for region in regions):
        return sorted(regions, key=lambda region: region["layout_order"])
    def position(region: dict[str, Any]) -> tuple[float, float]:
        bbox = region["bbox"]
        return (bbox[1], bbox[0]) if len(bbox) == 4 else (0.0, 0.0)
    if _estimate_columns(regions, page_width) == 1:
        return sorted(regions, key=position)
    # Full-width headings/tables split the page into independent column bands.
    spanning = sorted([region for region in regions if len(region["bbox"]) == 4 and region["bbox"][2] - region["bbox"][0] > page_width * 0.65], key=position)
    remaining = [region for region in regions if region not in spanning]
    ordered: list[dict[str, Any]] = []
    for separator in [*spanning, None]:
        stop = separator["bbox"][1] if separator is not None else float("inf")
        band = [region for region in remaining if position(region)[0] < stop]
        remaining = [region for region in remaining if region not in band]
        for right in (False, True):
            ordered.extend(sorted([region for region in band if (region["bbox"][0] >= page_width / 2) == right], key=position))
        if separator is not None:
            ordered.append(separator)
    return ordered


def _estimate_columns(regions: list[dict[str, Any]], page_width: float) -> int:
    if page_width <= 0:
        return 1
    narrow = [region["bbox"] for region in regions if region.get("region_type") not in {"figure", "formula"} and len(region.get("bbox", [])) == 4 and 0 < region["bbox"][2] - region["bbox"][0] < page_width * 0.6]
    left = [box for box in narrow if box[0] < page_width * 0.4]
    right = [box for box in narrow if box[0] >= page_width * 0.45]
    return 2 if any(min(a[3], b[3]) > max(a[1], b[1]) for a in left for b in right) else 1


def _looks_like_heading(text: str, max_size: float, font_sizes: list[float]) -> bool:
    compact = WHITESPACE_PATTERN.sub(" ", text).strip()
    if re.search(r"第[一二三四五六七八九十百]+[章节]|^[一二三四五六七八九十百]+、", compact):
        return True
    median = sorted(font_sizes)[len(font_sizes) // 2] if font_sizes else 0
    return bool(max_size and median and max_size >= median * 1.25 and len(compact) <= 120)


def _clean_region_text(text: str, repeated_lines: set[str]) -> str:
    lines: list[str] = []
    normalized = unicodedata.normalize("NFKC", text).replace("\x00", "").replace("\ufffd", "")
    for line in normalized.splitlines():
        compact = WHITESPACE_PATTERN.sub(" ", line).strip()
        if not compact or _normalize_line(compact) in repeated_lines:
            continue
        lines.append(compact)
    cleaned = "\n".join(lines)
    cleaned = re.sub(r"-[ \t]*\n", "", cleaned)
    cleaned = re.sub(r"(?<=[A-Za-z])-[ \n]+(?=[A-Za-z])", "", cleaned)
    cleaned = re.sub(r"(?<![。！？；：.!?;:\n])\n+(?=[\u4e00-\u9fffA-Za-z0-9])", " ", cleaned)
    cleaned = re.sub(r"[ \t]+", " ", cleaned)
    cleaned = re.sub(r"(?<=[\u4e00-\u9fff]) +(?=[\u4e00-\u9fff])", "", cleaned)
    return cleaned.strip()


def _find_repeated_headers_footers(pages: list[dict[str, Any]]) -> set[str]:
    counts: Counter[str] = Counter()
    page_count = len(pages)
    for page in pages:
        height = float(page.get("height") or 0)
        seen_on_page: set[str] = set()
        for region in page.get("regions") or []:
            bbox = region.get("bbox") or []
            if len(bbox) != 4:
                continue
            y0, y1 = float(bbox[1]), float(bbox[3])
            if y0 <= height * 0.08 or y1 >= height * 0.92:
                for line in str(region.get("text") or "").splitlines():
                    normalized = _normalize_line(line)
                    if normalized:
                        seen_on_page.add(normalized)
        counts.update(seen_on_page)
    threshold = max(2, math.ceil(page_count * 0.6))
    return {line for line, count in counts.items() if count >= threshold}


def _normalize_line(value: str) -> str:
    normalized = WHITESPACE_PATTERN.sub(" ", unicodedata.normalize("NFKC", value)).strip().casefold()
    return "<page-number>" if re.fullmatch(r"(?:第\s*)?\d+(?:\s*[页/]\s*\d*)?", normalized) else normalized


def _attach_captions(regions: list[dict[str, Any]]) -> None:
    for caption in regions:
        role = caption.get("semantic_role")
        target_type = "figure" if role == "figure_title" else "table" if role == "table_title" else None
        if target_type is None or len(caption["bbox"]) != 4:
            continue
        targets = [region for region in regions if region["region_type"] == target_type and len(region["bbox"]) == 4]
        if not targets:
            continue
        target = min(targets, key=lambda region: min(abs(region["bbox"][1] - caption["bbox"][3]), abs(region["bbox"][3] - caption["bbox"][1])))
        distance = min(abs(target["bbox"][1] - caption["bbox"][3]), abs(target["bbox"][3] - caption["bbox"][1]))
        if distance > 80:
            continue
        target["caption_region_id"] = caption["region_id"]
        if target_type == "table" and target.get("table") is not None:
            target["table"]["title"] = caption["clean_text"]
        elif target_type == "figure" and not target["clean_text"]:
            target["clean_text"] = caption["clean_text"]
            target["text"] = caption["clean_text"]


def _page_quality(page: dict[str, Any]) -> float:
    text = str(page.get("text") or "")
    if not text:
        return 0.0
    status_score = 1.0 if page.get("extraction_status") == "ok" else 0.5
    noise = len(re.findall(r"(.)\1{4,}", text))
    noise_score = max(0.0, 1.0 - min(0.5, noise / max(1, len(text))))
    return round(status_score * noise_score, 4)


def _table_markdown(rows: list[list[str]]) -> str:
    width = max(len(row) for row in rows)
    normalized = [row + [""] * (width - len(row)) for row in rows]
    lines = ["| " + " | ".join(normalized[0]) + " |", "| " + " | ".join(["---"] * width) + " |"]
    lines.extend("| " + " | ".join(row) + " |" for row in normalized[1:])
    return "\n".join(lines)


def _escape_html(value: str) -> str:
    return value.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace('"', "&quot;")
