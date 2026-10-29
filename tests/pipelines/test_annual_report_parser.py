from backend.pipelines.annual_report_parser import _classify_page, _clean_region_text
from backend.pipelines.annual_report_structure import annotate_sections


def test_section_builder_preserves_hierarchy_and_page_ranges() -> None:
    pages = [
        {"page_number": 1, "regions": [{"region_id": "p1", "region_type": "paragraph", "clean_text": "封面和重要提示"}]},
        {"page_number": 2, "regions": [{"region_id": "h1", "region_type": "heading", "clean_text": "第一章 公司业务概要"}]},
        {"page_number": 3, "regions": [{"region_id": "h2", "region_type": "heading", "clean_text": "一、公司主营业务"}]},
        {"page_number": 4, "regions": [{"region_id": "p2", "region_type": "paragraph", "clean_text": "业务说明"}]},
    ]
    sections = annotate_sections(pages, report_id="report-1", parse_run_id="run-1")
    assert sections[0]["start_page"] == 1
    assert sections[-1]["end_page"] == 4
    assert sections[-1]["parent_section_id"] == sections[-2]["section_id"]
    assert pages[-1]["regions"][0]["section_path"] == "第一章 公司业务概要/一、公司主营业务"


def test_page_router_distinguishes_scan_mixed_and_complex_pages() -> None:
    assert _classify_page({"text_chars": 0, "image_area_ratio": 0.8, "column_count": 1}, []) == "scan"
    assert _classify_page({"text_chars": 800, "image_area_ratio": 0.3, "column_count": 1}, []) == "mixed"
    assert _classify_page({"text_chars": 800, "image_area_ratio": 0.0, "column_count": 2}, []) == "complex"
    assert _classify_page({"text_chars": 800, "image_area_ratio": 0.0, "column_count": 1}, []) == "text"


def test_cleaning_removes_repeated_lines_and_repairs_breaks() -> None:
    cleaned = _clean_region_text("公司年报\n经营状况和财务表-\n现", {"公司年报"})
    assert "公司年报" not in cleaned
    assert "表现" in cleaned

def test_normalizes_real_paddle_result_keys_coordinates_and_confidence():
    from backend.pipelines.annual_report_parser import _normalize_paddle_results

    result = {'res': {'parsing_res_list': [{'block_bbox': [20, 40, 220, 140], 'block_label': 'text', 'block_content': '原始OCR', 'block_order': 1}], 'overall_ocr_res': {'rec_boxes': [[20, 40, 220, 140]], 'rec_scores': [0.6]}}}
    regions = _normalize_paddle_results(iter([result]), 2, 2.0, offset=(10, 20))
    assert regions[0]['bbox'] == [20, 40, 120, 90]
    assert regions[0]['text'] == '原始OCR'
    assert regions[0]['confidence'] == 0.6


def test_multicolumn_order_keeps_full_width_heading_above_both_columns():
    from backend.pipelines.annual_report_parser import _ordered_regions

    regions = [
        {'region_type': 'paragraph', 'text': '右列', 'bbox': [310, 80, 570, 200]},
        {'region_type': 'heading', 'text': '标题', 'bbox': [20, 20, 580, 50]},
        {'region_type': 'paragraph', 'text': '左列', 'bbox': [20, 80, 280, 200]},
    ]
    assert [r['text'] for r in _ordered_regions(regions, 600)] == ['标题', '左列', '右列']


def test_native_table_replaces_duplicate_native_cell_text():
    from backend.pipelines.annual_report_parser import _merge_regions

    native = [{'region_type': 'paragraph', 'text': '营收 100', 'bbox': [20, 20, 150, 50]}]
    table = [{'region_type': 'table', 'text': '| 营收 | 100 |', 'bbox': [10, 10, 200, 100]}]
    assert _merge_regions(native, table) == table


def test_cached_raw_page_skips_completed_page_and_persists_only_missing_page(monkeypatch):
    from types import SimpleNamespace
    from backend.pipelines.annual_report_parser import extract_document_ir

    class FakeDocument:
        def __init__(self):
            self.pages = [SimpleNamespace(rect=SimpleNamespace(width=595.0, height=842.0)) for _ in range(2)]

        def __len__(self):
            return len(self.pages)

        def __iter__(self):
            return iter(self.pages)

        def close(self):
            pass

    import backend.pipelines.annual_report_parser as parser
    monkeypatch.setattr(parser, "fitz", SimpleNamespace(open=lambda **kwargs: FakeDocument()))
    monkeypatch.setattr(parser, "pdfplumber", None)
    monkeypatch.setattr(
        parser,
        "_extract_native_regions",
        lambda page, page_number: (
            [{"region_type": "paragraph", "text": "Second page", "bbox": [1, 1, 100, 20]}],
            {"text_chars": 11, "image_area_ratio": 0.0, "column_count": 1},
        ),
    )
    pdf_bytes = b"pdf"
    cached_page = {
        "page_number": 1,
        "width": 595.0,
        "height": 842.0,
        "page_type": "text",
        "extraction_method": "checkpoint",
        "extraction_status": "ok",
        "metrics": {"text_chars": 3, "image_area_ratio": 0.0, "column_count": 1},
        "diagnostics": [],
        "regions": [{"region_type": "paragraph", "text": "Completed page", "bbox": [1, 1, 100, 20]}],
    }
    persisted: list[int] = []
    ir = extract_document_ir(
        pdf_bytes,
        report_id="report",
        stock_code="600519.SH",
        report_year=2024,
        parse_run_id="run",
        parser_version="test",
        cached_raw_pages={1: cached_page},
        on_raw_page=lambda page: persisted.append(page["page_number"]),
    )
    assert persisted == [2]
    assert ir["pages"][0]["text"] == "Completed page"
    assert "Second page" in ir["pages"][1]["text"]


def test_page_two_failure_retries_only_page_two(monkeypatch):
    from types import SimpleNamespace

    import pytest
    import backend.pipelines.annual_report_parser as parser

    class FakeDocument:
        def __init__(self):
            self.pages = [SimpleNamespace(rect=SimpleNamespace(width=595.0, height=842.0)) for _ in range(2)]

        def __len__(self):
            return len(self.pages)

        def __iter__(self):
            return iter(self.pages)

        def close(self):
            pass

    monkeypatch.setattr(parser, "fitz", SimpleNamespace(open=lambda **kwargs: FakeDocument()))
    monkeypatch.setattr(parser, "pdfplumber", None)
    calls = []
    fail_page_two = True

    def extract(page, page_number):
        nonlocal fail_page_two
        calls.append(page_number)
        if page_number == 2 and fail_page_two:
            raise RuntimeError("simulated page-two OCR failure")
        return ([{"region_type": "paragraph", "text": f"Page {page_number}", "bbox": [1, 1, 100, 20]}], {
            "text_chars": 10, "image_area_ratio": 0.0, "column_count": 1,
        })

    monkeypatch.setattr(parser, "_extract_native_regions", extract)
    checkpoints = {}

    with pytest.raises(parser.AnnualReportParseError, match="page-two OCR failure"):
        parser.extract_document_ir(
            b"pdf",
            report_id="report",
            stock_code="600519.SH",
            report_year=2024,
            parse_run_id="run",
            parser_version="test",
            on_raw_page=lambda page: checkpoints.__setitem__(page["page_number"], page),
        )
    assert set(checkpoints) == {1}
    assert calls == [1, 2]

    fail_page_two = False
    calls.clear()
    persisted = []
    ir = parser.extract_document_ir(
        b"pdf",
        report_id="report",
        stock_code="600519.SH",
        report_year=2024,
        parse_run_id="run",
        parser_version="test",
        cached_raw_pages=checkpoints,
        on_raw_page=lambda page: persisted.append(page["page_number"]),
    )
    assert calls == [2]
    assert persisted == [2]
    assert [page["page_number"] for page in ir["pages"]] == [1, 2]
