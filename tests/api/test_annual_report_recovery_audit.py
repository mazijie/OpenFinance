from __future__ import annotations

from backend.api.internal_jobs import _annual_report_ir_is_valid, _annual_report_silver_is_valid
from backend.rag.annual_report_chunking import build_ir_chunks


def _ir():
    return {
        "report_id": "report-1",
        "parse_run_id": "run-1",
        "pdf_sha256": "a" * 64,
        "parser_version": "parser-v1",
        "raw_page_manifest_path": "derived/raw_pages/manifest.json",
        "quality": {"needs_review_pages": []},
        "pages": [
            {
                "page_number": 1,
                "extraction_status": "ok",
                "ir_page_path": "derived/pages/page=0001.json",
                "regions": [
                    {
                        "region_id": "region-1",
                        "region_type": "paragraph",
                        "clean_text": "公司经营稳定。",
                        "text": "公司经营稳定。",
                        "section_path": "报告正文",
                    }
                ],
            }
        ],
        "section_tree": [],
    }


class Derived:
    def __init__(self, ir):
        self.ir = ir

    def read_json(self, path):
        if path == "derived/document.json":
            return self.ir
        if path == "derived/quality.json":
            return self.ir["quality"]
        if path == "derived/raw_pages/manifest.json":
            return {
                "report_id": "report-1",
                "parse_run_id": "run-1",
                "pdf_sha256": "a" * 64,
                "parser_version": "parser-v1",
                "page_count": 1,
                "complete": True,
            }
        if path == "derived/pages/page=0001.json":
            return {"page_number": 1}
        raise KeyError(path)

    def object_exists(self, path):
        return path != "derived/pages/page=0001.json.missing"


def test_recovery_audit_rejects_missing_page_artifact_and_accepts_complete_ir():
    ir = _ir()
    derived = Derived(ir)
    assert _annual_report_ir_is_valid(derived, "derived/document.json", "report-1", "a" * 64) == (True, ir)

    ir["pages"][0]["ir_page_path"] = "derived/pages/page=0001.json.missing"
    assert _annual_report_ir_is_valid(derived, "derived/document.json", "report-1", "a" * 64) == (False, None)


def test_recovery_audit_detects_missing_silver_chunk_projection():
    ir = _ir()
    chunks = build_ir_chunks(ir, report_id="report-1", parse_run_id="run-1")
    snapshot = {
        "reports": {("report-1", "run-1")},
        "pages": {("report-1", "run-1", "1")},
        "regions": {("report-1", "run-1", "region-1")},
        "sections": set(),
        "chunks": {("report-1", "run-1", chunks[0].chunk_id)},
        "parse_runs": {("report-1", "run-1")},
    }
    assert _annual_report_silver_is_valid(snapshot, ir, "report-1", "run-1")

    snapshot["chunks"].clear()
    assert not _annual_report_silver_is_valid(snapshot, ir, "report-1", "run-1")
