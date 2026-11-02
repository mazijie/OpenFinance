from datetime import UTC, datetime
from uuid import NAMESPACE_URL, uuid5
from copy import deepcopy

import pytest
from pydantic import ValidationError

from backend.pipelines.annual_reports_ir_to_silver import AnnualReportsIRToSilverPipeline
from backend.schemas.annual_report import AnnualReportPageRecord

HASH = 'a' * 64
REPORT = str(uuid5(NAMESPACE_URL, f'cninfo:announcement-1:{HASH}'))
NOW = datetime(2026, 1, 1, tzinfo=UTC).isoformat()


def manifest():
    return {'ingested_at': NOW, 'announcement': {'announcement_id': 'announcement-1', 'stock_code': '600519.SH', 'exchange': 'SH', 'report_year': 2024, 'title': '年报', 'announcement_time': NOW, 'pdf_url': 'https://example.com/report.pdf'}, 'pdf_sha256': HASH, 'pdf_size': 3, 'pdf_object_path': 'bronze/report.pdf'}


def document():
    return {'report_id': REPORT, 'parse_run_id': 'run-1', 'parser_version': 'v1', 'pdf_sha256': HASH, 'started_at': NOW, 'finished_at': NOW, 'section_tree': [], 'pages': [{'page_number': 1, 'page_type': 'text', 'text': '公司经营稳定。', 'extraction_method': 'pymupdf', 'extraction_status': 'ok', 'regions': [{'region_id': 'region-1', 'region_type': 'paragraph', 'bbox': [10, 10, 200, 50], 'reading_order': 1, 'clean_text': '公司经营稳定。'}]}]}


class Bronze:
    def read_json(self, path):
        return manifest()


class Derived:
    def __init__(self, ir):
        self.ir = ir

    def read_json(self, path):
        if path == "derived/quality.json":
            return {"needs_review_pages": []}
        return deepcopy(self.ir)


class Writer:
    def __init__(self):
        self.events = []
        self.runs = []
        self.fail_chunks = False

    def scan_parse_runs(self):
        return self.runs

    def append_parse_runs(self, rows):
        self.events.append('runs')
        self.runs.extend(rows)

    def verify_projection(self, **kwargs):
        return not self.fail_chunks and {'report', 'pages', 'regions', 'sections', 'chunks'} <= set(self.events)

    def has_parse_run(self, report_id, parse_run_id):
        return any(item.report_id == report_id and item.parse_run_id == parse_run_id for item in self.runs)

    def append_report(self, row):
        self.events.append('report')

    def append_pages(self, rows):
        self.events.append('pages')

    def append_regions(self, rows):
        self.events.append('regions')

    def append_sections(self, rows):
        self.events.append('sections')

    def append_chunks(self, rows):
        self.events.append('chunks')
        if self.fail_chunks:
            raise RuntimeError('failed chunk commit')


class State:
    def __init__(self):
        self.claims = 0
        self.failed = 0
        self.completed = 0

    def claim_stage(self, **kwargs):
        self.claims += 1
        return object()

    def fail_stage(self, lease, error):
        self.failed += 1

    def complete_stage(self, lease, **kwargs):
        self.completed += 1


def result():
    return {'ir_object_path': 'derived/document.json', 'quality_object_path': 'derived/quality.json', 'report_id': REPORT, 'parse_run_id': 'run-1', 'manifest_path': 'bronze/metadata.json', 'status': 'succeeded', 'page_count': 1, 'needs_review_pages': []}


def pipeline(ir=None, writer=None):
    return AnnualReportsIRToSilverPipeline(bronze_writer=Bronze(), derived_writer=Derived(ir or document()), report_writer=writer or Writer())


def test_missing_ir_text_fails_before_any_write():
    ir = document()
    del ir['pages'][0]['text']
    writer = Writer()
    with pytest.raises(ValueError, match='missing required field text'):
        pipeline(ir, writer).process_result(result())
    assert writer.events == []


def test_invalid_ir_does_not_leave_a_silver_lease_running():
    ir = document()
    del ir['pages'][0]['text']
    state = State()
    with pytest.raises(ValueError, match='missing required field text'):
        AnnualReportsIRToSilverPipeline(
            bronze_writer=Bronze(),
            derived_writer=Derived(ir),
            report_writer=Writer(),
            state_store=state,
        ).process_result(result())
    assert state.claims == 0
    assert state.failed == 0


def test_failed_silver_write_does_not_commit_run_and_retry_can_complete():
    writer = Writer()
    writer.fail_chunks = True
    job = pipeline(writer=writer)
    assert job.run([result()]).failed_count == 1
    assert not writer.runs
    assert 'report' not in writer.events
    writer.fail_chunks = False
    assert job.run([result()]).written_reports == 1
    assert writer.events[-2:] == ['report', 'runs']
    event_count = len(writer.events)
    assert job.run([result()]).written_reports == 0
    assert len(writer.events) == event_count


def test_forced_projection_uses_new_committed_version():
    writer = Writer()
    job = pipeline(writer=writer)
    assert job.run([result()]).written_reports == 1
    forced = {**result(), "force": True}
    assert job.run([forced]).written_reports == 1
    assert len(writer.runs) == 2
    assert writer.runs[0].parse_run_id != writer.runs[1].parse_run_id
    assert job.run([forced]).written_reports == 0


@pytest.mark.parametrize("method", ["append_pages", "append_regions", "append_sections", "append_chunks", "append_report"])
def test_failed_forced_projection_keeps_previous_commit(method):
    writer = Writer()
    job = pipeline(writer=writer)
    job.process_result(result())
    previous_run = writer.runs[0].parse_run_id
    original = getattr(writer, method)
    def fail_after_write(*args):
        original(*args)
        raise RuntimeError("injected write failure")
    setattr(writer, method, fail_after_write)
    with pytest.raises(RuntimeError, match="injected"):
        job.process_result({**result(), "force": True})
    assert [run.parse_run_id for run in writer.runs] == [previous_run]
    setattr(writer, method, original)
    job.process_result({**result(), "force": True})
    assert len(writer.runs) == 2


def test_empty_page_requires_review_status():
    page = {'report_id': REPORT, 'parse_run_id': 'run-1', 'page_number': 1, 'page_type': 'scan', 'text': '', 'extraction_method': 'paddleocr_pp_structurev3', 'extraction_status': 'ok'}
    with pytest.raises(ValidationError, match='cannot have extraction_status=ok'):
        AnnualReportPageRecord.model_validate(page)
    page['extraction_status'] = 'needs_review'
    assert AnnualReportPageRecord.model_validate(page).text == ''
