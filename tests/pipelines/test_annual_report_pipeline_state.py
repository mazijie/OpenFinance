from contextlib import contextmanager
from types import SimpleNamespace

from backend.pipelines.annual_report_pipeline_state import AnnualReportPipelineStateStore


class Cursor:
    def __init__(self, row):
        self.row = row
        self.sql = ""

    def execute(self, sql, params):
        self.sql = sql
        self.params = params

    def fetchone(self):
        return self.row

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False


def test_enqueue_stage_reads_status_and_attempt_count(monkeypatch):
    cursor = Cursor(("failed", 1))
    store = object.__new__(AnnualReportPipelineStateStore)

    @contextmanager
    def connection():
        yield SimpleNamespace(transaction=lambda: _NullContext(), cursor=lambda: cursor)

    store._connection = connection
    monkeypatch.setattr("backend.pipelines.annual_report_pipeline_state.settings.ANNUAL_REPORT_MAX_STAGE_ATTEMPTS", 3)

    queued = store.enqueue_stage(
        report_id="report-1",
        pdf_sha256="a" * 64,
        stage="parse",
        stage_version="parser-v1",
        error="missing IR",
    )

    assert queued is True
    assert "RETURNING status, attempts" in cursor.sql


class _NullContext:
    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False
