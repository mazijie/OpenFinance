from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from backend.lakehouse.annual_report_visibility import committed_reports


def test_uncommitted_new_projection_cannot_replace_last_committed_projection():
    now = datetime.now(UTC)
    old = SimpleNamespace(report_id="report", parse_run_id="old")
    partial = SimpleNamespace(report_id="report", parse_run_id="partial")
    runs = [SimpleNamespace(report_id="report", parse_run_id="old", started_at=now, finished_at=now)]
    assert committed_reports([old, partial], runs)["report"] is old
    runs.append(SimpleNamespace(report_id="report", parse_run_id="partial", started_at=now,
                                finished_at=now + timedelta(seconds=1)))
    assert committed_reports([old, partial], runs)["report"] is partial
