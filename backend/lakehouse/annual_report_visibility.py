"""Select a single committed Silver projection for each annual report."""


def committed_reports(reports, parse_runs):
    runs = {(run.report_id, run.parse_run_id): run for run in parse_runs}
    selected = {}
    for report in reports:
        run = runs.get((report.report_id, report.parse_run_id))
        if run is None:
            continue
        key = (run.finished_at or run.started_at, run.parse_run_id)
        current = selected.get(report.report_id)
        if current is None or key > current[0]:
            selected[report.report_id] = (key, report)
    return {report_id: item[1] for report_id, item in selected.items()}
