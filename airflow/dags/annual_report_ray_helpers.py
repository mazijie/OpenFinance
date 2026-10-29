"""Airflow helpers for submitting annual-report batches to Ray Jobs."""

from __future__ import annotations

import json
import os
import time
from urllib.request import Request, urlopen


def _request(
    method: str,
    path: str,
    payload: dict[str, object] | None = None,
    *,
    timeout_seconds: float | None = None,
) -> object:
    """Call the backend API; HTTP timeouts are distinct from Ray Job deadlines."""
    base = os.getenv("OPENFINANCE_INTERNAL_API_BASE_URL", "http://backend:8000").rstrip("/")
    token = os.getenv("OPENFINANCE_INTERNAL_API_TOKEN", "").strip()
    if not token:
        raise ValueError("OPENFINANCE_INTERNAL_API_TOKEN is not configured")
    data = json.dumps(payload).encode("utf-8") if payload is not None else None
    headers = {"X-OpenFinance-Token": token}
    if data is not None:
        headers["Content-Type"] = "application/json"
    request = Request(f"{base}{path}", data=data, headers=headers, method=method)
    if timeout_seconds is None:
        timeout_seconds = float(os.getenv("RAY_API_HTTP_TIMEOUT_SECONDS", "60"))
    with urlopen(request, timeout=timeout_seconds) as response:
        return json.loads(response.read().decode("utf-8"))


def submit_and_wait_annual_report_ray(manifest_paths: list[str], *, force: bool = False) -> dict[str, object]:
    """Submit one Ray Job and wait without holding an Airflow mapped parser slot."""

    submitted = _request(
        "POST",
        "/internal/jobs/annual-reports-ray",
        {"manifest_paths": manifest_paths, "force": force},
    )
    if not isinstance(submitted, dict) or not submitted.get("job_id"):
        raise ValueError(f"Ray submission returned an invalid response: {submitted}")
    job_id = str(submitted["job_id"])
    print(f"Ray batch submitted: job_id={job_id}, manifests={len(manifest_paths)}", flush=True)
    started = time.monotonic()
    deadline = started + float(os.getenv("RAY_JOB_TIMEOUT_SECONDS", "14400"))
    poll_seconds = max(1.0, float(os.getenv("RAY_JOB_POLL_SECONDS", "5")))
    last_log_at = started - 60.0
    last_status = None
    while time.monotonic() < deadline:
        current = _request("GET", f"/internal/jobs/annual-reports-ray/{job_id}")
        status = str(current.get("status", "")).lower() if isinstance(current, dict) else ""
        now = time.monotonic()
        if status != last_status or now - last_log_at >= 60.0:
            print(f"Ray batch job_id={job_id} status={status or 'unknown'} elapsed={int(now - started)}s", flush=True)
            last_log_at = now
            last_status = status
        if status == "succeeded":
            return {"job_id": job_id, "status": status}
        if status in {"failed", "stopped", "stopping"}:
            raise RuntimeError(f"Ray annual-report job {job_id} ended with status={status}")
        time.sleep(poll_seconds)
    # We deliberately do not submit a replacement automatically: the remote
    # Job may still be RUNNING and must be inspected before retrying.
    raise TimeoutError(
        f"Ray annual-report job {job_id} exceeded RAY_JOB_TIMEOUT_SECONDS; "
        "inspect/stop the remote job before manually retrying this Airflow batch"
    )


def collect_manifest_paths(batch_results: list[list[dict[str, object]]]) -> list[str]:
    """Collect unique complete Bronze manifests across ingestion batches."""
    paths = list(dict.fromkeys(
        str(path) for results in batch_results for result in results
        for path in (result.get("manifest_paths") or []) if path
    ))
    if not paths:
        raise ValueError("Annual-report ingestion produced no complete Bronze manifests")
    return paths


def reconcile_annual_report_index() -> dict[str, int]:
    result = _request(
        "POST",
        "/internal/jobs/annual-reports-rag-index",
        {"rebuild_all": False},
        timeout_seconds=float(os.getenv("RAG_INDEX_HTTP_TIMEOUT_SECONDS", "14400")),
    )
    if not isinstance(result, dict):
        raise ValueError("Annual-report index reconciliation returned an invalid response")
    return result


def assert_annual_report_run_succeeded(context: dict[str, object]) -> None:
    """Fail the terminal task when index reconciliation ran after an upstream failure."""
    run = context["dag_run"]
    current = context["task_instance"]
    failures = [
        f"{item.task_id}[{item.map_index}]={item.state}"
        for item in run.get_task_instances()
        if item.task_id != current.task_id and item.state in {"failed", "upstream_failed"}
    ]
    if failures:
        raise RuntimeError("Annual-report pipeline failed: " + ", ".join(failures))
