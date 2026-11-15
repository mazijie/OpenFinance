"""HTTP and batch-planning helpers shared by the A-share DAGs.

This module deliberately declares no DAGs, so importing it cannot register one.
"""

from __future__ import annotations

import json
import os
import time
from urllib.parse import urlencode
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


def _base_url() -> str:
    return os.getenv("OPENFINANCE_INTERNAL_API_BASE_URL", "http://backend:8000").rstrip("/")


def _internal_token() -> str:
    token = os.getenv("OPENFINANCE_INTERNAL_API_TOKEN", "").strip()
    if not token:
        raise ValueError("OPENFINANCE_INTERNAL_API_TOKEN is not configured")
    return token


def _post_internal_job(path: str, payload: dict[str, object]) -> dict[str, object]:
    request = Request(
        f"{_base_url()}{path}",
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json", "X-OpenFinance-Token": _internal_token()},
        method="POST",
    )
    try:
        with urlopen(request, timeout=300) as response:
            return json.loads(response.read().decode("utf-8"))
    except HTTPError as exc:  # pragma: no cover - Airflow runtime
        raise RuntimeError(f"Internal job request failed: {exc.code} {exc.read().decode('utf-8', errors='ignore')}") from exc
    except URLError as exc:  # pragma: no cover - Airflow runtime
        raise RuntimeError(f"Internal job request failed: {exc.reason}") from exc


def _submit_a_share_ingestion(payload: dict[str, object]) -> str:
    response = _post_internal_job("/internal/jobs/ingest-a-share-daily-bars/submit", payload)
    job_id = str(response.get("job_id") or "").strip()
    if not job_id:
        raise RuntimeError("A-share ingestion submission did not return a job id")
    return job_id


def _wait_for_a_share_ingestion(job_id: str) -> dict[str, object]:
    timeout_seconds = int(os.getenv("A_SHARE_JOB_TIMEOUT_SECONDS", "21600"))
    poll_seconds = max(1, int(os.getenv("A_SHARE_JOB_POLL_SECONDS", "10")))
    deadline = time.monotonic() + timeout_seconds
    last_transient_error: str | None = None
    while time.monotonic() < deadline:
        request = Request(
            f"{_base_url()}/internal/jobs/ingest-a-share-daily-bars/jobs/{job_id}",
            headers={"X-OpenFinance-Token": _internal_token()},
        )
        try:
            with urlopen(request, timeout=30) as response:
                payload = json.loads(response.read().decode("utf-8"))
        except HTTPError as exc:
            body = exc.read().decode("utf-8", errors="ignore")
            if exc.code in {408, 429} or exc.code >= 500:
                last_transient_error = f"{exc.code} {body}"
                print(f"A-share status poll temporarily failed; retrying: {last_transient_error}")
                time.sleep(poll_seconds)
                continue
            raise RuntimeError(f"A-share ingestion status request failed: {exc.code} {body}") from exc
        except URLError as exc:
            last_transient_error = str(exc.reason)
            print(f"A-share status poll temporarily unavailable; retrying: {last_transient_error}")
            time.sleep(poll_seconds)
            continue
        except (ConnectionError, TimeoutError) as exc:
            last_transient_error = str(exc)
            print(f"A-share status poll transiently failed; retrying: {last_transient_error}")
            time.sleep(poll_seconds)
            continue

        state = str(payload.get("status") or "").strip().lower()
        if state == "success":
            result = payload.get("result")
            if not isinstance(result, dict):
                raise RuntimeError(f"A-share ingestion job {job_id} completed without a result")
            return result
        if state == "failed":
            raise RuntimeError(f"A-share ingestion job {job_id} failed: {payload.get('error') or 'unknown error'}")
        time.sleep(poll_seconds)
    suffix = f"; last transient error: {last_transient_error}" if last_transient_error else ""
    raise TimeoutError(
        f"A-share ingestion job {job_id} did not finish within {timeout_seconds} seconds{suffix}"
    )


def recover_partial_a_share_ingestion(
    result: dict[str, object], window: dict[str, str], idempotency_key: str,
    *, max_retries: int = 2,
) -> dict[str, object]:
    """Retry failed symbols while retaining original and recovered Bronze objects."""
    combined = dict(result)
    paths = [str(result["object_path"])]
    pending = list(dict.fromkeys(str(s) for s in result.get("failed_symbols", [])))
    if pending:
        print(f"A-share ingestion partially failed: {len(pending)} symbols: {pending}")
    for retry in range(1, max_retries + 1):
        if not pending:
            break
        remaining = []
        for symbol in pending:
            payload = {
                "symbols": [symbol],
                "start_date": f"{window['start_date']}T00:00:00+00:00",
                "end_date": f"{window['end_date']}T00:00:00+00:00",
                "adjustment_type": window["adjustment_type"],
                "daily_provider_name": window["daily_provider_name"],
                "include_trading_calendar": False,
                "idempotency_key": f"{idempotency_key}:recover:{symbol}:{retry}",
            }
            try:
                recovery = _wait_for_a_share_ingestion(_submit_a_share_ingestion(payload))
            except (RuntimeError, TimeoutError) as exc:
                print(f"A-share recovery failed for {symbol}, attempt {retry}: {exc}")
                remaining.append(symbol)
                continue
            if recovery.get("failed_symbols") or recovery.get("no_data_symbols") or int(recovery.get("daily_bar_count", 0)) <= 0:
                print(f"A-share recovery returned no usable bars for {symbol}, attempt {retry}")
                remaining.append(symbol)
                continue
            paths.append(str(recovery["object_path"]))
            combined["daily_bar_count"] = int(combined.get("daily_bar_count", 0)) + int(recovery["daily_bar_count"])
            combined["processed_symbol_count"] = int(combined.get("processed_symbol_count", 0)) + int(recovery.get("processed_symbol_count", 0))
        pending = remaining
    combined["object_paths"] = list(dict.fromkeys(paths))
    combined["failed_symbols"] = pending
    if pending:
        print(f"ERROR: A-share recovery exhausted; unresolved symbols: {pending}")
    return combined


def _submit_finance_normalization(payload: dict[str, object]) -> str:
    response = _post_internal_job("/internal/jobs/finance-bronze-to-silver/submit", payload)
    job_id = str(response.get("job_id") or "").strip()
    if not job_id:
        raise RuntimeError("Finance normalization submission did not return a job id")
    return job_id


def _wait_for_finance_normalization(job_id: str) -> dict[str, object]:
    timeout_seconds = int(os.getenv("A_SHARE_JOB_TIMEOUT_SECONDS", "21600"))
    poll_seconds = max(1, int(os.getenv("A_SHARE_JOB_POLL_SECONDS", "10")))
    deadline = time.monotonic() + timeout_seconds
    last_transient_error: str | None = None
    while time.monotonic() < deadline:
        request = Request(f"{_base_url()}/internal/jobs/finance-bronze-to-silver/jobs/{job_id}", headers={"X-OpenFinance-Token": _internal_token()})
        try:
            with urlopen(request, timeout=30) as response:
                payload = json.loads(response.read().decode("utf-8"))
        except HTTPError as exc:
            body = exc.read().decode("utf-8", errors="ignore")
            if exc.code in {408, 429} or exc.code >= 500:
                last_transient_error = f"{exc.code} {body}"
                print(f"Finance normalization status poll temporarily failed; retrying: {last_transient_error}")
                time.sleep(poll_seconds)
                continue
            raise RuntimeError(f"Finance normalization status request failed: {exc.code} {body}") from exc
        except URLError as exc:
            last_transient_error = str(exc.reason)
            print(f"Finance normalization status poll temporarily unavailable; retrying: {last_transient_error}")
            time.sleep(poll_seconds)
            continue
        except (ConnectionError, TimeoutError) as exc:
            last_transient_error = str(exc)
            print(f"Finance normalization status poll transiently failed; retrying: {last_transient_error}")
            time.sleep(poll_seconds)
            continue
        state = str(payload.get("status") or "").strip().lower()
        if state == "success":
            result = payload.get("result")
            if not isinstance(result, dict): raise RuntimeError(f"Finance normalization job {job_id} completed without a result")
            return result
        if state == "failed": raise RuntimeError(f"Finance normalization job {job_id} failed: {payload.get('error') or 'unknown error'}")
        time.sleep(poll_seconds)
    suffix = f"; last transient error: {last_transient_error}" if last_transient_error else ""
    raise TimeoutError(
        f"Finance normalization job {job_id} did not finish within "
        f"{timeout_seconds} seconds{suffix}"
    )


def _get_a_share_symbols(adjustment_type: str, *, ignore_coverage: bool = False) -> dict[str, object]:
    params = {"adjustment_type": adjustment_type, "ignore_coverage": str(ignore_coverage).lower()}
    url = f"{_base_url()}/internal/jobs/list-a-share-symbols?{urlencode(params)}"
    print(f"Requesting A-share universe from {url}")
    request = Request(url, headers={"X-OpenFinance-Token": _internal_token()})
    try:
        with urlopen(request, timeout=300) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except HTTPError as exc:  # pragma: no cover - Airflow runtime
        raise RuntimeError(f"A-share universe request failed: {exc.code} {exc.read().decode('utf-8', errors='ignore')}") from exc
    except URLError as exc:  # pragma: no cover - Airflow runtime
        raise RuntimeError(f"A-share universe request failed: {exc.reason}") from exc
    print(f"Received A-share universe: status={payload.get('status')} date={payload.get('as_of_date')} symbols={payload.get('symbol_count')}")
    return payload


def _split_batches(symbols: list[str], batch_size: int) -> list[list[str]]:
    if batch_size <= 0:
        raise ValueError("A_SHARE_BATCH_SIZE must be positive")
    return [symbols[index:index + batch_size] for index in range(0, len(symbols), batch_size)]


def build_universe_plan(payload: dict[str, object], batch_size: int) -> dict[str, object]:
    status = str(payload.get("status") or "").strip()
    as_of_date = str(payload.get("as_of_date") or "").strip()
    if status not in {"available", "already_covered"} or not as_of_date:
        raise ValueError("A-share universe response is missing status or as_of_date")
    symbols = [str(symbol).strip().upper() for symbol in payload.get("symbols", []) if str(symbol).strip()]
    if status == "available" and not symbols:
        raise ValueError("Available A-share universe cannot be empty")
    return {
        "status": status,
        "universe_as_of_date": as_of_date,
        "provider": str(payload.get("provider") or os.getenv("FINANCE_DAILY_PROVIDER", "tencent")),
        "historical_universe_complete": bool(payload.get("historical_universe_complete", False)),
        "symbol_count": len(symbols),
        "batches": _split_batches(symbols, batch_size) if status == "available" else [],
    }


def build_gap_ingest_jobs(plan: dict[str, object], batch_size: int) -> list[dict[str, object]]:
    """Group equal gap windows into bounded Airflow ingestion jobs."""

    if batch_size <= 0:
        raise ValueError("A_SHARE_INGEST_BATCH_SIZE must be positive")
    grouped: dict[tuple[str, str, bool], list[dict[str, object]]] = {}
    for item in plan.get("plans", []):
        key = (
            str(item["start_date"]),
            str(item["end_date"]),
            bool(item.get("needs_metadata", False)),
        )
        grouped.setdefault(key, []).append(item)

    jobs: list[dict[str, object]] = []
    adjustment_type = str(plan["adjustment_type"])
    for (start_date, end_date, needs_metadata), items in sorted(grouped.items()):
        ordered = sorted(items, key=lambda item: str(item["symbol"]))
        for index in range(0, len(ordered), batch_size):
            chunk = ordered[index:index + batch_size]
            jobs.append(
                {
                    "symbols": [str(item["symbol"]) for item in chunk],
                    "window": {
                        "start_date": start_date,
                        "end_date": end_date,
                        "adjustment_type": adjustment_type,
                        "daily_provider_name": str(plan.get("provider") or os.getenv("FINANCE_DAILY_PROVIDER", "tencent")),
                    },
                    "missing_date_count": sum(int(item["missing_date_count"]) for item in chunk),
                    "include_security_master": needs_metadata,
                }
            )
    return jobs


def build_normalization_batches(
    ingest_results: list[dict[str, object]],
    batch_size: int,
) -> list[list[str]]:
    """Group stable Bronze object paths into bounded Iceberg commits."""

    if batch_size <= 0:
        raise ValueError("A_SHARE_NORMALIZATION_BATCH_SIZE must be positive")
    paths = []
    for item in ingest_results:
        object_paths = item.get("object_paths") or [item.get("object_path")]
        if not isinstance(object_paths, list) or not object_paths or any(
            not str(path or "").strip() for path in object_paths
        ):
            raise ValueError("Every successful ingestion batch must have one unique Bronze object or more")
        paths.extend(str(path).strip() for path in object_paths)
    unique_paths = list(dict.fromkeys(paths))
    if len(unique_paths) != len(paths):
        raise ValueError("Every successful ingestion batch must have one unique Bronze object")
    return [
        unique_paths[index:index + batch_size]
        for index in range(0, len(unique_paths), batch_size)
    ]
