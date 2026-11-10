"""Measure initial market pages and any separate availability check."""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
import json
import math
from time import monotonic
from urllib.parse import urlencode
from urllib.request import urlopen


def request(base, path, params, timeout):
    started = monotonic()
    with urlopen(f"{base}{path}?{urlencode(params)}", timeout=timeout) as response:
        result = json.load(response)
    return result, round(monotonic() - started, 3)


def sample(base, symbol, period, timeout):
    try:
        page, elapsed = request(base, "/api/market/history", {
            "symbol": symbol, "period": period, "adjustment_type": "none"}, timeout)
    except Exception as exc:
        return {"period": period, "page_error": str(exc)}
    result = {"period": period, "page_seconds": elapsed, "availability_seconds": 0.0,
              "has_more": page["has_more"]}
    if page["has_more"] is None:
        try:
            answer, elapsed = request(base, "/api/market/history/availability", {
                "symbol": symbol, "adjustment_type": "none", "before": page["next_before"],
                "snapshot_id": page["snapshot_id"], "table_uuid": page["table_uuid"],
                "query_time": page["query_time"]}, timeout)
            result.update(availability_seconds=elapsed, has_more=answer["has_more"])
        except Exception as exc:
            result["availability_error"] = str(exc)
    return result


def percentile(values, percentile_value):
    values = sorted(values)
    return values[max(0, math.ceil(len(values) * percentile_value) - 1)] if values else None


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", default="http://localhost:8000")
    parser.add_argument("--symbol", default="600519.SH")
    parser.add_argument("--iterations", type=int, default=3)
    parser.add_argument("--timeout", type=float, default=70)
    parser.add_argument("--concurrent", action="store_true", help="Run each round's three periods concurrently")
    args = parser.parse_args()
    if args.iterations < 1 or args.timeout <= 0:
        parser.error("iterations and timeout must be positive")
    results = []
    for _ in range(args.iterations):
        with ThreadPoolExecutor(max_workers=3 if args.concurrent else 1) as pool:
            results.extend(pool.map(lambda p: sample(args.base_url.rstrip("/"), args.symbol, p, args.timeout),
                                    ("daily", "weekly", "monthly")))
    pages = [x["page_seconds"] for x in results if "page_seconds" in x]
    per_period = {}
    for period in ("daily", "weekly", "monthly"):
        samples = [x for x in results if x["period"] == period]
        elapsed = [x["page_seconds"] for x in samples if "page_seconds" in x]
        per_period[period] = {"page_p50": percentile(elapsed, .5), "page_p95": percentile(elapsed, .95),
                              "page_errors": sum("page_error" in x for x in samples),
                              "availability_errors": sum("availability_error" in x for x in samples)}
    print(json.dumps({"concurrent": args.concurrent, "samples": results,
                      "page_p50": percentile(pages, .5), "page_p95": percentile(pages, .95),
                      "per_period": per_period,
                      "page_errors": sum("page_error" in x for x in results),
                      "availability_errors": sum("availability_error" in x for x in results)}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
