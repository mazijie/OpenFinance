"""Run serial, bounded provider probes without publishing data."""
import argparse
import json
import time
from datetime import datetime, timezone
from realtime.collector import timed_fetch


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--timeout", type=int, default=90)
    parser.add_argument("--provider", choices=("eastmoney", "tencent", "sina"),
                        help="Probe only one provider")
    args = parser.parse_args()
    for provider in (args.provider,) if args.provider else ("eastmoney", "tencent", "sina"):
        start = time.monotonic()
        report = {"provider": provider, "started_at": datetime.now(timezone.utc).isoformat()}
        try:
            rows, columns = timed_fetch(provider, args.timeout)
            report.update(success=True, rows=len(rows), columns=columns)
        except Exception as exc:
            report.update(success=False, error=f"{type(exc).__name__}: {exc}")
        report["elapsed_seconds"] = round(time.monotonic() - start, 2)
        print(json.dumps(report, ensure_ascii=False), flush=True)


if __name__ == "__main__":
    main()
