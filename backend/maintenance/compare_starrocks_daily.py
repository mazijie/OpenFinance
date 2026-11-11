"""Compare a pinned snapshot through PyIceberg and StarRocks without writes."""
from __future__ import annotations

import argparse
from collections import Counter
from datetime import UTC, date, datetime
import json
from time import perf_counter

from backend.lakehouse.catalog import get_catalog
from backend.lakehouse.finance_tables import finance_table_identifier
from backend.lakehouse.finance_writer import FinanceWriter
from backend.lakehouse.starrocks_daily_reader import StarRocksDailyReader
from backend.config import settings


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--symbol", required=True)
    parser.add_argument("--start-date", type=date.fromisoformat, required=True)
    parser.add_argument("--end-date", type=date.fromisoformat, required=True)
    parser.add_argument("--snapshot", type=int, help="Defaults to the current Iceberg snapshot")
    args = parser.parse_args()
    if args.start_date > args.end_date:
        parser.error("start-date must not be after end-date")
    table = get_catalog().load_table(finance_table_identifier(settings.ICEBERG_DAILY_BARS_TABLE))
    current = table.current_snapshot()
    snapshot_id = args.snapshot if args.snapshot is not None else (current.snapshot_id if current else None)
    if snapshot_id is None or table.snapshot_by_id(snapshot_id) is None:
        raise SystemExit("Snapshot unavailable")
    params = dict(symbol=args.symbol.upper(), security_id=None, start_date=args.start_date,
                  end_date=args.end_date, adjustment_type="none", as_of=datetime.now(UTC),
                  limit=500, table=table, snapshot_id=snapshot_id)
    results = {}
    for name, reader in (("iceberg", FinanceWriter()), ("starrocks", StarRocksDailyReader())):
        start = perf_counter()
        rows = reader.scan_daily_bars_for_query(**params)
        results[name] = (Counter(json.dumps(row.model_dump(mode="json"), sort_keys=True) for row in rows),
                         round(perf_counter() - start, 3))
    left, right = results["iceberg"][0], results["starrocks"][0]
    print(json.dumps({"snapshot_id": str(snapshot_id), "symbol": args.symbol.upper(),
                      "iceberg_rows": sum(left.values()), "starrocks_rows": sum(right.values()),
                      "missing_rows": sum((left - right).values()), "extra_rows": sum((right - left).values()),
                      "iceberg_seconds": results["iceberg"][1], "starrocks_seconds": results["starrocks"][1]}))
    if left != right:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
