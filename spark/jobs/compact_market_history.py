"""Compact a bounded date range of Iceberg daily bars using symbol/date sorting."""

from __future__ import annotations

import argparse
from datetime import date
import json
import re

from daily_metrics import create_spark


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--table", default="openfinance.openfinance.daily_bars")
    parser.add_argument("--start-date", type=date.fromisoformat, required=True)
    parser.add_argument("--end-date", type=date.fromisoformat, required=True, help="Exclusive end date")
    parser.add_argument("--min-input-files", type=int, default=8)
    args = parser.parse_args()
    if not re.fullmatch(r"openfinance\.[a-z][a-z0-9_]*\.[a-z][a-z0-9_]*", args.table):
        parser.error("Invalid Iceberg table identifier")
    if args.start_date >= args.end_date or args.min_input_files < 2:
        parser.error("Require start-date < end-date and min-input-files >= 2")
    identifier = args.table
    condition = (f"trading_date >= DATE ''{args.start_date.isoformat()}'' AND "
                 f"trading_date < DATE ''{args.end_date.isoformat()}''")
    spark = create_spark()
    try:
        rows = spark.sql(
            f"CALL openfinance.system.rewrite_data_files(table => '{identifier}', "
            "strategy => 'sort', sort_order => 'symbol ASC NULLS FIRST, trading_date ASC NULLS FIRST', "
            f"where => '{condition}', options => map('min-input-files', '{args.min_input_files}', "
            "'target-file-size-bytes', '134217728'))"
        ).collect()
        result = [row.asDict(recursive=True) for row in rows]
        print(json.dumps({"table": args.table, "start_date": args.start_date.isoformat(),
                          "end_date": args.end_date.isoformat(), "result": result}))
        if any(row.get("failed_data_files_count", 0) for row in result):
            raise RuntimeError("Some Iceberg files could not be rewritten")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
