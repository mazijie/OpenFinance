"""Read-only comparison of index input fields on identical Iceberg snapshots."""

from __future__ import annotations

import hashlib
import json
import time
from collections import Counter

from backend.rag.annual_reports.annual_report_index_reader import FIELDS, StarRocksAnnualReportIndexReader


def _hashes(rows):
    return Counter(hashlib.sha256(json.dumps(row, sort_keys=True, ensure_ascii=False,
                                           default=lambda value: value.isoformat()).encode()).hexdigest()
                   for row in rows)


def main():
    reader = StarRocksAnnualReportIndexReader()
    results = {}
    for kind in FIELDS:
        timings, hashes, counts = {}, {}, {}
        for engine in ("pyiceberg", "starrocks"):
            started = time.monotonic()
            rows = reader.read_rows(kind, engine=engine)
            timings[engine] = round(time.monotonic() - started, 3)
            counts[engine] = len(rows)
            hashes[engine] = _hashes(rows)
        missing = sum((hashes["pyiceberg"] - hashes["starrocks"]).values())
        extra = sum((hashes["starrocks"] - hashes["pyiceberg"]).values())
        results[kind] = {"snapshot_id": reader.snapshots[kind], "table_uuid": str(reader.tables[kind].metadata.table_uuid),
                         "rows": counts, "seconds": timings, "missing": missing, "extra": extra}
    print(json.dumps(results, ensure_ascii=False, indent=2))
    if any(item["missing"] or item["extra"] for item in results.values()):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
