"""Refresh the local SH trading calendar from BaoStock."""
from __future__ import annotations

import json
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

from backend.ingestion.writers.bronze_writer import BronzeWriter
from backend.lakehouse.finance_writer import FinanceWriter
from backend.pipelines.finance_bronze_to_silver import FinanceBronzeToSilverPipeline
from backend.providers.finance import BaoStockFinanceProvider

CHINA = ZoneInfo("Asia/Shanghai")


def validate_calendar(rows, start: date, end: date):
    by_date = {}
    for row in rows:
        day = row.trading_date
        if row.exchange != "SH" or day < start or day > end or day in by_date:
            raise ValueError("BaoStock calendar contains duplicate or unexpected dates")
        by_date[day] = row
    if start not in by_date or start + timedelta(days=1) not in by_date:
        raise ValueError("BaoStock calendar does not cover today and tomorrow")
    last = max(by_date)
    if set(by_date) != {start + timedelta(days=offset) for offset in range((last - start).days + 1)}:
        raise ValueError("BaoStock calendar contains a gap")
    return [by_date[day] for day in sorted(by_date)]


def run(*, today: date | None = None):
    day = today or datetime.now(CHINA).date()
    end = day + timedelta(days=30)
    rows = validate_calendar(BaoStockFinanceProvider().fetch_trading_calendar(
        exchange="SH", start_date=day, end_date=end), day, end)
    bronze = BronzeWriter()
    object_path = bronze.write_finance_raw(
        provider="baostock", source_version=day.isoformat(),
        request_params={"kind": "trading_calendar", "exchange": "SH",
                        "start_date": day.isoformat(), "end_date": end.isoformat()},
        raw_response={"trading_calendar": [row.model_dump(mode="json") for row in rows],
                      "security_master": [], "daily_bars": []})
    writer = FinanceWriter()
    stats = FinanceBronzeToSilverPipeline(bronze_writer=bronze, finance_writer=writer).process_object(object_path)
    if stats.failed_count or stats.filtered_count:
        raise RuntimeError("Calendar normalization did not complete")
    actual = {(row.exchange, row.trading_date): row.is_open for row in writer.scan_trading_calendar()}
    if any(actual.get((row.exchange, row.trading_date)) != row.is_open for row in rows):
        raise RuntimeError("Calendar verification failed after write")
    return {"status": "success", "sync_date": day.isoformat(), "object_path": object_path,
            "coverage_start": rows[0].trading_date.isoformat(), "coverage_end": rows[-1].trading_date.isoformat(),
            "record_count": len(rows), "today_is_open": rows[0].is_open, "tomorrow_is_open": rows[1].is_open}


if __name__ == "__main__":
    print(json.dumps(run(), ensure_ascii=False))
