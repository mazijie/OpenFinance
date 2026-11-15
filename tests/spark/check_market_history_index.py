"""Run with spark-submit to verify visibility and per-key minima."""
from datetime import date, datetime, timezone
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "spark" / "jobs"))
from build_market_history_index import summarize
from daily_metrics import create_spark


def main():
    spark = create_spark("local[2]")
    try:
        now = datetime(2026, 1, 3, tzinfo=timezone.utc)
        rows = [
            ("600519.SH", "none", date(2020, 1, 1), datetime(2020, 1, 2, tzinfo=timezone.utc), datetime(2020, 1, 2, tzinfo=timezone.utc)),
            ("600519.SH", "none", date(2019, 1, 1), datetime(2020, 1, 2, tzinfo=timezone.utc), datetime(2026, 1, 4, tzinfo=timezone.utc)),
            ("600519.SH", "qfq", date(2021, 1, 1), datetime(2020, 1, 2, tzinfo=timezone.utc), datetime(2020, 1, 2, tzinfo=timezone.utc)),
            ("000001.SZ", "none", date(2018, 1, 1), datetime(2026, 1, 5, tzinfo=timezone.utc), datetime(2020, 1, 2, tzinfo=timezone.utc)),
        ]
        frame = spark.createDataFrame(rows, "symbol string, adjustment_type string, trading_date date, available_at timestamp, ingested_at timestamp")
        found = {(r.symbol, r.adjustment_type): r for r in summarize(frame, now).collect()}
        assert found[("600519.SH", "none")].earliest_date == date(2020, 1, 1)
        assert found[("600519.SH", "none")].next_visible == datetime(2026, 1, 4)
        assert found[("600519.SH", "qfq")].earliest_date == date(2021, 1, 1)
        assert found[("000001.SZ", "none")].earliest_date is None
        assert found[("000001.SZ", "none")].next_visible == datetime(2026, 1, 5)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
