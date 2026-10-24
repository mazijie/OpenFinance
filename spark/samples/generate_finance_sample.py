"""Generate deterministic finance Bronze data for Spark exercises."""

from __future__ import annotations

import argparse
import json
import math
from datetime import UTC, date, datetime, timedelta
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, help="Directory for generated Bronze JSON.")
    parser.add_argument("--start-date", default="2026-01-05")
    parser.add_argument("--days", type=int, default=25)
    return parser.parse_args()


def trading_days(start: date, count: int) -> list[date]:
    result: list[date] = []
    current = start
    while len(result) < count:
        if current.weekday() < 5:
            result.append(current)
        current += timedelta(days=1)
    return result


def make_bar(
    *,
    security_id: str,
    symbol: str,
    exchange: str,
    trading_date: date,
    close: float,
    volume: float,
) -> dict[str, object]:
    open_price = round(close * 0.998, 2)
    high = round(max(open_price, close) * 1.004, 2)
    low = round(min(open_price, close) * 0.996, 2)
    return {
        "security_id": security_id,
        "symbol": symbol,
        "exchange": exchange,
        "trading_date": trading_date.isoformat(),
        "open": open_price,
        "high": high,
        "low": low,
        "close": round(close, 2),
        "volume": round(volume, 2),
        "amount": round(close * volume, 2),
        "turnover": round(volume / 10_000_000, 4),
        "is_suspended": False,
        "is_st": False,
        "adjustment_factor": 1.0,
        "adjustment_type": "qfq",
        "source_record_id": f"{symbol}-{trading_date.isoformat()}-qfq",
    }


def build_payload(start: date, days: int) -> dict[str, object]:
    dates = trading_days(start, days)
    bars: list[dict[str, object]] = []

    for index, trading_date in enumerate(dates):
        maotai_close = 1500 + index * 2.5 + math.sin(index / 2) * 18
        pingan_close = 12 + index * 0.035 + math.sin(index / 3) * 0.18
        bars.append(
            make_bar(
                security_id="600519.SH",
                symbol="600519",
                exchange="SH",
                trading_date=trading_date,
                close=maotai_close,
                volume=1_200_000 + index * 12_000,
            )
        )
        bars.append(
            make_bar(
                security_id="000001.SZ",
                symbol="000001",
                exchange="SZ",
                trading_date=trading_date,
                close=pingan_close,
                volume=86_000_000 + index * 450_000,
            )
        )

    ingested_at = datetime.combine(dates[-1], datetime.min.time(), tzinfo=UTC) + timedelta(hours=10)
    return {
        "domain": "finance",
        "provider": "baostock",
        "ingested_at": ingested_at.isoformat().replace("+00:00", "Z"),
        "source_version": dates[-1].isoformat(),
        "payload_hash": f"generated-{start.isoformat()}-{days}",
        "request_params": {
            "symbols": ["000001.SZ", "600519.SH"],
            "start_date": dates[0].isoformat(),
            "end_date": dates[-1].isoformat(),
            "adjustment_type": "qfq",
        },
        "raw_response": {
            "security_master": [
                {"security_id": "000001.SZ", "symbol": "000001", "exchange": "SZ", "name": "平安银行"},
                {"security_id": "600519.SH", "symbol": "600519", "exchange": "SH", "name": "贵州茅台"},
            ],
            "trading_calendar": [
                {"exchange": "SH", "trading_date": trading_date.isoformat(), "is_open": True}
                for trading_date in dates
            ],
            "daily_bars": bars,
        },
    }


def main() -> None:
    args = parse_args()
    start = date.fromisoformat(args.start_date)
    output_dir = (
        Path(args.output)
        / "finance"
        / "provider=baostock"
        / f"ingest_date={start.isoformat()}"
        / "hour=10"
    )
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "generated-finance.json"
    payload = build_payload(start, args.days)
    output_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(json.dumps({"output": str(output_path), "daily_bars": len(payload["raw_response"]["daily_bars"])}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
