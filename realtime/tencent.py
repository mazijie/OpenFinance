"""Tencent quote snapshots and bounded concurrent batch collection."""
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta
import math
import logging
import re
import time

import requests

CHINA = timezone(timedelta(hours=8))
SYMBOL = re.compile(r"^(\d{6})\.(SH|SZ|BJ)$")
RECORD = re.compile(r'v_([a-z]{2}\d{6})="([^"]*)";')
log = logging.getLogger(__name__)


def optional_number(fields, index, *, positive=False, allow_negative=False):
    try:
        value = float(fields[index])
        if not math.isfinite(value) or (positive and value <= 0) or (not positive and not allow_negative and value < 0):
            return None
        return value
    except (ValueError, IndexError, TypeError):
        return None


def to_tencent(symbol):
    match = SYMBOL.fullmatch(symbol.upper())
    if not match:
        return None
    return match.group(2).lower() + match.group(1)


def parse_response(content, symbols, round_time, captured_at):
    expected = {to_tencent(s): s for s in symbols}
    rows = {}
    for code, raw in RECORD.findall(content.decode("gb18030", errors="replace")):
        fields = raw.split("~")
        symbol = expected.get(code)
        if not symbol or len(fields) <= 30 or fields[2] != code[2:]:
            continue
        try:
            price, previous = float(fields[3]), float(fields[4])
            source_time = datetime.strptime(fields[30], "%Y%m%d%H%M%S").replace(tzinfo=CHINA)
            if not all(math.isfinite(x) and x > 0 for x in (price, previous)):
                continue
        except (ValueError, IndexError):
            continue
        age = (captured_at - source_time).total_seconds()
        status = "valid" if source_time.date() == round_time.date() and -60 <= age <= 300 else "not_updated"
        rows[symbol] = {"symbol": symbol, "name": fields[1], "price": price,
                        "previous_close": previous, "source": "tencent",
                        "day_open": optional_number(fields, 5, positive=True),
                        "day_high": optional_number(fields, 33, positive=True),
                        "day_low": optional_number(fields, 34, positive=True),
                        "volume_lots": optional_number(fields, 36),
                        "market_cap_cny": (cap * 100000000 if (cap := optional_number(fields, 45, positive=True)) is not None else None),
                        "pe_ratio": optional_number(fields, 39, allow_negative=True),
                        "source_time": source_time.astimezone(timezone.utc).isoformat(),
                        "captured_at": captured_at.astimezone(timezone.utc).isoformat(), "status": status}
    return rows


def fetch_batch(symbols, round_time, get=requests.get, sleep=time.sleep):
    pending = list(symbols)
    result = {}
    for attempt in range(4):
        if attempt:
            sleep((2, 4, 8)[attempt - 1])
        try:
            response = get("https://qt.gtimg.cn/q=" + ",".join(to_tencent(s) for s in pending), timeout=(5, 30))
            response.raise_for_status()
            captured = datetime.now(timezone.utc)
            result.update(parse_response(response.content, pending, round_time, captured))
            pending = [symbol for symbol in symbols if symbol not in result]
            if not pending:
                break
        except requests.RequestException as exc:
            log.warning("Tencent batch request failed attempt=%d pending=%d error=%s", attempt + 1, len(pending), exc)
    return result, pending


def collect_round(symbols, round_time, fetch=fetch_batch):
    started = time.monotonic()
    symbols = sorted(set(symbols))
    supported = [s for s in symbols if to_tencent(s)]
    batches = [supported[i:i + 500] for i in range(0, len(supported), 500)]
    found, failed_batches = {}, 0
    with ThreadPoolExecutor(max_workers=max(1, len(batches))) as pool:
        futures = {pool.submit(fetch, batch, round_time): batch for batch in batches}
        for future in as_completed(futures):
            try:
                rows, missing = future.result()
                found.update(rows)
                failed_batches += bool(missing)
                if missing:
                    log.warning("Tencent round=%s batch failed missing=%d examples=%s", round_time.isoformat(), len(missing), missing[:5])
            except Exception:
                failed_batches += 1
                log.exception("Tencent round=%s batch failed", round_time.isoformat())
    log.info("Tencent round=%s expected=%d returned=%d fresh=%d unsupported=%d failed_batches=%d elapsed_seconds=%.2f",
             round_time.isoformat(), len(symbols), len(found), sum(row['status'] == 'valid' for row in found.values()),
             len(symbols) - len(supported), failed_batches, time.monotonic() - started)
    captured = datetime.now(timezone.utc).isoformat()
    round_id = round_time.astimezone(timezone.utc).isoformat()
    for symbol in symbols:
        row = found.get(symbol, {"symbol": symbol, "name": "", "price": None,
              "previous_close": None, "source": "tencent", "source_time": None,
              "captured_at": captured, "status": "unsupported" if not to_tencent(symbol) else "missing"})
        row.update(round_id=round_id, expected=len(symbols), failed_batches=failed_batches)
        yield row
