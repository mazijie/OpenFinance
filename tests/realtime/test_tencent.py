from datetime import datetime, timezone

import requests
from threading import Barrier
from realtime.tencent import CHINA, collect_round, fetch_batch, parse_response, to_tencent


def test_symbol_mapping_and_response_parsing():
    assert to_tencent("600519.SH") == "sh600519"
    assert to_tencent("000001.SZ") == "sz000001"
    assert to_tencent("830799.BJ") == "bj830799"
    assert to_tencent("830799.XX") is None
    fields = [""] * 31
    fields[1:5] = ["贵州茅台", "600519", "10.2", "10"]
    fields[30] = "20261102093000"
    raw = ('v_sh600519="' + '~'.join(fields) + '";').encode("gb18030")
    moment = datetime(2026, 11, 2, 9, 30, 1, tzinfo=CHINA)
    rows = parse_response(raw, ["600519.SH"], moment, moment.astimezone(timezone.utc))
    assert rows["600519.SH"]["price"] == 10.2
    assert rows["600519.SH"]["status"] == "valid"
    assert rows["600519.SH"]["source_time"].startswith("2026-11-02T01:30:00")
    fields[3] = "nan"
    assert parse_response(('v_sh600519="' + '~'.join(fields) + '";').encode("gb18030"), ["600519.SH"], moment, moment) == {}


def test_optional_quote_metrics_have_explicit_units_and_invalid_values_are_missing():
    fields = [""] * 46
    fields[1:5] = ["测试股票", "600519", "10.2", "10"]
    fields[30] = "20261102093000"
    fields[36], fields[39], fields[45] = "12345", "-5.2", "180.5"
    fields[5], fields[33], fields[34] = "9.8", "10.8", "9.5"
    moment = datetime(2026, 11, 2, 9, 30, 1, tzinfo=CHINA)
    def parse():
        raw = ('v_sh600519="' + '~'.join(fields) + '";').encode("gb18030")
        return parse_response(raw, ["600519.SH"], moment, moment)["600519.SH"]
    row = parse()
    assert row["volume_lots"] == 12345
    assert row["market_cap_cny"] == 18050000000
    assert row["pe_ratio"] == -5.2
    assert (row["day_open"], row["day_high"], row["day_low"]) == (9.8, 10.8, 9.5)
    fields[36], fields[39], fields[45] = "-1", "nan", "0"
    row = parse()
    assert row["volume_lots"] is row["pe_ratio"] is row["market_cap_cny"] is None


def test_retry_is_bounded_and_round_contains_unsupported():
    delays, calls = [], []
    def fail(*args, **kwargs):
        calls.append(args)
        raise requests.Timeout("test")
    moment = datetime(2026, 11, 2, 9, 30, tzinfo=CHINA)
    found, missing = fetch_batch(["600519.SH"], moment, get=fail, sleep=delays.append)
    assert found == {} and missing == ["600519.SH"]
    assert len(calls) == 4 and delays == [2, 4, 8]
    rows = list(collect_round(["600519.SH", "830799.XX"], moment, fetch=lambda batch, t: ({}, batch)))
    assert {row["status"] for row in rows} == {"missing", "unsupported"}
    assert all(row["expected"] == 2 and row["failed_batches"] == 1 for row in rows)


def test_round_runs_every_batch_concurrently():
    barrier = Barrier(3, timeout=5)
    sizes = []
    def fetch(batch, moment):
        sizes.append(len(batch))
        barrier.wait()
        return {}, batch
    symbols = [f"{number:06d}.SZ" for number in range(1001)]
    rows = list(collect_round(symbols, datetime(2026, 11, 2, 9, 30, tzinfo=CHINA), fetch=fetch))
    assert sorted(sizes) == [1, 500, 500]
    assert len(rows) == 1001 and all(row["failed_batches"] == 3 for row in rows)


def test_schedule_only_starts_once_at_minute_boundary():
    from realtime.collector import scheduled_minute
    at_start = datetime(2026, 11, 2, 9, 30, tzinfo=CHINA)
    assert scheduled_minute(at_start, None) == at_start
    assert scheduled_minute(at_start, at_start) is None
    assert scheduled_minute(at_start.replace(second=15), None) is None
