"""Read-only Tencent daily latency probe. Writes files, never a finance table."""

from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta
import json
from pathlib import Path
import statistics
from threading import Lock, local
import time
from zoneinfo import ZoneInfo

from backend.providers.daily_bars import TencentDailyBarsProvider

SYMBOLS = ["600519.SH", "000001.SZ", "688981.SH", "300750.SZ",
           "601318.SH", "600036.SH", "920799.BJ", "920002.BJ"]


def latency_summary(seconds):
    values = sorted(seconds)
    if not values:
        return {"median_ms": None, "p95_ms": None, "min_ms": None, "max_ms": None}
    at = .95 * (len(values) - 1)
    low = int(at)
    p95 = values[low] + (values[min(low + 1, len(values) - 1)] - values[low]) * (at - low)
    return {"median_ms": round(statistics.median(values) * 1000, 3),
            "p95_ms": round(p95 * 1000, 3), "min_ms": round(values[0] * 1000, 3),
            "max_ms": round(values[-1] * 1000, 3)}


def run_mode(concurrency, end_date, *, rounds=3, warmups=1):
    state, providers, lock = local(), [], Lock()
    def run_one(symbol):
        if not hasattr(state, "provider"):
            state.provider = TencentDailyBarsProvider()
            with lock:
                providers.append(state.provider)
        events, bars, error = [], [], None
        state.provider.observer = events.append
        tick = time.perf_counter()
        try:
            bars = state.provider.fetch_daily_bars(
                symbols=[symbol], start_date=end_date - timedelta(days=45),
                end_date=end_date, adjustment_type="qfq", limit=5,
            )
            if len(bars) != 5:
                error = f"Expected 5 bars, received {len(bars)}"
        except Exception as exc:
            error = str(exc)
        return {"symbol": symbol, "end_to_end_seconds": time.perf_counter() - tick,
                "network_seconds": sum(e["network_seconds"] for e in events),
                "retries": sum(e["retries"] for e in events), "error": error,
                "bars": [bar.model_dump(mode="json") for bar in bars], "requests": events}
    observations, timings = [], []
    try:
        with ThreadPoolExecutor(max_workers=concurrency) as pool:
            for round_index in range(-warmups, rounds):
                tick = time.perf_counter()
                rows = list(pool.map(run_one, SYMBOLS))
                elapsed = time.perf_counter() - tick
                for row in rows:
                    row.update(round=round_index + 1, warmup=round_index < 0, concurrency=concurrency)
                observations.extend(rows)
                if round_index >= 0:
                    timings.append(elapsed)
    finally:
        for provider in providers:
            provider.close()
    measured = [r for r in observations if not r["warmup"]]
    successful = sum(r["error"] is None for r in measured)
    first_success = sum(r["error"] is None and r["retries"] == 0 for r in measured)
    return {
        "concurrency": concurrency, "sample_count": len(measured),
        "successful": successful, "final_failures": len(measured) - successful,
        "first_attempt_success_rate": first_success / len(measured),
        "retry_count": sum(r["retries"] for r in measured),
        "warmup_failures": sum(r["error"] is not None for r in observations if r["warmup"]),
        "round_seconds": [round(t, 6) for t in timings],
        "requests_per_second": successful / sum(timings),
        "network": latency_summary([r["network_seconds"] for r in measured]),
        "end_to_end": latency_summary([r["end_to_end_seconds"] for r in measured]),
    }, observations


def run(output: Path, end_date: date | None = None):
    output.mkdir(parents=True, exist_ok=False)
    now = datetime.now(ZoneInfo("Asia/Shanghai"))
    discovery = []
    if end_date is None:
        # Ask the source for actual dates rather than treating weekdays as trading days.
        candidate = now.date() if now.hour >= 16 else now.date() - timedelta(days=1)
        with TencentDailyBarsProvider(observer=discovery.append) as provider:
            bars = provider.fetch_daily_bars(symbols=[SYMBOLS[0]], start_date=candidate - timedelta(days=45),
                                             end_date=candidate, limit=5)
        if not bars:
            raise RuntimeError("Could not discover a completed trading date; provide --end-date")
        end_date = max(bar.trading_date for bar in bars)
    results, observations = [], []
    for concurrency in (1, 8):
        result, rows = run_mode(concurrency, end_date)
        results.append(result)
        observations.extend(rows)
    summary = {"tested_at": now.isoformat(), "end_date": str(end_date), "symbols": SYMBOLS,
               "bars_per_symbol": 5, "rounds_per_mode": 3, "warmups_per_mode": 1,
               "logical_requests": len(observations),
               "http_attempts": sum(len(e["attempts"]) for r in observations for e in r["requests"]),
               "discovery_http_attempts": sum(len(e["attempts"]) for e in discovery),
               "results": results,
               "batch_speedup": sum(results[0]["round_seconds"]) / sum(results[1]["round_seconds"]),
               "all_samples_valid": all(r["error"] is None for r in observations)}
    (output / "raw_responses.json").write_text(json.dumps({"discovery": discovery, "observations": observations}, ensure_ascii=False, indent=2), encoding="utf-8")
    (output / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    lines = ["# 腾讯日线小样本时延报告", "", f"测试时间：{now.isoformat()}；行情截止：{end_date}。",
             "", "固定 8 只股票，每次 5 根前复权日线。每种并发预热一轮、正式测试三轮。", "",
             "| 并发 | 请求数 | 首次成功率 | 重试数 | 最终失败 | 网络中位数 / P95（ms） | 含解析中位数 / P95（ms） | 各轮耗时（s） | 请求/秒 |",
             "|---|---|---|---|---|---|---|---|---|"]
    for r in results:
        n, e = r["network"], r["end_to_end"]
        lines.append(f"| {r['concurrency']} | {r['sample_count']} | {r['first_attempt_success_rate']:.1%} | {r['retry_count']} | {r['final_failures']} | {n['median_ms']} / {n['p95_ms']} | {e['median_ms']} / {e['p95_ms']} | {', '.join(f'{t:.3f}' for t in r['round_seconds'])} | {r['requests_per_second']:.2f} |")
    lines.extend(["", f"按整轮耗时计算，8 并发相对串行加速 {summary['batch_speedup']:.2f} 倍。",
                  f"含预热共 {summary['logical_requests']} 次逻辑查询，{summary['http_attempts']} 次 HTTP 尝试；日期探测另计 {summary['discovery_http_attempts']} 次。",
                  f"全部样本有效：{summary['all_samples_valid']}。", "",
                  "网络时延包含各次 HTTP 尝试读取响应的时间；含解析时延还包含重试等待、校验和数据转换，不含排队及写文件。",
                  "预热不计入时延统计。结果只反映当前机器和网络的小样本表现，不能证明长期稳定性或无限制并发。",
                  "本测试没有启动采集任务、连接业务数据库或写入行情表。", ""])
    (output / "report.md").write_text("\n".join(lines), encoding="utf-8")
    return summary


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=Path(".cache/tencent-daily") / datetime.now().strftime("%Y%m%d-%H%M%S"))
    parser.add_argument("--end-date", type=date.fromisoformat)
    args = parser.parse_args()
    summary = run(args.output, args.end_date)
    print(json.dumps({"output": str(args.output.resolve()), **summary}, ensure_ascii=False, indent=2))
    if not summary["all_samples_valid"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
