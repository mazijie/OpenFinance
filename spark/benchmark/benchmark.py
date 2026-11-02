"""Prepare and run isolated Spark benchmarks using Docker Compose."""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import statistics
import subprocess
import time
from pathlib import Path
from urllib.error import URLError
from urllib.request import urlopen

ROOT = Path(__file__).resolve().parents[2]
ARTIFACTS = ROOT / ".cache" / "benchmarks"
JOB = "/opt/openfinance/spark/benchmark/job.py"
VARIANTS = [
    ("baseline", 4, 4, "hash"),
    ("shuffle2", 2, 4, "hash"),
    ("shuffle8", 8, 4, "hash"),
    ("output2", 4, 2, "hash"),
    ("distribution_none", 4, 4, "none"),
]

def safe(value):
    if not value or not value.isascii() or not value[0].isalpha() or not value.replace("_", "").isalnum():
        raise ValueError("IDs must start with a letter and contain only ASCII letters, numbers or underscores")
    return value.lower()

def git(*args):
    return subprocess.run(["git", *args], cwd=ROOT, text=True, capture_output=True, check=True).stdout.strip()

def history(application_id):
    base = f"http://127.0.0.1:18080/api/v1/applications/{application_id}"
    for attempt in range(12):
        try:
            with urlopen(base, timeout=5) as response:
                app = json.load(response)
            if not app.get("attempts") or not app["attempts"][0].get("completed"):
                raise ValueError("History Server has not finished replaying the event log")
            with urlopen(base + "/stages?details=true", timeout=10) as response:
                stages = json.load(response)
            with urlopen(base + "/jobs", timeout=10) as response:
                jobs = json.load(response)
            try:
                with urlopen(base + "/sql", timeout=10) as response:
                    sql = json.load(response)
            except (URLError, OSError, ValueError):
                sql = None
            return {"application": app, "stages": stages, "jobs": jobs, "sql": sql}
        except (URLError, OSError, ValueError):
            if attempt < 11:
                time.sleep(5)
    return None

def submit(action, dataset, kind, run_id, workload, shuffle, partitions, distribution, folder, reference=None):
    folder.mkdir(parents=True, exist_ok=False)
    result = f"/opt/openfinance/.cache/benchmarks/{folder.relative_to(ARTIFACTS).as_posix()}/result.json"
    command = ["docker", "compose", "exec", "-T", "spark-master", "/opt/spark/bin/spark-submit",
        "--master", "spark://spark-master:7077", "--deploy-mode", "client",
        "--name", f"openfinance-{action}-{workload}-{run_id}",
        "--executor-cores", "2", "--executor-memory", "2g", "--total-executor-cores", "2",
        "--driver-memory", "1g", "--properties-file", "/opt/openfinance/spark/conf/iceberg.properties",
        "--conf", f"spark.sql.shuffle.partitions={shuffle}",
        "--conf", "spark.sql.adaptive.enabled=true", "--conf", "spark.dynamicAllocation.enabled=false",
        JOB, action, "--dataset", dataset, "--kind", kind, "--run-id", run_id,
        "--workload", workload, "--partitions", str(partitions), "--distribution", distribution, "--result", result]
    if action == "run":
        command += ["--manifest", f"/opt/openfinance/.cache/benchmarks/input_{dataset}/prepare/result.json"]
    if reference:
        command += ["--reference", reference]
    code_paths = ["spark/benchmark/benchmark.py", "spark/benchmark/job.py", "spark/jobs/daily_metrics.py", "spark/jobs/build_gold_security_daily_metrics.py", "spark/jobs/rebuild_silver_finance.py", "spark/conf/iceberg.properties"]
    source_hashes = {path: hashlib.sha256((ROOT / path).read_bytes()).hexdigest() for path in code_paths}
    start = time.monotonic()
    with (folder / "submit.log").open("w", encoding="utf-8") as log:
        completed = subprocess.run(command, cwd=ROOT, stdout=log, stderr=subprocess.STDOUT)
    result_path = folder / "result.json"
    record = json.loads(result_path.read_text(encoding="utf-8")) if result_path.exists() else {"status": "failed", "error": "Spark did not produce a result"}
    record.update({"returncode": completed.returncode, "wall_seconds": time.monotonic() - start,
                   "command": command, "git_revision": git("rev-parse", "HEAD"),
                   "git_status": git("status", "--short"), "cache_state": "uncontrolled",
                   "history_url": None})
    record["source_sha256"] = source_hashes
    if source_hashes != {path: hashlib.sha256((ROOT / path).read_bytes()).hexdigest() for path in code_paths}:
        record["status"] = "failed"
        record["error"] = "Benchmark source changed during execution"
    if completed.returncode:
        record["status"] = "failed"
    if record.get("targets") and record.get("elapsed_seconds"):
        count = sum(value["rows"] for value in record["targets"].values())
        record["output_rows_per_second"] = count / record["elapsed_seconds"]
    app_id = record.get("application_id")
    if app_id:
        record["history_url"] = f"http://localhost:18080/history/{app_id}/"
        record["event_logs"] = [str(path.relative_to(ROOT)) for path in (ROOT / ".cache" / "spark" / "events").glob(f"*{app_id}*")]
        data = history(app_id)
        if data is not None:
            (folder / "history.json").write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
            stages = data["stages"]
            metrics = ["shuffleReadBytes", "shuffleWriteBytes", "memoryBytesSpilled", "diskBytesSpilled", "jvmGcTime", "numFailedTasks", "numKilledTasks", "executorRunTime"]
            record["metrics"] = {key: sum(stage.get(key, 0) for stage in stages) for key in metrics}
            record["metrics"]["application_duration_ms"] = data["application"]["attempts"][0].get("duration")
        else:
            record["history_error"] = "History Server metrics unavailable"
    result_path.write_text(json.dumps(record, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"{run_id}: {record['status']} ({record['wall_seconds']:.1f}s)", flush=True)
    return record

def report(suite):
    directory = ARTIFACTS / safe(suite)
    rows = []
    for path in sorted(directory.glob("*/result.json")):
        item = json.loads(path.read_text(encoding="utf-8"))
        if item.get("action") != "run":
            continue
        item["artifact"] = path.parent.name
        rows.append(item)
    fields = ["artifact", "status", "workload", "dataset", "variant", "warmup", "application_id", "wall_seconds", "elapsed_seconds", "output_rows_per_second", "history_url", "error"]
    with (directory / "summary.csv").open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    lines = ["# Spark 性能对比", "", "缓存状态未控制；耗时包括作业校验和写入。每组按成功的计量运行汇总。", "", "| 工作负载 | 配置 | 次数 | 提交耗时中位数（秒） | 范围（秒） |", "|---|---|---:|---:|---:|"]
    for workload in ("gold", "silver"):
        for name, *_ in VARIANTS:
            samples = [r["wall_seconds"] for r in rows if r.get("workload") == workload and r.get("variant") == name and not r.get("warmup") and r["status"] == "success"]
            if samples:
                lines.append(f"| {workload} | {name} | {len(samples)} | {statistics.median(samples):.2f} | {min(samples):.2f}–{max(samples):.2f} |")
    failures = [r for r in rows if r["status"] != "success"]
    lines += ["", "| 工作负载 | 配置 | 输出行/秒中位数 | Shuffle 读 MiB 中位数 | Spill MiB 中位数 | 文件数中位数 |", "|---|---|---:|---:|---:|---:|"]
    for workload in ("gold", "silver"):
        for name, *_ in VARIANTS:
            samples = [r for r in rows if r.get("workload") == workload and r.get("variant") == name and not r.get("warmup") and r["status"] == "success"]
            if not samples:
                continue
            def median(values):
                values = [v for v in values if v is not None]
                return f"{statistics.median(values):.2f}" if values else "缺失"
            throughput = median([r.get("output_rows_per_second") for r in samples])
            shuffle = median([r["metrics"]["shuffleReadBytes"] / 1048576 if r.get("metrics") else None for r in samples])
            spill = median([(r["metrics"]["memoryBytesSpilled"] + r["metrics"]["diskBytesSpilled"]) / 1048576 if r.get("metrics") else None for r in samples])
            files = median([sum(t["file_count"] for t in r.get("targets", {}).values()) for r in samples])
            lines.append(f"| {workload} | {name} | {throughput} | {shuffle} | {spill} | {files} |")
    lines += ["", f"失败运行：{len(failures)}。具体应用、配置、文件和历史指标见各运行目录。", "", "输出摘要为快速检查；浮点舍入边界可能产生摘要差异，以逐行比较为准。逐行比较见每次运行的 comparison 字段，浮点允许绝对和相对误差各 1e-9。"]
    groups = {}
    for row in rows:
        if row["status"] != "success" or row.get("warmup"):
            continue
        hashes = sorted((name.split(".")[-1].split("_")[0], data["rows"], data["hash_sum"]) for name, data in row.get("targets", {}).items())
        groups.setdefault((row["dataset"], row["workload"]), []).append((row["artifact"], hashes))
    for group, values in groups.items():
        reference = values[0][1]
        mismatches = [name for name, value in values if value != reference]
        verdict = "样本不足" if len(values) < 2 else "不一致：" + ", ".join(mismatches) if mismatches else "输出摘要一致"
        lines.append(f"- {group[0]} / {group[1]}：{verdict}")
    (directory / "report.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(directory / "report.md")

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="action", required=True)
    prepare = sub.add_parser("prepare")
    prepare.add_argument("--dataset", required=True)
    prepare.add_argument("--kind", choices=["real", "small", "medium"], required=True)
    run = sub.add_parser("run")
    run.add_argument("--dataset", required=True)
    run.add_argument("--suite", required=True)
    run.add_argument("--workload", choices=["gold", "silver", "both"], default="both")
    run.add_argument("--repeats", type=int, default=3)
    run.add_argument("--baseline-only", action="store_true")
    run.add_argument("--variant", action="append", choices=[v[0] for v in VARIANTS], help="Repeat to select specific configurations")
    summary = sub.add_parser("report")
    summary.add_argument("--suite", required=True)
    a = parser.parse_args()
    if a.action == "prepare":
        dataset = safe(a.dataset)
        folder = ARTIFACTS / ("input_" + dataset) / "prepare"
        result = submit("prepare", dataset, a.kind, "prepare", "gold", 4, 4, "hash", folder)
        if result["status"] != "success" or result["returncode"]:
            raise SystemExit(1)
    elif a.action == "run":
        dataset, suite = safe(a.dataset), safe(a.suite)
        if len(suite) > 10:
            raise ValueError("Suite IDs must contain at most 10 characters to keep table names distinct")
        if a.repeats < 1:
            raise ValueError("repeats must be positive")
        prepared = ARTIFACTS / ("input_" + dataset) / "prepare" / "result.json"
        if not prepared.exists() or json.loads(prepared.read_text(encoding="utf-8")).get("status") != "success":
            raise ValueError("A successful dataset preparation is required")
        suite_dir = ARTIFACTS / suite
        suite_dir.mkdir(parents=True, exist_ok=False)
        if a.baseline_only and a.variant:
            raise ValueError("Choose either --baseline-only or --variant")
        variants = [v for v in VARIANTS if v[0] in a.variant] if a.variant else VARIANTS[:1] if a.baseline_only else VARIANTS
        workloads = ["gold", "silver"] if a.workload == "both" else [a.workload]
        failed = False
        for iteration in range(a.repeats + 1):
            ordered = variants[iteration % len(variants):] + variants[:iteration % len(variants)]
            for workload in workloads:
                for name, shuffle, partitions, distribution in ordered:
                    run_id = f"r{suite}_{workload}_{name}_{iteration}"
                    folder = suite_dir / run_id
                    reference = f"r{suite}_{workload}_{variants[0][0]}_0"
                    record = submit("run", dataset, "real", run_id, workload, shuffle, partitions, distribution, folder, None if run_id == reference else reference)
                    record.update({"variant": name, "warmup": iteration == 0})
                    (folder / "result.json").write_text(json.dumps(record, ensure_ascii=False, indent=2), encoding="utf-8")
                    failed |= record["status"] != "success" or record["returncode"] != 0
        report(suite)
        if failed:
            raise SystemExit(1)
    else:
        report(a.suite)

if __name__ == "__main__":
    main()
