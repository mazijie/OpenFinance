from __future__ import annotations

import importlib.util
import json
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[2] / "spark" / "benchmark" / "benchmark.py"
spec = importlib.util.spec_from_file_location("benchmark_cli", SCRIPT)
benchmark = importlib.util.module_from_spec(spec)
spec.loader.exec_module(benchmark)


def test_identifier_rejects_unsafe_values():
    assert benchmark.safe("Run_01") == "run_01"
    for value in ("", "../data", "1run", "with space", "中文"):
        with pytest.raises(ValueError):
            benchmark.safe(value)


def test_report_excludes_failures_and_warmups(tmp_path, monkeypatch):
    monkeypatch.setattr(benchmark, "ARTIFACTS", tmp_path)
    suite = tmp_path / "suite"
    for name, status, warmup, elapsed in [
        ("warmup", "success", True, 99),
        ("one", "success", False, 10),
        ("two", "success", False, 20),
        ("failed", "failed", False, 1),
    ]:
        target = suite / name
        target.mkdir(parents=True)
        (target / "result.json").write_text(json.dumps({
            "action": "run", "status": status, "warmup": warmup,
            "dataset": "input", "workload": "gold", "variant": "baseline",
            "wall_seconds": elapsed, "targets": {
                "openfinance.bench_input.gold_run": {"rows": 4, "hash_sum": "42", "file_count": 1}
            },
        }), encoding="utf-8")
    benchmark.report("suite")
    report = (suite / "report.md").read_text(encoding="utf-8")
    assert "| gold | baseline | 2 | 15.00 | 10.00–20.00 |" in report
    assert "失败运行：1" in report
    assert "输出摘要一致" in report
