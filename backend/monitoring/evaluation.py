"""Evaluate persisted shadow recommendations against subsequent Gold closes."""

from __future__ import annotations

import argparse
import json
from dataclasses import asdict, dataclass
from datetime import date, timedelta
from typing import Any, Iterable

from backend.monitoring.store import MonitorStore
from backend.repositories.gold_metrics_repository import GoldMetricsRepository
from backend.schemas.monitor import HorizonRecommendation, MonitorReport


@dataclass(slots=True)
class HorizonMetrics:
    horizon: str
    evaluable_count: int = 0
    directional_count: int = 0
    directional_hit_rate: float | None = None
    mean_net_return: float | None = None
    baseline_hit_rate: float | None = None
    lift: float | None = None


def _correct(recommendation: HorizonRecommendation, realized_return: float) -> bool | None:
    if recommendation.stance == "neutral":
        return None
    if recommendation.stance == "bullish":
        return realized_return > 0
    return realized_return < 0


def evaluate_reports(
    reports: Iterable[MonitorReport],
    repository: GoldMetricsRepository,
    *,
    round_trip_cost: float = 0.002,
) -> dict[str, Any]:
    """Calculate 5d/20d hit rates and net long-side returns."""

    materialized = list(reports)
    grouped: dict[int, list[tuple[MonitorReport, HorizonRecommendation, float, bool | None]]] = {
        5: [],
        20: [],
    }
    missing = 0
    for report in materialized:
        recommendations = {
            5: report.recommendation_5d,
            20: report.recommendation_20d,
        }
        if not any(recommendations.values()):
            continue
        max_horizon = 20
        rows = repository.scan_window(
            symbol=report.symbol,
            start_date=report.trading_date,
            end_date=report.trading_date + timedelta(days=max_horizon * 2 + 10),
            limit=max_horizon + 1,
        )
        if not rows or rows[0].trading_date != report.trading_date:
            missing += 1
            continue
        start_close = rows[0].close
        for horizon, recommendation in recommendations.items():
            if recommendation is None or start_close <= 0:
                continue
            if len(rows) <= horizon:
                missing += 1
                continue
            end_close = rows[horizon].close
            realized = end_close / start_close - 1.0
            correct = _correct(recommendation, realized)
            grouped[horizon].append((report, recommendation, realized, correct))

    results: dict[str, Any] = {"reports_with_recommendations": 0, "missing_outcomes": missing}
    for horizon, values in grouped.items():
        metrics = HorizonMetrics(horizon=f"{horizon}d", evaluable_count=len(values))
        directional = [item for item in values if item[3] is not None]
        metrics.directional_count = len(directional)
        if directional:
            metrics.directional_hit_rate = sum(bool(item[3]) for item in directional) / len(directional)
            net_returns = [
                item[2] - round_trip_cost if item[1].stance == "bullish" else 0.0
                for item in directional
                if item[1].actionable
            ]
            if net_returns:
                metrics.mean_net_return = sum(net_returns) / len(net_returns)
            if horizon == 20:
                baseline_correct = []
                for report, _, realized, _ in directional:
                    rows = repository.scan_window(
                        symbol=report.symbol,
                        start_date=report.trading_date,
                        end_date=report.trading_date,
                        limit=1,
                    )
                    if rows and rows[0].return_20d is not None:
                        baseline_correct.append((rows[0].return_20d > 0) == (realized > 0))
                if baseline_correct:
                    metrics.baseline_hit_rate = sum(baseline_correct) / len(baseline_correct)
                    metrics.lift = (metrics.directional_hit_rate or 0.0) - metrics.baseline_hit_rate
        results[metrics.horizon] = asdict(metrics)
    results["reports_with_recommendations"] = sum(
        1 for report in materialized if report.recommendation_5d or report.recommendation_20d
    )
    return results


def passes_gate(results: dict[str, Any]) -> bool:
    hit_5 = results.get("5d", {}).get("directional_hit_rate")
    hit_20 = results.get("20d", {}).get("directional_hit_rate")
    net_20 = results.get("20d", {}).get("mean_net_return")
    lift_20 = results.get("20d", {}).get("lift")
    return bool(
        hit_5 is not None
        and hit_20 is not None
        and net_20 is not None
        and lift_20 is not None
        and hit_5 >= 0.52
        and hit_20 >= 0.55
        and net_20 > 0
        and lift_20 >= 0.05
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Evaluate shadow recommendations after outcomes mature.")
    parser.add_argument("--since", help="Inclusive trading-date lower bound, YYYY-MM-DD.")
    parser.add_argument("--json", action="store_true", help="Emit one JSON document.")
    args = parser.parse_args()
    since = date.fromisoformat(args.since) if args.since else date.today() - timedelta(days=365)
    store = MonitorStore()
    reports, _ = store.list_reports(limit=100, offset=0)
    filtered = [report for report in reports if report.trading_date >= since]
    # list_reports is bounded by the public page size; fetch all pages for a full shadow audit.
    offset = len(reports)
    while reports:
        page, _ = store.list_reports(limit=100, offset=offset)
        filtered.extend(report for report in page if report.trading_date >= since)
        if len(page) < 100:
            break
        offset += 100
    results = evaluate_reports(filtered, GoldMetricsRepository())
    results["gate_passed"] = passes_gate(results)
    if args.json:
        print(json.dumps(results, ensure_ascii=False, indent=2))
    else:
        print(json.dumps(results, ensure_ascii=False, indent=2))
    if not results["gate_passed"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
