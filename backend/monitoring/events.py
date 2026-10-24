"""Deterministic event detection for the monitoring workflow."""

from __future__ import annotations

import hashlib
from datetime import UTC, date, datetime

from backend.schemas.finance import GoldMetricRecord
from backend.schemas.monitor import MonitorEvent, WatchlistThresholds


def _event_id(symbol: str, event_type: str, trading_date: date, detail: str) -> str:
    return hashlib.sha256(f"{symbol}|{event_type}|{trading_date}|{detail}".encode("utf-8")).hexdigest()


def detect_market_events(
    *,
    symbol: str,
    rows: list[GoldMetricRecord],
    thresholds: WatchlistThresholds,
    observed_at: datetime,
) -> list[MonitorEvent]:
    if not rows:
        return []
    latest = rows[-1]
    events: list[MonitorEvent] = []

    if latest.daily_return_1d is not None and abs(latest.daily_return_1d) >= thresholds.price_move:
        detail = f"{latest.daily_return_1d:+.4f}"
        events.append(
            MonitorEvent(
                event_id=_event_id(symbol, "price_move", latest.trading_date, detail),
                event_type="price_move",
                severity="high" if abs(latest.daily_return_1d) >= thresholds.price_move * 1.5 else "medium",
                symbol=symbol,
                trading_date=latest.trading_date,
                observed_at=observed_at.astimezone(UTC),
                metric_value=latest.daily_return_1d,
                threshold=thresholds.price_move,
                description=f"单日收益 {latest.daily_return_1d:.2%}，阈值 {thresholds.price_move:.2%}",
                dedupe_key=f"{symbol}:price_move:{latest.trading_date}",
            )
        )

    if latest.volume_ma_5d and latest.volume_ma_5d > 0:
        ratio = latest.volume / latest.volume_ma_5d
        if ratio >= thresholds.volume_ratio:
            events.append(
                MonitorEvent(
                    event_id=_event_id(symbol, "volume_surge", latest.trading_date, f"{ratio:.4f}"),
                    event_type="volume_surge",
                    severity="high" if ratio >= thresholds.volume_ratio * 1.5 else "medium",
                    symbol=symbol,
                    trading_date=latest.trading_date,
                    observed_at=observed_at.astimezone(UTC),
                    metric_value=ratio,
                    threshold=thresholds.volume_ratio,
                    description=f"成交量为 5 日均量的 {ratio:.2f} 倍",
                    dedupe_key=f"{symbol}:volume_surge:{latest.trading_date}",
                )
            )

    if len(rows) >= 2:
        previous = rows[-2]
        if (
            previous.ma_5d is not None
            and previous.ma_20d is not None
            and latest.ma_5d is not None
            and latest.ma_20d is not None
        ):
            crossed_up = previous.ma_5d <= previous.ma_20d and latest.ma_5d > latest.ma_20d
            crossed_down = previous.ma_5d >= previous.ma_20d and latest.ma_5d < latest.ma_20d
            if crossed_up or crossed_down:
                direction = "向上" if crossed_up else "向下"
                events.append(
                    MonitorEvent(
                        event_id=_event_id(symbol, "ma_cross", latest.trading_date, direction),
                        event_type="ma_cross",
                        severity="medium",
                        symbol=symbol,
                        trading_date=latest.trading_date,
                        observed_at=observed_at.astimezone(UTC),
                        metric_value=latest.ma_5d - latest.ma_20d,
                        threshold=0.0,
                        description=f"MA5/MA20 出现{direction}交叉",
                        dedupe_key=f"{symbol}:ma_cross:{latest.trading_date}:{direction}",
                    )
                )
    return events


def detect_news_event(
    *,
    symbol: str,
    article_count: int,
    trading_date: date,
    observed_at: datetime,
) -> MonitorEvent | None:
    if article_count <= 0:
        return None
    return MonitorEvent(
        event_id=_event_id(symbol, "news", trading_date, str(article_count)),
        event_type="news",
        severity="medium",
        symbol=symbol,
        trading_date=trading_date,
        observed_at=observed_at.astimezone(UTC),
        metric_value=float(article_count),
        threshold=None,
        description=f"监测窗口内检索到 {article_count} 篇相关新闻",
        dedupe_key=f"{symbol}:news:{trading_date}",
    )


def detect_annual_report_event(
    *,
    symbol: str,
    report_count: int,
    trading_date: date,
    observed_at: datetime,
) -> MonitorEvent | None:
    if report_count <= 0:
        return None
    return MonitorEvent(
        event_id=_event_id(symbol, "annual_report", trading_date, str(report_count)),
        event_type="annual_report",
        severity="medium",
        symbol=symbol,
        trading_date=trading_date,
        observed_at=observed_at.astimezone(UTC),
        metric_value=float(report_count),
        threshold=None,
        description=f"监测窗口内检索到 {report_count} 条年报证据",
        dedupe_key=f"{symbol}:annual_report:{trading_date}",
    )
