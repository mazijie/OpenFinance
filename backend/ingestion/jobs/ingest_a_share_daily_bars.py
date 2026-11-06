"""CLI job to ingest A-share daily bars into Bronze."""

from __future__ import annotations

import argparse
import json
import signal
from contextlib import ExitStack, contextmanager
from dataclasses import asdict, dataclass, field
from datetime import UTC, date, datetime, timedelta
from threading import current_thread, main_thread
import time as time_module
from typing import Callable, Iterator

from backend.config import settings
from backend.ingestion.writers.bronze_writer import BronzeWriter
from backend.logging_utils import configure_logging, get_logger
from backend.providers.finance import BaseFinanceProvider, BaoStockFinanceProvider, FinanceProviderError
from backend.providers.daily_bars import TencentDailyBarsProvider, create_daily_provider
from backend.providers.tencent_securities import TencentSecurityProvider
from backend.lakehouse.job_lease import JobLeaseLost

logger = get_logger(__name__)


@dataclass(slots=True)
class IngestAshareDailyBarsResult:
    provider: str
    object_path: str
    symbol_count: int
    daily_bar_count: int
    start_date: str
    end_date: str
    adjustment_type: str
    processed_symbol_count: int
    failed_symbols: list[str]
    no_data_symbols: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


def ingest_a_share_daily_bars(
    *,
    symbols: list[str],
    start_date: date,
    end_date: date,
    adjustment_type: str = "none",
    idempotency_key: str | None = None,
    include_security_master: bool = True,
    include_trading_calendar: bool = True,
    daily_provider_name: str | None = None,
    provider: BaseFinanceProvider | None = None,
    metadata_provider: BaseFinanceProvider | None = None,
    writer: BronzeWriter | None = None,
    check_lease: Callable[[], None] | None = None,
) -> IngestAshareDailyBarsResult:
    """Collect Tencent daily bars and metadata with BaoStock calendars."""

    normalized_symbols = list(dict.fromkeys(symbol.strip().upper() for symbol in symbols if symbol.strip()))
    if not normalized_symbols:
        raise ValueError("symbols cannot be empty")
    if start_date > end_date:
        raise ValueError("start_date must not be after end_date")
    if adjustment_type != "none":
        raise ValueError("Only unadjusted daily bars are supported")
    with ExitStack() as resources:
        daily = provider or create_daily_provider(daily_provider_name, check_lease=check_lease)
        if provider is None and hasattr(daily, "close"):
            resources.callback(daily.close)
        provider_name = getattr(daily, "name", daily_provider_name or settings.FINANCE_DAILY_PROVIDER)
        metadata = metadata_provider
        if (include_security_master or include_trading_calendar) and metadata is None:
            metadata = daily if not getattr(daily, "daily_only", False) else TencentSecurityProvider()
            if hasattr(metadata, "close") and metadata is not daily:
                resources.callback(metadata.close)
        calendar_provider = (BaoStockFinanceProvider() if include_trading_calendar and isinstance(metadata, TencentSecurityProvider) else metadata)
        resolved_writer = writer or BronzeWriter()
        security_master, daily_bars = [], []
        processed_symbols = 0
        failed_symbols, no_data_symbols = [], []
        # Tencent retries individual HTTP requests internally; do not multiply those retries.
        retries = 0 if getattr(daily, "handles_retries", False) else settings.BAOSTOCK_MAX_RETRIES
        for index, symbol in enumerate(normalized_symbols, start=1):
            if check_lease:
                check_lease()
            for attempt in range(retries + 1):
                try:
                    if check_lease:
                        check_lease()
                    timeout = 0 if getattr(daily, "handles_retries", False) else settings.BAOSTOCK_SYMBOL_TIMEOUT_SECONDS
                    with _symbol_timeout(timeout):
                        if include_security_master:
                            master = (_fetch_security_master_with_retry(metadata, symbol, check_lease)
                                      if getattr(daily, "handles_retries", False)
                                      else metadata.fetch_security_master(symbols=[symbol]))
                        else:
                            master = []
                        bars = daily.fetch_daily_bars(symbols=[symbol], start_date=start_date,
                                                     end_date=end_date, adjustment_type=adjustment_type)
                    security_master.extend(master)
                    daily_bars.extend(bars)
                    if not bars:
                        no_data_symbols.append(symbol)
                    processed_symbols += 1
                    logger.info("A-share symbol ingestion completed", event="a_share_symbol_ingestion_completed",
                                extra_fields={"symbol": symbol, "provider": provider_name, "symbol_index": index,
                                              "symbol_count": len(normalized_symbols), "attempt": attempt + 1})
                    break
                except JobLeaseLost:
                    raise
                except Exception as exc:
                    logger.warning("A-share symbol ingestion failed", event="a_share_symbol_ingestion_failed",
                                   extra_fields={"symbol": symbol, "provider": provider_name,
                                                 "attempt": attempt + 1, "max_attempts": retries + 1, "error": str(exc)})
                    if attempt == retries:
                        failed_symbols.append(symbol)
                    else:
                        time_module.sleep(settings.BAOSTOCK_RETRY_BACKOFF_SECONDS * (attempt + 1))
        if processed_symbols == 0:
            raise FinanceProviderError("No A-share symbols completed successfully")
        exchanges = sorted({record.exchange for record in security_master + daily_bars}) or sorted({
            TencentDailyBarsProvider.normalize_symbol(symbol)[0].rsplit(".", 1)[-1]
            for symbol in normalized_symbols
        })
        trading_calendar = []
        if include_trading_calendar:
            for exchange in exchanges:
                if check_lease:
                    check_lease()
                trading_calendar.extend(calendar_provider.fetch_trading_calendar(exchange=exchange, start_date=start_date, end_date=end_date))
        raw_response = {
            "security_master": [record.model_dump(mode="json") for record in security_master],
            "trading_calendar": [record.model_dump(mode="json") for record in trading_calendar],
            "daily_bars": [record.model_dump(mode="json") for record in daily_bars],
        }
        if check_lease:
            check_lease()
        ingested_at = datetime.now(UTC)
        object_path = resolved_writer.write_finance_raw(
            provider=provider_name, raw_response=raw_response, object_key=idempotency_key,
            request_params={"symbols": normalized_symbols, "start_date": start_date.isoformat(),
                            "end_date": end_date.isoformat(), "adjustment_type": adjustment_type,
                            "daily_provider_name": provider_name},
            ingested_at=ingested_at, source_version=ingested_at.isoformat(),
        )
        result = IngestAshareDailyBarsResult(
            provider=provider_name, object_path=object_path, symbol_count=len(normalized_symbols),
            daily_bar_count=len(daily_bars), start_date=start_date.isoformat(), end_date=end_date.isoformat(),
            adjustment_type=adjustment_type, processed_symbol_count=processed_symbols,
            failed_symbols=failed_symbols, no_data_symbols=no_data_symbols,
        )
        logger.info("A-share daily bars ingestion completed", event="ingest_a_share_daily_bars_completed",
                    extra_fields=result.to_dict())
    return result


def _fetch_security_master_with_retry(provider: BaseFinanceProvider, symbol: str,
                                      check_lease: Callable[[], None] | None) -> list:
    """Retry metadata independently of the daily provider's retry policy."""
    for attempt in range(settings.TENCENT_MAX_RETRIES + 1):
        if check_lease:
            check_lease()
        try:
            return provider.fetch_security_master(symbols=[symbol])
        except JobLeaseLost:
            raise
        except Exception:
            if attempt >= settings.TENCENT_MAX_RETRIES:
                raise
            logger.warning("A-share security metadata request failed; retrying",
                           extra_fields={"symbol": symbol, "attempt": attempt + 1})
            time_module.sleep(settings.TENCENT_RETRY_BACKOFF_SECONDS * (attempt + 1))
    raise RuntimeError("Unreachable metadata retry state")


@contextmanager
def _symbol_timeout(seconds: int) -> Iterator[None]:
    """Bound one symbol request even if the provider client blocks."""

    if seconds <= 0 or current_thread() is not main_thread() or not hasattr(signal, "SIGALRM"):
        yield
        return

    def handle_timeout(signum, frame):  # noqa: ANN001
        raise TimeoutError(f"symbol request exceeded {seconds} seconds")

    previous_handler = signal.signal(signal.SIGALRM, handle_timeout)
    signal.alarm(seconds)
    try:
        yield
    finally:
        signal.alarm(0)
        signal.signal(signal.SIGALRM, previous_handler)


def build_default_window(end_date: date | None = None, lookback_days: int = 365 * 3) -> tuple[date, date]:
    resolved_end = end_date or datetime.now(UTC).date()
    return resolved_end - timedelta(days=lookback_days), resolved_end


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Ingest A-share daily bars into Bronze")
    parser.add_argument("--symbols", nargs="+", required=True, help="One or many A-share symbols, e.g. 600519.SH")
    parser.add_argument("--start-date", help="Start date in YYYY-MM-DD format")
    parser.add_argument("--end-date", help="End date in YYYY-MM-DD format")
    parser.add_argument("--daily-provider", choices=("tencent",), default=settings.FINANCE_DAILY_PROVIDER)
    parser.add_argument("--adjustment-type", default=settings.FINANCE_DEFAULT_ADJUSTMENT_TYPE)
    parser.add_argument(
        "--use-default-window",
        action="store_true",
        help="Use default three-year backfill window when start/end date are not provided",
    )
    return parser


def main() -> None:
    configure_logging()
    args = build_parser().parse_args()

    start_date = date.fromisoformat(args.start_date) if args.start_date else None
    end_date = date.fromisoformat(args.end_date) if args.end_date else None
    if args.use_default_window and start_date is None and end_date is None:
        start_date, end_date = build_default_window()
    if start_date is None or end_date is None:
        raise SystemExit("start-date and end-date are required unless --use-default-window is set")

    result = ingest_a_share_daily_bars(
        symbols=list(args.symbols),
        start_date=start_date,
        end_date=end_date,
        adjustment_type=args.adjustment_type,
        daily_provider_name=args.daily_provider,
    )
    print(json.dumps(result.to_dict(), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
