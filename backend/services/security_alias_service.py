"""Build A-share aliases from the internal security master."""

from __future__ import annotations

from backend.lakehouse.finance_writer import FinanceWriter
from backend.utils.symbols import SymbolMatcher


def load_security_alias_mapping(writer: FinanceWriter | None = None) -> dict[str, list[str]]:
    """Return symbol/name aliases for configured A-share securities."""

    records = (writer or FinanceWriter()).scan_security_master()
    aliases: dict[str, list[str]] = {}
    for record in records:
        exchange = record.exchange.upper()
        if exchange not in {"SH", "SZ", "BJ"}:
            continue
        symbol = record.symbol.upper()
        values = {symbol}
        if record.name.strip():
            values.add(record.name.strip())
        aliases[symbol] = sorted(values)
    return aliases


def build_security_matcher(
    *,
    base_aliases: dict[str, list[str]] | None = None,
    writer: FinanceWriter | None = None,
) -> SymbolMatcher:
    """Merge configured aliases with names from the A-share security master."""

    merged = {symbol.upper(): list(values) for symbol, values in (base_aliases or {}).items()}
    for symbol, values in load_security_alias_mapping(writer).items():
        merged.setdefault(symbol, [])
        merged[symbol] = sorted(set(merged[symbol]) | set(values))
    return SymbolMatcher(merged)
