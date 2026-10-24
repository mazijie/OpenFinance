"""Symbol extraction helpers."""

from __future__ import annotations

import re
from collections import defaultdict

TICKER_PATTERN = re.compile(r"\b[A-Z]{1,5}(?:\.[A-Z])?\b")


class SymbolMatcher:
    """Resolve symbols from configured aliases and inline ticker mentions."""

    def __init__(self, alias_mapping: dict[str, list[str]]) -> None:
        self.alias_mapping = {symbol.upper(): aliases for symbol, aliases in alias_mapping.items()}
        self._reverse_lookup: dict[str, set[str]] = defaultdict(set)
        for symbol, aliases in self.alias_mapping.items():
            self._reverse_lookup[symbol.casefold()].add(symbol)
            for alias in aliases:
                self._reverse_lookup[alias.casefold()].add(symbol)

    def extract(self, *texts: str | None) -> tuple[list[str], list[str]]:
        """Extract matched symbols and matched entities from text."""

        haystack = " ".join(text for text in texts if text).strip()
        if not haystack:
            return [], []

        matched_symbols: set[str] = set()
        matched_entities: set[str] = set()
        lowered = haystack.casefold()

        for alias, symbols in self._reverse_lookup.items():
            if alias and alias in lowered:
                matched_symbols.update(symbols)
                matched_entities.add(alias)

        for ticker in TICKER_PATTERN.findall(haystack.upper()):
            if ticker in self.alias_mapping:
                matched_symbols.add(ticker)
                matched_entities.add(ticker)

        return sorted(matched_symbols), sorted(matched_entities)
