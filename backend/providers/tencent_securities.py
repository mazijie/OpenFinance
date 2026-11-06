"""Tencent A-share directory and basic security metadata.

The ranking endpoint is a current snapshot, not an exhaustive delisted
security directory. Listing and delisting dates are not inferred from quotes.
"""
from __future__ import annotations

from datetime import UTC, date, datetime
from hashlib import sha256
import json
import re
from zoneinfo import ZoneInfo

import httpx

from backend.providers.finance import AShareUniverseResult, FinanceProviderError
from backend.schemas.finance import SecurityMasterRecord

RANK_URL = "https://proxy.finance.qq.com/cgi/cgi-bin/rank/hs/getBoardRankList"
QUOTE_URL = "https://qt.gtimg.cn/q="
PAGE_SIZE = 200


def _ordinary_a_share(code: str) -> bool:
    return bool(re.fullmatch(r"(?:sh6\d{5}|sz(?:0\d{5}|30\d{4})|bj(?:4\d{5}|8\d{5}|92\d{4}))", code.lower()))


class TencentSecurityProvider:
    """Current snapshot from Tencent; historical universe completeness is unknown."""

    historical_universe_complete = False

    def __init__(self, client=None):
        self.client = client or httpx.Client(timeout=30.0)
        self._owns_client = client is None

    def close(self):
        if self._owns_client:
            self.client.close()

    def _rank(self) -> list[dict]:
        entries, seen = [], set()
        total = None
        for offset in range(0, 20000, PAGE_SIZE):
            response = self.client.get(RANK_URL, params={"_appver": "11.17.0", "board_code": "aStock",
                "sort_type": "price", "direct": "down", "offset": str(offset), "count": str(PAGE_SIZE)})
            response.raise_for_status()
            body = response.json()
            if body.get("code") != 0 or not isinstance(body.get("data"), dict):
                raise FinanceProviderError("Tencent security directory returned an invalid response")
            data = body["data"]
            page, page_total = data.get("rank_list"), data.get("total")
            if not isinstance(page, list) or not isinstance(page_total, int) or page_total <= 0:
                raise FinanceProviderError("Tencent security directory is empty or incomplete")
            if total is None:
                total = page_total
            elif total != page_total:
                raise FinanceProviderError("Tencent security directory changed during pagination")
            expected = min(PAGE_SIZE, total - offset)
            if len(page) != expected or data.get("offset") != offset:
                raise FinanceProviderError("Tencent security directory page is incomplete")
            for entry in page:
                code = str(entry.get("code", "")).lower() if isinstance(entry, dict) else ""
                if not code or code in seen:
                    raise FinanceProviderError("Tencent security directory contains missing or duplicate codes")
                seen.add(code)
                if _ordinary_a_share(code) and str(entry.get("stock_type", "")).startswith("GP"):
                    entries.append(entry)
            if offset + len(page) >= total:
                if not entries:
                    raise FinanceProviderError("Tencent security directory has no A shares")
                return entries
        raise FinanceProviderError("Tencent security directory exceeded the pagination limit")

    def fetch_a_share_symbols_for_day(self, *, day: date) -> list[str]:
        # Tencent's rank is a current snapshot; it cannot reconstruct past universes.
        return sorted(f"{e['code'][2:]}.{e['code'][:2].upper()}" for e in self._rank())

    def list_a_share_symbols(self, *, adjustment_type="none", ignore_coverage=False, **_):
        today = datetime.now(ZoneInfo("Asia/Shanghai")).date()
        return AShareUniverseResult("available", today, self.fetch_a_share_symbols_for_day(day=today))

    def fetch_security_master(self, *, symbols: list[str] | None = None) -> list[SecurityMasterRecord]:
        if not symbols:
            raise FinanceProviderError("Tencent metadata lookup requires explicit symbols")
        codes = []
        for symbol in symbols:
            parts = symbol.upper().split(".")
            if len(parts) != 2 or not _ordinary_a_share(parts[1].lower() + parts[0]):
                raise FinanceProviderError(f"Unsupported A-share symbol: {symbol}")
            codes.append(parts[1].lower() + parts[0])
        response = self.client.get(QUOTE_URL + ",".join(codes))
        response.raise_for_status()
        try:
            text = response.content.decode("gb18030")
        except UnicodeError as exc:
            raise FinanceProviderError("Tencent security metadata encoding is invalid") from exc
        matches = dict(re.findall(r'v_([a-z]{2}\d{6})="([^";]*)"', text))
        captured = datetime.now(UTC)
        records = []
        for code in codes:
            fields = matches.get(code, "").split("~")
            if len(fields) < 3 or not fields[1] or fields[2] != code[2:]:
                raise FinanceProviderError(f"Tencent security metadata unavailable for {code}")
            symbol = f"{code[2:]}.{code[:2].upper()}"
            records.append(SecurityMasterRecord(
                security_id=symbol, symbol=symbol, exchange=code[:2].upper(), security_type="stock",
                name=fields[1], list_date=None, delist_date=None, status="unknown", provider="tencent",
                source_record_id=code, source_version=captured.isoformat(),
                payload_hash=sha256(json.dumps(fields, ensure_ascii=False).encode()).hexdigest(),
                raw_object_path=QUOTE_URL + code, event_time=captured, available_at=captured, ingested_at=captured,
            ))
        return records
