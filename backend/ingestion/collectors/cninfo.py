"""CNInfo annual-report announcement and PDF collector."""

from __future__ import annotations

import re
from datetime import UTC, datetime
from typing import Any

import httpx

from backend.config import settings
from backend.schemas.annual_report import AnnualReportAnnouncement


class CNInfoCollectorError(Exception):
    """Raised when CNInfo returns an unusable response."""


class CNInfoCollector:
    """Client for the public CNInfo announcement query endpoint."""

    QUERY_URL = "https://www.cninfo.com.cn/new/hisAnnouncement/query"
    CATEGORY = "category_ndbg_szsh"

    def __init__(self, client: httpx.Client | None = None) -> None:
        self.client = client or httpx.Client(timeout=settings.NEWS_HTTP_TIMEOUT_SECONDS)
        self._owns_client = client is None

    def close(self) -> None:
        if self._owns_client:
            self.client.close()

    def list_annual_reports(
        self,
        *,
        stock_code: str,
        report_year: int,
        page_size: int = 30,
    ) -> list[AnnualReportAnnouncement]:
        code, exchange, explicit_org_id = self._normalize_stock(stock_code)
        column = {"SH": "sse", "SZ": "szse", "BJ": "bj"}[exchange]
        org_id = self._org_id(code, exchange, explicit_org_id)
        params = {
            "stock": f"{code},{org_id}",
            "tabName": "fulltext",
            "pageNum": 1,
            "pageSize": page_size,
            "column": column,
            "category": self.CATEGORY,
            "plate": "",
            "searchkey": "",
            "secid": "",
            "sortName": "",
            "sortType": "",
            "isHLtitle": "true",
        }
        try:
            response = self.client.post(
                self.QUERY_URL,
                data=params,
                headers={"User-Agent": settings.CNINFO_USER_AGENT, "Referer": "https://www.cninfo.com.cn/"},
            )
            response.raise_for_status()
            payload = response.json()
        except (httpx.HTTPError, ValueError) as exc:
            raise CNInfoCollectorError(f"CNInfo announcement query failed: {exc}") from exc

        rows = payload.get("announcements", []) if isinstance(payload, dict) else []
        reports: list[AnnualReportAnnouncement] = []
        for row in rows if isinstance(rows, list) else []:
            report = self._parse_announcement(row, code=code, exchange=exchange)
            if report and report.report_year == report_year and self._is_full_report(report.title):
                reports.append(report)
        return reports

    def download_pdf(self, report: AnnualReportAnnouncement) -> bytes:
        try:
            response = self.client.get(
                report.pdf_url,
                headers={"User-Agent": settings.CNINFO_USER_AGENT, "Referer": "https://www.cninfo.com.cn/"},
            )
            response.raise_for_status()
        except httpx.HTTPError as exc:
            raise CNInfoCollectorError(f"CNInfo PDF download failed: {exc}") from exc
        content = response.content
        if not content.startswith(b"%PDF"):
            raise CNInfoCollectorError("CNInfo response is not a PDF")
        return content

    @classmethod
    def _parse_announcement(cls, row: dict[str, Any], *, code: str, exchange: str) -> AnnualReportAnnouncement | None:
        announcement_id = str(row.get("announcementId") or "").strip()
        title = str(row.get("announcementTitle") or "").strip()
        announcement_time = row.get("announcementTime")
        if not announcement_id or not title or announcement_time is None:
            return None
        try:
            if isinstance(announcement_time, (int, float)) or str(announcement_time).strip().isdigit():
                timestamp = float(announcement_time)
                parsed_time = datetime.fromtimestamp(timestamp / 1000 if timestamp > 10_000_000_000 else timestamp, tz=UTC)
            else:
                parsed_time = datetime.fromisoformat(str(announcement_time).replace("Z", "+00:00")).astimezone(UTC)
        except (TypeError, ValueError, OSError):
            return None
        report_year_match = re.search(r"(20\d{2})年年度报告", title)
        if not report_year_match:
            return None
        adjunct_url = str(row.get("adjunctUrl") or "").strip()
        if adjunct_url.startswith("//"):
            adjunct_url = f"https:{adjunct_url}"
        if adjunct_url.startswith("/"):
            adjunct_url = f"https://www.cninfo.com.cn{adjunct_url}"
        elif adjunct_url and not adjunct_url.startswith("http"):
            adjunct_url = f"https://static.cninfo.com.cn/{adjunct_url.lstrip('/')}"
        if not adjunct_url:
            adjunct_url = f"https://static.cninfo.com.cn/finalpage/{parsed_time:%Y-%m-%d}/{announcement_id}.PDF"
        return AnnualReportAnnouncement(
            announcement_id=announcement_id,
            stock_code=code,
            exchange=exchange,
            company_name=str(row.get("secName") or "").strip() or None,
            org_id=str(row.get("orgId") or "").strip() or None,
            report_year=int(report_year_match.group(1)),
            title=title,
            announcement_time=parsed_time,
            pdf_url=adjunct_url,
            adjunct_size=int(row["adjunctSize"]) if str(row.get("adjunctSize") or "").isdigit() else None,
            raw=row,
        )

    @staticmethod
    def _is_full_report(title: str) -> bool:
        return "摘要" not in title and "英文版" not in title and "英文" not in title

    @staticmethod
    def _org_id(code: str, exchange: str, explicit_org_id: str | None = None) -> str:
        if explicit_org_id:
            return explicit_org_id
        if exchange == "SH":
            return f"gssh0{code}"
        if exchange == "SZ":
            return f"gssz{code}"
        raise CNInfoCollectorError("BJ orgId requires a security-master mapping; use CODE.BJ:ORG_ID")

    @staticmethod
    def _normalize_stock(stock_code: str) -> tuple[str, str, str | None]:
        raw_value = stock_code.strip()
        value, separator, raw_org_id = raw_value.partition(":")
        explicit_org_id = raw_org_id.strip().lower() if separator and raw_org_id.strip() else None
        value = value.upper()
        if "." in value:
            code, exchange = value.split(".", 1)
            if exchange not in {"SH", "SZ", "BJ"}:
                raise CNInfoCollectorError(f"Unsupported exchange: {exchange}")
            return code.zfill(6), exchange, explicit_org_id
        if not value.isdigit() or len(value) != 6:
            raise CNInfoCollectorError("stock_code must be a six-digit code or CODE.SH/CODE.SZ/CODE.BJ")
        exchange = "SH" if value.startswith(("5", "6", "68")) else "SZ"
        return value, exchange, explicit_org_id
