"""HTTP client used by backend jobs and Airflow-facing APIs."""

from __future__ import annotations

import httpx

from backend.config import settings
from backend.schemas.annual_report import AnnualReportParseResult


class PdfParserClientError(Exception):
    """Raised when the parser service rejects a request."""


class PdfParserClient:
    def __init__(self, *, base_url: str | None = None, token: str | None = None) -> None:
        self.base_url = (base_url or settings.PDF_PARSER_URL).rstrip("/")
        self.token = token if token is not None else settings.PDF_PARSER_TOKEN

    def parse(self, manifest_path: str, *, force: bool = False) -> dict[str, object]:
        headers = {"X-PDF-Parser-Token": self.token} if self.token else {}
        try:
            response = httpx.post(
                f"{self.base_url}/v1/parse",
                json={"manifest_path": manifest_path, "force": force},
                headers=headers,
                timeout=settings.PDF_PARSER_TIMEOUT_SECONDS,
            )
            if response.status_code >= 300:
                raise PdfParserClientError(f"PDF parser returned {response.status_code}: {response.text}")
            payload = AnnualReportParseResult.model_validate(response.json())
            if payload.manifest_path != manifest_path:
                raise PdfParserClientError("PDF parser returned a different manifest_path")
            return payload.model_dump(mode="json")
        except httpx.HTTPError as exc:
            raise PdfParserClientError(f"PDF parser request failed: {exc}") from exc

