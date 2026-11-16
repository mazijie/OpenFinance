"""Authentication for internal orchestration endpoints."""

from fastapi import HTTPException, status

from backend.config import settings


def _require_internal_token(x_openfinance_token: str | None) -> None:
    expected_token = settings.OPENFINANCE_INTERNAL_API_TOKEN.strip()
    provided_token = (x_openfinance_token or "").strip()
    if not expected_token or provided_token != expected_token:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Forbidden")
