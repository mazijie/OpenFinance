"""Request-scoped LLM credentials supplied by the caller."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Annotated

from fastapi import Header, HTTPException, status

from backend.config import settings

API_KEY_HEADER = "X-LLM-API-Key"
MODEL_HEADER = "X-LLM-Model"
MAX_API_KEY_LENGTH = 4096


@dataclass(frozen=True)
class LLMCredentials:
    """Credentials valid for one API request only."""

    api_key: str
    model_name: str


def require_llm_credentials(
    x_llm_api_key: Annotated[str | None, Header(alias=API_KEY_HEADER)] = None,
    x_llm_model: Annotated[str | None, Header(alias=MODEL_HEADER)] = None,
) -> LLMCredentials:
    """Validate request-scoped credentials without persisting or logging them."""

    api_key = (x_llm_api_key or "").strip()
    if not api_key:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=f"LLM API key is required; send it in the {API_KEY_HEADER} header.",
        )
    if len(api_key) > MAX_API_KEY_LENGTH:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail="LLM API key is too long.",
        )

    model_name = (x_llm_model or settings.DEEPSEEK_STANDARD_MODEL).strip()
    allowed_models = settings.llm_allowed_models
    if model_name not in allowed_models:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail=(
                f"Unsupported LLM model '{model_name}'. "
                f"Allowed models: {', '.join(allowed_models)}."
            ),
        )
    return LLMCredentials(api_key=api_key, model_name=model_name)
