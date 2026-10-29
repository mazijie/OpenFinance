"""LLM service abstraction.

This module wraps the concrete model client to keep agent code independent
from the underlying LLM vendor.
"""

from __future__ import annotations

import logging
import re
from typing import TypeVar

from langchain_core.messages import AIMessage, BaseMessage
from langchain_openai import ChatOpenAI
from pydantic import BaseModel

from backend.config import settings

logger = logging.getLogger(__name__)
T = TypeVar("T", bound=BaseModel)


class LLMServiceError(Exception):
    """Raised when LLM generation fails."""


def redact_llm_error(value: object) -> str:
    """Remove credential fragments that some providers include in auth errors."""

    text = str(value)
    text = re.sub(r"(?i)(api\s*key\s*:\s*)\S+", r"\1[redacted]", text)
    text = re.sub(r"sk-[A-Za-z0-9_-]+", "[redacted]", text)
    return text


class LLMService:
    """Unified LLM access layer."""

    def __init__(
        self,
        api_key: str | None = None,
        model_name: str | None = None,
        temperature: float = 0.2,
        base_url: str | None = None,
    ) -> None:
        self.model_name = model_name or settings.DEEPSEEK_STANDARD_MODEL
        self.temperature = temperature
        resolved_api_key = (api_key or "").strip()
        if not resolved_api_key:
            raise LLMServiceError(
                "No LLM API key supplied. Send the user's key in the X-LLM-API-Key header."
            )
        try:
            self.client = ChatOpenAI(
                api_key=resolved_api_key,
                model=self.model_name,
                temperature=self.temperature,
                base_url=base_url or settings.DEEPSEEK_BASE_URL,
                timeout=settings.LLM_TIMEOUT_SECONDS,
                max_retries=settings.LLM_MAX_RETRIES,
            )
        except Exception as exc:
            message = redact_llm_error(exc)
            logger.error("Failed to initialize LLM client: %s", message)
            raise LLMServiceError(f"LLM client initialization failed: {message}") from exc

    def chat(self, messages: list[BaseMessage]) -> str:
        """Generate text from chat messages."""

        logger.info("LLM生成: model=%s", self.model_name)

        try:
            response = self.client.invoke(messages)
        except Exception as exc:  # pragma: no cover - external API call
            message = redact_llm_error(exc)
            logger.error("LLM invocation failed: %s", message)
            raise LLMServiceError(f"LLM generation failed: {message}") from exc

        if not isinstance(response, AIMessage):
            raise LLMServiceError("LLM returned an unexpected response type")

        content = response.content
        if isinstance(content, str):
            return content

        if isinstance(content, list):
            return "\n".join(str(item) for item in content)

        return str(content)

    def structured_chat(self, messages: list[BaseMessage], schema: type[T]) -> T:
        """Generate a validated structured object using the best supported mode."""

        errors: list[str] = []
        for method in ("json_schema", "function_calling"):
            try:
                structured_client = self.client.with_structured_output(schema, method=method)
                response = structured_client.invoke(messages)
                return response if isinstance(response, schema) else schema.model_validate(response)
            except Exception as exc:  # pragma: no cover - provider capability dependent
                errors.append(f"{method}: {exc}")
                logger.warning("Structured LLM mode failed: %s", errors[-1])
        raise LLMServiceError("Structured LLM generation failed: " + " | ".join(errors))
