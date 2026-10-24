"""LLM service abstraction.

This module wraps the concrete model client to keep agent code independent
from the underlying LLM vendor.
"""

from __future__ import annotations

import logging
from typing import TypeVar

from langchain_core.messages import AIMessage, BaseMessage
from langchain_openai import ChatOpenAI
from pydantic import BaseModel

from backend.config import settings

logger = logging.getLogger(__name__)
T = TypeVar("T", bound=BaseModel)


class LLMServiceError(Exception):
    """Raised when LLM generation fails."""


class LLMService:
    """Unified LLM access layer."""

    def __init__(self, model_name: str | None = None, temperature: float = 0.2) -> None:
        self.model_name = model_name or settings.LLM_MODEL
        self.temperature = temperature
        api_key = settings.resolved_llm_api_key
        if not api_key:
            raise LLMServiceError(
                "No LLM API key configured. Please set LLM_API_KEY, DOUBAO_API_KEY, or ARK_API_KEY."
            )
        try:
            self.client = ChatOpenAI(
                api_key=api_key,
                model=self.model_name,
                temperature=self.temperature,
                base_url=settings.LLM_BASE_URL,
                timeout=settings.LLM_TIMEOUT_SECONDS,
                max_retries=settings.LLM_MAX_RETRIES,
            )
        except Exception as exc:
            logger.exception("Failed to initialize LLM client")
            raise LLMServiceError(f"LLM client initialization failed: {exc}") from exc

    def chat(self, messages: list[BaseMessage]) -> str:
        """Generate text from chat messages."""

        logger.info("LLM生成: model=%s", self.model_name)

        try:
            response = self.client.invoke(messages)
        except Exception as exc:  # pragma: no cover - external API call
            logger.exception("LLM invocation failed")
            raise LLMServiceError(f"LLM generation failed: {exc}") from exc

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
