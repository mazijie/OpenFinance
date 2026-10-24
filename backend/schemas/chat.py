"""Request and response schemas for chat API."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class ChatRequest(BaseModel):
    """Incoming request payload for finance chat."""

    question: str = Field(..., min_length=1, description="User question for the finance agent")


class ChatResponse(BaseModel):
    """Outgoing response payload for finance chat."""

    question: str
    symbol: str | None = None
    analysis: str
    tool_results: dict[str, Any] | None = None
