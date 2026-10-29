"""Request and response schemas for chat API."""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field


class ChatRequest(BaseModel):
    """Incoming request payload for finance chat."""

    question: str = Field(..., min_length=1, description="User question for the finance agent")
    thread_id: str | None = None
    mode: Literal["standard", "deep"] = "standard"


class ChatResponse(BaseModel):
    """Outgoing response payload for finance chat."""

    question: str
    symbol: str | None = None
    analysis: str
    tool_results: dict[str, Any] | None = None
    thread_id: str | None = None
    turn_id: str | None = None
    requires_approval: bool = False
    approvals: list[dict[str, Any]] = Field(default_factory=list)
