"""Agent state definitions for the LangGraph workflow."""

from __future__ import annotations

from typing import Any, Literal, TypedDict


class AgentState(TypedDict, total=False):
    """Shared state across all agent workflow nodes."""

    question: str
    symbol: str | None
    market_data: dict[str, Any] | None
    news_data: dict[str, Any] | None
    rag_data: dict[str, Any] | None
    answer: str
    should_call_tool: bool
    selected_tool: Literal["market", "rag", "annual_report", "none"]
    tool_error: str | None
    tool_results: dict[str, Any] | None
    next_step: Literal["market_tool_node", "rag_tool_node", "annual_report_tool_node", "generate_answer_node"]
