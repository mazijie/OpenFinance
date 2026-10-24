"""Tool layer base abstractions.

This module defines the common return format and base interfaces for all tools.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, TypedDict


class ToolResult(TypedDict):
    """Unified tool return structure."""

    success: bool
    data: dict[str, Any] | None
    error: str | None


class BaseTool(ABC):
    """Base class for all tools."""

    name: str
    description: str

    @abstractmethod
    def run(self, **kwargs: Any) -> ToolResult:
        """Execute the tool and return a unified result."""


def build_success_result(data: dict[str, Any]) -> ToolResult:
    """Build a successful tool result."""

    return {
        "success": True,
        "data": data,
        "error": None,
    }


def build_error_result(message: str) -> ToolResult:
    """Build a failed tool result."""

    return {
        "success": False,
        "data": None,
        "error": message,
    }
