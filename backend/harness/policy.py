"""Run policy and budgets for the application-owned harness."""

from __future__ import annotations

from dataclasses import dataclass

from backend.config import settings

BUDGET_TIER_ORDER = ("simple", "medium", "complex", "hard")


@dataclass(frozen=True, slots=True)
class HarnessBudget:
    max_tool_calls: int
    max_steps: int
    timeout_seconds: int
    max_parallel_tools: int

    def to_dict(self) -> dict[str, int]:
        return {
            "max_tool_calls": self.max_tool_calls,
            "max_steps": self.max_steps,
            "timeout_seconds": self.timeout_seconds,
            "max_parallel_tools": self.max_parallel_tools,
        }


READ_TOOLS = {
    "research_search",
    "annual_report_search",
    "resolve_security",
    "list_watchlist",
    "fetch_web_page",
    "get_monitor_status",
    "list_monitor_reports",
    "get_monitor_report",
}

WRITE_TOOLS = {
    "add_watchlist_item",
    "update_watchlist_item",
    "delete_watchlist_item",
    "submit_monitor_run",
}

MONITOR_ALLOWED_TOOLS = {
    "research_search",
    "annual_report_search",
    "resolve_security",
    "list_watchlist",
}


class PolicyGuard:
    """Enforce tool, data-source, and budget policy per run kind."""

    @staticmethod
    def budget(mode: str) -> HarnessBudget:
        """Return the legacy fixed budget used when dynamic budgeting is disabled."""

        if mode == "deep":
            return HarnessBudget(
                max_tool_calls=settings.HARNESS_DEEP_MAX_TOOL_CALLS,
                max_steps=settings.HARNESS_DEEP_MAX_STEPS,
                timeout_seconds=settings.HARNESS_DEEP_TIMEOUT_SECONDS,
                max_parallel_tools=settings.HARNESS_MAX_PARALLEL_TOOLS,
            )
        return HarnessBudget(
            max_tool_calls=settings.HARNESS_STANDARD_MAX_TOOL_CALLS,
            max_steps=settings.HARNESS_STANDARD_MAX_STEPS,
            timeout_seconds=settings.HARNESS_STANDARD_TIMEOUT_SECONDS,
            max_parallel_tools=settings.HARNESS_MAX_PARALLEL_TOOLS,
        )

    @staticmethod
    def tier_budget(tier: str) -> HarnessBudget:
        limits = {
            "simple": (
                settings.HARNESS_BUDGET_SIMPLE_MAX_TOOL_CALLS,
                settings.HARNESS_BUDGET_SIMPLE_MAX_STEPS,
                settings.HARNESS_BUDGET_SIMPLE_TIMEOUT_SECONDS,
            ),
            "medium": (
                settings.HARNESS_BUDGET_MEDIUM_MAX_TOOL_CALLS,
                settings.HARNESS_BUDGET_MEDIUM_MAX_STEPS,
                settings.HARNESS_BUDGET_MEDIUM_TIMEOUT_SECONDS,
            ),
            "complex": (
                settings.HARNESS_BUDGET_COMPLEX_MAX_TOOL_CALLS,
                settings.HARNESS_BUDGET_COMPLEX_MAX_STEPS,
                settings.HARNESS_BUDGET_COMPLEX_TIMEOUT_SECONDS,
            ),
            "hard": (
                settings.HARNESS_BUDGET_HARD_MAX_TOOL_CALLS,
                settings.HARNESS_BUDGET_HARD_MAX_STEPS,
                settings.HARNESS_BUDGET_HARD_TIMEOUT_SECONDS,
            ),
        }
        if tier not in limits:
            raise ValueError(f"Unknown budget tier: {tier}")
        max_tool_calls, max_steps, timeout_seconds = limits[tier]
        return HarnessBudget(
            max_tool_calls=max_tool_calls,
            max_steps=max_steps,
            timeout_seconds=timeout_seconds,
            max_parallel_tools=settings.HARNESS_MAX_PARALLEL_TOOLS,
        )

    @staticmethod
    def hard_budget(mode: str) -> HarnessBudget:
        del mode
        return PolicyGuard.tier_budget("hard")

    @staticmethod
    def pending_budget(mode: str) -> HarnessBudget:
        """Return a conservative placeholder until task analysis completes."""

        del mode
        return PolicyGuard.tier_budget("medium")

    @staticmethod
    def next_tier(tier: str) -> str | None:
        try:
            index = BUDGET_TIER_ORDER.index(tier)
        except ValueError:
            index = BUDGET_TIER_ORDER.index("medium")
        if index >= len(BUDGET_TIER_ORDER) - 1:
            return None
        return BUDGET_TIER_ORDER[index + 1]

    @staticmethod
    def is_write_tool(tool_name: str) -> bool:
        return tool_name in WRITE_TOOLS

    @staticmethod
    def allows(run_kind: str, tool_name: str) -> bool:
        if run_kind == "monitor":
            return tool_name in MONITOR_ALLOWED_TOOLS
        return tool_name in READ_TOOLS or tool_name in WRITE_TOOLS

    @staticmethod
    def allowed_sources(run_kind: str) -> set[str]:
        if run_kind == "monitor":
            return {"market", "news", "annual_report"}
        return {"market", "news", "annual_report", "web"}

