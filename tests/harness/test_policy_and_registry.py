from __future__ import annotations

from datetime import UTC, datetime

from backend.harness.policy import PolicyGuard
from backend.harness.tools import ToolContext, ToolRegistry


class FakeFinanceService:
    pass


def test_policy_budgets_and_tool_boundary():
    standard = PolicyGuard.budget("standard")
    deep = PolicyGuard.budget("deep")
    simple = PolicyGuard.tier_budget("simple")
    hard = PolicyGuard.hard_budget("standard")
    assert standard.max_tool_calls == 6
    assert deep.max_tool_calls == 12
    assert simple.max_tool_calls == 3
    assert hard.max_tool_calls == 20
    assert PolicyGuard.hard_budget("deep") == hard
    assert PolicyGuard.allows("interactive", "research_search")
    assert PolicyGuard.allows("interactive", "fetch_web_page")
    assert not PolicyGuard.allows("interactive", "submit_monitor_run")


def test_registry_exposes_unified_tools_and_marks_writes():
    context = ToolContext(
        thread_id="00000000-0000-0000-0000-000000000001",
        turn_id="00000000-0000-0000-0000-000000000002",
        run_kind="interactive",
        as_of=datetime(2026, 10, 26, tzinfo=UTC),
        store=object(),
        evidence_service=object(),
        web_service=object(),
    )
    registry = ToolRegistry(
        context=context,
        finance_service=FakeFinanceService(),
    )

    names = set(registry.tool_map)
    assert "research_search" in names
    assert "fetch_web_page" in names
    assert "submit_monitor_run" not in names
    assert "list_watchlist" not in names
    assert not PolicyGuard.is_write_tool("research_search")

