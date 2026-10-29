from __future__ import annotations

from backend.harness.budget import (
    BudgetController,
    BudgetReviewer,
    TaskAnalyzer,
    build_budget_payload,
    fallback_task_profile,
)
from backend.harness.policy import PolicyGuard
from backend.schemas.harness import BudgetReview, TaskProfile


class StructuredLLM:
    def __init__(self, response=None, error: Exception | None = None):
        self.response = response
        self.error = error
        self.calls = 0

    def structured_chat(self, messages, schema):
        self.calls += 1
        if self.error:
            raise self.error
        return self.response


def _decision_profile() -> TaskProfile:
    return TaskProfile(
        task_type="decision_support",
        complexity="complex",
        risk_level="high",
        uncertainty="high",
        needs_cross_validation=True,
        required_sources=["market", "news"],
        confidence=0.9,
        reason="Decision support requires cross-source evidence.",
    )


def _review(
    *,
    sufficient: bool = False,
    action: str = "expand",
    confidence: float = 0.8,
    conflict: bool = False,
) -> BudgetReview:
    return BudgetReview(
        evidence_sufficient=sufficient,
        missing_goals=[] if sufficient else ["关键证据"],
        conflict_detected=conflict,
        expected_value="high" if not sufficient else "low",
        recommended_action=action,
        confidence=confidence,
        reason="Test review.",
    )


def test_rules_cover_simple_query_without_llm():
    llm = StructuredLLM(error=AssertionError("LLM should not be called"))
    result = TaskAnalyzer(llm_factory=lambda: llm).analyze(
        "查询贵州茅台最近一个月行情",
        run_kind="interactive",
    )

    assert result.source == "rule"
    assert result.profile.complexity == "simple"
    assert result.profile.required_sources == ["market"]
    assert llm.calls == 0


def test_ambiguous_task_uses_flash_classifier():
    llm = StructuredLLM(response=_decision_profile())
    result = TaskAnalyzer(llm_factory=lambda: llm).analyze(
        "帮我看看这家公司最近怎么样",
        run_kind="interactive",
    )

    assert result.source == "llm"
    assert result.profile.task_type == "decision_support"
    assert llm.calls == 1


def test_task_classifier_failure_falls_back_to_medium():
    llm = StructuredLLM(error=RuntimeError("provider unavailable"))
    result = TaskAnalyzer(llm_factory=lambda: llm).analyze(
        "帮我看看这家公司最近怎么样",
        run_kind="interactive",
    )

    assert result.source == "fallback"
    assert result.profile.complexity == "medium"
    assert "provider unavailable" in (result.error or "")


def test_budget_reviewer_failure_uses_deterministic_review():
    reviewer = BudgetReviewer(
        llm_factory=lambda: StructuredLLM(error=RuntimeError("reviewer failed"))
    )
    result = reviewer.review(
        profile=_decision_profile(),
        evidence=[],
        missing=["news unavailable"],
        required_missing_sources=["news"],
        metrics={"new_evidence_count": 0},
    )

    assert result.source == "fallback"
    assert not result.review.evidence_sufficient
    assert result.review.recommended_action == "continue"


def test_budget_controller_escalates_one_tier_and_stops_on_no_progress():
    controller = BudgetController()
    profile = fallback_task_profile("分析公司风险")
    expand = controller.decide(
        mode="standard",
        tier="simple",
        profile=profile,
        used_tool_calls=1,
        used_steps=1,
        elapsed_seconds=10,
        required_missing_sources=["news"],
        web_substitute_available=False,
        no_progress_rounds=0,
        review=_review(),
    )
    no_progress = controller.decide(
        mode="standard",
        tier="medium",
        profile=profile,
        used_tool_calls=2,
        used_steps=1,
        elapsed_seconds=20,
        required_missing_sources=["news"],
        web_substitute_available=False,
        no_progress_rounds=2,
        review=_review(),
    )

    assert expand.action == "expand"
    assert expand.tier == "medium"
    assert no_progress.action == "stop"
    assert no_progress.stop_reason == "no_progress"


def test_budget_controller_stops_when_required_evidence_is_complete():
    controller = BudgetController()
    decision = controller.decide(
        mode="standard",
        tier="medium",
        profile=_decision_profile(),
        used_tool_calls=2,
        used_steps=1,
        elapsed_seconds=20,
        required_missing_sources=[],
        web_substitute_available=False,
        no_progress_rounds=0,
        review=_review(sufficient=True, action="stop", confidence=0.9),
    )

    assert decision.action == "stop"
    assert decision.stop_reason == "evidence_complete"


def test_web_evidence_can_substitute_for_empty_internal_sources():
    controller = BudgetController()
    decision = controller.decide(
        mode="standard",
        tier="medium",
        profile=fallback_task_profile("分析公司新闻"),
        used_tool_calls=1,
        used_steps=1,
        elapsed_seconds=15,
        required_missing_sources=["market", "news"],
        web_substitute_available=True,
        no_progress_rounds=0,
        review=_review(sufficient=True, action="stop", confidence=0.9),
    )

    assert decision.action == "stop"
    assert decision.stop_reason == "evidence_complete"


def test_budget_controller_never_exceeds_hard_cap():
    controller = BudgetController()
    decision = controller.decide(
        mode="standard",
        tier="hard",
        profile=_decision_profile(),
        used_tool_calls=20,
        used_steps=8,
        elapsed_seconds=300,
        required_missing_sources=["annual_report"],
        web_substitute_available=False,
        no_progress_rounds=0,
        review=_review(action="expand", confidence=0.95),
    )

    assert decision.action == "stop"
    assert decision.stop_reason == "hard_budget"
    assert decision.tier == "hard"


def test_budget_payload_preserves_hard_cap_and_revision_history():
    profile = _decision_profile()
    budget = build_budget_payload(mode="deep", profile=profile, source="llm")
    hard = PolicyGuard.hard_budget("standard")
    deep_hard = PolicyGuard.hard_budget("deep")

    assert budget["current_tier"] == "complex"
    assert budget["hard_limits"]["max_tool_calls"] == hard.max_tool_calls
    assert deep_hard == hard
