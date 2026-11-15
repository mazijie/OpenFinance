"""Public schemas for the application-owned research agent harness."""

from __future__ import annotations

from datetime import UTC, datetime
import re
from typing import Any, Literal

from pydantic import BaseModel, Field, field_validator


AgentMode = Literal["standard", "deep"]
AgentRunKind = Literal["interactive"]
TaskType = Literal["query", "analysis", "research", "decision_support"]
TaskComplexity = Literal["simple", "medium", "complex"]
TaskRisk = Literal["low", "medium", "high"]
TaskUncertainty = Literal["low", "medium", "high"]
BudgetTier = Literal["simple", "medium", "complex", "hard"]
BudgetAction = Literal["continue", "expand", "stop"]
AgentTurnStatus = Literal[
    "queued",
    "running",
    "waiting_approval",
    "completed",
    "failed",
    "cancelled",
]
AgentMessageKind = Literal["text", "tool", "approval", "system"]
AgentEventType = Literal[
    "turn.started",
    "task.analyzed",
    "step.completed",
    "tool.started",
    "tool.completed",
    "budget.reviewed",
    "budget.updated",
    "budget.stopped",
    "approval.required",
    "message.delta",
    "message.completed",
    "turn.completed",
    "turn.cancelled",
    "turn.failed",
]
EvidenceSourceType = Literal["market", "news", "annual_report", "web"]


def _utc(value: datetime | str) -> datetime:
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00")) if isinstance(value, str) else value
    return parsed.replace(tzinfo=UTC) if parsed.tzinfo is None else parsed.astimezone(UTC)


class AgentCitation(BaseModel):
    citation_id: int = Field(ge=1)
    source_type: EvidenceSourceType
    source_id: str
    title: str
    url: str | None = None
    published_at: datetime | None = None
    fetched_at: datetime | None = None
    locator: str | None = None
    excerpt: str | None = None
    content_type: str | None = None

    report_id: str | None = None
    page_start: int | None = None
    page_end: int | None = None
    section_path: str | None = None
    region_ids: list[str] = Field(default_factory=list)

    @field_validator("published_at", "fetched_at", mode="before")
    @classmethod
    def _normalize_datetime(cls, value: datetime | str | None) -> datetime | None:
        return _utc(value) if value is not None else None


class EvidenceDocument(BaseModel):
    evidence_id: str
    source_type: EvidenceSourceType
    source_id: str
    title: str
    text: str
    url: str | None = None
    published_at: datetime | None = None
    fetched_at: datetime | None = None
    locator: str | None = None
    score: float | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)

    @field_validator("published_at", "fetched_at", mode="before")
    @classmethod
    def _normalize_datetime(cls, value: datetime | str | None) -> datetime | None:
        return _utc(value) if value is not None else None


class EvidencePack(BaseModel):
    documents: list[EvidenceDocument] = Field(default_factory=list)
    missing: list[str] = Field(default_factory=list)
    sources: list[EvidenceSourceType] = Field(default_factory=list)
    as_of: datetime | None = None


class TaskProfile(BaseModel):
    task_type: TaskType
    complexity: TaskComplexity
    risk_level: TaskRisk
    uncertainty: TaskUncertainty
    needs_cross_validation: bool = False
    required_sources: list[EvidenceSourceType] = Field(default_factory=list)
    confidence: float = Field(ge=0, le=1)
    reason: str = Field(min_length=1, max_length=1000)


class BudgetReview(BaseModel):
    evidence_sufficient: bool
    missing_goals: list[str] = Field(default_factory=list)
    conflict_detected: bool = False
    expected_value: Literal["low", "medium", "high"]
    recommended_action: BudgetAction
    confidence: float = Field(ge=0, le=1)
    reason: str = Field(min_length=1, max_length=1000)


class AgentThreadCreate(BaseModel):
    title: str | None = Field(default=None, max_length=200)


class AgentThreadUpdate(BaseModel):
    title: str | None = Field(default=None, min_length=1, max_length=200)
    status: Literal["active", "archived"] | None = None


class AgentThread(BaseModel):
    thread_id: str
    title: str
    status: Literal["active", "archived"]
    kind: Literal["user", "system"]
    rolling_summary: str = ""
    unread_count: int = 0
    last_seq: int = 0
    last_message_at: datetime | None = None
    created_at: datetime
    updated_at: datetime
    metadata: dict[str, Any] = Field(default_factory=dict)


class AgentMessage(BaseModel):
    message_id: str
    thread_id: str
    seq: int
    role: Literal["user", "assistant", "system"]
    kind: AgentMessageKind
    content: str
    citations: list[AgentCitation] = Field(default_factory=list)
    payload: dict[str, Any] = Field(default_factory=dict)
    turn_id: str | None = None
    created_at: datetime


class AgentTurnRequest(BaseModel):
    question: str = Field(min_length=1, max_length=20_000)
    mode: AgentMode = "standard"
    as_of: datetime | None = None

    @field_validator("as_of")
    @classmethod
    def _as_of(cls, value: datetime | None) -> datetime | None:
        return _utc(value) if value is not None else None


class AgentTurn(BaseModel):
    turn_id: str
    thread_id: str
    question: str
    mode: AgentMode
    status: AgentTurnStatus
    checkpoint_id: str | None = None
    budget: dict[str, Any] = Field(default_factory=dict)
    stats: dict[str, Any] = Field(default_factory=dict)
    error: str | None = None
    created_at: datetime
    started_at: datetime | None = None
    finished_at: datetime | None = None


class AgentToolCall(BaseModel):
    call_id: str
    turn_id: str
    thread_id: str
    tool_name: str
    args: dict[str, Any] = Field(default_factory=dict)
    result_summary: dict[str, Any] = Field(default_factory=dict)
    status: Literal["started", "completed", "failed", "rejected"]
    requires_approval: bool = False
    error: str | None = None
    started_at: datetime
    finished_at: datetime | None = None


class AgentApproval(BaseModel):
    approval_id: str
    thread_id: str
    turn_id: str
    call_id: str
    tool_name: str
    proposed_action: dict[str, Any] = Field(default_factory=dict)
    status: Literal["pending", "approved", "rejected", "expired"]
    idempotency_key: str
    created_at: datetime
    decided_at: datetime | None = None


class ApprovalDecision(BaseModel):
    approved: bool
    note: str | None = Field(default=None, max_length=1000)


class AgentEvent(BaseModel):
    event_id: str
    event_seq: int
    thread_id: str
    turn_id: str | None = None
    event_type: AgentEventType
    payload: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime


class AgentTurnDetail(BaseModel):
    turn: AgentTurn
    tool_calls: list[AgentToolCall] = Field(default_factory=list)
    approvals: list[AgentApproval] = Field(default_factory=list)
    events: list[AgentEvent] = Field(default_factory=list)


class WebDomainCreate(BaseModel):
    host_pattern: str = Field(min_length=3, max_length=255)

    @field_validator("host_pattern")
    @classmethod
    def _normalize(cls, value: str) -> str:
        normalized = value.strip().casefold().removeprefix("https://").removeprefix("http://").rstrip("/")
        if (
            "/" in normalized
            or not normalized
            or not re.fullmatch(r"(?:\*\.)?[a-z0-9.-]+\.[a-z]{2,}", normalized)
            or (normalized.startswith("*.") and normalized.count(".") < 2)
        ):
            raise ValueError("host_pattern must contain only a hostname or wildcard domain")
        return normalized


class WebDomain(BaseModel):
    host_pattern: str
    enabled: bool = True
    created_at: datetime


class WebDomainUpdate(BaseModel):
    enabled: bool


class WebDocument(BaseModel):
    document_id: str
    url: str
    canonical_url: str
    domain: str
    title: str
    text: str
    content_hash: str
    fetched_at: datetime
    metadata: dict[str, Any] = Field(default_factory=dict)
