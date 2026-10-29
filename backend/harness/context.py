"""Context-window construction and rolling conversation summaries."""

from __future__ import annotations

from typing import Any, Callable

from langchain_core.messages import AIMessage, BaseMessage, HumanMessage, SystemMessage, ToolMessage

from backend.config import settings


class ContextManager:
    def __init__(self, summarizer: Callable[[str], str] | None = None) -> None:
        self.summarizer = summarizer

    def compact(
        self,
        *,
        previous_summary: str,
        messages: list[Any],
    ) -> tuple[str, list[dict[str, Any]]]:
        raw = [self.message_to_dict(message) for message in messages]
        char_count = len(previous_summary) + sum(len(item["content"]) for item in raw)
        if (
            len(raw) <= settings.HARNESS_CONTEXT_RECENT_MESSAGES
            and char_count <= settings.HARNESS_CONTEXT_MAX_CHARS
        ):
            return previous_summary, raw
        split = max(0, len(raw) - settings.HARNESS_CONTEXT_RECENT_MESSAGES)
        older = raw[:split]
        recent = raw[split:]
        while recent and len(previous_summary) + sum(len(item["content"]) for item in recent) > settings.HARNESS_CONTEXT_MAX_CHARS:
            older.append(recent.pop(0))
        summary_source = "\n".join(
            f"{item['role']}: {item['content'][:2000]}" for item in older
        )
        if not summary_source:
            return previous_summary, recent
        if self.summarizer is not None:
            summary = self.summarizer(
                "请压缩以下投研会话，保留用户目标、证券、时间范围、已确认事实、"
                "待审批操作、失败原因和引用 ID，不要添加新事实。\n\n"
                f"已有摘要：{previous_summary}\n\n新增历史：\n{summary_source}"
            )
        else:
            summary = (
                f"{previous_summary}\n" if previous_summary else ""
            ) + summary_source[-6000:]
        return summary[:12_000], recent

    @staticmethod
    def message_to_dict(message: Any) -> dict[str, Any]:
        if isinstance(message, BaseMessage):
            role = "assistant" if isinstance(message, AIMessage) else "tool" if isinstance(message, ToolMessage) else "user"
            return {"role": role, "content": _content_text(message.content)}
        if isinstance(message, dict):
            return {
                "role": str(message.get("role") or "user"),
                "content": str(message.get("content") or ""),
            }
        return {"role": "user", "content": str(message)}

    @staticmethod
    def to_messages(
        *,
        summary: str,
        recent: list[dict[str, Any]],
        system_prompt: str,
    ) -> list[BaseMessage]:
        messages: list[BaseMessage] = [SystemMessage(content=system_prompt)]
        if summary:
            messages.append(SystemMessage(content=f"此前研究会话摘要：\n{summary}"))
        for item in recent:
            role = item["role"]
            content = item["content"]
            if role == "assistant":
                messages.append(AIMessage(content=content))
            elif role == "tool":
                messages.append(HumanMessage(content=f"[工具结果] {content}"))
            else:
                messages.append(HumanMessage(content=content))
        return messages


def _content_text(content: Any) -> str:
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        return "\n".join(str(item) for item in content)
    return str(content)

