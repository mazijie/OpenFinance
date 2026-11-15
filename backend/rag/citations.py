"""Citation formatting and post-generation validation."""

from __future__ import annotations

import re

from backend.schemas.rag import CitationAssessment, RagArticleResult

_CITATION = re.compile(r"\[(\d+)]")


def build_citation_context(articles: list[RagArticleResult]) -> str:
    """Create the only evidence block the answering model may rely on."""

    sections: list[str] = []
    for article in articles:
        evidence = "\n".join(f"- {item.chunk_text}" for item in article.evidence_chunks)
        sections.append(
            f"[{article.citation_id}] 标题：{article.title}\n来源：{article.source_name or article.provider}\n"
            f"发布时间：{article.published_at.isoformat()}\nURL：{article.url}\n"
            f"{'内容类型：新闻摘要' if article.metadata.get('content_type') == 'summary' else '内容类型：新闻正文'}\n证据：\n{evidence}"
        )
    return "\n\n".join(sections)


def validate_citations(answer: str, articles: list[RagArticleResult]) -> tuple[bool, str]:
    """Require every substantive answer line to cite a returned article."""

    valid_ids = {article.citation_id for article in articles}
    used_ids = {int(value) for value in _CITATION.findall(answer)}
    if not used_ids:
        return False, "回答没有引用。"
    if not used_ids.issubset(valid_ids):
        return False, "回答包含不存在的引用编号。"
    substantive = [line.strip() for line in answer.splitlines() if line.strip() and not line.lstrip().startswith("#")]
    missing = [line for line in substantive if not _CITATION.search(line)]
    if missing:
        return False, "每个实质性段落或列表项都必须包含引用。"
    return True, ""


def assess_citations(answer: str, articles: list[RagArticleResult]) -> CitationAssessment:
    """Assess completeness/validity only; factual entailment is an evaluation task."""
    valid, _ = validate_citations(answer, articles)
    has_any = bool(_CITATION.search(answer))
    return CitationAssessment(completeness=valid, validity=valid and has_any, correctness=None, correctness_notes="Requires human or entailment-model evaluation.")


def conservative_evidence_answer(articles: list[RagArticleResult]) -> str:
    """Safe fallback when the model cannot produce a valid cited answer."""

    lines = ["以下为已检索到的证据；未生成可验证的分析结论："]
    for article in articles:
        lines.append(f"- {article.title}（{article.source_name or article.provider}，{article.published_at.isoformat()}）[{article.citation_id}]")
    return "\n".join(lines)
