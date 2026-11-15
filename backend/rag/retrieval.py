"""Hybrid retrieval ranking, article aggregation, and MMR selection."""

from __future__ import annotations

import math
from collections import defaultdict
from datetime import UTC, datetime
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from backend.config import settings
from backend.schemas.rag import RagArticleResult, RagEvidence, RagSearchHit


def reciprocal_rank_fusion(*ranked_lists: list[RagSearchHit]) -> list[RagSearchHit]:
    """Fuse rank lists without comparing incompatible score ranges."""

    if not 0 < settings.RAG_MMR_LAMBDA <= 1 or settings.RAG_TIME_DECAY_HALF_LIFE_DAYS <= 0 or settings.RAG_RERANK_MIN_SCORE < 0:
        raise ValueError("Invalid RAG ranking configuration")
    merged: dict[str, RagSearchHit] = {}
    scores: defaultdict[str, float] = defaultdict(float)
    for hits in ranked_lists:
        for rank, hit in enumerate(hits, start=1):
            scores[hit.chunk_id] += 1 / (settings.RAG_RRF_K + rank)
            merged.setdefault(hit.chunk_id, hit)
    return [
        merged[chunk_id].model_copy(update={"rrf_score": score})
        for chunk_id, score in sorted(scores.items(), key=lambda item: item[1], reverse=True)
    ]


def filter_by_threshold(hits: list[RagSearchHit], *, reranker_enabled: bool) -> list[RagSearchHit]:
    if not reranker_enabled:
        return hits
    return [hit for hit in hits if (hit.rerank_score or 0.0) >= settings.RAG_RERANK_MIN_SCORE]


def aggregate_articles(hits: list[RagSearchHit], *, now: datetime | None = None) -> list[RagArticleResult]:
    """Group chunks by article and preserve the strongest citable evidence."""

    current = now or datetime.now(UTC)
    grouped: defaultdict[str, list[RagSearchHit]] = defaultdict(list)
    for hit in hits:
        grouped[hit.news_id].append(hit)

    articles: list[RagArticleResult] = []
    for article_hits in grouped.values():
        article_hits.sort(key=lambda hit: hit.rerank_score or 0.0, reverse=True)
        primary = article_hits[0]
        age_days = max(0.0, (current - primary.published_at).total_seconds() / 86400)
        source_weight = settings.rag_source_weights.get((primary.source_name or primary.provider).casefold(), 1.0)
        final_score = (primary.rerank_score or 0.0) * math.pow(2, -age_days / settings.RAG_TIME_DECAY_HALF_LIFE_DAYS) * source_weight
        evidence = [
            RagEvidence(
                chunk_id=hit.chunk_id,
                chunk_text=hit.chunk_text,
                rerank_score=hit.rerank_score or 0.0,
                final_score=final_score,
            )
            for hit in article_hits[: settings.RAG_EVIDENCE_PER_ARTICLE]
        ]
        articles.append(
            RagArticleResult(
                citation_id=0,
                news_id=primary.news_id,
                content_hash=primary.content_hash,
                title=primary.title,
                source_name=primary.source_name,
                provider=primary.provider,
                published_at=primary.published_at,
                url=primary.url,
                final_score=final_score,
                metadata={"content_type": "summary"} if primary.provider == "caixin" and not primary.content else {},
                evidence_chunks=evidence,
            )
        )
    deduplicated: list[RagArticleResult] = []
    seen_hashes: set[str] = set()
    seen_urls: set[str] = set()
    for article in sorted(articles, key=lambda item: item.final_score, reverse=True):
        normalized_url = _canonical_url(article.url)
        if article.content_hash in seen_hashes or normalized_url in seen_urls:
            continue
        seen_hashes.add(article.content_hash)
        seen_urls.add(normalized_url)
        deduplicated.append(article)
    return deduplicated


def mmr_select(articles: list[RagArticleResult], hits: list[RagSearchHit], *, limit: int) -> list[RagArticleResult]:
    """Select diverse articles using their highest-ranked evidence vectors."""

    vectors = {hit.chunk_id: hit.embedding for hit in hits if hit.embedding}
    remaining = list(articles)
    selected: list[RagArticleResult] = []
    while remaining and len(selected) < limit:
        def score(article: RagArticleResult) -> float:
            relevance = article.final_score
            vector = vectors.get(article.evidence_chunks[0].chunk_id)
            redundancy = max((_cosine(vector, vectors.get(other.evidence_chunks[0].chunk_id)) for other in selected), default=0.0)
            return settings.RAG_MMR_LAMBDA * relevance - (1 - settings.RAG_MMR_LAMBDA) * redundancy

        chosen = max(remaining, key=score)
        selected.append(chosen)
        remaining.remove(chosen)
    return [article.model_copy(update={"citation_id": index}) for index, article in enumerate(selected, start=1)]


def _cosine(left: list[float] | None, right: list[float] | None) -> float:
    if not left or not right or len(left) != len(right):
        return 0.0
    denominator = math.sqrt(sum(value * value for value in left)) * math.sqrt(sum(value * value for value in right))
    return sum(a * b for a, b in zip(left, right)) / denominator if denominator else 0.0


def _canonical_url(value: str) -> str:
    parsed = urlsplit(value)
    ignored = {"utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content"}
    query = urlencode([(key, item) for key, item in parse_qsl(parsed.query, keep_blank_values=True) if key.casefold() not in ignored])
    return urlunsplit((parsed.scheme.casefold(), parsed.netloc.casefold(), parsed.path.rstrip("/"), query, ""))
