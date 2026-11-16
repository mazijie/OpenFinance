from datetime import UTC, datetime, timedelta

from backend.rag.common.citations import validate_citations
from backend.rag.common.retrieval import aggregate_articles, mmr_select, reciprocal_rank_fusion
from backend.schemas.rag import RagSearchHit


def _hit(chunk_id: str, news_id: str, score: float, embedding: list[float]) -> RagSearchHit:
    return RagSearchHit(
        chunk_id=chunk_id, news_id=news_id, provider="juhe", source_name="Source", title=f"Title {news_id}",
        url=f"https://example.com/{news_id}", published_at=datetime.now(UTC) - timedelta(days=1),
        ingested_at=datetime.now(UTC), content_hash=f"hash-{news_id}", raw_object_path="news/test.json",
        chunk_index=0, chunk_text=f"Evidence {news_id}", similarity=score, embedding=embedding,
    )


def test_rrf_deduplicates_same_chunk_and_aggregates_articles() -> None:
    first = _hit("a", "news-1", 0.9, [1.0, 0.0])
    second = _hit("b", "news-1", 0.8, [0.9, 0.1])
    third = _hit("c", "news-2", 0.7, [0.0, 1.0])
    fused = reciprocal_rank_fusion([first, second], [first, third])
    assert [item.chunk_id for item in fused].count("a") == 1
    reranked = [item.model_copy(update={"rerank_score": 0.9 - index * 0.1}) for index, item in enumerate(fused)]
    articles = mmr_select(aggregate_articles(reranked), reranked, limit=5)
    assert len(articles) == 2
    assert all(article.citation_id for article in articles)


def test_citation_validator_rejects_uncited_lines() -> None:
    hit = _hit("a", "news-1", 0.9, [1.0, 0.0]).model_copy(update={"rerank_score": 0.9})
    articles = mmr_select(aggregate_articles([hit]), [hit], limit=1)
    assert validate_citations("有证据的结论。[1]", articles)[0] is True
    assert validate_citations("没有引用的结论。", articles)[0] is False


def test_article_aggregation_deduplicates_tracking_url() -> None:
    first = _hit("a", "news-1", 0.9, [1.0, 0.0]).model_copy(update={"rerank_score": 0.9, "url": "https://example.com/story?utm_source=x"})
    second = _hit("b", "news-2", 0.8, [0.0, 1.0]).model_copy(update={"rerank_score": 0.8, "url": "https://example.com/story"})
    assert len(aggregate_articles([first, second])) == 1
