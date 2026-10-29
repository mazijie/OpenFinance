"""LangGraph node implementations."""

from __future__ import annotations

import logging
import re
from datetime import UTC, datetime, timedelta

from langchain_core.messages import HumanMessage, SystemMessage

from backend.agent.state import AgentState
from backend.rag.citations import build_citation_context, conservative_evidence_answer, validate_citations
from backend.schemas.rag import RagArticleResult
from backend.services.llm import LLMService, LLMServiceError
from backend.tools.market import get_market_data
from backend.tools.rag import search_rag
from backend.tools.annual_reports import search_annual_reports

logger = logging.getLogger(__name__)

NUMERIC_SYMBOL_PATTERN = re.compile(r"\b\d{6}(?:\.(?:SH|SZ|BJ))?\b", re.IGNORECASE)
ALPHA_SYMBOL_PATTERN = re.compile(r"\b[A-Z]{1,5}(?:\.[A-Z]{1,4})?\b")

NEWS_KEYWORDS = (
    "新闻",
    "消息",
    "事件",
    "原因",
    "为何",
    "公告",
    "问询函",
    "业绩预告",
    "最近发生",
    "近期事件",
)

MARKET_ONLY_KEYWORDS = (
    "价格",
    "股价",
    "涨跌",
    "收益",
    "波动",
    "复权",
    "ohlcv",
    "rsi",
    "macd",
    "k线",
)

REPORT_KEYWORDS = ("年报", "年度报告", "管理层讨论", "财务报告", "研发投入", "风险因素")


def analyze_question_node(state: AgentState) -> AgentState:
    """Analyze the user question and extract a possible stock symbol."""

    question = state["question"].strip()
    logger.info("Agent start analyze_question_node")
    logger.info("Question: %s", question)

    extracted_symbol = _extract_symbol(question)
    logger.info("Extracted symbol: %s", extracted_symbol or "none")

    return {
        **state,
        "question": question,
        "symbol": extracted_symbol,
    }


def tool_decision_node(state: AgentState) -> AgentState:
    """Decide which tool should be called."""

    logger.info("Agent start tool_decision_node")

    symbol = state.get("symbol")
    selected_tool = _select_tool(question=state.get("question", ""), symbol=symbol)
    should_call_tool = selected_tool in {"market", "rag", "annual_report"}
    next_step = (
        "market_tool_node"
        if selected_tool == "market"
        else "rag_tool_node"
        if selected_tool == "rag"
        else "annual_report_tool_node"
        if selected_tool == "annual_report"
        else "generate_answer_node"
    )
    logger.info(
        "Agent decision: should_call_tool=%s, selected_tool=%s, next_step=%s",
        should_call_tool,
        selected_tool,
        next_step,
    )

    return {
        **state,
        "should_call_tool": should_call_tool,
        "selected_tool": selected_tool,
        "next_step": next_step,
    }


def market_tool_node(state: AgentState) -> AgentState:
    """Call market data tool when a symbol is available."""

    logger.info("Agent start market_tool_node")

    symbol = state.get("symbol")
    if not symbol:
        return {
            **state,
            "market_data": None,
            "news_data": None,
            "rag_data": None,
            "tool_results": None,
            "tool_error": "No stock symbol detected",
        }

    tool_result = get_market_data(symbol=symbol)
    return {
        **state,
        "market_data": tool_result.get("data"),
        "news_data": None,
        "rag_data": None,
        "tool_results": tool_result,
        "tool_error": tool_result.get("error"),
    }


def rag_tool_node(state: AgentState) -> AgentState:
    """Call the news RAG tool when the question is event oriented."""

    logger.info("Agent start rag_tool_node")

    question = state.get("question", "").strip()
    if not question:
        return {
            **state,
            "rag_data": None,
            "tool_results": None,
            "tool_error": "Empty question",
        }

    symbol = state.get("symbol")
    end_date = datetime.now(UTC).date()
    start_date = end_date - timedelta(days=30)
    tool_result = search_rag(question=question, symbol=symbol, start_date=start_date, end_date=end_date, limit=5)
    return {
        **state,
        "market_data": None,
        "news_data": None,
        "rag_data": tool_result.get("data"),
        "tool_results": tool_result,
        "tool_error": tool_result.get("error"),
    }


def annual_report_tool_node(state: AgentState) -> AgentState:
    """Retrieve annual-report evidence when the question is report-oriented."""

    question = state.get("question", "").strip()
    year_match = re.search(r"\b(20\d{2})\b", question)
    result = search_annual_reports(question=question, stock_code=state.get("symbol"), report_year=int(year_match.group(1)) if year_match else None, limit=5)
    return {
        **state,
        "market_data": None,
        "news_data": None,
        "rag_data": result.get("data"),
        "tool_results": result,
        "tool_error": result.get("error"),
    }


def generate_answer_node(state: AgentState, llm_service: LLMService | None = None) -> AgentState:
    """Generate final analysis answer using the LLM."""

    logger.info("Agent start generate_answer_node")

    rag_articles = [RagArticleResult.model_validate(item) for item in (state.get("rag_data") or {}).get("articles", [])]
    prompt = _build_cited_prompt(state, rag_articles) if rag_articles else _build_analysis_prompt(state)

    try:
        llm = llm_service or LLMService()
        answer = llm.chat(
            [
                SystemMessage(
                    content=(
                        "You are a professional financial analysis assistant. "
                        "Provide a concise, structured answer in Chinese. "
                        "If tool data is unavailable, clearly explain the limitation "
                        "and avoid making up facts or numbers. When evidence is supplied, "
                        "every substantive line must include its [citation_id]."
                    )
                ),
                HumanMessage(content=prompt),
            ]
        )
        if rag_articles:
            valid, reason = validate_citations(answer, rag_articles)
            if not valid:
                answer = llm.chat(
                    [
                        SystemMessage(content="Repair the answer. Use only supplied evidence and put a valid [citation_id] on every substantive line."),
                        HumanMessage(content=f"Validation error: {reason}\n\n{prompt}\n\nDraft:\n{answer}"),
                    ]
                )
                valid, _ = validate_citations(answer, rag_articles)
                if not valid:
                    answer = conservative_evidence_answer(rag_articles)
    except LLMServiceError as exc:
        answer = f"LLM service unavailable: {exc}"

    return {
        **state,
        "answer": answer,
    }


def _build_cited_prompt(state: AgentState, articles: list[RagArticleResult]) -> str:
    """Build an evidence-only prompt for a news-backed response."""

    return (
        f"用户问题: {state['question']}\n\n"
        "只能基于以下证据回答，不得补充证据之外的事实或数字。"
        "每个实质性段落或列表项都必须带上[citation_id]。\n\n"
        f"证据:\n{build_citation_context(articles)}"
    )


def route_after_decision(state: AgentState) -> str:
    """Routing function used by LangGraph after the decision node."""

    return state.get("next_step", "generate_answer_node")


def _extract_symbol(question: str) -> str | None:
    """Extract a possible stock symbol from the question."""

    normalized_question = question.upper()
    numeric_match = NUMERIC_SYMBOL_PATTERN.search(normalized_question)
    if numeric_match:
        return numeric_match.group(0)

    matches = ALPHA_SYMBOL_PATTERN.findall(normalized_question)
    for match in matches:
        if match not in {"LLM", "API", "USD", "HKD", "A", "AN"}:
            return match
    return None


def _build_analysis_prompt(state: AgentState) -> str:
    """Build the prompt for final answer generation."""

    symbol = state.get("symbol")
    tool_error = state.get("tool_error")
    market_data = state.get("market_data")
    rag_data = state.get("rag_data")

    return (
        f"用户问题: {state['question']}\n"
        f"识别到的证券代码: {symbol or '未识别'}\n"
        f"内部行情工具结果: {market_data or '无'}\n"
        f"新闻RAG结果: {rag_data or '无'}\n"
        f"工具错误: {tool_error or '无'}\n\n"
        "请输出结构化分析，至少包含：\n"
        "1. 问题理解\n"
        "2. 关键数据或事件解读\n"
        "3. 风险提示\n"
        "如果内部湖仓暂无匹配行情或新闻，请明确说明，不要虚构实时数据。"
    )


def _select_tool(question: str, symbol: str | None) -> str:
    """Route questions between market and news tools."""

    if not symbol:
        return "none"

    normalized_question = question.lower()
    if any(keyword in normalized_question for keyword in REPORT_KEYWORDS):
        return "annual_report"
    if any(keyword in normalized_question for keyword in MARKET_ONLY_KEYWORDS):
        return "market"
    if any(keyword in normalized_question for keyword in NEWS_KEYWORDS):
        return "rag"
    return "market"
