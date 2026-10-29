from __future__ import annotations

from backend.agent.nodes import analyze_question_node, tool_decision_node


def test_news_question_routes_to_news_tool() -> None:
    result = tool_decision_node({"question": "NVDA 最近上涨原因是什么？", "symbol": "NVDA"})
    assert result["selected_tool"] == "rag"
    assert result["next_step"] == "rag_tool_node"


def test_market_question_routes_to_market_tool() -> None:
    result = tool_decision_node({"question": "NVDA 当前股价和 RSI 是多少？", "symbol": "NVDA"})
    assert result["selected_tool"] == "market"
    assert result["next_step"] == "market_tool_node"


def test_a_share_code_is_extracted_from_question() -> None:
    result = analyze_question_node({"question": "请分析 600519.SH 最近 20 个交易日的复权收益"})
    assert result["symbol"] == "600519.SH"
