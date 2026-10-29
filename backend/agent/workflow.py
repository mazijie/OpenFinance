"""LangGraph workflow builder for the finance agent."""

from __future__ import annotations

import logging

from langgraph.graph import END, START, StateGraph

from backend.agent.nodes import (
    analyze_question_node,
    generate_answer_node,
    market_tool_node,
    annual_report_tool_node,
    rag_tool_node,
    route_after_decision,
    tool_decision_node,
)
from backend.agent.state import AgentState
from backend.services.llm import LLMService

logger = logging.getLogger(__name__)


def build_agent_workflow(llm_service: LLMService | None = None):
    """Build and compile the LangGraph workflow."""

    workflow = StateGraph(AgentState)

    workflow.add_node("analyze_question_node", analyze_question_node)
    workflow.add_node("tool_decision_node", tool_decision_node)
    workflow.add_node("market_tool_node", market_tool_node)
    workflow.add_node("annual_report_tool_node", annual_report_tool_node)
    workflow.add_node("rag_tool_node", rag_tool_node)
    workflow.add_node(
        "generate_answer_node",
        lambda state: generate_answer_node(state, llm_service=llm_service),
    )

    workflow.add_edge(START, "analyze_question_node")
    workflow.add_edge("analyze_question_node", "tool_decision_node")
    workflow.add_conditional_edges(
        "tool_decision_node",
            route_after_decision,
            {
                "market_tool_node": "market_tool_node",
                "rag_tool_node": "rag_tool_node",
                "annual_report_tool_node": "annual_report_tool_node",
                "generate_answer_node": "generate_answer_node",
            },
        )
    workflow.add_edge("market_tool_node", "generate_answer_node")
    workflow.add_edge("rag_tool_node", "generate_answer_node")
    workflow.add_edge("annual_report_tool_node", "generate_answer_node")
    workflow.add_edge("generate_answer_node", END)

    return workflow.compile()


class FinanceAgent:
    """Application-facing wrapper around the LangGraph workflow."""

    def __init__(self, llm_service: LLMService | None = None) -> None:
        self.workflow = build_agent_workflow(llm_service=llm_service)

    def run(self, question: str) -> AgentState:
        """Execute the agent workflow."""

        logger.info("Agent开始执行 finance_agent.run")
        initial_state: AgentState = {
            "question": question,
            "symbol": None,
            "market_data": None,
            "news_data": None,
            "rag_data": None,
            "answer": "",
            "tool_error": None,
            "tool_results": None,
        }
        return self.workflow.invoke(initial_state)
