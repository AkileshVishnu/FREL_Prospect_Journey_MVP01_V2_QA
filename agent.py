"""
agent.py
--------
LangGraph conversational agent with persistent session history.

Architecture:
  - LLM:        OpenAI GPT-4o (configurable)
  - Memory:     LangGraph MemorySaver (in-process, keyed by session_id)
  - Tools:      8 Snowflake query tools from tools.py
  - Prompt:     Full semantic model context from semantic_model.py
  - Graph:      create_react_agent — ReAct loop (agent → tools → agent → ...)

Public API:
  chat(session_id, user_message)  -> str    (main entry point)
  reset_session(session_id)       -> None   (clear a session's history)
  get_session_history(session_id) -> list   (retrieve raw messages)
"""

from __future__ import annotations

import logging
import re
from datetime import date
from typing import Any

from langchain_core.messages import HumanMessage, AIMessage, SystemMessage, BaseMessage
from langchain_openai import ChatOpenAI
from langgraph.checkpoint.memory import MemorySaver
from langgraph.prebuilt import create_react_agent

from config import app_config
from semantic_model import SYSTEM_PROMPT
from tools import ALL_TOOLS

logger = logging.getLogger(__name__)


def _normalize_response_markdown(text: str) -> str:
    """
    Keep final chatbot output readable and aligned with the business response format
    even when the model drifts into older footer-oriented headings.
    """
    if not text:
        return text

    normalized = text.replace("\r\n", "\n").replace("\r", "\n").strip()

    replacements = {
        "**TL;DR**": "## Quick Explanation",
        "TL;DR": "## Quick Explanation",
        "**Key Insights**": "## Insights",
        "Key Insights": "## Insights",
        "**Dig Deeper**": "## Follow-up Questions",
        "Dig Deeper": "## Follow-up Questions",
    }
    for source, target in replacements.items():
        normalized = normalized.replace(source, target)

    normalized = re.sub(r"(?m)^\s*---\s*$", "", normalized)
    normalized = re.sub(r"(?m)^\s*[•●◦]\s+", "- ", normalized)

    has_primary_heading = any(
        marker in normalized
        for marker in (
            "## Quick Explanation",
            "## Data Snapshot",
            "## Chart",
            "## AI Summary",
            "## Insights",
            "## Recommendations",
            "## Follow-up Questions",
        )
    )
    if not has_primary_heading and "\n" in normalized and len(normalized) > 220:
        normalized = "## Quick Explanation\n" + normalized

    normalized = re.sub(r"\n{3,}", "\n\n", normalized).strip()
    return normalized

# ---------------------------------------------------------------------------
# LLM setup
# ---------------------------------------------------------------------------

_llm = ChatOpenAI(
    model=app_config.openai_model,
    api_key=app_config.openai_api_key,
    temperature=0.1,        # Slight warmth allows more natural phrasing without sacrificing accuracy
    max_tokens=8192,        # Increased for richer, more detailed responses
    streaming=False,
)

# ---------------------------------------------------------------------------
# Memory (in-process; one saver shared across all sessions)
# ---------------------------------------------------------------------------

_checkpointer = MemorySaver()

# ---------------------------------------------------------------------------
# Build the agent
# ---------------------------------------------------------------------------

def _state_modifier(state: dict) -> list[BaseMessage]:
    """
    Inject a live date header into the system prompt on every agent invocation.
    Provides current date context and date-resolution rules so the agent
    applies date filters only when the user explicitly specifies a time period.
    """
    from datetime import timedelta
    today = date.today()
    yesterday = today - timedelta(days=1)
    mtd_start = today.strftime('%Y-%m-01')
    ytd_start = f"{today.year}-01-01"

    date_header = (
        f"TODAY'S DATE: {today.strftime('%d %B %Y')} (YYYY-MM-DD: {today.isoformat()})\n"
        f"YESTERDAY: {yesterday.isoformat()}\n"
        f"CURRENT MONTH: {today.strftime('%B %Y')} | MTD range: {mtd_start} → {today.isoformat()}\n"
        f"CURRENT YEAR: {today.year} | YTD range: {ytd_start} → {today.isoformat()}\n"
        "\n"
        "DATE RESOLUTION RULES (CRITICAL — apply before every tool call):\n"
        "\n"
        "  NO DATE MENTIONED (e.g. 'Why is there a volume drop?', 'What are the top reasons?')\n"
        f"    → Call tools with NO date filter. Use default params (all available data).\n"
        "    → NEVER assume today. NEVER default to today's date silently.\n"
        "\n"
        "  EXPLICIT DATE KEYWORDS → filter as follows:\n"
        f"    'today'              → single date: {today.isoformat()}\n"
        f"    'yesterday'          → single date: {yesterday.isoformat()}\n"
        f"    'this week'          → BETWEEN {(today - timedelta(days=today.weekday())).isoformat()} AND {today.isoformat()}\n"
        f"    'this month' / 'MTD' → BETWEEN {mtd_start} AND {today.isoformat()}\n"
        f"    'last month'         → full prior calendar month\n"
        f"    'YTD' / 'this year'  → BETWEEN {ytd_start} AND {today.isoformat()}\n"
        "    'Jan 2026'           → BETWEEN 2026-01-01 AND 2026-01-31\n"
        "    specific date/range  → use exactly what the user specified\n"
        "\n"
        "  AMBIGUOUS WORDS ('recent', 'latest', 'current') WITHOUT A DATE:\n"
        "    → Ask the user to clarify the time period before calling any date-filtered tool.\n"
        "    → Do NOT silently default to today.\n"
        "\n"
        "JOURNEY MODEL (CRITICAL — enforce in every response):\n"
        "  - There is EXACTLY ONE journey: 'Prospect Journey' with 9 stages.\n"
        "  - NEVER say 'Welcome Journey', 'Nurture Journey', 'Conversion Journey', or 'Re-engagement Journey'.\n"
        "  - These are PHASES (Welcome Phase, Nurture Phase, High Engagement Phase, Low Engagement Phase).\n"
        "  - Always use stage business names: 'Stage 01 — Welcome Email', 'Stage 03 — Education Email 1', etc.\n"
        "  - SUPPRESSION: When IS_SUPPRESSED=TRUE, NULL stage columns after last TRUE stage are cutoffs NOT gaps.\n"
        "  - LOWER STAGE COUNTS ARE NOT DROP-OFF: A lower count at a later stage means either suppression\n"
        "    OR the prospect is awaiting the next scheduled interval. NEVER call this 'attrition'.\n"
        "  - SUPPRESSION IS THE ONLY PERMANENT EXIT from the journey within stages 01–09.\n\n"
        "TOOL ROUTING RULES (CRITICAL — never mix these up):\n"
        "  Q: 'Where are prospects dropping off?' / 'Stage reach' / 'Stage-by-stage'\n"
        "    → ALWAYS call get_journey_stage_dropoff (shows email reach + suppression by stage).\n"
        "  Q: 'Journey health overview' / 'How is the journey performing overall?'\n"
        "    → ALWAYS call get_journey_overview first.\n"
        "  Q: 'Where are prospects being suppressed?' / 'Suppression by stage'\n"
        "    → ALWAYS call get_journey_suppression_linkage (suppression by stage + anomalies).\n"
        "  Q: 'Are emails going out on time?' / 'Stage timing' / 'Days between stages'\n"
        "    → ALWAYS call get_journey_pace_analysis.\n"
        "  Q: 'Overall funnel' / 'Lead to prospect conversion' / 'How many leads vs prospects?'\n"
        "    → Use get_funnel_metrics (top-level pipeline ONLY — NOT within-journey stages).\n\n"
        "RESPONSE STYLE REMINDER: Use the structured business response format from the system prompt. "
        "Lead with the answer, keep quantitative sections clean, and avoid generic footer blocks. "
        "Use charts whenever the answer is quantitative or comparative.\n"
    )
    full_prompt = date_header + "\n" + SYSTEM_PROMPT
    messages = state.get("messages", [])
    return [SystemMessage(content=full_prompt)] + list(messages)


_agent = create_react_agent(
    model=_llm,
    tools=ALL_TOOLS,
    state_modifier=_state_modifier,
    checkpointer=_checkpointer,
)

# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def chat(session_id: str, user_message: str) -> str:
    """
    Send a message in a session and return the assistant's response.

    Session history is automatically maintained by the LangGraph checkpointer.
    Each unique session_id has its own isolated message history.

    Parameters
    ----------
    session_id : str
        A string that uniquely identifies the conversation session.
        Use a stable ID (e.g. UUID, username) so that multiple turns
        within the same conversation share history.
    user_message : str
        The human's message / question.

    Returns
    -------
    str
        The agent's final text response (after any tool calls).
    """
    config: dict[str, Any] = {"configurable": {"thread_id": session_id}}

    input_state = {"messages": [HumanMessage(content=user_message)]}

    try:
        result = _agent.invoke(input_state, config=config)
    except Exception as exc:
        logger.error("Agent error for session %s: %s", session_id, exc)
        return f"I encountered an error while processing your request: {exc}"

    # The last message in the result state is the assistant's final response
    messages: list[BaseMessage] = result.get("messages", [])
    for msg in reversed(messages):
        if isinstance(msg, AIMessage) and msg.content:
            return _normalize_response_markdown(str(msg.content))

    return "I was unable to generate a response. Please try rephrasing your question."


def reset_session(session_id: str) -> None:
    """
    Clear all history for a given session.
    Subsequent calls to chat() with the same session_id start fresh.
    """
    # MemorySaver stores state under the thread_id key.
    # The cleanest way to reset is to write an empty state.
    config: dict[str, Any] = {"configurable": {"thread_id": session_id}}
    try:
        _checkpointer.put(
            config,
            {"v": 1, "ts": "0", "channel_values": {"messages": []}, "channel_versions": {}, "versions_seen": {}, "pending_sends": []},
            {},
            {},
        )
        logger.info("Session %s reset.", session_id)
    except Exception as exc:
        logger.warning("Could not reset session %s: %s", session_id, exc)


def get_session_history(session_id: str) -> list[dict[str, str]]:
    """
    Return the conversation history for a session as a list of
    {'role': 'human'|'assistant', 'content': '...'} dicts.
    """
    config: dict[str, Any] = {"configurable": {"thread_id": session_id}}
    try:
        checkpoint = _checkpointer.get(config)
        if checkpoint is None:
            return []
        messages: list[BaseMessage] = (
            checkpoint.get("channel_values", {}).get("messages", [])
        )
        history = []
        for msg in messages:
            if isinstance(msg, HumanMessage):
                history.append({"role": "human", "content": str(msg.content)})
            elif isinstance(msg, AIMessage):
                history.append({"role": "assistant", "content": str(msg.content)})
        return history
    except Exception as exc:
        logger.warning("Could not retrieve session history for %s: %s", session_id, exc)
        return []


# ---------------------------------------------------------------------------
# Quick smoke-test (run directly: python agent.py)
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    TEST_SESSION = "smoke-test-001"

    questions = [
        "Give me a quick funnel summary — how many leads became prospects and how many were rejected?",
        "Why might there be a drop in prospects — what are the most common rejection reasons?",
        "What are the top SFMC engagement events recorded? Break it down by journey.",
    ]

    for q in questions:
        print(f"\n{'='*70}\nQ: {q}\n{'-'*70}")
        answer = chat(TEST_SESSION, q)
        print(f"A: {answer}")
