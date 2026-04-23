"""
agent_sf.py — v2.0 Dynamic Response Engine
--------------------------------------------
Intent-aware orchestration for FIPSAR Prospect Journey Intelligence.

Key v2 changes:
  1. Modular system prompt: lean base + intent-specific block (no monolithic prompt)
  2. Data pre-processing: trims tool results BEFORE LLM sees them
  3. Dynamic LLM params: temperature & max_tokens tuned per intent
  4. Strong negative constraints per intent to prevent repetitive structure
"""

from __future__ import annotations
import json
import re
from datetime import date, timedelta
from typing import Any

import tools_sf as T


# ---------------------------------------------------------------------------
# Cortex Complete wrapper
# ---------------------------------------------------------------------------

def _cortex_complete(session, model: str, messages: list[dict], temperature: float = 0.1, max_tokens: int = 4096) -> str:
    try:
        messages_json = json.dumps(messages)
        sql = f"""
            SELECT SNOWFLAKE.CORTEX.COMPLETE(
                '{model}',
                PARSE_JSON($${messages_json}$$),
                OBJECT_CONSTRUCT('temperature', {temperature}, 'max_tokens', {max_tokens})
            )::STRING AS response
        """
        result = session.sql(sql).collect()
        if result and result[0]["RESPONSE"]:
            raw = result[0]["RESPONSE"]
            try:
                parsed = json.loads(raw)
                choices = parsed.get("choices", [])
                if choices:
                    msg = choices[0].get("messages", choices[0].get("message", {}).get("content", ""))
                    return str(msg).strip()
            except Exception:
                pass
            return str(raw).strip()
    except Exception as e:
        return f"_Cortex error: {e}_"
    return ""


# ---------------------------------------------------------------------------
# Date context builder
# ---------------------------------------------------------------------------

def _build_date_context() -> dict:
    today = date.today()
    yesterday = today - timedelta(days=1)
    return {
        "today": today.isoformat(),
        "yesterday": yesterday.isoformat(),
        "mtd_start": today.strftime("%Y-%m-01"),
        "ytd_start": f"{today.year}-01-01",
        "current_month": today.strftime("%B %Y"),
        "current_year": str(today.year),
    }


def _resolve_dates(question: str, ctx: dict) -> tuple[str, str]:
    """
    Parse user question for date intent.
    Returns (start_date, end_date) or ("2020-01-01", "2099-12-31") if no date mentioned.
    """
    q = question.lower()

    if "today" in q:
        return ctx["today"], ctx["today"]
    if "yesterday" in q:
        return ctx["yesterday"], ctx["yesterday"]
    if "mtd" in q or "this month" in q or "current month" in q or "month to date" in q:
        return ctx["mtd_start"], ctx["today"]
    if "ytd" in q or "this year" in q or "year to date" in q:
        return ctx["ytd_start"], ctx["today"]
    if "last month" in q:
        today = date.fromisoformat(ctx["today"])
        first_of_this = today.replace(day=1)
        last_of_prev = first_of_this - timedelta(days=1)
        first_of_prev = last_of_prev.replace(day=1)
        return first_of_prev.isoformat(), last_of_prev.isoformat()
    if "last week" in q:
        today = date.fromisoformat(ctx["today"])
        monday = today - timedelta(days=today.weekday() + 7)
        sunday = monday + timedelta(days=6)
        return monday.isoformat(), sunday.isoformat()
    if "this week" in q:
        today = date.fromisoformat(ctx["today"])
        monday = today - timedelta(days=today.weekday())
        return monday.isoformat(), ctx["today"]

    # Try to detect YYYY-MM-DD patterns
    dates = re.findall(r"\d{4}-\d{2}-\d{2}", question)
    if len(dates) >= 2:
        return dates[0], dates[1]
    if len(dates) == 1:
        return dates[0], dates[0]

    # Try "Jan 2026", "January 2026", "Q1 2026"
    months = {
        "jan": "01", "feb": "02", "mar": "03", "apr": "04",
        "may": "05", "jun": "06", "jul": "07", "aug": "08",
        "sep": "09", "oct": "10", "nov": "11", "dec": "12",
    }
    for mon_abbr, mon_num in months.items():
        pattern = rf"{mon_abbr}[a-z]*\s*(\d{{4}})"
        match = re.search(pattern, q)
        if match:
            year = match.group(1)
            import calendar
            last_day = calendar.monthrange(int(year), int(mon_num))[1]
            return f"{year}-{mon_num}-01", f"{year}-{mon_num}-{last_day:02d}"

    # No date — return all-data defaults
    return "2020-01-01", "2099-12-31"


# ---------------------------------------------------------------------------
# Format injection — injected directly into user message for strict compliance
# ---------------------------------------------------------------------------

def _get_format_injection(intent: str) -> str:
    injections = {
        "SIMPLE_COUNT": (
            "ANSWER IN 2-3 SENTENCES MAXIMUM. NO TABLES. NO CHARTS. NO HEADERS. NO SECTIONS.\n"
            "Sentence 1: State the exact number in bold. Example: **700 prospects** have entered the journey.\n"
            "Sentence 2-3: Add status breakdown if available (completed/suppressed/in-progress).\n"
            "THEN STOP. Do NOT add anything else. No recommendations, no follow-up questions, "
            "no bullet points, no stage tables, no insights section."
        ),
        "DROP_OFF": (
            "STRICT FORMAT — follow exactly:\n"
            "## Where Prospects Drop Off\n"
            "First sentence: Name the biggest drop-off point with exact count lost.\n"
            "[CHART:bar-h] showing drop count per stage transition.\n"
            "| Stage Transition | Entered | Lost | Loss % |\n"
            "(table with ALL transitions)\n"
            "## Root Cause Indicators\n"
            "2-3 bullets connecting drops to suppression data. Cite numbers.\n"
            "## Actions\n"
            "2 specific recommendations citing exact metrics.\n"
            "Two follow-up questions.\n"
            "FORBIDDEN: No 'Journey Health Snapshot'. No 'AI Summary'. No 'Quick Explanation'."
        ),
        "ANOMALY": (
            "STRICT FORMAT — follow exactly:\n"
            "## Anomaly Assessment\n"
            "FIRST LINE MUST BE ONE OF:\n"
            "  - '**No significant anomalies detected.** The journey metrics are within expected ranges.' (if normal)\n"
            "  - '**[N] anomalies detected** requiring attention:' (if anomalies exist)\n\n"
            "IF NO ANOMALIES: Add 1-2 sentences on areas worth monitoring. THEN STOP. Maximum 5 sentences total.\n"
            "IF ANOMALIES EXIST: For each, state what/where/how much and why it's abnormal. Add chart if multiple.\n"
            "FORBIDDEN: Do NOT fabricate anomalies. Do NOT include stage-by-stage tables. "
            "Do NOT include journey overview or generic insights."
        ),
        "RATE_BREAKDOWN": (
            "STRICT FORMAT — follow exactly:\n"
            "## Progression Rates\n"
            "First sentence: State overall journey completion rate (Stage 1 to Stage 9 as %).\n"
            "[CHART:bar-v] showing % Reached per stage.\n"
            "| SN | Stage | Phase | Entered | % of Total | Stage-over-Stage Drop % |\n"
            "(complete table with all stages)\n"
            "## Steepest Drop\n"
            "One paragraph identifying which transition loses the most prospects and why.\n"
            "Two follow-up questions.\n"
            "FORBIDDEN: No suppression analysis. No recommendations. No health snapshot."
        ),
        "DQ_CHECK": (
            "STRICT FORMAT — follow exactly:\n"
            "## Data Quality Status: [GOOD / ISSUES FOUND / CRITICAL]\n"
            "First line: Bold verdict with count of issues found (or 'No issues found').\n"
            "IF ISSUES: List each as numbered item: What | Impact (count) | Severity (H/M/L).\n"
            "IF CLEAN: State in 2 sentences what was checked. THEN STOP.\n"
            "FORBIDDEN: No journey overview. No generic recommendations unless issues found."
        ),
        "ENGAGEMENT_METRICS": (
            "STRICT FORMAT — follow exactly:\n"
            "## Engagement Rates\n"
            "Show as rate card — one line per metric, bold the rate:\n"
            "- **Open Rate**: X% (Y of Z unique prospects)\n"
            "- **Click Rate**: X%\n"
            "- **Bounce Rate**: X%\n"
            "- **Unsubscribe Rate**: X%\n"
            "- **Spam Rate**: X%\n"
            "[CHART:bar-v] showing event counts by type.\n"
            "## Key Insight\n"
            "One paragraph: what these rates mean for journey health. Flag any rate above thresholds.\n"
            "Two follow-up questions.\n"
            "FORBIDDEN: No LaTeX formulas. No stage tables. No A-B-C-D-E format. "
            "Just show 'X% (Y of Z)' for each rate."
        ),
        "RECOMMENDATION": (
            "STRICT FORMAT — follow exactly:\n"
            "## Top 3 Recommendations\n\n"
            "**1. [Action Title]**\n"
            "Data basis: [cite exact metric]. Expected impact: [what improves].\n\n"
            "**2. [Action Title]**\n"
            "Data basis: [cite exact metric]. Expected impact: [what improves].\n\n"
            "**3. [Action Title]**\n"
            "Data basis: [cite exact metric]. Expected impact: [what improves].\n\n"
            "## Supporting Data\n"
            "Compact table with ONLY the metrics that justify recommendations.\n"
            "Two follow-up questions.\n"
            "FORBIDDEN: No full stage-by-stage table. No journey overview. Max 3 recommendations."
        ),
        "JOURNEY_HEALTH": (
            "STRICT FORMAT — follow exactly:\n"
            "## Journey Health Summary\n"
            "2-3 sentences: total prospects, completion rate, biggest concern.\n"
            "## Stage-by-Stage Reach\n"
            "[CHART:bar-v]\n"
            "| SN | Stage | Phase | Sent | Drop | % Reached |\n"
            "(full table)\n"
            "## Suppression Hotspots\n"
            "(only if suppression data available)\n"
            "[CHART:bar-h]\n"
            "| Suppressed At | Count | % of All Suppressed |\n"
            "## Top 3 Recommendations\n"
            "Each citing a specific number.\n"
            "Three follow-up questions."
        ),
        "ANALYTICAL": (
            "STRICT FORMAT — follow exactly:\n"
            "## [Direct Answer]\n"
            "1-2 sentences answering the question. Lead with the answer.\n"
            "Relevant [CHART:type] + focused data table.\n"
            "## Key Insight\n"
            "One paragraph — business meaning.\n"
            "2 recommendations citing specific numbers.\n"
            "Two follow-up questions."
        ),
    }
    return injections.get(intent, injections["ANALYTICAL"])


# ---------------------------------------------------------------------------
# Intent classification (keyword-based, no LLM credits)
# ---------------------------------------------------------------------------

def _classify_intent(question: str) -> str:
    """
    Classify the user's question into a response-format intent.
    This intent is passed to Cortex so it knows which template to apply.
    """
    q = question.lower()

    # Simple count / "how many"
    if any(p in q for p in ["how many", "count of", "total number", "number of", "how much"]):
        if any(w in q for w in ["entered", "in journey", "are in", "prospects", "leads", "completed", "suppressed"]):
            return "SIMPLE_COUNT"

    # Drop-off / funnel loss — check before journey_health to be more specific
    if any(p in q for p in ["dropping off", "drop off", "drop-off", "where are", "losing", "falling off",
                              "attrition", "not progressing", "not reaching", "not making it"]):
        return "DROP_OFF"

    # Anomaly / unusual patterns
    if any(p in q for p in ["anomal", "unusual", "strange", "odd", "inconsisten", "unexpected",
                              "pattern", "weird", "spike", "outlier", "concern"]):
        return "ANOMALY"

    # Percentage / rate breakdown
    if any(p in q for p in ["what percentage", "what %", "what percent", "progression rate",
                              "completion rate", "how much of", "each step", "each stage",
                              "progressing through"]):
        return "RATE_BREAKDOWN"

    # Data quality
    if any(p in q for p in ["missing", "inconsistent data", "data quality", "data issue",
                              "null value", "gaps in data", "affecting", "data problem"]):
        return "DQ_CHECK"

    # Engagement metrics
    if any(p in q for p in ["engagement rate", "open rate", "click rate", "bounce rate",
                              "unsubscribe rate", "current engagement", "email engagement",
                              "email performance"]):
        return "ENGAGEMENT_METRICS"

    # Recommendations / actions
    if any(p in q for p in ["what actions", "how to improve", "recommendations", "suggest",
                              "what can", "what should", "help improve", "optimize",
                              "what would you recommend", "best practice"]):
        return "RECOMMENDATION"

    # Journey health / full overview
    if any(p in q for p in ["overall", "overview", "health", "how is the journey",
                              "how is the prospect journey", "performing", "general status",
                              "full picture", "summary of the journey"]):
        return "JOURNEY_HEALTH"

    # Default
    return "ANALYTICAL"


# ---------------------------------------------------------------------------
# Tool routing (intent-aware, no LLM credits)
# ---------------------------------------------------------------------------

def _route_tools(question: str, intent: str) -> list[str]:
    """
    Intent-aware keyword router.
    Selects only the tools needed for the given intent — avoids over-fetching data
    which causes the LLM to produce padded, repetitive responses.
    """
    q = question.lower()
    tools = []

    # Intent-specific routing — minimal data fetch per intent
    if intent == "SIMPLE_COUNT":
        tools.append("get_journey_overview")

    elif intent == "DROP_OFF":
        tools.append("get_journey_stage_dropoff")
        tools.append("get_journey_suppression_linkage")

    elif intent == "ANOMALY":
        tools.append("get_journey_stage_dropoff")
        tools.append("get_journey_suppression_linkage")
        tools.append("get_journey_pace_analysis")

    elif intent == "RATE_BREAKDOWN":
        tools.append("get_journey_stage_dropoff")

    elif intent == "DQ_CHECK":
        tools.append("get_funnel_metrics")
        tools.append("get_rejection_analysis")

    elif intent == "ENGAGEMENT_METRICS":
        tools.append("get_sfmc_engagement_stats")

    elif intent == "RECOMMENDATION":
        tools.append("get_journey_stage_dropoff")
        tools.append("get_journey_suppression_linkage")
        tools.append("get_sfmc_engagement_stats")

    elif intent == "JOURNEY_HEALTH":
        tools.append("get_journey_overview")
        tools.append("get_journey_stage_dropoff")
        tools.append("get_journey_suppression_linkage")

    else:
        # ANALYTICAL — fallback: keyword routing
        is_journey = any(w in q for w in ["stage", "journey", "prospect journey", "phase"])
        if is_journey:
            if any(w in q for w in ["overview", "health", "summary", "total", "how is"]):
                tools.append("get_journey_overview")
            if any(w in q for w in ["drop", "reach", "progression", "funnel", "where"]):
                tools.append("get_journey_stage_dropoff")
            if any(w in q for w in ["suppressed", "suppression", "why", "reason", "blocked"]):
                tools.append("get_journey_suppression_linkage")
            if any(w in q for w in ["timing", "pace", "days", "interval", "on time"]):
                tools.append("get_journey_pace_analysis")
            if not tools:
                tools.append("get_journey_stage_dropoff")

        if any(w in q for w in ["rejection", "rejected", "invalid", "refused"]):
            tools.append("get_rejection_analysis")
        if any(w in q for w in ["engagement", "open rate", "click rate", "bounce", "unsubscribe"]):
            tools.append("get_sfmc_engagement_stats")
        if any(w in q for w in ["what happened on", "drop on", "issue on"]) and any(c.isdigit() for c in q):
            tools.append("get_drop_analysis")
        if not tools:
            tools.append("get_funnel_metrics")

    # Always allow prospect tracing regardless of intent
    if any(w in q for w in ["trace", "track", "fip", "individual prospect", "specific prospect"]):
        tools.append("trace_prospect")

    return list(dict.fromkeys(tools))  # deduplicate, preserve order


# ---------------------------------------------------------------------------
# Tool executor
# ---------------------------------------------------------------------------

def _execute_tools(session, tool_names: list[str], question: str, ctx: dict) -> list[str]:
    """Execute each tool and return list of result strings."""
    start_date, end_date = _resolve_dates(question, ctx)
    results = []

    for tool_name in tool_names:
        try:
            if tool_name == "get_funnel_metrics":
                r = T.get_funnel_metrics(session, start_date, end_date)
            elif tool_name == "get_rejection_analysis":
                # Determine category from question
                q = question.lower()
                category = "sfmc" if any(w in q for w in ["sfmc", "suppressed", "send fail", "suppression"]) else "intake"
                r = T.get_rejection_analysis(session, start_date, end_date, category=category)
            elif tool_name == "get_sfmc_engagement_stats":
                r = T.get_sfmc_engagement_stats(session, start_date, end_date)
            elif tool_name == "get_drop_analysis":
                # For drop analysis, use today if no specific date found
                target = start_date if start_date != "2020-01-01" else ctx["today"]
                r = T.get_drop_analysis(session, target)
            elif tool_name == "get_journey_overview":
                r = T.get_journey_overview(session)
            elif tool_name == "get_journey_stage_dropoff":
                r = T.get_journey_stage_dropoff(session)
            elif tool_name == "get_journey_suppression_linkage":
                r = T.get_journey_suppression_linkage(session)
            elif tool_name == "get_journey_pace_analysis":
                r = T.get_journey_pace_analysis(session)
            elif tool_name == "trace_prospect":
                # Extract identifier from question
                id_match = re.search(r"(FIP\w+|\b[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}\b)", question, re.I)
                if id_match:
                    r = T.trace_prospect(session, id_match.group(1))
                else:
                    r = "_Please provide a Prospect ID (e.g. FIP001234) or email address._"
            else:
                r = f"_Tool '{tool_name}' not found._"
        except Exception as e:
            r = f"_Error executing {tool_name}: {e}_"
        results.append(r)

    return results


# ---------------------------------------------------------------------------
# Lean base system prompt (replaces monolithic SYSTEM_PROMPT from semantic_model_sf)
# ---------------------------------------------------------------------------

BASE_SYSTEM_PROMPT = """You are FIPSAR Intelligence — a data analyst for the FREL Prospect Journey.

CORE RULES:
- Use ONLY the data provided in TOOL RESULTS below. Never invent numbers.
- If data is insufficient to answer, say so — do not fabricate.
- Be precise: cite exact numbers from the data.
- Never repeat the same insight in multiple sections.
- Use markdown formatting for tables and bold text.

TERMINOLOGY:
- Lead: A record in STG_PROSPECT_INTAKE. Pre-validation. Never call it a Prospect.
- Prospect: A validated record in DIM_PROSPECT. Has MASTER_PATIENT_ID (FIP...).
- Suppressed: A prospect flagged by SFMC as ineligible for email sends.
- Journey Stage: One of 9 email stages from Welcome through Final Reminder.
- Drop-off: A prospect who reached stage N but not stage N+1.

CHART MARKERS (place on their own line):
- [CHART:bar-v] = vertical bar chart (for stage progression, counts)
- [CHART:bar-h] = horizontal bar chart (for rankings, comparisons)
- [CHART:donut] = donut chart (for proportions, status breakdown)
"""


# ---------------------------------------------------------------------------
# Intent-specific system block (injected into system message per intent)
# ---------------------------------------------------------------------------

def _get_intent_system_block(intent: str) -> str:
    blocks = {
        "SIMPLE_COUNT": (
            "RESPONSE TYPE: DIRECT FACTUAL ANSWER\n"
            "- State the number in the FIRST sentence. Bold it.\n"
            "- Add 1-2 sentences of context ONLY if noteworthy.\n"
            "- Maximum total response: 3-4 sentences.\n"
            "- FORBIDDEN: tables, charts, ## headers, bullet lists, recommendations, follow-up questions, stage breakdowns.\n"
        ),
        "DROP_OFF": (
            "RESPONSE TYPE: FUNNEL DROP-OFF ANALYSIS\n"
            "- Lead with biggest drop-off point.\n"
            "- Include chart marker and funnel transition table.\n"
            "- Connect to suppression data if available.\n"
            "- End with 2 specific actions.\n"
            "- FORBIDDEN: Journey Health Snapshot, generic overviews.\n"
        ),
        "ANOMALY": (
            "RESPONSE TYPE: ANOMALY DETECTION\n"
            "- Lead with CLEAR verdict: anomalies or not.\n"
            "- If no anomalies: 2-3 sentences then STOP.\n"
            "- If anomalies: detail each with data points.\n"
            "- FORBIDDEN: Do NOT fabricate anomalies. Do NOT pad with stage tables if normal.\n"
        ),
        "RATE_BREAKDOWN": (
            "RESPONSE TYPE: PROGRESSION RATES TABLE\n"
            "- Lead with overall completion rate.\n"
            "- Show chart + full stage table with drop %.\n"
            "- One insight on steepest drop.\n"
            "- FORBIDDEN: Suppression analysis, recommendations, overview.\n"
        ),
        "DQ_CHECK": (
            "RESPONSE TYPE: DATA QUALITY ASSESSMENT\n"
            "- Lead with verdict (issues found or clean).\n"
            "- If clean: 2 sentences, then STOP.\n"
            "- If issues: list each with count and severity.\n"
            "- FORBIDDEN: Journey overview, generic advice.\n"
        ),
        "ENGAGEMENT_METRICS": (
            "RESPONSE TYPE: ENGAGEMENT RATE CARD\n"
            "- Show rates as compact card: X% (Y of Z).\n"
            "- One insight paragraph.\n"
            "- FORBIDDEN: LaTeX formulas, stage tables, A-B-C-D-E format.\n"
        ),
        "RECOMMENDATION": (
            "RESPONSE TYPE: ACTIONABLE RECOMMENDATIONS\n"
            "- Exactly 3 recommendations, numbered.\n"
            "- Each must cite a specific metric and estimate impact.\n"
            "- Compact supporting data table.\n"
            "- FORBIDDEN: Full stage table, journey overview.\n"
        ),
        "JOURNEY_HEALTH": (
            "RESPONSE TYPE: COMPREHENSIVE JOURNEY OVERVIEW\n"
            "- Health summary, stage table with chart, suppression hotspots, 3 recommendations.\n"
            "- This is the ONLY intent that uses the full A/B/C/D/E structure.\n"
        ),
        "ANALYTICAL": (
            "RESPONSE TYPE: ANALYTICAL\n"
            "- Lead with direct answer.\n"
            "- Show relevant chart + table.\n"
            "- One insight, 2 recommendations.\n"
        ),
    }
    return blocks.get(intent, blocks["ANALYTICAL"])


# ---------------------------------------------------------------------------
# Data pre-processing — trim tool results BEFORE LLM sees them
# ---------------------------------------------------------------------------

def _preprocess_results(intent: str, tool_results: list[str]) -> str:
    if not tool_results:
        return "_No data available._"

    combined = "\n\n---\n\n".join(tool_results)

    if intent == "SIMPLE_COUNT":
        return (
            "CONDENSED DATA:\n" + combined + "\n\n"
            "TASK: Find the total prospect count from above. "
            "Answer with JUST that number. Do NOT expand into stage analysis."
        )

    if intent == "ENGAGEMENT_METRICS":
        return (
            "ENGAGEMENT DATA:\n" + combined + "\n\n"
            "TASK: Calculate Open/Click/Bounce/Unsubscribe/Spam rates as percentages. "
            "Show as 'X% (Y of Z)'. Do NOT use LaTeX or mathematical notation."
        )

    if intent == "ANOMALY":
        return (
            "DATA FOR ANOMALY CHECK:\n" + combined + "\n\n"
            "ANOMALY THRESHOLDS:\n"
            "- Stage drop >15% between consecutive stages: noteworthy but may be normal for email journeys\n"
            "- Suppression >30% at single point: flaggable\n"
            "- Timing deviation >2x expected interval: anomalous\n"
            "- If all within reasonable ranges: say NO ANOMALIES and keep response SHORT."
        )

    if intent == "DQ_CHECK":
        return (
            "DATA QUALITY DATA:\n" + combined + "\n\n"
            "CHECK FOR: null values, inconsistent counts, rejection reasons, date gaps. "
            "If clean, say so briefly."
        )

    return combined


# ---------------------------------------------------------------------------
# Dynamic LLM parameters per intent
# ---------------------------------------------------------------------------

def _get_llm_params(intent: str) -> dict:
    params = {
        "SIMPLE_COUNT":       {"temperature": 0.0, "max_tokens": 512},
        "DROP_OFF":           {"temperature": 0.1, "max_tokens": 3072},
        "ANOMALY":            {"temperature": 0.05, "max_tokens": 2048},
        "RATE_BREAKDOWN":     {"temperature": 0.05, "max_tokens": 2048},
        "DQ_CHECK":           {"temperature": 0.0,  "max_tokens": 1536},
        "ENGAGEMENT_METRICS": {"temperature": 0.05, "max_tokens": 2048},
        "RECOMMENDATION":     {"temperature": 0.2,  "max_tokens": 2048},
        "JOURNEY_HEALTH":     {"temperature": 0.1,  "max_tokens": 4096},
        "ANALYTICAL":         {"temperature": 0.1,  "max_tokens": 3072},
    }
    return params.get(intent, params["ANALYTICAL"])


# ---------------------------------------------------------------------------
# Main chat function
# ---------------------------------------------------------------------------

def chat(
    session,
    model: str,
    user_message: str,
    conversation_history: list[dict],
) -> str:
    """
    Main chat entry point.

    Parameters
    ----------
    session           : Snowflake Snowpark session
    model             : Cortex model name (e.g. 'mistral-large', 'llama3-70b')
    user_message      : The user's question
    conversation_history : List of prior {"role": "user"|"assistant", "content": "..."} dicts

    Returns
    -------
    str : Assistant's response
    """
    ctx = _build_date_context()

    intent = _classify_intent(user_message)

    tool_names = _route_tools(user_message, intent)

    tool_results = _execute_tools(session, tool_names, user_message, ctx)

    processed_data = _preprocess_results(intent, tool_results)

    llm_params = _get_llm_params(intent)

    date_context_block = (
        f"TODAY: {ctx['today']} | YESTERDAY: {ctx['yesterday']} | "
        f"MTD: {ctx['mtd_start']} → {ctx['today']} | YTD: {ctx['ytd_start']} → {ctx['today']}\n"
        "DATE RULE: When the user specifies NO date → answer covers ALL available data.\n"
    )

    intent_block = _get_intent_system_block(intent)

    system_content = (
        date_context_block
        + "\n"
        + BASE_SYSTEM_PROMPT
        + "\n"
        + intent_block
        + "\n"
        + "═══ TOOL RESULTS (use ONLY this data) ═══\n\n"
        + processed_data
    )

    messages: list[dict] = [{"role": "system", "content": system_content}]

    for turn in conversation_history[-6:]:
        messages.append({"role": turn["role"], "content": turn["content"]})

    format_instruction = _get_format_injection(intent)
    messages.append({
        "role": "user",
        "content": f"{format_instruction}\n\nQUESTION: {user_message}",
    })

    response = _cortex_complete(
        session, model, messages,
        temperature=llm_params["temperature"],
        max_tokens=llm_params["max_tokens"],
    )

    if not response or response.startswith("_Cortex error"):
        return (
            "⚠️ _Cortex LLM unavailable — showing raw data results below._\n\n"
            + processed_data
        )

    return response
