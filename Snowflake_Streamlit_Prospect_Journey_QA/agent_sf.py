"""
agent_sf.py
-----------
Lightweight ReAct-style agent for Snowflake Streamlit.
Uses keyword-based tool routing (no LLM credits) + Snowflake Cortex Complete
for narrative response generation.

Architecture:
  Step 1: Keyword routing → select which tool(s) to call
  Step 2: Execute tools via Snowpark session
  Step 3: Cortex Complete → generate structured narrative from results
"""

from __future__ import annotations
import json
import re
from datetime import date, timedelta
from typing import Any

from semantic_model_sf import RUNTIME_PROMPT
import tools_sf as T


# ---------------------------------------------------------------------------
# Cortex Complete wrapper
# ---------------------------------------------------------------------------

def _cortex_complete(session, model: str, messages: list[dict], temperature: float = 0.1) -> str:
    """
    Call Snowflake Cortex Complete.
    Tries the native Python API first (available in Streamlit in Snowflake),
    then falls back to SQL. The Python API avoids all SQL string-escaping
    issues that can cause failures with large JSON payloads.
    """
    # --- Attempt 1: Native Python API (preferred — no SQL string escaping) ---
    try:
        from snowflake.cortex import Complete  # available in SiS runtime
        options = {"temperature": temperature, "max_tokens": 4096}
        response = Complete(model, messages, session=session, options=options)
        if response:
            return str(response).strip()
    except ImportError:
        pass  # Not in SiS runtime — fall through to SQL
    except Exception as e:
        pass  # API failed — fall through to SQL

    # --- Attempt 2: SQL via Snowpark (fallback) ---
    try:
        messages_json = json.dumps(messages)
        # $$ quoting ends at next $$. Sanitise any $$ inside the JSON so the
        # SQL string is never prematurely terminated. Replacing with "$ $" is
        # safe: $$ is vanishingly rare in medical/pharma text, and the LLM
        # handles "$ $" identically for understanding purposes.
        messages_json_safe = messages_json.replace("$$", "$ $")
        sql = f"""
            SELECT SNOWFLAKE.CORTEX.COMPLETE(
                '{model}',
                PARSE_JSON($${messages_json_safe}$$),
                OBJECT_CONSTRUCT('temperature', {temperature}, 'max_tokens', 4096)
            )::STRING AS response
        """
        result = session.sql(sql).collect()
        if result and result[0]["RESPONSE"]:
            raw = result[0]["RESPONSE"]
            # Cortex returns {"choices": [{"messages": "..."}]} — extract content
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
    """
    Returns a short, explicit format instruction placed directly in the user
    message. Models obey instructions closer to their question much more
    reliably than rules buried in the system prompt.
    """
    if intent == "SIMPLE_COUNT":
        return (
            "FORMAT: SIMPLE_COUNT\n"
            "Line 1: **Bold sentence with the exact count.** (e.g. '**700 prospects have entered the Prospect Journey.**')\n"
            "Line 2: One sentence — status breakdown (completed / suppressed / in-progress numbers).\n"
            "[CHART:donut] then a 3-row status table only.\n"
            "PROHIBITED: no stage-by-stage table, no AI Summary, no Insights, no Recommendations, no Follow-up Questions."
        )
    elif intent == "DROP_OFF":
        return (
            "FORMAT: DROP_OFF\n"
            "## Where Prospects Are Dropping Off\n"
            "First sentence: name the top 2 stages by loss count with exact numbers.\n"
            "[CHART:bar-v] then: Stage | Reached | Lost After | Loss %\n"
            "## Root Cause (2-3 bullets, cite actual numbers)\n"
            "## Actions (2 bullets max, cite a specific metric each)\n"
            "2 follow-up questions only.\n"
            "PROHIBITED: no 'Quick Explanation', 'Data Snapshot', 'AI Summary', 'Insights' headers."
        )
    elif intent == "ANOMALY":
        return (
            "FORMAT: ANOMALY\n"
            "## Anomalies Detected (or: 'No significant anomalies found.' if none)\n"
            "Per anomaly: name it, quantify it (count/%), explain WHY it is anomalous.\n"
            "[CHART:bar-h] only if ranking multiple anomalies.\n"
            "## Likely Causes (1-2 bullets per anomaly)\n"
            "## Investigation Steps (specific table/field checks)\n"
            "PROHIBITED: no generic advice, no 'continuously monitor'."
        )
    elif intent == "RATE_BREAKDOWN":
        return (
            "FORMAT: RATE_BREAKDOWN\n"
            "First sentence: overall journey completion rate as a percentage.\n"
            "[CHART:bar-v] then: Stage | Prospects Sent | % of Total | % from Prior Stage\n"
            "## Biggest Drops (1-2 bullets on widest gaps between consecutive stages)\n"
            "PROHIBITED: no AI Summary, no generic recommendations."
        )
    elif intent == "DQ_CHECK":
        return (
            "FORMAT: DQ_CHECK\n"
            "## Data Quality Status: GOOD | ISSUES FOUND | CRITICAL\n"
            "List specific issues with exact counts. If none: short confirmation with supporting numbers.\n"
            "[CHART:bar-h] only if multiple issue types.\n"
            "## Impact (one line per issue)\n"
            "## Resolution (specific table/field/fix)\n"
            "PROHIBITED: do not say 'looks fine' without citing numbers."
        )
    elif intent == "ENGAGEMENT_METRICS":
        return (
            "FORMAT: ENGAGEMENT_METRICS\n"
            "## Engagement Scorecard\n"
            "Bold headline: **Open Rate: X% | Click Rate: X%** (calculate from tool data).\n"
            "[CHART:donut] then: Metric | Count | Rate\n"
            "## What the Rates Mean (2-3 bullets)\n"
            "## Watch Points (flag: bounce >5%, unsubscribe >2%, spam >0.1%)\n"
            "2 follow-up questions only.\n"
            "PROHIBITED: no A-B-C-D-E journey format."
        )
    elif intent == "RECOMMENDATION":
        return (
            "FORMAT: RECOMMENDATION\n"
            "## Top Improvement Opportunities (max 4, ranked by impact)\n"
            "Each: **Action:** X | **Data basis:** [cite exact number] | **Expected outcome:** Y\n"
            "## Quick Wins (executable in 1 week)\n"
            "2 follow-up questions.\n"
            "PROHIBITED: every action MUST cite a number — no data-free recommendations."
        )
    elif intent == "JOURNEY_HEALTH":
        return (
            "FORMAT: JOURNEY_HEALTH\n"
            "## A — Snapshot (2-3 sentences: completion %, suppression %, top concern)\n"
            "## B — Stage-by-Stage Reach\n"
            "  [CHART:bar-v] Stage | Prospects | Emails to be Sent | % Reached\n"
            "## C — Suppression Hotspots\n"
            "  [CHART:bar-h] Stage | Suppressed | % of All Suppressed\n"
            "## D — Anomalies (skip if none)\n"
            "## E — Top 3 Recommendations (cite a number for each)\n"
            "3 follow-up questions."
        )
    else:  # ANALYTICAL
        return (
            "FORMAT: ANALYTICAL\n"
            "## Direct Answer (1-2 sentences — lead with the answer)\n"
            "[CHART:type] then focused data table.\n"
            "## Key Insight (1 paragraph)\n"
            "## 2-3 Recommendations (each citing a data number)\n"
            "2 follow-up questions."
        )


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
        # get_journey_stage_dropoff already includes its own suppression table —
        # do NOT add get_journey_suppression_linkage or the data duplicates
        tools.append("get_journey_stage_dropoff")

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

    # Step 1: Classify intent (determines response format + tool selection)
    intent = _classify_intent(user_message)

    # Step 2: Route to tools based on intent
    tool_names = _route_tools(user_message, intent)

    # Step 3: Execute tools
    tool_results = _execute_tools(session, tool_names, user_message, ctx)

    # Step 4: Build Cortex messages
    date_context_block = (
        f"TODAY: {ctx['today']} | YESTERDAY: {ctx['yesterday']} | "
        f"MTD: {ctx['mtd_start']} to {ctx['today']} | YTD: {ctx['ytd_start']} to {ctx['today']}\n"
        "DATE RULE: When the user specifies NO date → answer covers ALL available data (no date filter).\n"
        "NEVER assume today as a default date unless the user says 'today'.\n"
    )

    tool_results_block = "\n\n---\n\n".join(tool_results) if tool_results else "_No tool data retrieved._"

    system_content = (
        date_context_block
        + "\n"
        + RUNTIME_PROMPT
        + "\n\n"
        + "═══ TOOL RESULTS (use ONLY this data in your response) ═══\n\n"
        + tool_results_block
    )

    messages: list[dict] = [{"role": "system", "content": system_content}]

    # Include last 4 turns of history (reduced from 6 to keep token count low)
    for turn in conversation_history[-4:]:
        messages.append({"role": turn["role"], "content": turn["content"]})

    # Inject explicit format instruction + intent directly into the user message.
    # Models obey instructions placed immediately before the question far more
    # reliably than rules buried in the system prompt.
    format_instruction = _get_format_injection(intent)
    messages.append({
        "role": "user",
        "content": f"{format_instruction}\n\nQUESTION: {user_message}",
    })

    # Step 5: Generate response
    response = _cortex_complete(session, model, messages, temperature=0.1)

    if not response or response.startswith("_Cortex error"):
        # Fallback: return raw tool results with a note
        return (
            "⚠️ _Cortex LLM unavailable — showing raw data results below._\n\n"
            + tool_results_block
        )

    return response
