"""
tools.py
--------
LangChain tools that the conversational agent can call to retrieve
live data from Snowflake.  Every tool is read-only.

DATA TOOLS (14):
  1.  run_sql                       — Execute any agent-generated SELECT statement.
  2.  get_funnel_metrics            — Full F01-F07 funnel counts (lead→prospect→SFMC level ONLY).
  3.  get_rejection_analysis        — Who was rejected, why, and when.
  4.  get_sfmc_engagement_stats     — Sent/Open/Click/Bounce summary per journey/stage.
  5.  get_drop_analysis             — Diagnose volume drop on a specific date.
  NEW get_journey_stage_dropoff     — Stage 01–09 progression: sent counts, drop-off, suppression breakdown.
  NEW get_journey_overview          — Journey health: suppression rate, phase completions, engagement path split.
  NEW get_journey_suppression_linkage — Suppression by stage (Q3 pattern): where suppressed prospects stopped + anomaly detection.
  NEW get_journey_pace_analysis     — Avg days between stage transitions vs expected business intervals.
  6.  trace_prospect                — Trace a prospect end-to-end by email or ID.
  7.  get_ai_intelligence           — Schema-safe AI table discovery + sample data.
  8.  get_prospect_conversion_analysis — Engagement-derived conversion & drop-off scores.
  9.  get_pipeline_observability    — Pipeline run health and DQ signal counts.
  10. get_rejected_lead_details     — Row-level rejected lead records with parsed fields.
  11. get_prospect_details          — Row-level valid mastered prospect records.
  12. get_sfmc_stage_suppression    — Per-stage suppression analysis (Stages 1-9).
  13. get_sfmc_prospect_outbound_match — DIM_PROSPECT vs RAW_SFMC_PROSPECT_C reconciliation.

CHART TOOLS (13):
  14. chart_smart                   — Universal chart engine (SQL + chart type = any visual).
  15. chart_funnel                  — Funnel bar: Lead → Prospect → Sent → Opened → Clicked.
  16. chart_funnel_waterfall        — Waterfall showing drop-off loss at each funnel stage.
  17. chart_rejections              — Rejection reasons donut chart.
  18. chart_engagement              — SFMC engagement grouped bar by journey & event type.
  19. chart_email_kpi_scorecard     — KPI rates: open/click/bounce/unsub % (horizontal bars).
  20. chart_bounce_analysis         — Hard vs Soft bounce breakdown by journey (grouped bar).
  21. chart_daily_engagement_trend  — Day-by-day SENT/OPEN/CLICK multi-line time series.
  22. chart_journey_stage_progression — Prospects reaching each of the 9 journey stages.
  23. chart_sfmc_stage_fishbone     — Expected vs Sent vs Suppressed per stage on a date.
  24. chart_conversion_segments     — Engagement segment donut + Active vs Inactive donut.
  25. chart_prospect_channel_mix    — Prospect distribution by lead source channel (donut).
  26. chart_intake_trend            — Lead & prospect volume over time (line/area).
"""

from __future__ import annotations

import json
import textwrap
from datetime import timedelta
from typing import Optional

import pandas as pd

from langchain_core.tools import tool

from snowflake_connector import execute_query, execute_query_as_string

# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------

def _run(sql: str, max_rows: int = 100) -> str:
    return execute_query_as_string(sql.strip(), max_rows=max_rows)


# Prospect Journey — 9 stages, 1 journey.
# Stage names use business-friendly labels (no journey code suffixes).
# Physical columns remain unchanged — only presentation names updated.
_SFMC_STAGE_CONFIG = [
    {
        "stage_order": 1,
        "stage_name": "Stage 01 — Welcome Email",
        "phase": "Welcome Phase",
        "prev_stage_date_col": None,  # Stage 01 has no predecessor
        "curr_stage_sent_col": "WELCOMEJOURNEY_WELCOMEEMAIL_SENT",
        "curr_stage_date_col": "WELCOMEJOURNEY_WELCOMEEMAIL_SENT_DATE",
        "interval_days": None,
    },
    {
        "stage_order": 2,
        "stage_name": "Stage 02 — Education Email",
        "phase": "Welcome Phase",
        "prev_stage_date_col": "WELCOMEJOURNEY_WELCOMEEMAIL_SENT_DATE",
        "curr_stage_sent_col": "WELCOMEJOURNEY_EDUCATIONEMAIL_SENT",
        "curr_stage_date_col": "WELCOMEJOURNEY_EDUCATIONEMAIL_SENT_DATE",
        "interval_days": 3,
    },
    {
        "stage_order": 3,
        "stage_name": "Stage 03 — Education Email 1",
        "phase": "Nurture Phase",
        "prev_stage_date_col": "WELCOMEJOURNEY_EDUCATIONEMAIL_SENT_DATE",
        "curr_stage_sent_col": "NURTUREJOURNEY_EDUCATIONEMAIL1_SENT",
        "curr_stage_date_col": "NURTUREJOURNEY_EDUCATIONEMAIL1_SENT_DATE",
        "interval_days": 5,
    },
    {
        "stage_order": 4,
        "stage_name": "Stage 04 — Education Email 2",
        "phase": "Nurture Phase",
        "prev_stage_date_col": "NURTUREJOURNEY_EDUCATIONEMAIL1_SENT_DATE",
        "curr_stage_sent_col": "NURTUREJOURNEY_EDUCATIONEMAIL2_SENT",
        "curr_stage_date_col": "NURTUREJOURNEY_EDUCATIONEMAIL2_SENT_DATE",
        "interval_days": 8,
    },
    {
        "stage_order": 5,
        "stage_name": "Stage 05 — Prospect Story Email",
        "phase": "Nurture Phase",
        "prev_stage_date_col": "NURTUREJOURNEY_EDUCATIONEMAIL2_SENT_DATE",
        "curr_stage_sent_col": "NURTUREJOURNEY_PROSPECTSTORYEMAIL_SENT",
        "curr_stage_date_col": "NURTUREJOURNEY_PROSPECTSTORYEMAIL_SENT_DATE",
        "interval_days": 3,
    },
    {
        "stage_order": 6,
        "stage_name": "Stage 06 — Conversion Email",
        "phase": "High Engagement Phase",
        "prev_stage_date_col": "NURTUREJOURNEY_PROSPECTSTORYEMAIL_SENT_DATE",
        "curr_stage_sent_col": "HIGHENGAGEMENT_CONVERSIONEMAIL_SENT",
        "curr_stage_date_col": "HIGHENGAGEMENT_CONVERSIONEMAIL_SENT_DATE",
        "interval_days": 2,
    },
    {
        "stage_order": 7,
        "stage_name": "Stage 07 — Reminder Email",
        "phase": "High Engagement Phase",
        "prev_stage_date_col": "HIGHENGAGEMENT_CONVERSIONEMAIL_SENT_DATE",
        "curr_stage_sent_col": "HIGHENGAGEMENT_REMINDEREMAIL_SENT",
        "curr_stage_date_col": "HIGHENGAGEMENT_REMINDEREMAIL_SENT_DATE",
        "interval_days": 2,
    },
    {
        "stage_order": 8,
        "stage_name": "Stage 08 — Re-engagement Email",
        "phase": "Low Engagement Phase",
        "prev_stage_date_col": "HIGHENGAGEMENT_REMINDEREMAIL_SENT_DATE",
        "curr_stage_sent_col": "LOWENGAGEMENT_REENGAGEMENTEMAIL_SENT",
        "curr_stage_date_col": "LOWENGAGEMENT_REENGAGEMENTEMAIL_SENT_DATE",
        "interval_days": 2,
    },
    {
        "stage_order": 9,
        "stage_name": "Stage 09 — Final Reminder Email",
        "phase": "Low Engagement Phase",
        "prev_stage_date_col": "LOWENGAGEMENT_REENGAGEMENTEMAIL_SENT_DATE",
        "curr_stage_sent_col": "LOWENGAGEMENTFINALREMINDEREMAIL_SENT",
        "curr_stage_date_col": "LOWENGAGEMENTFINALREMINDEREMAIL_SENT_DATE",
        "interval_days": 2,
    },
]


def _sfmc_bool(value: object) -> bool:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return False
    return str(value).strip().upper() in {"TRUE", "T", "YES", "Y", "1"}


def _sfmc_date(value: object):
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    parsed = pd.to_datetime(value, errors="coerce")
    if pd.isna(parsed):
        return None
    return parsed.date()


def _fetch_sfmc_journey_detail_df(prospect_id: Optional[str] = None) -> pd.DataFrame:
    prospect_filter = f"WHERE PROSPECT_ID = '{prospect_id}'" if prospect_id else ""
    sql = f"""
        SELECT
            PROSPECT_ID,
            SUPPRESSION_FLAG,
            WELCOMEJOURNEY_WELCOMEEMAIL_SENT,
            WELCOMEJOURNEY_WELCOMEEMAIL_SENT_DATE,
            WELCOMEJOURNEY_EDUCATIONEMAIL_SENT,
            WELCOMEJOURNEY_EDUCATIONEMAIL_SENT_DATE,
            NURTUREJOURNEY_EDUCATIONEMAIL1_SENT,
            NURTUREJOURNEY_EDUCATIONEMAIL1_SENT_DATE,
            NURTUREJOURNEY_EDUCATIONEMAIL2_SENT,
            NURTUREJOURNEY_EDUCATIONEMAIL2_SENT_DATE,
            NURTUREJOURNEY_PROSPECTSTORYEMAIL_SENT,
            NURTUREJOURNEY_PROSPECTSTORYEMAIL_SENT_DATE,
            HIGHENGAGEMENT_CONVERSIONEMAIL_SENT,
            HIGHENGAGEMENT_CONVERSIONEMAIL_SENT_DATE,
            HIGHENGAGEMENT_REMINDEREMAIL_SENT,
            HIGHENGAGEMENT_REMINDEREMAIL_SENT_DATE,
            LOWENGAGEMENT_REENGAGEMENTEMAIL_SENT,
            LOWENGAGEMENT_REENGAGEMENTEMAIL_SENT_DATE,
            LOWENGAGEMENTFINALREMINDEREMAIL_SENT,
            LOWENGAGEMENTFINALREMINDEREMAIL_SENT_DATE
        FROM QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_PROSPECT_JOURNEY_DETAILS
        {prospect_filter}
    """
    df = execute_query(sql, max_rows=500000)
    if df is None or df.empty:
        return pd.DataFrame()
    df.columns = [c.upper() for c in df.columns]
    return df


def _compute_sfmc_stage_expectations(df: pd.DataFrame, target_date: str) -> tuple[list[dict], list[dict]]:
    """
    Prospect Journey — 9 stages, stage-aware suppression logic.

    Classification rules per prospect per stage:
    1. Stage 01 has no predecessor — expected if curr_stage_sent_date == target_date.
    2. Stages 02–09: expected if prev_stage_date + interval_days == target_date.
    3. expected_and_sent:       expected AND curr_sent_flag=True AND curr_sent_date==target
    4. expected_and_suppressed: expected AND SUPPRESSION_FLAG=True AND not sent
       (NULL stage values after suppression point are intentional cutoffs, not missing data)
    5. expected_but_not_sent:   expected AND not sent AND not suppressed
    6. not_expected:            previous stage date missing or interval date != target_date
    """
    target = pd.to_datetime(target_date).date()
    stage_rows: list[dict] = []
    drilldown_rows: list[dict] = []

    for cfg in _SFMC_STAGE_CONFIG:
        expected_count = 0
        sent_count = 0
        suppressed_count = 0
        not_sent_count = 0

        for row in df.to_dict(orient="records"):
            current_sent_flag = _sfmc_bool(row.get(cfg["curr_stage_sent_col"]))
            current_sent_date = _sfmc_date(row.get(cfg["curr_stage_date_col"]))
            suppression_flag = _sfmc_bool(row.get("SUPPRESSION_FLAG"))

            # Stage 01: no predecessor — directly check if sent on target date
            if cfg["prev_stage_date_col"] is None:
                if current_sent_date == target:
                    expected_date = target
                    expected_count += 1
                    sent_count += 1
                    classification = "expected_and_sent"
                else:
                    expected_date = None
                    classification = "not_expected"
            else:
                previous_date = _sfmc_date(row.get(cfg["prev_stage_date_col"]))
                expected_date = previous_date + timedelta(days=cfg["interval_days"]) if previous_date else None
                classification = "not_expected"

                if expected_date == target:
                    expected_count += 1
                    if current_sent_flag and current_sent_date == target:
                        sent_count += 1
                        classification = "expected_and_sent"
                    elif suppression_flag:
                        # NULL after suppression = intentional cutoff; count as suppressed
                        suppressed_count += 1
                        classification = "expected_and_suppressed"
                    else:
                        not_sent_count += 1
                        classification = "expected_but_not_sent"

            drilldown_rows.append({
                "prospect_id": row.get("PROSPECT_ID"),
                "stage_order": cfg["stage_order"],
                "stage_name": cfg["stage_name"],
                "phase": cfg.get("phase", ""),
                "previous_stage_sent_date": (
                    _sfmc_date(row.get(cfg["prev_stage_date_col"])).isoformat()
                    if cfg["prev_stage_date_col"] and _sfmc_date(row.get(cfg["prev_stage_date_col"]))
                    else None
                ),
                "expected_date": expected_date.isoformat() if expected_date else None,
                "actual_current_stage_sent_date": current_sent_date.isoformat() if current_sent_date else None,
                "suppression_flag": suppression_flag,
                "classification": classification,
            })

        stage_rows.append({
            "stage_order": cfg["stage_order"],
            "stage_name": cfg["stage_name"],
            "phase": cfg.get("phase", ""),
            "expected_count": expected_count,
            "sent": sent_count,
            "suppressed": suppressed_count,
            "not_sent": not_sent_count,
        })

    return stage_rows, drilldown_rows


# ---------------------------------------------------------------------------
# Tool 1: Raw SQL execution
# ---------------------------------------------------------------------------

@tool
def run_sql(sql: str) -> str:
    """
    Execute any read-only SELECT statement against the FIPSAR Snowflake databases
    and return the result as a formatted markdown table.

    Use this tool when you need a custom query not covered by the other tools.
    Always use fully-qualified table names (DATABASE.SCHEMA.TABLE).
    Only SELECT / WITH ... SELECT statements are permitted.

    Args:
        sql: A valid Snowflake SELECT statement.

    Returns:
        A markdown table of results, or an ERROR message.
    """
    return _run(sql)


# ---------------------------------------------------------------------------
# Tool 2: Funnel metrics
# ---------------------------------------------------------------------------

@tool
def get_funnel_metrics(start_date: str = "2020-01-01", end_date: str = "2099-12-31") -> str:
    """
    Return a full lead-to-engagement funnel summary across the TOP-LEVEL pipeline stages:
    F01 Lead Intake → F02 Mastering (Valid Prospects) → F04 SFMC Sent/Suppressed
    → F05 Delivered → F06 Engagement (Opens, Clicks).

    Use this when the user asks about:
    - Overall funnel performance or conversion rates (with or without a date range)
    - How many leads vs prospects vs SFMC sends
    - Top-level pipeline volume
    - "Why is there a volume drop?" / "What are top rejection reasons?" — with NO date specified
      (call with default params to get all-data view, then the user can narrow to a date range)

    When no date is specified by the user, call with defaults (start_date="2020-01-01",
    end_date="2099-12-31") to return the full dataset — do NOT substitute today's date.

    CRITICAL — DO NOT use this tool when the user asks about:
    - "Where are prospects dropping off in the journey?"
    - "Which stage has the biggest drop-off?"
    - "Stage-by-stage suppression"
    - Anything about Stage 01 through Stage 09 within the Prospect Journey
    For those questions use get_journey_stage_dropoff instead.

    Args:
        start_date: Inclusive start date in YYYY-MM-DD format (default: no lower bound).
        end_date:   Inclusive end date in YYYY-MM-DD format (default: no upper bound).

    Returns:
        Top-level funnel stage metrics as a markdown table.
    """
    # STG_PROSPECT_INTAKE.FILE_DATE is VARCHAR with mixed formats:
    #   'YYYY-MM-DD' (bulk historical data) and 'DD-MM-YYYY' (recent campaign files).
    # Must use COALESCE(TRY_TO_DATE(...,'YYYY-MM-DD'), TRY_TO_DATE(...,'DD-MM-YYYY'))
    # for correct date range filtering. Plain BETWEEN on the raw varchar fails for DD-MM-YYYY
    # records because alphabetical sort puts '05-04-2026' before '2026-...' strings.
    stg_date_filter = (
        f"COALESCE(TRY_TO_DATE(FILE_DATE::STRING,'YYYY-MM-DD'),"
        f"TRY_TO_DATE(FILE_DATE::STRING,'DD-MM-YYYY'))"
        f" BETWEEN '{start_date}' AND '{end_date}'"
    )
    raw_event_date_filter = (
        f"TRY_TO_DATE(SPLIT(EVENT_DATE, ' ')[0]::STRING,'MM/DD/YYYY') "
        f"BETWEEN '{start_date}' AND '{end_date}'"
    )
    sql = textwrap.dedent(f"""
        WITH
        leads AS (
            SELECT COUNT(*) AS lead_count
            FROM QA_FIPSAR_PHI_HUB.STAGING.STG_PROSPECT_INTAKE
            WHERE {stg_date_filter}
        ),
        invalid_leads AS (
            -- Compute arithmetically: leads that did NOT make it to PHI_PROSPECT_MASTER.
            -- REJECTED_AT in DQ_REJECTION_LOG reflects pipeline processing time, which
            -- may differ from FILE_DATE, so arithmetic is the reliable source of truth.
            SELECT
                (SELECT COUNT(*) FROM QA_FIPSAR_PHI_HUB.STAGING.STG_PROSPECT_INTAKE
                 WHERE {stg_date_filter})
                -
                (SELECT COUNT(*) FROM QA_FIPSAR_PHI_HUB.PHI_CORE.PHI_PROSPECT_MASTER
                 WHERE FILE_DATE BETWEEN '{start_date}' AND '{end_date}')
            AS invalid_lead_count
        ),
        prospects AS (
            SELECT COUNT(*) AS prospect_count
            FROM QA_FIPSAR_PHI_HUB.PHI_CORE.PHI_PROSPECT_MASTER
            WHERE FILE_DATE BETWEEN '{start_date}' AND '{end_date}'
        ),
        prospect_facts AS (
            SELECT COUNT(*) AS prospect_intake_events
            FROM QA_FIPSAR_DW.GOLD.FACT_PROSPECT_INTAKE fi
            JOIN QA_FIPSAR_DW.GOLD.DIM_DATE d ON fi.DATE_KEY = d.DATE_KEY
            WHERE d.FULL_DATE BETWEEN '{start_date}' AND '{end_date}'
        ),
        sent AS (
            WITH gold AS (
                SELECT COUNT(*) AS cnt
                FROM QA_FIPSAR_DW.GOLD.FACT_SFMC_ENGAGEMENT fe
                WHERE fe.EVENT_TYPE = 'SENT'
                  AND DATE(fe.EVENT_TIMESTAMP) BETWEEN '{start_date}' AND '{end_date}'
            ),
            raw AS (
                SELECT COUNT(*) AS cnt
                FROM QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_SENT
                WHERE {raw_event_date_filter}
            )
            SELECT CASE WHEN gold.cnt > 0 THEN gold.cnt ELSE raw.cnt END AS sent_count
            FROM gold, raw
        ),
        opens AS (
            WITH gold AS (
                SELECT
                    COUNT(*) AS open_count,
                    SUM(CASE WHEN IS_UNIQUE = 1 THEN 1 ELSE 0 END) AS unique_open_count
                FROM QA_FIPSAR_DW.GOLD.FACT_SFMC_ENGAGEMENT fe
                WHERE fe.EVENT_TYPE = 'OPEN'
                  AND DATE(fe.EVENT_TIMESTAMP) BETWEEN '{start_date}' AND '{end_date}'
            ),
            raw AS (
                SELECT
                    COUNT(*) AS open_count,
                    COUNT(DISTINCT SUBSCRIBER_KEY) AS unique_open_count
                FROM QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_OPENS
                WHERE {raw_event_date_filter}
            )
            SELECT
                CASE WHEN gold.open_count > 0 THEN gold.open_count ELSE raw.open_count END AS open_count,
                CASE WHEN COALESCE(gold.unique_open_count, 0) > 0 THEN gold.unique_open_count ELSE raw.unique_open_count END AS unique_open_count
            FROM gold, raw
        ),
        clicks AS (
            WITH gold AS (
                SELECT
                    COUNT(*) AS click_count,
                    SUM(CASE WHEN IS_UNIQUE = 1 THEN 1 ELSE 0 END) AS unique_click_count
                FROM QA_FIPSAR_DW.GOLD.FACT_SFMC_ENGAGEMENT fe
                WHERE fe.EVENT_TYPE = 'CLICK'
                  AND DATE(fe.EVENT_TIMESTAMP) BETWEEN '{start_date}' AND '{end_date}'
            ),
            raw AS (
                SELECT
                    COUNT(*) AS click_count,
                    COUNT(DISTINCT SUBSCRIBER_KEY) AS unique_click_count
                FROM QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_CLICKS
                WHERE {raw_event_date_filter}
            )
            SELECT
                CASE WHEN gold.click_count > 0 THEN gold.click_count ELSE raw.click_count END AS click_count,
                CASE WHEN COALESCE(gold.unique_click_count, 0) > 0 THEN gold.unique_click_count ELSE raw.unique_click_count END AS unique_click_count
            FROM gold, raw
        ),
        bounces AS (
            WITH gold AS (
                SELECT COUNT(*) AS cnt
                FROM QA_FIPSAR_DW.GOLD.FACT_SFMC_ENGAGEMENT fe
                WHERE fe.EVENT_TYPE = 'BOUNCE'
                  AND DATE(fe.EVENT_TIMESTAMP) BETWEEN '{start_date}' AND '{end_date}'
            ),
            raw AS (
                SELECT COUNT(*) AS cnt
                FROM QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_BOUNCES
                WHERE {raw_event_date_filter}
            )
            SELECT CASE WHEN gold.cnt > 0 THEN gold.cnt ELSE raw.cnt END AS bounce_count
            FROM gold, raw
        ),
        unsubs AS (
            WITH gold AS (
                SELECT COUNT(*) AS cnt
                FROM QA_FIPSAR_DW.GOLD.FACT_SFMC_ENGAGEMENT fe
                WHERE fe.EVENT_TYPE = 'UNSUBSCRIBE'
                  AND DATE(fe.EVENT_TIMESTAMP) BETWEEN '{start_date}' AND '{end_date}'
            ),
            raw AS (
                SELECT COUNT(*) AS cnt
                FROM QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_UNSUBSCRIBES
                WHERE {raw_event_date_filter}
            )
            SELECT CASE WHEN gold.cnt > 0 THEN gold.cnt ELSE raw.cnt END AS unsubscribe_count
            FROM gold, raw
        ),
        suppressed AS (
            SELECT COUNT(*) AS suppressed_count
            FROM QA_FIPSAR_AUDIT.PIPELINE_AUDIT.DQ_REJECTION_LOG
            WHERE UPPER(REJECTION_REASON) IN ('SUPPRESSED_PROSPECT', 'FATAL_ERROR', 'SUPPRESSED')
              AND CAST(REJECTED_AT AS DATE) BETWEEN '{start_date}' AND '{end_date}'
        )
        SELECT
            l.lead_count                                                    AS "F01 Leads Intake",
            il.invalid_lead_count                                           AS "F02 Invalid Leads",
            p.prospect_count                                                AS "F02 Valid Prospects",
            ROUND(p.prospect_count * 100.0 / NULLIF(l.lead_count, 0), 2)  AS "Lead->Prospect Conv%",
            pf.prospect_intake_events                                       AS "F03 Intake Events",
            s.sent_count                                                    AS "F04 Sent",
            sup.suppressed_count                                            AS "F04 Suppressed",
            (s.sent_count - b.bounce_count)                                AS "F05 Estimated Delivered",
            b.bounce_count                                                  AS "F05 Bounces",
            o.open_count                                                    AS "F06 Opens",
            o.unique_open_count                                             AS "F06 Unique Opens",
            c.click_count                                                   AS "F06 Clicks",
            c.unique_click_count                                            AS "F06 Unique Clicks",
            u.unsubscribe_count                                             AS "F06 Unsubscribes",
            ROUND(o.open_count * 100.0 / NULLIF(s.sent_count - b.bounce_count, 0), 2) AS "Open Rate%",
            ROUND(c.click_count * 100.0 / NULLIF(s.sent_count - b.bounce_count, 0), 2) AS "Click Rate%"
        FROM leads l, invalid_leads il, prospects p, prospect_facts pf,
             sent s, opens o, clicks c, bounces b, unsubs u, suppressed sup
    """)
    return _run(sql, max_rows=10)


# ---------------------------------------------------------------------------
# Tool 3: Rejection / DQ analysis
# ---------------------------------------------------------------------------

@tool
def get_rejection_analysis(
    start_date: str = "2020-01-01",
    end_date: str = "2099-12-31",
    rejection_reason: Optional[str] = None,
    rejection_category: str = "all",
) -> str:
    """
    Show rejected record counts broken down by reason.

    CRITICAL DISTINCTION — two entirely separate rejection categories exist:

    1. rejection_category="intake"  → Lead-to-Prospect mastering rejections.
       These are leads that FAILED validation and never became Prospects.
       Reasons: NULL_EMAIL, NO_CONSENT, NULL_FIRST_NAME, NULL_LAST_NAME, NULL_PHONE_NUMBER.
       Source table in the log: QA_FIPSAR_PHI_HUB.STAGING.STG_PROSPECT_INTAKE or PHI_PROSPECT_MASTER.
       USE THIS when the user asks about: lead rejection reasons, why leads didn't convert,
       invalid leads, leads that failed mastering.

    2. rejection_category="sfmc"    → SFMC send suppression outcomes.
       These are valid Prospects whose email SEND was blocked or errored.
       Reasons: SUPPRESSED, FATAL_ERROR.
       USE THIS when the user asks about: suppressed sends, fatal errors, SFMC failures,
       send-level drops.

    3. rejection_category="all"     → Everything (default, use only when comparing both).

    DO NOT mix intake rejections with SFMC suppressions when answering funnel questions
    about lead-to-prospect conversion — they are completely different pipeline stages.

    Args:
        start_date:          Start of date range (YYYY-MM-DD).
        end_date:            End of date range (YYYY-MM-DD).
        rejection_reason:    Optional specific reason filter.
        rejection_category:  "intake", "sfmc", or "all".

    Returns:
        Rejection summary as a markdown table with counts per reason.
    """
    reason_filter = (
        f"AND UPPER(REJECTION_REASON) = '{rejection_reason.upper()}'"
        if rejection_reason
        else ""
    )
    if rejection_category == "intake":
        # Intake mastering rejections only — exclude SFMC suppression outcomes
        category_filter = "AND UPPER(REJECTION_REASON) NOT IN ('SUPPRESSED_PROSPECT', 'FATAL_ERROR', 'SUPPRESSED')"
    elif rejection_category == "sfmc":
        # SFMC suppression / fatal outcomes only
        category_filter = "AND UPPER(REJECTION_REASON) IN ('SUPPRESSED_PROSPECT', 'FATAL_ERROR', 'SUPPRESSED')"
    else:
        category_filter = ""
    sql = textwrap.dedent(f"""
        SELECT
            COALESCE(
                TRY_TO_DATE(TRY_PARSE_JSON(REJECTED_RECORD):FILE_DATE::STRING, 'YYYY-MM-DD'),
                TRY_TO_DATE(TRY_PARSE_JSON(REJECTED_RECORD):FILE_DATE::STRING, 'DD-MM-YYYY'),
                CAST(REJECTED_AT AS DATE)
            )                                           AS lead_file_date,
            TABLE_NAME,
            REJECTION_REASON,
            COUNT(*)                                    AS rejected_count,
            LISTAGG(DISTINCT
                TRY_PARSE_JSON(REJECTED_RECORD):EMAIL::STRING, ', ')
                WITHIN GROUP (ORDER BY TRY_PARSE_JSON(REJECTED_RECORD):EMAIL::STRING) AS sample_emails
        FROM QA_FIPSAR_AUDIT.PIPELINE_AUDIT.DQ_REJECTION_LOG
        WHERE
            (
                COALESCE(
                    TRY_TO_DATE(TRY_PARSE_JSON(REJECTED_RECORD):FILE_DATE::STRING, 'YYYY-MM-DD'),
                    TRY_TO_DATE(TRY_PARSE_JSON(REJECTED_RECORD):FILE_DATE::STRING, 'DD-MM-YYYY')
                ) BETWEEN '{start_date}' AND '{end_date}'
            )
            OR (
                COALESCE(
                    TRY_TO_DATE(TRY_PARSE_JSON(REJECTED_RECORD):FILE_DATE::STRING, 'YYYY-MM-DD'),
                    TRY_TO_DATE(TRY_PARSE_JSON(REJECTED_RECORD):FILE_DATE::STRING, 'DD-MM-YYYY')
                ) IS NULL
                AND CAST(REJECTED_AT AS DATE) BETWEEN '{start_date}' AND '{end_date}'
            )
          {reason_filter}
          {category_filter}
        GROUP BY 1, 2, 3
        ORDER BY 1 DESC, 4 DESC
        LIMIT 100
    """)
    return _run(sql)


# ---------------------------------------------------------------------------
# Tool 4: SFMC engagement stats
# ---------------------------------------------------------------------------

@tool
def get_sfmc_engagement_stats(
    start_date: str = "2020-01-01",
    end_date: str = "2099-12-31",
    journey_type: Optional[str] = None,
) -> str:
    """
    Return SFMC engagement event counts broken down by journey type and event type
    (SENT, OPEN, CLICK, BOUNCE, UNSUBSCRIBE, SPAM, UNSENT).

    Use this when the user asks about:
    - Email performance by journey or stage
    - Open / click / bounce rates per journey
    - Which journey has the most engagement or drop-off
    - SFMC event trends
    - All emails sent, opened, clicked, bounced, suppressed across journeys

    Args:
        start_date:   Start of date range (YYYY-MM-DD).
        end_date:     End of date range (YYYY-MM-DD).
        journey_type: Optional journey name filter, e.g. 'Welcome', 'Nurture',
                      'Conversion', 'ReEngagement'.

    Returns:
        Engagement stats by journey and event type as a markdown table.
    """
    journey_filter_gold = (
        f"AND UPPER(j.JOURNEY_TYPE) LIKE '%{journey_type.upper()}%'"
        if journey_type else ""
    )
    journey_filter_raw = (
        f"AND UPPER(m.JOURNEY_TYPE) LIKE '%{journey_type.upper()}%'"
        if journey_type else ""
    )

    # ------------------------------------------------------------------
    # PATH A: FACT_SFMC_ENGAGEMENT — filter by EVENT_TIMESTAMP directly.
    # NOTE: The DIM_DATE join via DATE_KEY is unreliable (surrogate key
    # mismatches return 0 rows).  Always use EVENT_TIMESTAMP for date range.
    # ------------------------------------------------------------------
    gold_check = execute_query_as_string(
        f"""SELECT COUNT(*) AS cnt FROM QA_FIPSAR_DW.GOLD.FACT_SFMC_ENGAGEMENT
            WHERE DATE(EVENT_TIMESTAMP) BETWEEN '{start_date}' AND '{end_date}'""",
        max_rows=1,
    )
    gold_rows = 0
    try:
        for line in gold_check.splitlines():
            if "|" in line and "cnt" not in line.lower() and "---" not in line:
                val = line.strip().strip("|").strip()
                if val.isdigit():
                    gold_rows = int(val)
                    break
    except Exception:
        pass

    if gold_rows > 0:
        sql = textwrap.dedent(f"""
            SELECT
                COALESCE(j.JOURNEY_TYPE, 'Unknown Journey')  AS journey_type,
                COALESCE(j.MAPPED_STAGE, 'Unknown Stage')    AS stage,
                fe.EVENT_TYPE,
                COUNT(*)                                      AS event_count,
                SUM(CASE WHEN fe.IS_UNIQUE = 1 THEN 1 ELSE 0 END) AS unique_event_count,
                MIN(fe.EVENT_TIMESTAMP)                       AS first_event,
                MAX(fe.EVENT_TIMESTAMP)                       AS last_event
            FROM QA_FIPSAR_DW.GOLD.FACT_SFMC_ENGAGEMENT fe
            LEFT JOIN QA_FIPSAR_DW.GOLD.DIM_SFMC_JOB j ON fe.JOB_KEY = j.JOB_KEY
            WHERE DATE(fe.EVENT_TIMESTAMP) BETWEEN '{start_date}' AND '{end_date}'
              {journey_filter_gold}
            GROUP BY 1, 2, 3
            ORDER BY 1, 2, 3
            LIMIT 200
        """)
        result = _run(sql)
        return f"**Source: FACT_SFMC_ENGAGEMENT (gold table — {gold_rows:,} events in range)**\n\n{result}"

    # ------------------------------------------------------------------
    # PATH B: Raw SFMC event tables — fallback when gold table is empty
    # or has no rows for the requested date range.
    # ------------------------------------------------------------------
    raw_sql = textwrap.dedent(f"""
        WITH raw_events AS (
            SELECT 'SENT'        AS event_type, SUBSCRIBER_KEY, JOB_ID
              FROM QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_SENT
            UNION ALL
            SELECT 'OPEN',        SUBSCRIBER_KEY, JOB_ID
              FROM QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_OPENS
            UNION ALL
            SELECT 'CLICK',       SUBSCRIBER_KEY, JOB_ID
              FROM QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_CLICKS
            UNION ALL
            SELECT 'BOUNCE',      SUBSCRIBER_KEY, JOB_ID
              FROM QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_BOUNCES
            UNION ALL
            SELECT 'UNSUBSCRIBE', SUBSCRIBER_KEY, JOB_ID
              FROM QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_UNSUBSCRIBES
            UNION ALL
            SELECT 'SPAM',        SUBSCRIBER_KEY, JOB_ID
              FROM QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_SPAM
        )
        SELECT
            COALESCE(m.JOURNEY_TYPE, 'Unknown Journey')  AS journey_type,
            COALESCE(m.MAPPED_STAGE, 'Unknown Stage')    AS stage,
            e.event_type,
            COUNT(*)                                     AS event_count,
            COUNT(DISTINCT e.SUBSCRIBER_KEY)             AS unique_subscribers
        FROM raw_events e
        LEFT JOIN QA_FIPSAR_DW.GOLD.DIM_SFMC_JOB m ON e.JOB_ID = m.JOB_ID
        WHERE 1=1
          {journey_filter_raw}
        GROUP BY 1, 2, 3
        ORDER BY 1, 2, 3
        LIMIT 200
    """)
    raw_result = _run(raw_sql)

    # Also add suppressed/fatal counts from DQ_REJECTION_LOG
    supp_sql = textwrap.dedent(f"""
        SELECT
            REJECTION_REASON                AS suppression_type,
            COUNT(*)                        AS count,
            MIN(CAST(REJECTED_AT AS DATE))  AS first_seen,
            MAX(CAST(REJECTED_AT AS DATE))  AS last_seen
        FROM QA_FIPSAR_AUDIT.PIPELINE_AUDIT.DQ_REJECTION_LOG
        WHERE UPPER(REJECTION_REASON) IN ('SUPPRESSED_PROSPECT', 'FATAL_ERROR', 'SUPPRESSED')
          AND (
              TRY_TO_DATE(TRY_PARSE_JSON(REJECTED_RECORD):FILE_DATE::STRING)
                  BETWEEN '{start_date}' AND '{end_date}'
              OR CAST(REJECTED_AT AS DATE) BETWEEN '{start_date}' AND '{end_date}'
          )
        GROUP BY 1
        ORDER BY 2 DESC
    """)
    supp_result = _run(supp_sql)

    parts = [
        "**Source: Raw SFMC event tables (FACT_SFMC_ENGAGEMENT had no rows for this date range)**\n",
        "### Engagement Events by Journey / Stage\n" + raw_result,
        "### Suppression & Fatal Issues (DQ_REJECTION_LOG)\n" + supp_result,
    ]
    return "\n\n".join(parts)


# ---------------------------------------------------------------------------
# Tool 5: Date-specific drop analysis
# ---------------------------------------------------------------------------

@tool
def get_drop_analysis(target_date: str) -> str:
    """
    Investigate why prospect or send volume dropped on a SPECIFIC date.
    Returns intake counts, rejection counts by reason, SFMC sent vs suppressed,
    and any pipeline errors logged on that date.

    ONLY call this tool when the user has explicitly mentioned a specific date
    (e.g. 'yesterday', 'April 15', '2026-04-10') in their question.

    DO NOT call this tool when:
    - The user asks a general question with NO date mentioned (e.g. "why is volume down?",
      "what are the top reasons for volume drop?") — use get_funnel_metrics instead.
    - The user asks about a date range — use get_funnel_metrics with start_date/end_date.

    Use this only for:
    - "Why is there a drop on [SPECIFIC DATE]?"
    - "What happened on [SPECIFIC DATE]?"
    - "Why did we see fewer prospects on [SPECIFIC DATE]?"

    Args:
        target_date: The SPECIFIC date to investigate (YYYY-MM-DD). Must be provided
                     by the user — never infer or default to today's date.

    Returns:
        Multi-signal drop diagnosis as markdown tables.
    """
    sql = textwrap.dedent(f"""
        -- Intake vs mastering on target date
        -- FILE_DATE is VARCHAR with mixed formats (YYYY-MM-DD and DD-MM-YYYY) — must parse both
        SELECT 'Lead Intake' AS signal,
               COUNT(*) AS count,
               '{target_date}' AS date
        FROM QA_FIPSAR_PHI_HUB.STAGING.STG_PROSPECT_INTAKE
        WHERE COALESCE(TRY_TO_DATE(FILE_DATE::STRING,'YYYY-MM-DD'),
                       TRY_TO_DATE(FILE_DATE::STRING,'DD-MM-YYYY')) = '{target_date}'

        UNION ALL

        SELECT 'Valid Prospects Mastered' AS signal,
               COUNT(*) AS count,
               '{target_date}' AS date
        FROM QA_FIPSAR_PHI_HUB.PHI_CORE.PHI_PROSPECT_MASTER
        WHERE FILE_DATE = '{target_date}'

        UNION ALL

        SELECT 'Rejected Leads - ' || REJECTION_REASON AS signal,
               COUNT(*) AS count,
               '{target_date}' AS date
        FROM QA_FIPSAR_AUDIT.PIPELINE_AUDIT.DQ_REJECTION_LOG
        WHERE CAST(REJECTED_AT AS DATE) = '{target_date}'
          AND TABLE_NAME LIKE '%STG_PROSPECT_INTAKE%'
        GROUP BY REJECTION_REASON

        UNION ALL

        SELECT 'SFMC - ' || fe.EVENT_TYPE AS signal,
               COUNT(*) AS count,
               '{target_date}' AS date
        FROM QA_FIPSAR_DW.GOLD.FACT_SFMC_ENGAGEMENT fe
        WHERE DATE(fe.EVENT_TIMESTAMP) = '{target_date}'
        GROUP BY fe.EVENT_TYPE

        UNION ALL

        SELECT 'SFMC Suppression - ' || REJECTION_REASON AS signal,
               COUNT(*) AS count,
               '{target_date}' AS date
        FROM QA_FIPSAR_AUDIT.PIPELINE_AUDIT.DQ_REJECTION_LOG
        WHERE UPPER(REJECTION_REASON) IN ('SUPPRESSED_PROSPECT', 'FATAL_ERROR', 'SUPPRESSED')
          AND CAST(REJECTED_AT AS DATE) = '{target_date}'
        GROUP BY REJECTION_REASON

        ORDER BY signal
    """)
    return _run(sql, max_rows=50)


# ---------------------------------------------------------------------------
# Tool 6: Prospect lineage trace
# ---------------------------------------------------------------------------

@tool
def trace_prospect(identifier: str) -> str:
    """
    Trace a specific prospect or lead through the entire FIPSAR pipeline:
    from intake → mastering → identity bridge → engagement events.

    Use this when the user asks:
    - "What happened to prospect X?"
    - "Show me the journey for email abc@example.com"
    - "Trace MASTER_PATIENT_ID P12345 through the pipeline"

    Args:
        identifier: Email address OR MASTER_PATIENT_ID of the prospect/lead.

    Returns:
        Pipeline trace results as markdown tables showing each layer.
    """
    # Determine if the identifier looks like an email or an ID
    is_email = "@" in identifier

    if is_email:
        match_clause_intake  = f"LOWER(EMAIL) = LOWER('{identifier}')"
        match_clause_master  = f"LOWER(EMAIL) = LOWER('{identifier}')"
        match_clause_xref    = f"LOWER(EMAIL) = LOWER('{identifier}')"
    else:
        # Treat as MASTER_PATIENT_ID
        match_clause_intake  = f"LOWER(FIRST_NAME) || ' ' || LOWER(LAST_NAME) LIKE '%{identifier.lower()}%'"
        match_clause_master  = f"MASTER_PATIENT_ID = '{identifier}'"
        match_clause_xref    = f"MASTER_PATIENT_ID = '{identifier}'"

    sql = textwrap.dedent(f"""
        -- Step 1: Intake / Lead layer
        SELECT 'A_Intake' AS layer, EMAIL, FIRST_NAME, LAST_NAME,
               CHANNEL, SUBMISSION_TIMESTAMP::STRING AS ts,
               'Lead' AS lifecycle_label
        FROM QA_FIPSAR_PHI_HUB.STAGING.STG_PROSPECT_INTAKE
        WHERE {match_clause_intake}

        UNION ALL

        -- Step 2: Rejection log
        SELECT 'B_Rejected' AS layer,
               TRY_PARSE_JSON(REJECTED_RECORD):EMAIL::STRING AS EMAIL,
               TRY_PARSE_JSON(REJECTED_RECORD):FIRST_NAME::STRING AS FIRST_NAME,
               TRY_PARSE_JSON(REJECTED_RECORD):LAST_NAME::STRING AS LAST_NAME,
               REJECTION_REASON AS CHANNEL,
               REJECTED_AT::STRING AS ts,
               'Invalid Lead' AS lifecycle_label
        FROM QA_FIPSAR_AUDIT.PIPELINE_AUDIT.DQ_REJECTION_LOG
        WHERE LOWER(TRY_PARSE_JSON(REJECTED_RECORD):EMAIL::STRING) = LOWER('{identifier if is_email else ""}')

        UNION ALL

        -- Step 3: Mastered Prospect
        SELECT 'C_Prospect' AS layer, EMAIL, FIRST_NAME, LAST_NAME,
               CHANNEL, SUBMISSION_TIMESTAMP::STRING AS ts,
               'Prospect' AS lifecycle_label
        FROM QA_FIPSAR_PHI_HUB.PHI_CORE.PHI_PROSPECT_MASTER
        WHERE {match_clause_master}

        UNION ALL

        -- Step 4: Identity crosswalk (SFMC bridge)
        SELECT 'D_Identity_Xref' AS layer, EMAIL, FIRST_NAME, LAST_NAME,
               IDENTITY_KEY AS CHANNEL, NULL AS ts,
               'Identity Bridge' AS lifecycle_label
        FROM QA_FIPSAR_PHI_HUB.PHI_CORE.PATIENT_IDENTITY_XREF
        WHERE {match_clause_xref}

        ORDER BY layer
        LIMIT 50
    """)
    base_result = _run(sql, max_rows=50)

    # Step 5: Pull engagement events — SUBSCRIBER_KEY IS the MASTER_PATIENT_ID (FIP... format).
    # Join directly to PHI_PROSPECT_MASTER or DIM_PROSPECT — no PATIENT_IDENTITY_XREF needed.
    if is_email:
        sub_filter = f"LOWER(p.EMAIL) = LOWER('{identifier}')"
    else:
        sub_filter = f"p.MASTER_PATIENT_ID = '{identifier}'"

    engagement_sql = textwrap.dedent(f"""
        SELECT fe.EVENT_TYPE, fe.EVENT_TIMESTAMP, j.JOURNEY_TYPE, j.MAPPED_STAGE,
               fe.SUBSCRIBER_KEY, fe.IS_SUPPRESSED, fe.SUPPRESSION_REASON
        FROM QA_FIPSAR_PHI_HUB.PHI_CORE.PHI_PROSPECT_MASTER p
        JOIN QA_FIPSAR_DW.GOLD.FACT_SFMC_ENGAGEMENT fe
             ON fe.SUBSCRIBER_KEY = p.MASTER_PATIENT_ID
        LEFT JOIN QA_FIPSAR_DW.GOLD.DIM_SFMC_JOB j ON fe.JOB_KEY = j.JOB_KEY
        WHERE {sub_filter}
        ORDER BY fe.EVENT_TIMESTAMP
        LIMIT 50
    """)
    engagement_result = _run(engagement_sql, max_rows=50)

    return (
        "### Pipeline Trace — Intake / Mastering / Identity\n"
        + base_result
        + "\n\n### Pipeline Trace — SFMC Engagement Events\n"
        + engagement_result
    )


# ---------------------------------------------------------------------------
# Tool 7: AI intelligence — schema-safe, column-discovery based
# ---------------------------------------------------------------------------

def _discover_columns(full_table_name: str) -> list[str]:
    """Return actual column names for a table via INFORMATION_SCHEMA."""
    db, schema, table = full_table_name.split(".")
    sql = f"""
        SELECT COLUMN_NAME
        FROM {db}.INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = '{schema}'
          AND TABLE_NAME   = '{table}'
        ORDER BY ORDINAL_POSITION
    """
    result = execute_query_as_string(sql, max_rows=200)
    if result.startswith("ERROR") or "no rows" in result.lower():
        return []
    # Parse column names out of the markdown table
    cols = []
    for line in result.splitlines():
        line = line.strip()
        if line.startswith("|") and "COLUMN_NAME" not in line and "---" not in line:
            col = line.strip("|").strip()
            if col:
                cols.append(col)
    return cols


@tool
def get_ai_intelligence() -> str:
    """
    Discover the actual columns in all FIPSAR AI tables and return a
    representative sample of data from each table.

    Use this when the user asks about:
    - AI outcomes, AI scores, or model results
    - What AI data is available
    - Conversion probability, drop-off probability, suppression risk
    - Signal trust or send-time optimization scores

    This tool first discovers real column names, then queries each table safely.

    Returns:
        Schema + sample rows from each AI table as markdown.
    """
    ai_tables = [
        "QA_FIPSAR_AI.AI_SEMANTIC.SEM_UCA_PROSPECT_360_SCORES",
        "QA_FIPSAR_AI.AI_SEMANTIC.SEM_UCB_SIGNAL_TRUST_SCORES",
        "QA_FIPSAR_AI.AI_SEMANTIC.SEM_UC03_SEND_TIME_SCORES",
        "QA_FIPSAR_AI.AI_FEATURES.FEAT_UCA_PROSPECT_360",
        "QA_FIPSAR_AI.AI_FEATURES.FEAT_UC03_SEND_TIME",
    ]
    output_parts = []
    for tbl in ai_tables:
        cols = _discover_columns(tbl)
        if not cols:
            output_parts.append(f"### {tbl}\n_Table not accessible or empty._\n")
            continue
        col_list = ", ".join(cols)
        count_sql  = f"SELECT COUNT(*) AS total_rows FROM {tbl}"
        sample_sql = f"SELECT {col_list} FROM {tbl} LIMIT 5"
        count_result  = execute_query_as_string(count_sql, max_rows=1)
        sample_result = execute_query_as_string(sample_sql, max_rows=5)
        output_parts.append(
            f"### {tbl}\n"
            f"**Columns:** {col_list}\n\n"
            f"**Row count:** {count_result}\n\n"
            f"**Sample rows:**\n{sample_result}\n"
        )
    return "\n\n".join(output_parts)


@tool
def get_prospect_conversion_analysis(
    start_date: str = "2020-01-01",
    end_date: str = "2099-12-31",
    active_only: bool = True,
) -> str:
    """
    Compute conversion probability and drop-off risk for active Prospects.

    Uses a 3-path strategy — always returns data regardless of which tables are populated:
      Path A: FACT_SFMC_ENGAGEMENT (engagement-based scores — richest signals)
      Path B: RAW_SFMC event tables (if gold engagement table is empty)
      Path C: PHI_PROSPECT_MASTER + DQ_REJECTION_LOG (always available — intake signals)

    Derived metrics:
      conversion_signal_score  (0–100): weighted click + open intensity
      dropoff_risk_score       (0–100): weighted bounce + unsub + suppression signals
      engagement_segment: High Engagement / Mid Engagement / At Risk / Low / No Activity

    Use this for:
    - "What is the conversion probability for active prospects?"
    - "What is the drop-off probability?"
    - "Which prospects are at risk of dropping off?"
    - "Show AI-level insights on active prospects"
    - "Who is most/least likely to convert?"

    Args:
        start_date:  Start of date range (YYYY-MM-DD).
        end_date:    End of date range (YYYY-MM-DD).
        active_only: Restrict to IS_ACTIVE = True prospects (default: True).

    Returns:
        Segment summary + ranked individual prospect scores.
    """
    active_filter = "AND p.IS_ACTIVE = TRUE" if active_only else ""
    parts = []

    # ------------------------------------------------------------------
    # PATH A: Gold engagement table — try without DIM_DATE join
    # Use EVENT_TIMESTAMP directly to avoid broken DATE_KEY → DIM_DATE join
    # ------------------------------------------------------------------
    path_a_check = execute_query_as_string(
        "SELECT COUNT(*) AS cnt FROM QA_FIPSAR_DW.GOLD.FACT_SFMC_ENGAGEMENT", max_rows=1
    )
    engagement_rows = 0
    try:
        for line in path_a_check.splitlines():
            if "|" in line and "cnt" not in line and "---" not in line:
                val = line.strip("|").strip()
                if val.isdigit():
                    engagement_rows = int(val)
                    break
    except Exception:
        pass

    if engagement_rows > 0:
        seg_sql = textwrap.dedent(f"""
            WITH pe AS (
                -- SUBSCRIBER_KEY in FACT_SFMC_ENGAGEMENT IS the MASTER_PATIENT_ID (FIP... format).
                -- Join directly — no PATIENT_IDENTITY_XREF needed.
                SELECT
                    p.MASTER_PATIENT_ID,
                    p.FIRST_NAME, p.LAST_NAME, p.EMAIL, p.CHANNEL,
                    p.FILE_DATE AS intake_date,
                    COUNT(CASE WHEN fe.EVENT_TYPE = 'SENT'        THEN 1 END) AS sends,
                    COUNT(CASE WHEN fe.EVENT_TYPE = 'OPEN'        THEN 1 END) AS opens,
                    COUNT(CASE WHEN fe.EVENT_TYPE = 'CLICK'       THEN 1 END) AS clicks,
                    COUNT(CASE WHEN fe.EVENT_TYPE = 'BOUNCE'      THEN 1 END) AS bounces,
                    COUNT(CASE WHEN fe.EVENT_TYPE = 'UNSUBSCRIBE' THEN 1 END) AS unsubscribes,
                    COUNT(CASE WHEN fe.EVENT_TYPE = 'SPAM'        THEN 1 END) AS spam_complaints,
                    MAX(fe.EVENT_TIMESTAMP)                                    AS last_event_ts
                FROM QA_FIPSAR_PHI_HUB.PHI_CORE.PHI_PROSPECT_MASTER p
                JOIN QA_FIPSAR_DW.GOLD.FACT_SFMC_ENGAGEMENT fe
                     ON fe.SUBSCRIBER_KEY = p.MASTER_PATIENT_ID
                WHERE fe.EVENT_TIMESTAMP BETWEEN '{start_date}' AND '{end_date}'
                  {active_filter}
                GROUP BY 1,2,3,4,5,6
            )
            SELECT
                MASTER_PATIENT_ID        AS master_prospect_id,
                FIRST_NAME, LAST_NAME, EMAIL, CHANNEL, intake_date,
                sends, opens, clicks, bounces, unsubscribes,
                ROUND((opens + clicks) * 100.0 / NULLIF(sends,0), 1)       AS engagement_rate_pct,
                ROUND(LEAST(100,(clicks*2.0 + opens*1.0)/NULLIF(sends,0)*50),1) AS conversion_signal_score,
                ROUND(LEAST(100,(bounces*1.5 + unsubscribes*2.0 + spam_complaints*3.0)/NULLIF(sends,0)*50),1) AS dropoff_risk_score,
                CASE
                    WHEN clicks > 0                           THEN 'High Engagement — Conversion Candidate'
                    WHEN opens > 0 AND clicks = 0             THEN 'Mid Engagement — Nurture Needed'
                    WHEN bounces > 0 OR unsubscribes > 0      THEN 'At Risk — Drop-off Signal'
                    WHEN sends > 0 AND opens = 0              THEN 'Low Engagement — Re-engagement Candidate'
                    ELSE 'No Activity'
                END AS engagement_segment
            FROM pe
            ORDER BY conversion_signal_score DESC, dropoff_risk_score ASC
            LIMIT 100
        """)
        detail_result = _run(seg_sql, max_rows=100)

        summary_sql = textwrap.dedent(f"""
            WITH pe AS (
                SELECT p.MASTER_PATIENT_ID,
                    COUNT(CASE WHEN fe.EVENT_TYPE='SENT'        THEN 1 END) AS sends,
                    COUNT(CASE WHEN fe.EVENT_TYPE='OPEN'        THEN 1 END) AS opens,
                    COUNT(CASE WHEN fe.EVENT_TYPE='CLICK'       THEN 1 END) AS clicks,
                    COUNT(CASE WHEN fe.EVENT_TYPE='BOUNCE'      THEN 1 END) AS bounces,
                    COUNT(CASE WHEN fe.EVENT_TYPE='UNSUBSCRIBE' THEN 1 END) AS unsubscribes
                FROM QA_FIPSAR_PHI_HUB.PHI_CORE.PHI_PROSPECT_MASTER p
                JOIN QA_FIPSAR_DW.GOLD.FACT_SFMC_ENGAGEMENT fe
                     ON fe.SUBSCRIBER_KEY = p.MASTER_PATIENT_ID
                WHERE fe.EVENT_TIMESTAMP BETWEEN '{start_date}' AND '{end_date}'
                  {active_filter}
                GROUP BY 1
            ),
            seg AS (
                SELECT
                    CASE WHEN clicks>0 THEN 'High Engagement — Conversion Candidate'
                         WHEN opens>0 AND clicks=0 THEN 'Mid Engagement — Nurture Needed'
                         WHEN bounces>0 OR unsubscribes>0 THEN 'At Risk — Drop-off Signal'
                         WHEN sends>0 AND opens=0 THEN 'Low Engagement — Re-engagement Candidate'
                         ELSE 'No Activity' END AS segment,
                    COUNT(*) AS prospect_count,
                    ROUND(AVG((opens+clicks)*100.0/NULLIF(sends,0)),1) AS avg_engagement_rate_pct
                FROM pe GROUP BY 1
            )
            SELECT segment, prospect_count, avg_engagement_rate_pct,
                   ROUND(prospect_count*100.0/SUM(prospect_count) OVER(),1) AS pct_of_total
            FROM seg ORDER BY prospect_count DESC
        """)
        parts.append("**Data source: SFMC engagement events (Path A — richest signals)**\n")
        parts.append("### Segment Summary\n" + _run(summary_sql, max_rows=10))
        parts.append("### Individual Prospect Scores (ranked by conversion signal)\n" + detail_result)

    # ------------------------------------------------------------------
    # PATH B: RAW_SFMC event tables — fallback when gold table is empty
    # ------------------------------------------------------------------
    elif engagement_rows == 0:
        raw_check = execute_query_as_string(
            "SELECT COUNT(*) AS cnt FROM QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_SENT",
            max_rows=1,
        )
        raw_rows = 0
        try:
            for line in raw_check.splitlines():
                if "|" in line and "cnt" not in line and "---" not in line:
                    val = line.strip("|").strip()
                    if val.isdigit():
                        raw_rows = int(val)
                        break
        except Exception:
            pass

        if raw_rows > 0:
            raw_sql = textwrap.dedent(f"""
                SELECT
                    s.SUBSCRIBER_KEY,
                    COUNT(DISTINCT s.JOB_ID)                              AS total_sends,
                    COUNT(DISTINCT o.JOB_ID)                              AS total_opens,
                    COUNT(DISTINCT c.JOB_ID)                              AS total_clicks,
                    COUNT(DISTINCT b.JOB_ID)                              AS total_bounces,
                    COUNT(DISTINCT u.JOB_ID)                              AS total_unsubscribes,
                    ROUND(COUNT(DISTINCT o.JOB_ID)*100.0/NULLIF(COUNT(DISTINCT s.JOB_ID),0),1)
                                                                          AS open_rate_pct,
                    ROUND(COUNT(DISTINCT c.JOB_ID)*100.0/NULLIF(COUNT(DISTINCT s.JOB_ID),0),1)
                                                                          AS click_rate_pct,
                    CASE
                        WHEN COUNT(DISTINCT c.JOB_ID) > 0             THEN 'High Engagement — Conversion Candidate'
                        WHEN COUNT(DISTINCT o.JOB_ID) > 0             THEN 'Mid Engagement — Nurture Needed'
                        WHEN COUNT(DISTINCT b.JOB_ID) > 0
                          OR COUNT(DISTINCT u.JOB_ID) > 0             THEN 'At Risk — Drop-off Signal'
                        ELSE 'Low Engagement — Re-engagement Candidate'
                    END AS engagement_segment
                FROM QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_SENT s
                LEFT JOIN QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_OPENS       o ON s.SUBSCRIBER_KEY = o.SUBSCRIBER_KEY
                LEFT JOIN QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_CLICKS      c ON s.SUBSCRIBER_KEY = c.SUBSCRIBER_KEY
                LEFT JOIN QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_BOUNCES     b ON s.SUBSCRIBER_KEY = b.SUBSCRIBER_KEY
                LEFT JOIN QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_UNSUBSCRIBES u ON s.SUBSCRIBER_KEY = u.SUBSCRIBER_KEY
                GROUP BY 1
                ORDER BY click_rate_pct DESC, open_rate_pct DESC
                LIMIT 100
            """)
            parts.append("**Data source: Raw SFMC event tables (Path B — gold table currently empty)**\n")
            parts.append("### Prospect Engagement Scores from Raw Events\n" + _run(raw_sql, max_rows=100))

    # ------------------------------------------------------------------
    # PATH C: Prospect master — ALWAYS runs, provides intake-based signals
    # Active = mastered + IS_ACTIVE; Dropped = mastered but IS_ACTIVE = FALSE
    # Suppression = in DQ_REJECTION_LOG with SUPPRESSED/FATAL_ERROR
    # ------------------------------------------------------------------
    master_sql = textwrap.dedent(f"""
        SELECT
            p.CHANNEL,
            p.PATIENT_CONSENT                                              AS consent_status,
            COUNT(*)                                                       AS total_prospects,
            SUM(CASE WHEN p.IS_ACTIVE = TRUE  THEN 1 ELSE 0 END)          AS active_prospects,
            SUM(CASE WHEN p.IS_ACTIVE = FALSE THEN 1 ELSE 0 END)          AS inactive_prospects,
            ROUND(SUM(CASE WHEN p.IS_ACTIVE=TRUE  THEN 1 ELSE 0 END)*100.0/COUNT(*),1)
                                                                           AS active_rate_pct,
            ROUND(SUM(CASE WHEN p.IS_ACTIVE=FALSE THEN 1 ELSE 0 END)*100.0/COUNT(*),1)
                                                                           AS dropoff_rate_pct
        FROM QA_FIPSAR_PHI_HUB.PHI_CORE.PHI_PROSPECT_MASTER p
        WHERE p.FILE_DATE BETWEEN '{start_date}' AND '{end_date}'
        GROUP BY 1, 2
        ORDER BY total_prospects DESC
        LIMIT 50
    """)

    suppression_sql = textwrap.dedent(f"""
        SELECT
            REJECTION_REASON,
            COUNT(*)                                                       AS count,
            ROUND(COUNT(*)*100.0 / (
                SELECT COUNT(*) FROM QA_FIPSAR_PHI_HUB.PHI_CORE.PHI_PROSPECT_MASTER
                WHERE FILE_DATE BETWEEN '{start_date}' AND '{end_date}'
            ), 2)                                                          AS pct_of_prospects
        FROM QA_FIPSAR_AUDIT.PIPELINE_AUDIT.DQ_REJECTION_LOG
        WHERE UPPER(REJECTION_REASON) IN ('SUPPRESSED_PROSPECT','FATAL_ERROR','SUPPRESSED')
          AND (
              TRY_TO_DATE(TRY_PARSE_JSON(REJECTED_RECORD):FILE_DATE::STRING)
                  BETWEEN '{start_date}' AND '{end_date}'
              OR CAST(REJECTED_AT AS DATE) BETWEEN '{start_date}' AND '{end_date}'
          )
        GROUP BY 1
        ORDER BY 2 DESC
    """)

    parts.append(
        "**Data source: Prospect Master — intake-based activity & drop-off signals (Path C)**\n"
        "\n### Active vs Inactive Breakdown by Channel & Consent\n"
        + _run(master_sql, max_rows=50)
        + "\n\n### SFMC Send Suppression Signals (drop-off risk for mastered prospects)\n"
        + _run(suppression_sql, max_rows=10)
    )

    return "\n\n---\n\n".join(parts)


# ---------------------------------------------------------------------------
# Tool 8: Pipeline observability
# ---------------------------------------------------------------------------

@tool
def get_pipeline_observability(
    start_date: str = "2020-01-01",
    end_date: str = "2099-12-31",
) -> str:
    """
    Return pipeline run health: execution logs, DQ signal counts, and
    table-level row counts from the audit layer.

    Use this when the user asks about:
    - Pipeline health or run status
    - Data quality issue counts
    - Whether a pipeline run succeeded or failed
    - Observability across the data flow

    Args:
        start_date: Start of date range (YYYY-MM-DD).
        end_date:   End of date range (YYYY-MM-DD).

    Returns:
        Pipeline run + DQ signal summary as a markdown table.
    """
    sql = textwrap.dedent(f"""
        -- Pipeline run log summary
        SELECT
            TABLE_NAME,
            STATUS,
            COUNT(*) AS run_count,
            MIN(RUN_START_TIME)::STRING AS earliest_run,
            MAX(RUN_END_TIME)::STRING AS latest_run
        FROM QA_FIPSAR_AUDIT.PIPELINE_AUDIT.PIPELINE_RUN_LOG
        WHERE CAST(RUN_START_TIME AS DATE) BETWEEN '{start_date}' AND '{end_date}'
        GROUP BY 1, 2
        ORDER BY 3 DESC
        LIMIT 50
    """)
    run_result = _run(sql)

    dq_sql = textwrap.dedent(f"""
        SELECT
            TABLE_NAME,
            REJECTION_REASON,
            COUNT(*) AS rejection_count,
            MIN(CAST(REJECTED_AT AS DATE))::STRING AS first_seen,
            MAX(CAST(REJECTED_AT AS DATE))::STRING AS last_seen
        FROM QA_FIPSAR_AUDIT.PIPELINE_AUDIT.DQ_REJECTION_LOG
        WHERE CAST(REJECTED_AT AS DATE) BETWEEN '{start_date}' AND '{end_date}'
        GROUP BY 1, 2
        ORDER BY 3 DESC
        LIMIT 50
    """)
    dq_result = _run(dq_sql)

    return (
        "### Pipeline Run Log\n" + run_result
        + "\n\n### DQ Rejection Summary\n" + dq_result
    )


# ---------------------------------------------------------------------------
# Tool 9: Individual rejected lead records
# ---------------------------------------------------------------------------

@tool
def get_rejected_lead_details(
    start_date: str = "2020-01-01",
    end_date: str = "2099-12-31",
    rejection_reason: Optional[str] = None,
    rejection_category: str = "intake",
    limit: int = 100,
) -> str:
    """
    Return individual rejected lead records (row-level) from DQ_REJECTION_LOG.

    CRITICAL DISTINCTION — use rejection_category to target the right records:

    - rejection_category="intake"  (DEFAULT) → Individual leads rejected during mastering.
      These are Invalid Leads: NULL_EMAIL, NO_CONSENT, missing mandatory fields.
      Use this when user asks: "list the rejected leads", "show me who got rejected",
      "display the invalid leads", "what are the rejected lead IDs".

    - rejection_category="sfmc"  → Individual SFMC send suppressions/errors.
      Use this when user asks: "list suppressed sends", "show FATAL_ERROR records".

    - rejection_category="all"   → All rejection records.

    Args:
        start_date:          Start of FILE_DATE range (YYYY-MM-DD).
        end_date:            End of FILE_DATE range (YYYY-MM-DD).
        rejection_reason:    Optional specific reason filter (e.g. 'NULL_EMAIL').
        rejection_category:  "intake" (default), "sfmc", or "all".
        limit:               Max rows to return (default 100).

    Returns:
        Row-level rejected lead records as a markdown table.
    """
    reason_filter = (
        f"AND UPPER(REJECTION_REASON) = '{rejection_reason.upper()}'"
        if rejection_reason
        else ""
    )
    if rejection_category == "intake":
        category_filter = "AND UPPER(REJECTION_REASON) NOT IN ('SUPPRESSED_PROSPECT', 'FATAL_ERROR', 'SUPPRESSED')"
    elif rejection_category == "sfmc":
        category_filter = "AND UPPER(REJECTION_REASON) IN ('SUPPRESSED_PROSPECT', 'FATAL_ERROR', 'SUPPRESSED')"
    else:
        category_filter = ""
    sql = textwrap.dedent(f"""
        SELECT
            REJECTION_ID,
            CAST(REJECTED_AT AS DATE)                                   AS rejected_at_date,
            TABLE_NAME,
            REJECTION_REASON,
            TRY_PARSE_JSON(REJECTED_RECORD):FILE_DATE::STRING           AS lead_file_date,
            TRY_PARSE_JSON(REJECTED_RECORD):FIRST_NAME::STRING          AS first_name,
            TRY_PARSE_JSON(REJECTED_RECORD):LAST_NAME::STRING           AS last_name,
            TRY_PARSE_JSON(REJECTED_RECORD):EMAIL::STRING               AS email,
            TRY_PARSE_JSON(REJECTED_RECORD):PHONE_NUMBER::STRING        AS phone_number,
            TRY_PARSE_JSON(REJECTED_RECORD):CHANNEL::STRING             AS channel,
            TRY_PARSE_JSON(REJECTED_RECORD):PATIENT_CONSENT::STRING     AS consent_flag
        FROM QA_FIPSAR_AUDIT.PIPELINE_AUDIT.DQ_REJECTION_LOG
        WHERE
            -- Primary match: FILE_DATE embedded in the rejected record JSON
            (
                TRY_TO_DATE(TRY_PARSE_JSON(REJECTED_RECORD):FILE_DATE::STRING)
                    BETWEEN '{start_date}' AND '{end_date}'
            )
            -- Fallback: if FILE_DATE is not in the JSON, match on pipeline processing date
            OR (
                TRY_TO_DATE(TRY_PARSE_JSON(REJECTED_RECORD):FILE_DATE::STRING) IS NULL
                AND CAST(REJECTED_AT AS DATE) BETWEEN '{start_date}' AND '{end_date}'
            )
          {reason_filter}
          {category_filter}
        ORDER BY lead_file_date DESC NULLS LAST, rejected_at_date DESC
        LIMIT {limit}
    """)
    return _run(sql, max_rows=limit)


# ---------------------------------------------------------------------------
# Tool 10: Individual valid prospect records
# ---------------------------------------------------------------------------

@tool
def get_prospect_details(
    start_date: str = "2020-01-01",
    end_date: str = "2099-12-31",
    channel: Optional[str] = None,
    state: Optional[str] = None,
    limit: int = 100,
) -> str:
    """
    Return individual valid mastered Prospect records from PHI_PROSPECT_MASTER
    (or DIM_PROSPECT for enriched analytics view), with key identity and
    intake details.

    Use this when the user asks to:
    - "List the prospects who converted in January 2026"
    - "Show me individual prospect records"
    - "Give me the granularity / details of valid prospects"
    - "Who are the prospects that came in via channel X?"
    - "List valid prospects in state Y"

    Args:
        start_date: Inclusive start of FILE_DATE range (YYYY-MM-DD).
        end_date:   Inclusive end of FILE_DATE range (YYYY-MM-DD).
        channel:    Optional channel filter, e.g. 'WEB', 'APP', 'FORM'.
        state:      Optional US state filter, e.g. 'CA', 'TX'.
        limit:      Maximum number of records to return (default 100).

    Returns:
        Row-level valid prospect records as a markdown table.
    """
    channel_filter = f"AND UPPER(CHANNEL) = '{channel.upper()}'" if channel else ""
    state_filter   = f"AND UPPER(STATE) = '{state.upper()}'"     if state   else ""

    sql = textwrap.dedent(f"""
        SELECT
            MASTER_PATIENT_ID           AS master_prospect_id,
            RECORD_ID,
            FIRST_NAME,
            LAST_NAME,
            EMAIL,
            PHONE_NUMBER,
            PATIENT_CONSENT             AS consent_flag,
            CHANNEL,
            FILE_DATE,
            SUBMISSION_TIMESTAMP::STRING AS submitted_at,
            IS_ACTIVE
        FROM QA_FIPSAR_PHI_HUB.PHI_CORE.PHI_PROSPECT_MASTER
        WHERE FILE_DATE BETWEEN '{start_date}' AND '{end_date}'
          {channel_filter}
          {state_filter}
        ORDER BY FILE_DATE DESC, SUBMISSION_TIMESTAMP DESC
        LIMIT {limit}
    """)
    return _run(sql, max_rows=limit)


# ---------------------------------------------------------------------------
# Tool 11b: Per-stage SFMC journey suppression analysis
# ---------------------------------------------------------------------------

@tool
def get_sfmc_stage_suppression(
    target_date: Optional[str] = None,
    prospect_id: Optional[str] = None,
) -> str:
    """
    Analyse suppression across all 9 SFMC journey stages.

    Answers questions like:
    - "How many Stage 3 emails were expected today but not sent?"
    - "Prospect FIP000023 — which stage were they suppressed at and why?"
    - "Show per-stage expected vs actual send counts for a given date"
    - "Which prospects were suppressed and at what stage?"
    - "Why did prospect X not receive the Stage 3 email?"
    - "Today 100 Stage 3 emails expected — how many were suppressed?"

    Logic:
      - Uses RAW_SFMC_PROSPECT_JOURNEY_DETAILS (wide table) for per-stage sent flags + dates.
      - Identifies the LAST stage a suppressed prospect received (sent = 'True') to determine
        AT WHICH STAGE suppression took effect.
      - Joins RAW_SFMC_UNSUBSCRIBES (SUBSCRIBER_KEY = PROSPECT_ID) to show WHY the prospect
        did not receive the next expected email (unsubscribe reason + date).

    Args:
        target_date: Optional date (YYYY-MM-DD) to scope expected-vs-actual to a specific day.
        prospect_id: Optional specific MASTER_PATIENT_ID (FIP... format) for single-prospect trace.

    Returns:
        Stage-level summary + suppressed prospect detail with unsubscribe reason.
    """
    prospect_filter = f"AND jd.PROSPECT_ID = '{prospect_id}'" if prospect_id else ""
    date_filter_s1  = f"AND jd.WELCOMEJOURNEY_WELCOMEEMAIL_SENT_DATE = '{target_date}'"       if target_date else ""
    date_filter_s2  = f"AND jd.WELCOMEJOURNEY_EDUCATIONEMAIL_SENT_DATE = '{target_date}'"      if target_date else ""
    date_filter_s3  = f"AND jd.NURTUREJOURNEY_EDUCATIONEMAIL1_SENT_DATE = '{target_date}'"    if target_date else ""
    date_filter_s4  = f"AND jd.NURTUREJOURNEY_EDUCATIONEMAIL2_SENT_DATE = '{target_date}'"    if target_date else ""
    date_filter_s5  = f"AND jd.NURTUREJOURNEY_PROSPECTSTORYEMAIL_SENT_DATE = '{target_date}'" if target_date else ""
    date_filter_s6  = f"AND jd.HIGHENGAGEMENT_CONVERSIONEMAIL_SENT_DATE = '{target_date}'"    if target_date else ""
    date_filter_s7  = f"AND jd.HIGHENGAGEMENT_REMINDEREMAIL_SENT_DATE = '{target_date}'"      if target_date else ""
    date_filter_s8  = f"AND jd.LOWENGAGEMENT_REENGAGEMENTEMAIL_SENT_DATE = '{target_date}'"   if target_date else ""
    date_filter_s9  = f"AND jd.LOWENGAGEMENTFINALREMINDEREMAIL_SENT_DATE = '{target_date}'"   if target_date else ""

    # --- PART 1: Per-stage expected vs actual counts ---
    stage_summary_sql = textwrap.dedent(f"""
        SELECT
            'Stage 01 — Welcome Email'               AS stage,
            COUNT(*)                                 AS total_prospects,
            SUM(CASE WHEN UPPER(TRIM(WELCOMEJOURNEY_WELCOMEEMAIL_SENT)) = 'TRUE'    THEN 1 ELSE 0 END) AS sent,
            SUM(CASE WHEN UPPER(TRIM(WELCOMEJOURNEY_WELCOMEEMAIL_SENT)) != 'TRUE'   THEN 1 ELSE 0 END) AS not_sent,
            SUM(CASE WHEN UPPER(TRIM(SUPPRESSION_FLAG)) IN ('YES','Y','TRUE','1')   THEN 1 ELSE 0 END) AS suppressed
        FROM QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_PROSPECT_JOURNEY_DETAILS jd
        WHERE 1=1 {prospect_filter}
        UNION ALL
        SELECT 'Stage 02 — Education Email', COUNT(*),
            SUM(CASE WHEN UPPER(TRIM(WELCOMEJOURNEY_EDUCATIONEMAIL_SENT)) = 'TRUE'  THEN 1 ELSE 0 END),
            SUM(CASE WHEN UPPER(TRIM(WELCOMEJOURNEY_EDUCATIONEMAIL_SENT)) != 'TRUE' THEN 1 ELSE 0 END),
            SUM(CASE WHEN UPPER(TRIM(SUPPRESSION_FLAG)) IN ('YES','Y','TRUE','1')   THEN 1 ELSE 0 END)
        FROM QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_PROSPECT_JOURNEY_DETAILS jd
        WHERE 1=1 {prospect_filter}
        UNION ALL
        SELECT 'Stage 03 — Education Email 1', COUNT(*),
            SUM(CASE WHEN UPPER(TRIM(NURTUREJOURNEY_EDUCATIONEMAIL1_SENT)) = 'TRUE'  THEN 1 ELSE 0 END),
            SUM(CASE WHEN UPPER(TRIM(NURTUREJOURNEY_EDUCATIONEMAIL1_SENT)) != 'TRUE' THEN 1 ELSE 0 END),
            SUM(CASE WHEN UPPER(TRIM(SUPPRESSION_FLAG)) IN ('YES','Y','TRUE','1')    THEN 1 ELSE 0 END)
        FROM QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_PROSPECT_JOURNEY_DETAILS jd
        WHERE 1=1 {prospect_filter}
        UNION ALL
        SELECT 'Stage 04 — Education Email 2', COUNT(*),
            SUM(CASE WHEN UPPER(TRIM(NURTUREJOURNEY_EDUCATIONEMAIL2_SENT)) = 'TRUE'  THEN 1 ELSE 0 END),
            SUM(CASE WHEN UPPER(TRIM(NURTUREJOURNEY_EDUCATIONEMAIL2_SENT)) != 'TRUE' THEN 1 ELSE 0 END),
            SUM(CASE WHEN UPPER(TRIM(SUPPRESSION_FLAG)) IN ('YES','Y','TRUE','1')    THEN 1 ELSE 0 END)
        FROM QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_PROSPECT_JOURNEY_DETAILS jd
        WHERE 1=1 {prospect_filter}
        UNION ALL
        SELECT 'Stage 05 — Prospect Story Email', COUNT(*),
            SUM(CASE WHEN UPPER(TRIM(NURTUREJOURNEY_PROSPECTSTORYEMAIL_SENT)) = 'TRUE'  THEN 1 ELSE 0 END),
            SUM(CASE WHEN UPPER(TRIM(NURTUREJOURNEY_PROSPECTSTORYEMAIL_SENT)) != 'TRUE' THEN 1 ELSE 0 END),
            SUM(CASE WHEN UPPER(TRIM(SUPPRESSION_FLAG)) IN ('YES','Y','TRUE','1')       THEN 1 ELSE 0 END)
        FROM QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_PROSPECT_JOURNEY_DETAILS jd
        WHERE 1=1 {prospect_filter}
        UNION ALL
        SELECT 'Stage 06 — Conversion Email', COUNT(*),
            SUM(CASE WHEN UPPER(TRIM(HIGHENGAGEMENT_CONVERSIONEMAIL_SENT)) = 'TRUE'  THEN 1 ELSE 0 END),
            SUM(CASE WHEN UPPER(TRIM(HIGHENGAGEMENT_CONVERSIONEMAIL_SENT)) != 'TRUE' THEN 1 ELSE 0 END),
            SUM(CASE WHEN UPPER(TRIM(SUPPRESSION_FLAG)) IN ('YES','Y','TRUE','1')    THEN 1 ELSE 0 END)
        FROM QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_PROSPECT_JOURNEY_DETAILS jd
        WHERE 1=1 {prospect_filter}
        UNION ALL
        SELECT 'Stage 07 — Reminder Email', COUNT(*),
            SUM(CASE WHEN UPPER(TRIM(HIGHENGAGEMENT_REMINDEREMAIL_SENT)) = 'TRUE'  THEN 1 ELSE 0 END),
            SUM(CASE WHEN UPPER(TRIM(HIGHENGAGEMENT_REMINDEREMAIL_SENT)) != 'TRUE' THEN 1 ELSE 0 END),
            SUM(CASE WHEN UPPER(TRIM(SUPPRESSION_FLAG)) IN ('YES','Y','TRUE','1')  THEN 1 ELSE 0 END)
        FROM QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_PROSPECT_JOURNEY_DETAILS jd
        WHERE 1=1 {prospect_filter}
        UNION ALL
        SELECT 'Stage 08 — Re-engagement Email', COUNT(*),
            SUM(CASE WHEN UPPER(TRIM(LOWENGAGEMENT_REENGAGEMENTEMAIL_SENT)) = 'TRUE'  THEN 1 ELSE 0 END),
            SUM(CASE WHEN UPPER(TRIM(LOWENGAGEMENT_REENGAGEMENTEMAIL_SENT)) != 'TRUE' THEN 1 ELSE 0 END),
            SUM(CASE WHEN UPPER(TRIM(SUPPRESSION_FLAG)) IN ('YES','Y','TRUE','1')     THEN 1 ELSE 0 END)
        FROM QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_PROSPECT_JOURNEY_DETAILS jd
        WHERE 1=1 {prospect_filter}
        UNION ALL
        SELECT 'Stage 09 — Final Reminder Email', COUNT(*),
            SUM(CASE WHEN UPPER(TRIM(LOWENGAGEMENTFINALREMINDEREMAIL_SENT)) = 'TRUE'  THEN 1 ELSE 0 END),
            SUM(CASE WHEN UPPER(TRIM(LOWENGAGEMENTFINALREMINDEREMAIL_SENT)) != 'TRUE' THEN 1 ELSE 0 END),
            SUM(CASE WHEN UPPER(TRIM(SUPPRESSION_FLAG)) IN ('YES','Y','TRUE','1')     THEN 1 ELSE 0 END)
        FROM QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_PROSPECT_JOURNEY_DETAILS jd
        WHERE 1=1 {prospect_filter}
        ORDER BY stage
    """)
    if target_date:
        stage_summary_sql = textwrap.dedent(f"""
            WITH base AS (
                SELECT *
                FROM QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_PROSPECT_JOURNEY_DETAILS jd
                WHERE 1=1 {prospect_filter}
            )
            SELECT * FROM (
                SELECT 2 AS stage_order, 'Stage 02 — Education Email' AS stage,
                    COUNT(*) AS expected_count,
                    SUM(CASE WHEN UPPER(TRIM(WELCOMEJOURNEY_EDUCATIONEMAIL_SENT)) = 'TRUE'
                              AND TRY_TO_DATE(WELCOMEJOURNEY_EDUCATIONEMAIL_SENT_DATE::STRING) = '{target_date}' THEN 1 ELSE 0 END) AS sent,
                    SUM(CASE WHEN UPPER(TRIM(WELCOMEJOURNEY_EDUCATIONEMAIL_SENT)) != 'TRUE'
                              AND UPPER(TRIM(SUPPRESSION_FLAG)) IN ('YES','Y','TRUE','1') THEN 1 ELSE 0 END) AS suppressed,
                    SUM(CASE WHEN UPPER(TRIM(WELCOMEJOURNEY_EDUCATIONEMAIL_SENT)) != 'TRUE'
                              AND UPPER(TRIM(SUPPRESSION_FLAG)) NOT IN ('YES','Y','TRUE','1') THEN 1 ELSE 0 END) AS not_sent
                FROM base
                WHERE UPPER(TRIM(WELCOMEJOURNEY_WELCOMEEMAIL_SENT)) = 'TRUE'
                  AND DATEADD(DAY, 3, TRY_TO_DATE(WELCOMEJOURNEY_WELCOMEEMAIL_SENT_DATE::STRING)) = '{target_date}'
                UNION ALL
                SELECT 3, 'Stage 03 — Education Email 1',
                    COUNT(*),
                    SUM(CASE WHEN UPPER(TRIM(NURTUREJOURNEY_EDUCATIONEMAIL1_SENT)) = 'TRUE'
                              AND TRY_TO_DATE(NURTUREJOURNEY_EDUCATIONEMAIL1_SENT_DATE::STRING) = '{target_date}' THEN 1 ELSE 0 END),
                    SUM(CASE WHEN UPPER(TRIM(NURTUREJOURNEY_EDUCATIONEMAIL1_SENT)) != 'TRUE'
                              AND UPPER(TRIM(SUPPRESSION_FLAG)) IN ('YES','Y','TRUE','1') THEN 1 ELSE 0 END),
                    SUM(CASE WHEN UPPER(TRIM(NURTUREJOURNEY_EDUCATIONEMAIL1_SENT)) != 'TRUE'
                              AND UPPER(TRIM(SUPPRESSION_FLAG)) NOT IN ('YES','Y','TRUE','1') THEN 1 ELSE 0 END)
                FROM base
                WHERE UPPER(TRIM(WELCOMEJOURNEY_EDUCATIONEMAIL_SENT)) = 'TRUE'
                  AND DATEADD(DAY, 5, TRY_TO_DATE(WELCOMEJOURNEY_EDUCATIONEMAIL_SENT_DATE::STRING)) = '{target_date}'
                UNION ALL
                SELECT 4, 'Stage 04 — Education Email 2',
                    COUNT(*),
                    SUM(CASE WHEN UPPER(TRIM(NURTUREJOURNEY_EDUCATIONEMAIL2_SENT)) = 'TRUE'
                              AND TRY_TO_DATE(NURTUREJOURNEY_EDUCATIONEMAIL2_SENT_DATE::STRING) = '{target_date}' THEN 1 ELSE 0 END),
                    SUM(CASE WHEN UPPER(TRIM(NURTUREJOURNEY_EDUCATIONEMAIL2_SENT)) != 'TRUE'
                              AND UPPER(TRIM(SUPPRESSION_FLAG)) IN ('YES','Y','TRUE','1') THEN 1 ELSE 0 END),
                    SUM(CASE WHEN UPPER(TRIM(NURTUREJOURNEY_EDUCATIONEMAIL2_SENT)) != 'TRUE'
                              AND UPPER(TRIM(SUPPRESSION_FLAG)) NOT IN ('YES','Y','TRUE','1') THEN 1 ELSE 0 END)
                FROM base
                WHERE UPPER(TRIM(NURTUREJOURNEY_EDUCATIONEMAIL1_SENT)) = 'TRUE'
                  AND DATEADD(DAY, 8, TRY_TO_DATE(NURTUREJOURNEY_EDUCATIONEMAIL1_SENT_DATE::STRING)) = '{target_date}'
                UNION ALL
                SELECT 5, 'Stage 05 — Prospect Story Email',
                    COUNT(*),
                    SUM(CASE WHEN UPPER(TRIM(NURTUREJOURNEY_PROSPECTSTORYEMAIL_SENT)) = 'TRUE'
                              AND TRY_TO_DATE(NURTUREJOURNEY_PROSPECTSTORYEMAIL_SENT_DATE::STRING) = '{target_date}' THEN 1 ELSE 0 END),
                    SUM(CASE WHEN UPPER(TRIM(NURTUREJOURNEY_PROSPECTSTORYEMAIL_SENT)) != 'TRUE'
                              AND UPPER(TRIM(SUPPRESSION_FLAG)) IN ('YES','Y','TRUE','1') THEN 1 ELSE 0 END),
                    SUM(CASE WHEN UPPER(TRIM(NURTUREJOURNEY_PROSPECTSTORYEMAIL_SENT)) != 'TRUE'
                              AND UPPER(TRIM(SUPPRESSION_FLAG)) NOT IN ('YES','Y','TRUE','1') THEN 1 ELSE 0 END)
                FROM base
                WHERE UPPER(TRIM(NURTUREJOURNEY_EDUCATIONEMAIL2_SENT)) = 'TRUE'
                  AND DATEADD(DAY, 3, TRY_TO_DATE(NURTUREJOURNEY_EDUCATIONEMAIL2_SENT_DATE::STRING)) = '{target_date}'
                UNION ALL
                SELECT 6, 'Stage 06 — Conversion Email',
                    COUNT(*),
                    SUM(CASE WHEN UPPER(TRIM(HIGHENGAGEMENT_CONVERSIONEMAIL_SENT)) = 'TRUE'
                              AND TRY_TO_DATE(HIGHENGAGEMENT_CONVERSIONEMAIL_SENT_DATE::STRING) = '{target_date}' THEN 1 ELSE 0 END),
                    SUM(CASE WHEN UPPER(TRIM(HIGHENGAGEMENT_CONVERSIONEMAIL_SENT)) != 'TRUE'
                              AND UPPER(TRIM(SUPPRESSION_FLAG)) IN ('YES','Y','TRUE','1') THEN 1 ELSE 0 END),
                    SUM(CASE WHEN UPPER(TRIM(HIGHENGAGEMENT_CONVERSIONEMAIL_SENT)) != 'TRUE'
                              AND UPPER(TRIM(SUPPRESSION_FLAG)) NOT IN ('YES','Y','TRUE','1') THEN 1 ELSE 0 END)
                FROM base
                WHERE UPPER(TRIM(NURTUREJOURNEY_PROSPECTSTORYEMAIL_SENT)) = 'TRUE'
                  AND DATEADD(DAY, 2, TRY_TO_DATE(NURTUREJOURNEY_PROSPECTSTORYEMAIL_SENT_DATE::STRING)) = '{target_date}'
                UNION ALL
                SELECT 7, 'Stage 07 — Reminder Email',
                    COUNT(*),
                    SUM(CASE WHEN UPPER(TRIM(HIGHENGAGEMENT_REMINDEREMAIL_SENT)) = 'TRUE'
                              AND TRY_TO_DATE(HIGHENGAGEMENT_REMINDEREMAIL_SENT_DATE::STRING) = '{target_date}' THEN 1 ELSE 0 END),
                    SUM(CASE WHEN UPPER(TRIM(HIGHENGAGEMENT_REMINDEREMAIL_SENT)) != 'TRUE'
                              AND UPPER(TRIM(SUPPRESSION_FLAG)) IN ('YES','Y','TRUE','1') THEN 1 ELSE 0 END),
                    SUM(CASE WHEN UPPER(TRIM(HIGHENGAGEMENT_REMINDEREMAIL_SENT)) != 'TRUE'
                              AND UPPER(TRIM(SUPPRESSION_FLAG)) NOT IN ('YES','Y','TRUE','1') THEN 1 ELSE 0 END)
                FROM base
                WHERE UPPER(TRIM(HIGHENGAGEMENT_CONVERSIONEMAIL_SENT)) = 'TRUE'
                  AND DATEADD(DAY, 2, TRY_TO_DATE(HIGHENGAGEMENT_CONVERSIONEMAIL_SENT_DATE::STRING)) = '{target_date}'
                UNION ALL
                SELECT 8, 'Stage 08 — Re-engagement Email',
                    COUNT(*),
                    SUM(CASE WHEN UPPER(TRIM(LOWENGAGEMENT_REENGAGEMENTEMAIL_SENT)) = 'TRUE'
                              AND TRY_TO_DATE(LOWENGAGEMENT_REENGAGEMENTEMAIL_SENT_DATE::STRING) = '{target_date}' THEN 1 ELSE 0 END),
                    SUM(CASE WHEN UPPER(TRIM(LOWENGAGEMENT_REENGAGEMENTEMAIL_SENT)) != 'TRUE'
                              AND UPPER(TRIM(SUPPRESSION_FLAG)) IN ('YES','Y','TRUE','1') THEN 1 ELSE 0 END),
                    SUM(CASE WHEN UPPER(TRIM(LOWENGAGEMENT_REENGAGEMENTEMAIL_SENT)) != 'TRUE'
                              AND UPPER(TRIM(SUPPRESSION_FLAG)) NOT IN ('YES','Y','TRUE','1') THEN 1 ELSE 0 END)
                FROM base
                WHERE UPPER(TRIM(HIGHENGAGEMENT_REMINDEREMAIL_SENT)) = 'TRUE'
                  AND DATEADD(DAY, 2, TRY_TO_DATE(HIGHENGAGEMENT_REMINDEREMAIL_SENT_DATE::STRING)) = '{target_date}'
                UNION ALL
                SELECT 9, 'Stage 09 — Final Reminder Email',
                    COUNT(*),
                    SUM(CASE WHEN UPPER(TRIM(LOWENGAGEMENTFINALREMINDEREMAIL_SENT)) = 'TRUE'
                              AND TRY_TO_DATE(LOWENGAGEMENTFINALREMINDEREMAIL_SENT_DATE::STRING) = '{target_date}' THEN 1 ELSE 0 END),
                    SUM(CASE WHEN UPPER(TRIM(LOWENGAGEMENTFINALREMINDEREMAIL_SENT)) != 'TRUE'
                              AND UPPER(TRIM(SUPPRESSION_FLAG)) IN ('YES','Y','TRUE','1') THEN 1 ELSE 0 END),
                    SUM(CASE WHEN UPPER(TRIM(LOWENGAGEMENTFINALREMINDEREMAIL_SENT)) != 'TRUE'
                              AND UPPER(TRIM(SUPPRESSION_FLAG)) NOT IN ('YES','Y','TRUE','1') THEN 1 ELSE 0 END)
                FROM base
                WHERE UPPER(TRIM(LOWENGAGEMENT_REENGAGEMENTEMAIL_SENT)) = 'TRUE'
                  AND DATEADD(DAY, 2, TRY_TO_DATE(LOWENGAGEMENT_REENGAGEMENTEMAIL_SENT_DATE::STRING)) = '{target_date}'
            )
            ORDER BY stage_order
        """)
    stage_result = _run(stage_summary_sql, max_rows=9)

    # --- PART 2: Per-suppressed-prospect: last stage reached + unsubscribe reason ---
    # Determines WHERE in the journey suppression hit, and WHY (via UNSUBSCRIBES table).
    # SUBSCRIBER_KEY in RAW_SFMC_UNSUBSCRIBES = PROSPECT_ID (both = MASTER_PATIENT_ID).
    suppressed_detail_sql = textwrap.dedent(f"""
        WITH journey AS (
            SELECT
                jd.PROSPECT_ID,
                jd.SUPPRESSION_FLAG,
                -- Identify last stage reached (last email sent = True)
                CASE
                    WHEN UPPER(TRIM(jd.LOWENGAGEMENTFINALREMINDEREMAIL_SENT))  = 'TRUE' THEN 'Stage 9 — Final Reminder'
                    WHEN UPPER(TRIM(jd.LOWENGAGEMENT_REENGAGEMENTEMAIL_SENT))  = 'TRUE' THEN 'Stage 8 — Re-engagement'
                    WHEN UPPER(TRIM(jd.HIGHENGAGEMENT_REMINDEREMAIL_SENT))     = 'TRUE' THEN 'Stage 7 — Reminder'
                    WHEN UPPER(TRIM(jd.HIGHENGAGEMENT_CONVERSIONEMAIL_SENT))   = 'TRUE' THEN 'Stage 6 — Conversion'
                    WHEN UPPER(TRIM(jd.NURTUREJOURNEY_PROSPECTSTORYEMAIL_SENT))= 'TRUE' THEN 'Stage 5 — Prospect Story'
                    WHEN UPPER(TRIM(jd.NURTUREJOURNEY_EDUCATIONEMAIL2_SENT))   = 'TRUE' THEN 'Stage 4 — Nurture Edu2'
                    WHEN UPPER(TRIM(jd.NURTUREJOURNEY_EDUCATIONEMAIL1_SENT))   = 'TRUE' THEN 'Stage 3 — Nurture Edu1'
                    WHEN UPPER(TRIM(jd.WELCOMEJOURNEY_EDUCATIONEMAIL_SENT))    = 'TRUE' THEN 'Stage 2 — Education Email'
                    WHEN UPPER(TRIM(jd.WELCOMEJOURNEY_WELCOMEEMAIL_SENT))      = 'TRUE' THEN 'Stage 1 — Welcome Email'
                    ELSE 'No emails sent yet'
                END AS last_stage_reached,
                -- Last email date (most recent date across all stages)
                GREATEST(
                    COALESCE(TRY_TO_DATE(jd.WELCOMEJOURNEY_WELCOMEEMAIL_SENT_DATE), '1900-01-01'),
                    COALESCE(TRY_TO_DATE(jd.WELCOMEJOURNEY_EDUCATIONEMAIL_SENT_DATE), '1900-01-01'),
                    COALESCE(TRY_TO_DATE(jd.NURTUREJOURNEY_EDUCATIONEMAIL1_SENT_DATE), '1900-01-01'),
                    COALESCE(TRY_TO_DATE(jd.NURTUREJOURNEY_EDUCATIONEMAIL2_SENT_DATE), '1900-01-01'),
                    COALESCE(TRY_TO_DATE(jd.NURTUREJOURNEY_PROSPECTSTORYEMAIL_SENT_DATE), '1900-01-01'),
                    COALESCE(TRY_TO_DATE(jd.HIGHENGAGEMENT_CONVERSIONEMAIL_SENT_DATE), '1900-01-01'),
                    COALESCE(TRY_TO_DATE(jd.HIGHENGAGEMENT_REMINDEREMAIL_SENT_DATE), '1900-01-01'),
                    COALESCE(TRY_TO_DATE(jd.LOWENGAGEMENT_REENGAGEMENTEMAIL_SENT_DATE), '1900-01-01'),
                    COALESCE(TRY_TO_DATE(jd.LOWENGAGEMENTFINALREMINDEREMAIL_SENT_DATE), '1900-01-01')
                ) AS last_email_date
            FROM QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_PROSPECT_JOURNEY_DETAILS jd
            WHERE UPPER(TRIM(jd.SUPPRESSION_FLAG)) IN ('YES','Y','TRUE','1')
              {prospect_filter}
        )
        SELECT
            j.PROSPECT_ID,
            j.last_stage_reached,
            j.last_email_date,
            -- Unsubscribe details — why this prospect stopped receiving emails
            u.EVENT_DATE          AS unsubscribe_date,
            u.REASON              AS unsubscribe_reason,
            u.JOB_ID              AS unsubscribe_job_id,
            CASE
                WHEN u.SUBSCRIBER_KEY IS NOT NULL THEN 'Unsubscribed'
                ELSE 'Suppressed (non-unsubscribe reason)'
            END AS suppression_type
        FROM journey j
        LEFT JOIN QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_UNSUBSCRIBES u
               ON u.SUBSCRIBER_KEY = j.PROSPECT_ID
        ORDER BY j.last_email_date DESC NULLS LAST
        LIMIT 100
    """)
    detail_result = _run(suppressed_detail_sql, max_rows=100)

    # --- PART 3: Unsubscribe reason summary ---
    unsub_summary_sql = textwrap.dedent("""
        SELECT
            COALESCE(u.REASON, '(no reason provided)') AS unsubscribe_reason,
            COUNT(DISTINCT u.SUBSCRIBER_KEY)            AS prospect_count,
            MIN(u.EVENT_DATE)                           AS first_unsubscribe_date,
            MAX(u.EVENT_DATE)                           AS last_unsubscribe_date
        FROM QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_UNSUBSCRIBES u
        GROUP BY 1
        ORDER BY 2 DESC
    """)
    unsub_result = _run(unsub_summary_sql, max_rows=20)

    return (
        "### Per-Stage Send / Not-Sent / Suppressed Count\n"
        + stage_result
        + "\n\n### Suppressed Prospects — Last Stage Reached + Unsubscribe Reason\n"
        + detail_result
        + "\n\n### Unsubscribe Reason Summary (RAW_SFMC_UNSUBSCRIBES)\n"
        + unsub_result
    )


# ---------------------------------------------------------------------------
# Tool 11c: SFMC Outbound / Inbound prospect reconciliation
# ---------------------------------------------------------------------------

@tool
def get_sfmc_prospect_outbound_match(
    limit: int = 100,
) -> str:
    """
    Reconcile DIM_PROSPECT (outbound to SFMC) vs RAW_SFMC_PROSPECT_C (what is actually loaded in SFMC).

    Only ACTIVE records from DIM_PROSPECT are exported to SFMC via VW_SFMC_PROSPECT_OUTBOUND.
    This tool identifies:
    - Prospects that went outbound (DIM_PROSPECT) but are not yet in SFMC (RAW_SFMC_PROSPECT_C)
    - Prospects in SFMC but with no matching DIM_PROSPECT (data integrity issue)
    - Matching prospects — confirming successful outbound → SFMC load
    - SFMC engagement volume per active prospect (sent, opened, clicked)

    Use this when the user asks:
    - "Are all active prospects loaded into SFMC?"
    - "How many prospects in DIM_PROSPECT are reflected in SFMC?"
    - "Match outbound to SFMC prospect data"
    - "Which prospects are in SFMC vs in DIM_PROSPECT?"
    - "SFMC history vs inbound reconciliation"

    Args:
        limit: Max rows for the mismatch details (default 100).

    Returns:
        Reconciliation summary and per-status breakdown as markdown tables.
    """
    # Summary match/miss counts
    match_sql = textwrap.dedent(f"""
        WITH dim AS (
            SELECT MASTER_PATIENT_ID, FIRST_NAME, LAST_NAME, EMAIL,
                   PRIMARY_CHANNEL, FIRST_INTAKE_DATE
            FROM QA_FIPSAR_DW.GOLD.DIM_PROSPECT
        ),
        sfmc AS (
            SELECT PROSPECT_ID, FIRST_NAME, LAST_NAME, EMAIL_ADDRESS,
                   MARKETING_CONSENT, HIGH_ENGAGEMENT
            FROM QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_PROSPECT_C
        )
        SELECT
            COUNT(DISTINCT d.MASTER_PATIENT_ID)               AS dim_prospect_total,
            COUNT(DISTINCT s.PROSPECT_ID)                     AS sfmc_prospect_c_total,
            COUNT(DISTINCT CASE WHEN s.PROSPECT_ID IS NOT NULL
                           THEN d.MASTER_PATIENT_ID END)      AS matched_in_both,
            COUNT(DISTINCT CASE WHEN s.PROSPECT_ID IS NULL
                           THEN d.MASTER_PATIENT_ID END)      AS in_dim_not_sfmc,
            COUNT(DISTINCT CASE WHEN d.MASTER_PATIENT_ID IS NULL
                           THEN s.PROSPECT_ID END)            AS in_sfmc_not_dim
        FROM dim d
        FULL OUTER JOIN sfmc s ON d.MASTER_PATIENT_ID = s.PROSPECT_ID
    """)
    match_result = _run(match_sql, max_rows=5)

    # Prospects in DIM but not yet in SFMC
    missing_sql = textwrap.dedent(f"""
        SELECT d.MASTER_PATIENT_ID, d.FIRST_NAME, d.LAST_NAME, d.EMAIL,
               d.PRIMARY_CHANNEL, d.FIRST_INTAKE_DATE,
               'In DIM_PROSPECT but NOT in SFMC (not yet exported or load failed)' AS status
        FROM QA_FIPSAR_DW.GOLD.DIM_PROSPECT d
        LEFT JOIN QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_PROSPECT_C s
               ON d.MASTER_PATIENT_ID = s.PROSPECT_ID
        WHERE s.PROSPECT_ID IS NULL
        ORDER BY d.FIRST_INTAKE_DATE DESC
        LIMIT {limit}
    """)
    missing_result = _run(missing_sql, max_rows=limit)

    # Matched prospects with engagement summary
    engaged_sql = textwrap.dedent(f"""
        SELECT
            d.MASTER_PATIENT_ID,
            d.FIRST_NAME, d.LAST_NAME,
            s.MARKETING_CONSENT,
            s.HIGH_ENGAGEMENT,
            COUNT(CASE WHEN fe.EVENT_TYPE='SENT'  THEN 1 END)  AS total_sends,
            COUNT(CASE WHEN fe.EVENT_TYPE='OPEN'  THEN 1 END)  AS total_opens,
            COUNT(CASE WHEN fe.EVENT_TYPE='CLICK' THEN 1 END)  AS total_clicks,
            ROUND(COUNT(CASE WHEN fe.EVENT_TYPE='OPEN'  THEN 1 END)*100.0
                  /NULLIF(COUNT(CASE WHEN fe.EVENT_TYPE='SENT' THEN 1 END),0),1) AS open_rate_pct,
            MAX(fe.EVENT_TIMESTAMP) AS last_engagement
        FROM QA_FIPSAR_DW.GOLD.DIM_PROSPECT d
        JOIN QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_PROSPECT_C s
             ON d.MASTER_PATIENT_ID = s.PROSPECT_ID
        LEFT JOIN QA_FIPSAR_DW.GOLD.FACT_SFMC_ENGAGEMENT fe
             ON fe.SUBSCRIBER_KEY = d.MASTER_PATIENT_ID
        GROUP BY 1,2,3,4,5
        ORDER BY total_sends DESC
        LIMIT {limit}
    """)
    engaged_result = _run(engaged_sql, max_rows=limit)

    return (
        "### Outbound Reconciliation Summary (DIM_PROSPECT vs RAW_SFMC_PROSPECT_C)\n"
        + match_result
        + "\n\n### Prospects in DIM_PROSPECT but NOT yet in SFMC\n"
        + missing_result
        + "\n\n### Matched Prospects — Engagement Summary\n"
        + engaged_result
    )


# ---------------------------------------------------------------------------
# Chart tools (12–16): wrap charts.py generators as LangChain tools
# ---------------------------------------------------------------------------

import charts as _charts   # late import to avoid circular at module level


@tool
def chart_smart(
    sql: str,
    title: str,
    chart_type: str = "auto",
    x_col: Optional[str] = None,
    y_col: Optional[str] = None,
    color_col: Optional[str] = None,
    orientation: str = "v",
) -> str:
    """
    GENERALISED chart tool — use this for ANY question where the user wants a visual chart
    and none of the specific chart tools fit, OR when you want full control over what is plotted.

    Steps to use:
      1. Write a SELECT query that returns the data you want to visualise.
      2. Choose chart_type: "bar", "line", "area", "pie", "donut", "funnel", "scatter", or "auto".
      3. Specify x_col (category/time axis) and y_col (value axis). Leave None to auto-detect.

    When to use this vs specific chart tools:
      - Use chart_funnel / chart_rejections / chart_engagement etc. for common pre-built charts.
      - Use THIS tool for: custom breakdowns, ad-hoc comparisons, channel mix charts,
        state/region distributions, consent rate charts, age group charts, any custom metric.

    Examples:
      - "Show me prospect count by state" → bar chart, x=STATE, y=count
      - "Plot rejection trend by month"   → line chart on monthly grouped SQL
      - "Compare channel mix for 2025 vs 2026" → grouped bar with color_col=year

    Args:
        sql:         A valid Snowflake SELECT returning at least 2 columns.
        title:       Chart title shown to the user.
        chart_type:  "auto", "bar", "line", "area", "pie", "donut", "funnel", "scatter".
        x_col:       Column name for x-axis / labels (auto-detect if None).
        y_col:       Column name for y-axis / values (auto-detect if None).
        color_col:   Column for grouping/colouring series (optional).
        orientation: "v" (vertical, default) or "h" (horizontal bars).
    """
    return _charts.smart_chart(
        sql=sql, chart_type=chart_type, title=title,
        x_col=x_col, y_col=y_col, color_col=color_col, orientation=orientation,
    )


@tool
def chart_funnel(start_date: str = "2020-01-01", end_date: str = "2099-12-31") -> str:
    """
    Generate and display a Prospect Journey Funnel chart showing volume at each stage:
    Lead Intake → Valid Prospects → SFMC Sent → Opened → Clicked.

    Use this whenever the user asks for a funnel chart, funnel visualisation,
    or wants to SEE the funnel visually (not just numbers).

    Args:
        start_date: Start of date range (YYYY-MM-DD).
        end_date:   End of date range (YYYY-MM-DD).
    """
    return _charts.funnel_chart(start_date, end_date)


@tool
def chart_rejections(
    start_date: str = "2020-01-01",
    end_date: str = "2099-12-31",
    rejection_category: str = "all",
) -> str:
    """
    Generate and display a donut chart of rejection reasons.

    Use this when the user asks to visualise/chart:
    - Rejection reasons breakdown
    - Why leads were dropped (chart)
    - SFMC suppression chart

    Args:
        start_date:          Start of date range (YYYY-MM-DD).
        end_date:            End of date range (YYYY-MM-DD).
        rejection_category:  "intake" for mastering rejections, "sfmc" for suppressions, "all" for both.
    """
    return _charts.rejection_chart(start_date, end_date, rejection_category)


@tool
def chart_engagement(
    start_date: str = "2020-01-01",
    end_date: str = "2099-12-31",
    journey_type: Optional[str] = None,
) -> str:
    """
    Generate and display a grouped bar chart of SFMC engagement events by journey and event type
    (SENT, OPEN, CLICK, BOUNCE, UNSUBSCRIBE, SPAM).

    Use this when the user asks to visualise/chart:
    - SFMC engagement
    - Journey performance visually
    - Email event breakdown chart

    Args:
        start_date:   Start of date range (YYYY-MM-DD).
        end_date:     End of date range (YYYY-MM-DD).
        journey_type: Optional journey filter (e.g. 'Welcome', 'Nurture').
    """
    return _charts.engagement_chart(start_date, end_date, journey_type)


@tool
def chart_conversion_segments(
    start_date: str = "2020-01-01",
    end_date: str = "2099-12-31",
) -> str:
    """
    Generate and display a dual donut chart:
    - Left: Engagement segments (High / Mid / At Risk / Low / No Activity)
    - Right: Active vs Inactive/Dropped prospect status

    Use this when the user asks to visualise/chart:
    - Conversion segments
    - Drop-off risk chart
    - Engagement distribution visually
    - Active vs inactive prospects chart

    Args:
        start_date: Start of date range (YYYY-MM-DD).
        end_date:   End of date range (YYYY-MM-DD).
    """
    return _charts.conversion_segment_chart(start_date, end_date)


@tool
def chart_intake_trend(
    start_date: str = "2020-01-01",
    end_date: str = "2099-12-31",
    group_by: str = "month",
) -> str:
    """
    Generate and display a time-series line chart of lead intake volume and
    valid prospect count over time.

    Use this when the user asks to visualise/chart:
    - Lead or prospect intake trend
    - Volume over time
    - Monthly/weekly/daily intake chart

    Args:
        start_date: Start of date range (YYYY-MM-DD).
        end_date:   End of date range (YYYY-MM-DD).
        group_by:   Time granularity — "day", "week", or "month".
    """
    return _charts.intake_trend_chart(start_date, end_date, group_by)


@tool
def chart_sfmc_stage_fishbone(
    target_date: str,
    prospect_id: Optional[str] = None,
) -> str:
    """
    Generate a stage-by-stage fishbone-style chart for expected vs sent vs suppressed SFMC sends.

    Use this when the user asks:
    - "How many stage 3 emails were expected today vs actually sent?"
    - "Show stage dips between journeys"
    - "Visualise suppression between stages"
    - "Show a fishbone chart for today's expected vs actual sends"

    Args:
        target_date: Required business date in YYYY-MM-DD format.
        prospect_id: Optional FIP... prospect id for a single-prospect trace.
    """
    return _charts.sfmc_stage_fishbone_chart(target_date, prospect_id)


# ---------------------------------------------------------------------------
# Chart Tools 13–18: Six additional purpose-built visual tools
# ---------------------------------------------------------------------------

@tool
def chart_bounce_analysis(
    start_date: str = "2020-01-01",
    end_date: str = "2099-12-31",
) -> str:
    """
    Generate a grouped bar chart breaking down Hard vs Soft bounces per SFMC journey.

    Use this when the user asks:
    - "Show me bounce breakdown" / "hard vs soft bounces"
    - "Which journey has the most bounces?"
    - "Bounce analysis chart"

    Args:
        start_date: Start of date range (YYYY-MM-DD).
        end_date:   End of date range (YYYY-MM-DD).
    """
    return _charts.bounce_analysis_chart(start_date, end_date)


@tool
def chart_email_kpi_scorecard(
    start_date: str = "2020-01-01",
    end_date: str = "2099-12-31",
) -> str:
    """
    Generate a horizontal bar KPI scorecard showing open rate, click rate,
    bounce rate, unsubscribe rate, and spam rate as percentages.

    Use this when the user asks:
    - "Show me email performance metrics" / "email KPIs"
    - "What are our open rates, click rates, bounce rates?"
    - "Email health scorecard" / "KPI overview chart"

    Args:
        start_date: Start of date range (YYYY-MM-DD).
        end_date:   End of date range (YYYY-MM-DD).
    """
    return _charts.email_kpi_scorecard_chart(start_date, end_date)


@tool
def chart_journey_stage_progression() -> str:
    """
    Generate a horizontal bar chart showing how many prospects reached (had sent = TRUE)
    each of the 9 SFMC journey stages. Reveals exactly where prospects drop off across
    Stage 1 (Welcome) through Stage 9 (Final Reminder).

    Use this when the user asks:
    - "How many prospects reached each stage?"
    - "Journey progression chart" / "Stage completion chart"
    - "Which stage has the biggest drop-off?"
    - "Show stage-by-stage reach"
    """
    return _charts.journey_stage_progression_chart()


@tool
def chart_daily_engagement_trend(
    start_date: str = "2020-01-01",
    end_date: str = "2099-12-31",
    event_types: str = "SENT,OPEN,CLICK",
) -> str:
    """
    Generate a multi-line time-series chart of daily SFMC engagement events over time.
    Shows SENT, OPEN, CLICK (and any others specified) as separate coloured lines.

    Use this when the user asks:
    - "Show daily email engagement trend"
    - "How have opens/clicks changed over time?"
    - "Plot SFMC events over the last 30 days"
    - "Daily trend chart"

    Args:
        start_date:   Start of date range (YYYY-MM-DD).
        end_date:     End of date range (YYYY-MM-DD).
        event_types:  Comma-separated event types to plot (e.g. "SENT,OPEN,CLICK,BOUNCE").
    """
    return _charts.daily_engagement_trend_chart(start_date, end_date, event_types)


@tool
def chart_prospect_channel_mix(
    start_date: str = "2020-01-01",
    end_date: str = "2099-12-31",
) -> str:
    """
    Generate a donut chart showing the distribution of prospects by lead source channel.

    Use this when the user asks:
    - "Where do our leads come from?"
    - "Channel mix chart" / "Source distribution"
    - "Which channel drives the most prospects?"
    - "Lead source breakdown chart"

    Args:
        start_date: Start of date range (YYYY-MM-DD).
        end_date:   End of date range (YYYY-MM-DD).
    """
    return _charts.prospect_channel_mix_chart(start_date, end_date)


@tool
def chart_funnel_waterfall(
    start_date: str = "2020-01-01",
    end_date: str = "2099-12-31",
) -> str:
    """
    Generate a waterfall chart showing absolute volume and loss at each funnel stage:
    Lead Intake → Valid Prospects → SFMC Sent → Opened → Clicked.
    Each bar shows the drop-off (negative delta) from the previous stage.

    Use this when the user asks:
    - "Show me funnel drop-off" / "where are we losing prospects?"
    - "Funnel waterfall" / "funnel loss chart"
    - "How much drops between each stage?"

    Args:
        start_date: Start of date range (YYYY-MM-DD).
        end_date:   End of date range (YYYY-MM-DD).
    """
    return _charts.funnel_waterfall_chart(start_date, end_date)


# ---------------------------------------------------------------------------
# Tool 11d: Journey Stage Drop-off (Stage 01 → 09 progression)
# ---------------------------------------------------------------------------

@tool
def get_journey_stage_dropoff() -> str:
    """
    Show the stage-by-stage email reach across all 9 stages of the Prospect Journey.

    IMPORTANT INTERPRETATION RULES:
    - A lower count at a later stage does NOT mean prospects "dropped off".
    - Prospects only receive the next stage email after a defined interval (3/5/8/3/2/2/2/2 days).
    - The only reason a prospect does NOT receive the next stage email is SUPPRESSION (IS_SUPPRESSED=TRUE).
    - Prospects who are not yet suppressed and have not yet reached a later stage are simply
      "awaiting their next interval" — they have NOT dropped off.
    - "Emails to be sent" = prospects who received the prior stage but have not yet received
      this stage. This count includes both suppressed prospects AND those awaiting the next interval.

    Returns:
        Part 1: Stage-by-stage email reach table (Prospects Sent, Emails to be sent, % Reached).
        Part 2: Suppression by stage — where in the journey were suppressed prospects stopped.

    ALWAYS use this tool when the user asks:
    - "Where are prospects dropping off in the journey?"
    - "How many prospects reached each stage?"
    - "Stage-by-stage progression", "journey funnel", "stage reach"
    - "Show me suppression between stages"

    DO NOT use get_funnel_metrics for within-journey stage questions.
    """
    # ------------------------------------------------------------------
    # PART 1: Stage-by-stage email reach — COUNT_IF per stage (Q4 pattern)
    # Columns: Stage Name, Phase, Prospects Sent, Emails to be sent, % Reached
    # "Emails to be sent" = prior stage sent count minus this stage sent count
    # (includes suppressed + awaiting interval — NOT labelled as "dropped off")
    # ------------------------------------------------------------------
    progression_sql = textwrap.dedent("""
        WITH base AS (
            SELECT
                COUNT_IF(UPPER(TRIM(WELCOMEJOURNEY_WELCOMEEMAIL_SENT))      = 'TRUE') AS s01,
                COUNT_IF(UPPER(TRIM(WELCOMEJOURNEY_EDUCATIONEMAIL_SENT))    = 'TRUE') AS s02,
                COUNT_IF(UPPER(TRIM(NURTUREJOURNEY_EDUCATIONEMAIL1_SENT))   = 'TRUE') AS s03,
                COUNT_IF(UPPER(TRIM(NURTUREJOURNEY_EDUCATIONEMAIL2_SENT))   = 'TRUE') AS s04,
                COUNT_IF(UPPER(TRIM(NURTUREJOURNEY_PROSPECTSTORYEMAIL_SENT))= 'TRUE') AS s05,
                COUNT_IF(UPPER(TRIM(HIGHENGAGEMENT_CONVERSIONEMAIL_SENT))   = 'TRUE') AS s06,
                COUNT_IF(UPPER(TRIM(HIGHENGAGEMENT_REMINDEREMAIL_SENT))     = 'TRUE') AS s07,
                COUNT_IF(UPPER(TRIM(LOWENGAGEMENT_REENGAGEMENTEMAIL_SENT))  = 'TRUE') AS s08,
                COUNT_IF(UPPER(TRIM(LOWENGAGEMENTFINALREMINDEREMAIL_SENT))  = 'TRUE') AS s09
            FROM QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_PROSPECT_JOURNEY_DETAILS
        )
        SELECT
            sn                                                              AS "Stage #",
            stage_name                                                      AS "Stage Name",
            phase                                                           AS "Phase",
            sent                                                            AS "Prospects Sent",
            CASE WHEN sn = 1 THEN NULL ELSE (prev - sent) END              AS "Emails to be sent",
            ROUND(sent * 100.0 / NULLIF(s01, 0), 1)                       AS "% Reached"
        FROM (
            SELECT 1  AS sn, 'Stage 01 — Welcome Email'       AS stage_name, 'Welcome Phase'         AS phase, s01 AS sent, NULL AS prev, s01 FROM base
            UNION ALL SELECT 2,  'Stage 02 — Education Email',       'Welcome Phase',         s02, s01, s01 FROM base
            UNION ALL SELECT 3,  'Stage 03 — Education Email 1',     'Nurture Phase',         s03, s02, s01 FROM base
            UNION ALL SELECT 4,  'Stage 04 — Education Email 2',     'Nurture Phase',         s04, s03, s01 FROM base
            UNION ALL SELECT 5,  'Stage 05 — Prospect Story Email',  'Nurture Phase',         s05, s04, s01 FROM base
            UNION ALL SELECT 6,  'Stage 06 — Conversion Email',      'High Engagement Phase', s06, s05, s01 FROM base
            UNION ALL SELECT 7,  'Stage 07 — Reminder Email',        'High Engagement Phase', s07, s06, s01 FROM base
            UNION ALL SELECT 8,  'Stage 08 — Re-engagement Email',   'Low Engagement Phase',  s08, s07, s01 FROM base
            UNION ALL SELECT 9,  'Stage 09 — Final Reminder Email',  'Low Engagement Phase',  s09, s08, s01 FROM base
        )
        ORDER BY sn
    """)
    progression_result = _run(progression_sql, max_rows=9)

    # ------------------------------------------------------------------
    # PART 2: Suppression by journey stage — Q3 pattern
    # Only suppressed prospects (SUPPRESSION_FLAG = 'true').
    # Identifies which stage transition triggered the suppression cutoff.
    # ------------------------------------------------------------------
    suppression_sql = textwrap.dedent("""
        SELECT
            CASE
                WHEN UPPER(TRIM(WELCOMEJOURNEY_WELCOMEEMAIL_SENT))      = 'TRUE'
                 AND UPPER(TRIM(WELCOMEJOURNEY_EDUCATIONEMAIL_SENT))    != 'TRUE'
                THEN 'Suppressed after Stage 01 — Welcome Email'
                WHEN UPPER(TRIM(WELCOMEJOURNEY_EDUCATIONEMAIL_SENT))    = 'TRUE'
                 AND UPPER(TRIM(NURTUREJOURNEY_EDUCATIONEMAIL1_SENT))   != 'TRUE'
                THEN 'Suppressed after Stage 02 — Education Email'
                WHEN UPPER(TRIM(NURTUREJOURNEY_EDUCATIONEMAIL1_SENT))   = 'TRUE'
                 AND UPPER(TRIM(NURTUREJOURNEY_EDUCATIONEMAIL2_SENT))   != 'TRUE'
                THEN 'Suppressed after Stage 03 — Education Email 1'
                WHEN UPPER(TRIM(NURTUREJOURNEY_EDUCATIONEMAIL2_SENT))   = 'TRUE'
                 AND UPPER(TRIM(NURTUREJOURNEY_PROSPECTSTORYEMAIL_SENT))!= 'TRUE'
                THEN 'Suppressed after Stage 04 — Education Email 2'
                WHEN UPPER(TRIM(NURTUREJOURNEY_PROSPECTSTORYEMAIL_SENT))= 'TRUE'
                 AND UPPER(TRIM(HIGHENGAGEMENT_CONVERSIONEMAIL_SENT))   != 'TRUE'
                 AND UPPER(TRIM(LOWENGAGEMENT_REENGAGEMENTEMAIL_SENT))  != 'TRUE'
                THEN 'Suppressed after Stage 05 — Prospect Story (pre-branch)'
                WHEN UPPER(TRIM(HIGHENGAGEMENT_CONVERSIONEMAIL_SENT))   = 'TRUE'
                 AND UPPER(TRIM(HIGHENGAGEMENT_REMINDEREMAIL_SENT))     != 'TRUE'
                THEN 'Suppressed after Stage 06 — Conversion Email'
                WHEN UPPER(TRIM(HIGHENGAGEMENT_REMINDEREMAIL_SENT))     = 'TRUE'
                THEN 'Suppressed after Stage 07 — Reminder Email'
                WHEN UPPER(TRIM(LOWENGAGEMENT_REENGAGEMENTEMAIL_SENT))  = 'TRUE'
                 AND UPPER(TRIM(LOWENGAGEMENTFINALREMINDEREMAIL_SENT))  != 'TRUE'
                THEN 'Suppressed after Stage 08 — Re-engagement Email'
                WHEN UPPER(TRIM(LOWENGAGEMENTFINALREMINDEREMAIL_SENT))  = 'TRUE'
                THEN 'Suppressed after Stage 09 — Final Reminder (journey end)'
                ELSE 'Suppressed before Stage 01 (never entered journey)'
            END                                                            AS "Suppressed At",
            COUNT(*)                                                       AS "Suppressed Prospect Count",
            ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1)           AS "% of All Suppressed"
        FROM QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_PROSPECT_JOURNEY_DETAILS
        WHERE UPPER(TRIM(SUPPRESSION_FLAG)) IN ('YES','Y','TRUE','1')
        GROUP BY 1
        ORDER BY 2 DESC
    """)
    suppression_result = _run(suppression_sql, max_rows=20)

    return (
        "### Prospect Journey — Stage-by-Stage Email Reach (Stage 01 → 09)\n"
        "Note: 'Emails to be sent' = prospects who received the prior stage email but not this stage yet.\n"
        "      This includes suppressed prospects AND those awaiting the next scheduled interval.\n"
        "      A lower count at a later stage does NOT mean drop-off — see Part 2 for suppression detail.\n\n"
        + progression_result
        + "\n\n### Suppression by Journey Stage — Where Were Suppressed Prospects Stopped?\n"
        "Note: Only prospects with SUPPRESSION_FLAG=TRUE are included below.\n\n"
        + suppression_result
    )


# ---------------------------------------------------------------------------
# Tool 11e: Journey Overview — total / suppressed / active / phase completions
# ---------------------------------------------------------------------------

@tool
def get_journey_overview() -> str:
    """
    Return a high-level Prospect Journey health summary:
    - Total prospects in the journey table
    - Suppressed vs active counts and suppression rate %
    - Welcome Phase completion count
    - Nurture Phase completion count
    - Prospects with zero emails sent (pre-journey suppression candidates)
    - High Engagement path vs Low Engagement path vs Not Yet Branched

    ALWAYS call this tool first when the user asks:
    - "Give me a journey overview / summary"
    - "How is the Prospect Journey performing?"
    - "What is the suppression rate for the journey?"
    - "How many prospects completed the Welcome Phase or Nurture Phase?"
    - Any broad journey health or status question before drilling into stages

    Returns:
        Journey health summary and engagement path split as markdown tables.
    """
    overview_sql = textwrap.dedent("""
        SELECT
            COUNT(*)                                                                                       AS "Total Prospects in Journey",
            SUM(CASE WHEN UPPER(TRIM(SUPPRESSION_FLAG)) IN ('YES','Y','TRUE','1') THEN 1 ELSE 0 END)     AS "Suppressed",
            SUM(CASE WHEN UPPER(TRIM(SUPPRESSION_FLAG)) NOT IN ('YES','Y','TRUE','1')
                       OR SUPPRESSION_FLAG IS NULL THEN 1 ELSE 0 END)                                    AS "Active (Not Suppressed)",
            ROUND(SUM(CASE WHEN UPPER(TRIM(SUPPRESSION_FLAG)) IN ('YES','Y','TRUE','1') THEN 1 ELSE 0 END)
                  * 100.0 / COUNT(*), 1)                                                                  AS "Suppression Rate %",
            SUM(CASE WHEN UPPER(TRIM(WELCOME_JOURNEY_COMPLETE)) = 'TRUE' THEN 1 ELSE 0 END)              AS "Welcome Phase Complete",
            SUM(CASE WHEN UPPER(TRIM(NURTURE_JOURNEY_COMPLETE))  = 'TRUE' THEN 1 ELSE 0 END)             AS "Nurture Phase Complete",
            SUM(CASE WHEN UPPER(TRIM(WELCOMEJOURNEY_WELCOMEEMAIL_SENT)) != 'TRUE'
                      AND UPPER(TRIM(WELCOMEJOURNEY_EDUCATIONEMAIL_SENT)) != 'TRUE'
                      AND UPPER(TRIM(NURTUREJOURNEY_EDUCATIONEMAIL1_SENT)) != 'TRUE'
                      AND UPPER(TRIM(NURTUREJOURNEY_EDUCATIONEMAIL2_SENT)) != 'TRUE'
                      AND UPPER(TRIM(NURTUREJOURNEY_PROSPECTSTORYEMAIL_SENT)) != 'TRUE'
                      AND UPPER(TRIM(HIGHENGAGEMENT_CONVERSIONEMAIL_SENT)) != 'TRUE'
                      AND UPPER(TRIM(HIGHENGAGEMENT_REMINDEREMAIL_SENT)) != 'TRUE'
                      AND UPPER(TRIM(LOWENGAGEMENT_REENGAGEMENTEMAIL_SENT)) != 'TRUE'
                      AND UPPER(TRIM(LOWENGAGEMENTFINALREMINDEREMAIL_SENT)) != 'TRUE'
                 THEN 1 ELSE 0 END)                                                                       AS "Zero Emails Sent (Never Entered)"
        FROM QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_PROSPECT_JOURNEY_DETAILS
    """)

    path_sql = textwrap.dedent("""
        SELECT
            CASE
                WHEN UPPER(TRIM(HIGHENGAGEMENT_CONVERSIONEMAIL_SENT)) = 'TRUE'
                  OR UPPER(TRIM(HIGHENGAGEMENT_REMINDEREMAIL_SENT))   = 'TRUE'
                THEN 'High Engagement Path'
                WHEN UPPER(TRIM(LOWENGAGEMENT_REENGAGEMENTEMAIL_SENT))     = 'TRUE'
                  OR UPPER(TRIM(LOWENGAGEMENTFINALREMINDEREMAIL_SENT)) = 'TRUE'
                THEN 'Low Engagement Path'
                ELSE 'Not Yet Branched'
            END                                                                                         AS "Engagement Path",
            COUNT(*)                                                                                    AS "Prospect Count",
            ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1)                                        AS "% of Total",
            SUM(CASE WHEN UPPER(TRIM(SUPPRESSION_FLAG)) IN ('YES','Y','TRUE','1') THEN 1 ELSE 0 END)  AS "Suppressed in Path"
        FROM QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_PROSPECT_JOURNEY_DETAILS
        GROUP BY 1
        ORDER BY 2 DESC
    """)

    zero_email_sql = textwrap.dedent("""
        SELECT
            SUM(CASE WHEN UPPER(TRIM(SUPPRESSION_FLAG)) IN ('YES','Y','TRUE','1') THEN 1 ELSE 0 END)    AS "Zero-Email + Suppressed (pre-journey suppression)",
            SUM(CASE WHEN UPPER(TRIM(SUPPRESSION_FLAG)) NOT IN ('YES','Y','TRUE','1')
                       OR SUPPRESSION_FLAG IS NULL THEN 1 ELSE 0 END)                                   AS "Zero-Email + Not Suppressed (awaiting journey entry)"
        FROM QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_PROSPECT_JOURNEY_DETAILS
        WHERE UPPER(TRIM(WELCOMEJOURNEY_WELCOMEEMAIL_SENT))      != 'TRUE'
          AND UPPER(TRIM(WELCOMEJOURNEY_EDUCATIONEMAIL_SENT))    != 'TRUE'
          AND UPPER(TRIM(NURTUREJOURNEY_EDUCATIONEMAIL1_SENT))   != 'TRUE'
          AND UPPER(TRIM(NURTUREJOURNEY_EDUCATIONEMAIL2_SENT))   != 'TRUE'
          AND UPPER(TRIM(NURTUREJOURNEY_PROSPECTSTORYEMAIL_SENT))!= 'TRUE'
          AND UPPER(TRIM(HIGHENGAGEMENT_CONVERSIONEMAIL_SENT))   != 'TRUE'
          AND UPPER(TRIM(HIGHENGAGEMENT_REMINDEREMAIL_SENT))     != 'TRUE'
          AND UPPER(TRIM(LOWENGAGEMENT_REENGAGEMENTEMAIL_SENT))  != 'TRUE'
          AND UPPER(TRIM(LOWENGAGEMENTFINALREMINDEREMAIL_SENT))  != 'TRUE'
    """)

    return (
        "### Prospect Journey — Overall Health Summary\n"
        + _run(overview_sql, max_rows=1)
        + "\n\n### Engagement Path Split (High vs Low vs Not Yet Branched)\n"
        + _run(path_sql, max_rows=5)
        + "\n\n### Zero-Email Prospects — Pre-Journey Suppression vs Awaiting Entry\n"
        + _run(zero_email_sql, max_rows=1)
    )


# ---------------------------------------------------------------------------
# Tool 11f: Journey Suppression Linkage — per-stage transition analysis
# ---------------------------------------------------------------------------

@tool
def get_journey_suppression_linkage() -> str:
    """
    Detailed suppression analysis for the Prospect Journey.

    SUPPRESSION IS THE ONLY REASON a prospect does not advance to the next stage.
    If IS_SUPPRESSED=FALSE and a later stage has not been sent, the prospect is simply
    awaiting their next scheduled interval — they have NOT dropped off.

    This tool returns:
    - Part 1: Total suppressed prospects and suppression rate (Q1 summary)
    - Part 2: For suppressed prospects only — exactly which stage transition triggered
              the suppression cutoff (Q3 pattern: sent Stage N but NOT Stage N+1, suppressed)
    - Part 3: Sequencing anomalies — later stage sent without the prior stage (data quality flag)

    ALWAYS call this tool for:
    - "Why are prospects being suppressed?"
    - "At which stage is suppression highest?"
    - "How many prospects were suppressed at Stage N?"
    - "Are there data quality issues in the journey?"
    - "Show me suppression by stage transition"

    Returns:
        Suppression summary + per-stage suppression breakdown + anomaly flags.
    """
    # ------------------------------------------------------------------
    # PART 1: Overall suppression summary (Q1 pattern)
    # ------------------------------------------------------------------
    summary_sql = textwrap.dedent("""
        SELECT
            COUNT(*)                                                                                        AS "Total Prospects",
            COUNT_IF(UPPER(TRIM(SUPPRESSION_FLAG)) IN ('YES','Y','TRUE','1'))                              AS "Suppressed",
            COUNT_IF(UPPER(TRIM(SUPPRESSION_FLAG)) NOT IN ('YES','Y','TRUE','1')
                     OR SUPPRESSION_FLAG IS NULL)                                                           AS "Active (Not Suppressed)",
            ROUND(COUNT_IF(UPPER(TRIM(SUPPRESSION_FLAG)) IN ('YES','Y','TRUE','1'))
                  * 100.0 / COUNT(*), 1)                                                                    AS "Suppression Rate %",
            COUNT_IF(UPPER(TRIM(WELCOME_JOURNEY_COMPLETE))  = 'TRUE')                                      AS "Welcome Phase Complete",
            COUNT_IF(UPPER(TRIM(NURTURE_JOURNEY_COMPLETE))  = 'TRUE')                                      AS "Nurture Phase Complete"
        FROM QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_PROSPECT_JOURNEY_DETAILS
    """)
    summary_result = _run(summary_sql, max_rows=1)

    # ------------------------------------------------------------------
    # PART 2: Suppression by journey stage (Q3 pattern)
    # Filters to SUPPRESSION_FLAG=TRUE only.
    # CASE logic: Stage N sent AND Stage N+1 NOT sent → suppressed at that transition.
    # ------------------------------------------------------------------
    stage_sql = textwrap.dedent("""
        SELECT
            CASE
                WHEN UPPER(TRIM(WELCOMEJOURNEY_WELCOMEEMAIL_SENT))      = 'TRUE'
                 AND UPPER(TRIM(WELCOMEJOURNEY_EDUCATIONEMAIL_SENT))    != 'TRUE'
                THEN 'Suppressed after Stage 01 — Welcome Email'
                WHEN UPPER(TRIM(WELCOMEJOURNEY_EDUCATIONEMAIL_SENT))    = 'TRUE'
                 AND UPPER(TRIM(NURTUREJOURNEY_EDUCATIONEMAIL1_SENT))   != 'TRUE'
                THEN 'Suppressed after Stage 02 — Education Email'
                WHEN UPPER(TRIM(NURTUREJOURNEY_EDUCATIONEMAIL1_SENT))   = 'TRUE'
                 AND UPPER(TRIM(NURTUREJOURNEY_EDUCATIONEMAIL2_SENT))   != 'TRUE'
                THEN 'Suppressed after Stage 03 — Education Email 1'
                WHEN UPPER(TRIM(NURTUREJOURNEY_EDUCATIONEMAIL2_SENT))   = 'TRUE'
                 AND UPPER(TRIM(NURTUREJOURNEY_PROSPECTSTORYEMAIL_SENT))!= 'TRUE'
                THEN 'Suppressed after Stage 04 — Education Email 2'
                WHEN UPPER(TRIM(NURTUREJOURNEY_PROSPECTSTORYEMAIL_SENT))= 'TRUE'
                 AND UPPER(TRIM(HIGHENGAGEMENT_CONVERSIONEMAIL_SENT))   != 'TRUE'
                 AND UPPER(TRIM(LOWENGAGEMENT_REENGAGEMENTEMAIL_SENT))  != 'TRUE'
                THEN 'Suppressed after Stage 05 — Prospect Story (pre-branch)'
                WHEN UPPER(TRIM(HIGHENGAGEMENT_CONVERSIONEMAIL_SENT))   = 'TRUE'
                 AND UPPER(TRIM(HIGHENGAGEMENT_REMINDEREMAIL_SENT))     != 'TRUE'
                THEN 'Suppressed after Stage 06 — Conversion Email'
                WHEN UPPER(TRIM(HIGHENGAGEMENT_REMINDEREMAIL_SENT))     = 'TRUE'
                THEN 'Suppressed after Stage 07 — Reminder Email'
                WHEN UPPER(TRIM(LOWENGAGEMENT_REENGAGEMENTEMAIL_SENT))  = 'TRUE'
                 AND UPPER(TRIM(LOWENGAGEMENTFINALREMINDEREMAIL_SENT))  != 'TRUE'
                THEN 'Suppressed after Stage 08 — Re-engagement Email'
                WHEN UPPER(TRIM(LOWENGAGEMENTFINALREMINDEREMAIL_SENT))  = 'TRUE'
                THEN 'Suppressed after Stage 09 — Final Reminder (journey end)'
                ELSE 'Suppressed before Stage 01 (never entered journey)'
            END                                                            AS "Suppressed At Stage",
            COUNT(*)                                                       AS "Suppressed Prospect Count",
            ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1)           AS "% of All Suppressed"
        FROM QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_PROSPECT_JOURNEY_DETAILS
        WHERE UPPER(TRIM(SUPPRESSION_FLAG)) IN ('YES','Y','TRUE','1')
        GROUP BY 1
        ORDER BY 2 DESC
    """)
    stage_result = _run(stage_sql, max_rows=15)

    # ------------------------------------------------------------------
    # PART 3: Sequencing anomalies — later stage sent but earlier blank
    # (Only non-suppressed prospects — suppressed ones may have gaps intentionally)
    # ------------------------------------------------------------------
    anomaly_sql = textwrap.dedent("""
        SELECT
            'Stage 02 sent but Stage 01 not sent' AS "Anomaly Type",
            COUNT(*) AS "Count"
        FROM QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_PROSPECT_JOURNEY_DETAILS
        WHERE UPPER(TRIM(WELCOMEJOURNEY_EDUCATIONEMAIL_SENT)) = 'TRUE'
          AND UPPER(TRIM(WELCOMEJOURNEY_WELCOMEEMAIL_SENT))  != 'TRUE'
          AND (UPPER(TRIM(SUPPRESSION_FLAG)) NOT IN ('YES','Y','TRUE','1') OR SUPPRESSION_FLAG IS NULL)
        UNION ALL
        SELECT 'Stage 03 sent but Stage 02 not sent', COUNT(*)
        FROM QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_PROSPECT_JOURNEY_DETAILS
        WHERE UPPER(TRIM(NURTUREJOURNEY_EDUCATIONEMAIL1_SENT)) = 'TRUE'
          AND UPPER(TRIM(WELCOMEJOURNEY_EDUCATIONEMAIL_SENT)) != 'TRUE'
          AND (UPPER(TRIM(SUPPRESSION_FLAG)) NOT IN ('YES','Y','TRUE','1') OR SUPPRESSION_FLAG IS NULL)
        UNION ALL
        SELECT 'Stage 04 sent but Stage 03 not sent', COUNT(*)
        FROM QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_PROSPECT_JOURNEY_DETAILS
        WHERE UPPER(TRIM(NURTUREJOURNEY_EDUCATIONEMAIL2_SENT))  = 'TRUE'
          AND UPPER(TRIM(NURTUREJOURNEY_EDUCATIONEMAIL1_SENT)) != 'TRUE'
          AND (UPPER(TRIM(SUPPRESSION_FLAG)) NOT IN ('YES','Y','TRUE','1') OR SUPPRESSION_FLAG IS NULL)
        UNION ALL
        SELECT 'Stage 05 sent but Stage 04 not sent', COUNT(*)
        FROM QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_PROSPECT_JOURNEY_DETAILS
        WHERE UPPER(TRIM(NURTUREJOURNEY_PROSPECTSTORYEMAIL_SENT))= 'TRUE'
          AND UPPER(TRIM(NURTUREJOURNEY_EDUCATIONEMAIL2_SENT)) != 'TRUE'
          AND (UPPER(TRIM(SUPPRESSION_FLAG)) NOT IN ('YES','Y','TRUE','1') OR SUPPRESSION_FLAG IS NULL)
        UNION ALL
        SELECT 'Stage 06 sent but Stage 05 not sent', COUNT(*)
        FROM QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_PROSPECT_JOURNEY_DETAILS
        WHERE UPPER(TRIM(HIGHENGAGEMENT_CONVERSIONEMAIL_SENT))   = 'TRUE'
          AND UPPER(TRIM(NURTUREJOURNEY_PROSPECTSTORYEMAIL_SENT))!= 'TRUE'
          AND (UPPER(TRIM(SUPPRESSION_FLAG)) NOT IN ('YES','Y','TRUE','1') OR SUPPRESSION_FLAG IS NULL)
    """)
    anomaly_result = _run(anomaly_sql, max_rows=10)

    return (
        "### Prospect Journey — Suppression Summary\n"
        "Note: Suppression is the ONLY reason a prospect does not advance to the next stage.\n"
        "      Prospects with IS_SUPPRESSED=FALSE who have not yet reached a later stage\n"
        "      are awaiting their next scheduled interval — they have NOT been suppressed.\n\n"
        + summary_result
        + "\n\n### Suppression by Journey Stage — Where Were Prospects Suppressed? (Q3 pattern)\n"
        "Filtered to SUPPRESSION_FLAG=TRUE only. Each row = Stage N was sent but Stage N+1 was not.\n\n"
        + stage_result
        + "\n\n### Sequencing Anomalies — Later Stage Sent Without Prior Stage (non-suppressed only)\n"
        "A count > 0 indicates a data quality or journey logic issue that should be investigated.\n\n"
        + anomaly_result
    )


# ---------------------------------------------------------------------------
# Tool 11g: Journey Pace Analysis — avg days between stages
# ---------------------------------------------------------------------------

@tool
def get_journey_pace_analysis() -> str:
    """
    Calculate the average number of days between consecutive journey stages
    for prospects who successfully progressed through each transition.

    Use this to understand:
    - Whether journey stage timing is on schedule vs the expected intervals
    - Which transitions have unusually long or short gaps
    - Whether pace anomalies correlate with suppression points

    Expected stage intervals (business rules):
    Stage 01→02: 3 days | Stage 02→03: 5 days | Stage 03→04: 8 days
    Stage 04→05: 3 days | Stage 05→06: 2 days | Stage 06→07: 2 days
    Stage 07→08: 2 days | Stage 08→09: 2 days

    Use this when the user asks:
    - "How fast are prospects moving through the journey?"
    - "Are stage emails being sent on time?"
    - "What is the average pace of the prospect journey?"
    - "Are there timing anomalies between stages?"

    Returns:
        Table of average days per stage transition vs expected interval.
    """
    pace_sql = textwrap.dedent("""
        SELECT
            'Stage 01 → Stage 02 (expected: 3 days)'  AS transition,
            3                                          AS expected_days,
            ROUND(AVG(DATEDIFF('day',
                TRY_TO_TIMESTAMP(WELCOMEJOURNEY_WELCOMEEMAIL_SENT_DATE),
                TRY_TO_TIMESTAMP(WELCOMEJOURNEY_EDUCATIONEMAIL_SENT_DATE)
            )), 1)                                     AS avg_actual_days,
            COUNT(*)                                   AS prospects_in_transition
        FROM QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_PROSPECT_JOURNEY_DETAILS
        WHERE UPPER(TRIM(WELCOMEJOURNEY_WELCOMEEMAIL_SENT))   = 'TRUE'
          AND UPPER(TRIM(WELCOMEJOURNEY_EDUCATIONEMAIL_SENT)) = 'TRUE'
          AND WELCOMEJOURNEY_WELCOMEEMAIL_SENT_DATE   IS NOT NULL
          AND WELCOMEJOURNEY_EDUCATIONEMAIL_SENT_DATE IS NOT NULL
        UNION ALL
        SELECT 'Stage 02 → Stage 03 (expected: 5 days)', 5,
            ROUND(AVG(DATEDIFF('day',
                TRY_TO_TIMESTAMP(WELCOMEJOURNEY_EDUCATIONEMAIL_SENT_DATE),
                TRY_TO_TIMESTAMP(NURTUREJOURNEY_EDUCATIONEMAIL1_SENT_DATE)
            )), 1), COUNT(*)
        FROM QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_PROSPECT_JOURNEY_DETAILS
        WHERE UPPER(TRIM(WELCOMEJOURNEY_EDUCATIONEMAIL_SENT))  = 'TRUE'
          AND UPPER(TRIM(NURTUREJOURNEY_EDUCATIONEMAIL1_SENT)) = 'TRUE'
          AND WELCOMEJOURNEY_EDUCATIONEMAIL_SENT_DATE   IS NOT NULL
          AND NURTUREJOURNEY_EDUCATIONEMAIL1_SENT_DATE  IS NOT NULL
        UNION ALL
        SELECT 'Stage 03 → Stage 04 (expected: 8 days)', 8,
            ROUND(AVG(DATEDIFF('day',
                TRY_TO_TIMESTAMP(NURTUREJOURNEY_EDUCATIONEMAIL1_SENT_DATE),
                TRY_TO_TIMESTAMP(NURTUREJOURNEY_EDUCATIONEMAIL2_SENT_DATE)
            )), 1), COUNT(*)
        FROM QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_PROSPECT_JOURNEY_DETAILS
        WHERE UPPER(TRIM(NURTUREJOURNEY_EDUCATIONEMAIL1_SENT)) = 'TRUE'
          AND UPPER(TRIM(NURTUREJOURNEY_EDUCATIONEMAIL2_SENT)) = 'TRUE'
          AND NURTUREJOURNEY_EDUCATIONEMAIL1_SENT_DATE IS NOT NULL
          AND NURTUREJOURNEY_EDUCATIONEMAIL2_SENT_DATE IS NOT NULL
        UNION ALL
        SELECT 'Stage 04 → Stage 05 (expected: 3 days)', 3,
            ROUND(AVG(DATEDIFF('day',
                TRY_TO_TIMESTAMP(NURTUREJOURNEY_EDUCATIONEMAIL2_SENT_DATE),
                TRY_TO_TIMESTAMP(NURTUREJOURNEY_PROSPECTSTORYEMAIL_SENT_DATE)
            )), 1), COUNT(*)
        FROM QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_PROSPECT_JOURNEY_DETAILS
        WHERE UPPER(TRIM(NURTUREJOURNEY_EDUCATIONEMAIL2_SENT))   = 'TRUE'
          AND UPPER(TRIM(NURTUREJOURNEY_PROSPECTSTORYEMAIL_SENT))= 'TRUE'
          AND NURTUREJOURNEY_EDUCATIONEMAIL2_SENT_DATE    IS NOT NULL
          AND NURTUREJOURNEY_PROSPECTSTORYEMAIL_SENT_DATE IS NOT NULL
        ORDER BY transition
    """)
    pace_result = _run(pace_sql, max_rows=10)
    return "### Prospect Journey — Average Days Between Stages vs Expected Intervals\n" + pace_result


# ---------------------------------------------------------------------------
# Tool registry
# ---------------------------------------------------------------------------

ALL_TOOLS = [
    run_sql,
    get_funnel_metrics,
    get_rejection_analysis,
    get_sfmc_engagement_stats,
    get_drop_analysis,
    trace_prospect,
    get_ai_intelligence,
    get_prospect_conversion_analysis,
    get_pipeline_observability,
    get_rejected_lead_details,
    get_prospect_details,
    get_sfmc_stage_suppression,
    get_journey_stage_dropoff,
    get_journey_overview,
    get_journey_suppression_linkage,
    get_journey_pace_analysis,
    get_sfmc_prospect_outbound_match,
    # Chart tools — purpose-built
    chart_smart,
    chart_funnel,
    chart_rejections,
    chart_engagement,
    chart_conversion_segments,
    chart_intake_trend,
    chart_sfmc_stage_fishbone,
    # Chart tools — new (v2)
    chart_bounce_analysis,
    chart_email_kpi_scorecard,
    chart_journey_stage_progression,
    chart_daily_engagement_trend,
    chart_prospect_channel_mix,
    chart_funnel_waterfall,
]


# ---------------------------------------------------------------------------
# Tool 18: Report Email (FREL Agent only)
# ---------------------------------------------------------------------------

@tool
def send_report_email(subject: str, report_content: str) -> str:
    """
    Send a formatted FIPSAR Intelligence report email to akilesh@fipsar.com.

    Use this tool AFTER you have already gathered all the data and (optionally)
    generated charts using the chart tools. This tool will:
      - Compose a professional branded HTML email
      - Embed any charts that were generated during this conversation turn as inline images
      - Send via SMTP and return a delivery confirmation

    WHEN TO USE:
      - User says "send me an email", "email the report", "send this over email",
        "mail me the results", "send the chart by email", etc.

    WORKFLOW (always follow this order):
      1. Call the relevant data tools to gather the information requested
      2. Call chart tools if the user wants a chart in the email
      3. Call send_report_email LAST with a clear subject and the full report as report_content

    Args:
        subject: A clear, descriptive email subject line.
                 Example: "FIPSAR Funnel Report — January 2026"
        report_content: The full report in markdown format. Include all tables,
                        metrics, and insights gathered from your data tool calls.
                        Be comprehensive — the recipient will read this in their inbox.

    Returns:
        Delivery confirmation string with recipient, subject, and chart count.
    """
    import chart_store
    from email_sender import send_email

    # Grab any charts generated during this agent turn (non-destructive peek)
    chart_figures = chart_store.peek_all_current()

    result = send_email(
        subject=subject,
        report_markdown=report_content,
        chart_figures=chart_figures,
    )

    if result["success"]:
        charts_note = (
            f" {result['charts_attached']} chart(s) embedded as inline images."
            if result["charts_attached"] > 0
            else " No charts were attached (no chart tools were called before this tool)."
        )
        return (
            f"✅ Email sent successfully.\n"
            f"  To: {result['to']}\n"
            f"  Subject: {result['subject']}\n"
            f" {charts_note}"
        )
    else:
        return f"❌ Email could not be sent.\n  Reason: {result['message']}"


# FREL Agent tool list: all 17 data+chart tools + the email tool
FREL_TOOLS = ALL_TOOLS + [send_report_email]
