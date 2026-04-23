"""
analytics_sf.py
---------------
Analytics dashboard helpers for the Snowflake Streamlit app.
All functions use Snowpark session; return pandas DataFrames or dicts for Plotly charting.
No external dependencies — only pandas + snowflake-snowpark-python.
"""

from __future__ import annotations
import pandas as pd
from typing import Optional


# ---------------------------------------------------------------------------
# Internal helper
# ---------------------------------------------------------------------------

def _df(session, sql: str) -> pd.DataFrame:
    """Execute SQL and return a pandas DataFrame. Returns empty DF on error."""
    try:
        return session.sql(sql).to_pandas()
    except Exception:
        return pd.DataFrame()


# ---------------------------------------------------------------------------
# KPI Cards
# ---------------------------------------------------------------------------

def get_kpi_summary(session, start_date: str = "2020-01-01", end_date: str = "2099-12-31") -> dict:
    """
    Returns top-level pipeline KPIs as a dict.
    Strategy:
      - total_leads / valid_prospects / suppressed → reuse get_funnel_df (proven to work)
      - rejections / total_journey                 → simple COUNT(*) queries, positional read
      - SFMC engagement stats                      → single-row aggregation, positional read
    All positional reads avoid Snowpark uppercase column name issues.
    """

    def _safe(df, pos=0):
        """Read a single integer from a DataFrame by position. Never raises."""
        try:
            v = df.iloc[0, pos]
            return int(v) if v is not None else 0
        except Exception:
            return 0

    # ── 1. Leads, Valid Prospects, Suppressed ──────────────────────────────
    # get_funnel_df is the UNION ALL query that demonstrably returns correct numbers
    funnel = get_funnel_df(session, start_date, end_date)
    total_leads = valid_prospects = suppressed = 0
    if not funnel.empty:
        for idx in range(len(funnel)):
            stage = str(funnel.iloc[idx, 0]).strip()
            count = int(funnel.iloc[idx, 1]) if funnel.iloc[idx, 1] is not None else 0
            if "Lead" in stage:
                total_leads = count
            elif "Valid" in stage or "Prospect" in stage:
                valid_prospects = count
            elif "Suppress" in stage:
                suppressed = count

    # ── 2. Total prospects in journey table ───────────────────────────────
    j_df = _df(session,
        "SELECT COUNT(*) FROM QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_PROSPECT_JOURNEY_DETAILS"
    )
    total_journey = _safe(j_df)

    # ── 3. Intake rejections ───────────────────────────────────────────────
    rej_df = _df(session, f"""
        SELECT COUNT(*) FROM QA_FIPSAR_AUDIT.PIPELINE_AUDIT.DQ_REJECTION_LOG
        WHERE TABLE_NAME = 'PHI_PROSPECT_MASTER'
          AND CAST(REJECTED_AT AS DATE) BETWEEN '{start_date}' AND '{end_date}'
    """)
    rejections = _safe(rej_df)

    # ── 4. SFMC engagement (single row, 4 columns, read positionally) ──────
    e_df = _df(session, f"""
        SELECT
            COUNT(*),
            SUM(CASE WHEN UPPER(TRIM(EVENT_TYPE)) IN ('SENT')           THEN 1 ELSE 0 END),
            SUM(CASE WHEN UPPER(TRIM(EVENT_TYPE)) IN ('OPEN','OPENED')  THEN 1 ELSE 0 END),
            SUM(CASE WHEN UPPER(TRIM(EVENT_TYPE)) IN ('CLICK','CLICKED') THEN 1 ELSE 0 END)
        FROM QA_FIPSAR_DW.GOLD.FACT_SFMC_ENGAGEMENT
        WHERE DATE(EVENT_TIMESTAMP) BETWEEN '{start_date}' AND '{end_date}'
    """)
    total_events = _safe(e_df, 0)
    sent_count   = _safe(e_df, 1)
    opens        = _safe(e_df, 2)
    clicks       = _safe(e_df, 3)

    # ── 5. Derived rates ───────────────────────────────────────────────────
    rejection_rate = round(rejections / total_leads * 100, 1) if total_leads > 0 else 0.0
    open_rate      = round(opens / sent_count * 100, 1)       if sent_count  > 0 else 0.0
    click_rate     = round(clicks / sent_count * 100, 1)      if sent_count  > 0 else 0.0

    return {
        "total_leads":          total_leads,
        "valid_prospects":      valid_prospects,
        "rejection_rate":       rejection_rate,
        "suppressed_prospects": suppressed,
        "active_in_journey":    total_journey - suppressed,
        "total_sfmc_events":    total_events,
        "open_rate":            open_rate,
        "click_rate":           click_rate,
    }


# ---------------------------------------------------------------------------
# Funnel chart data
# ---------------------------------------------------------------------------

def get_funnel_df(session, start_date: str = "2020-01-01", end_date: str = "2099-12-31") -> pd.DataFrame:
    """
    Returns a DataFrame with columns [Stage, Count] for a funnel chart.
    Stages: Leads Received → Valid Prospects → In Journey → Suppressed
    """
    sql = f"""
        SELECT 'Leads Received' AS stage, COUNT(*) AS cnt, 1 AS sort_order
        FROM QA_FIPSAR_PHI_HUB.STAGING.STG_PROSPECT_INTAKE
        WHERE COALESCE(
            TRY_TO_DATE(FILE_DATE::STRING, 'YYYY-MM-DD'),
            TRY_TO_DATE(FILE_DATE::STRING, 'DD-MM-YYYY')
        ) BETWEEN '{start_date}' AND '{end_date}'
        UNION ALL
        SELECT 'Valid Prospects', COUNT(*), 2
        FROM QA_FIPSAR_DW.GOLD.DIM_PROSPECT
        WHERE FIRST_INTAKE_DATE BETWEEN '{start_date}' AND '{end_date}'
        UNION ALL
        SELECT 'Entered Journey', COUNT(*), 3
        FROM QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_PROSPECT_JOURNEY_DETAILS
        WHERE UPPER(TRIM(WELCOMEJOURNEY_WELCOMEEMAIL_SENT)) = 'TRUE'
        UNION ALL
        SELECT 'Suppressed', COUNT(*), 4
        FROM QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_PROSPECT_JOURNEY_DETAILS
        WHERE UPPER(TRIM(SUPPRESSION_FLAG)) IN ('YES','Y','TRUE','1')
        ORDER BY sort_order
    """
    df = _df(session, sql)
    if df.empty:
        return pd.DataFrame({"Stage": [], "Count": []})
    df.columns = ["Stage", "Count", "Sort"]
    return df[["Stage", "Count"]]


# ---------------------------------------------------------------------------
# Journey stage reach data
# ---------------------------------------------------------------------------

def get_stage_reach_df(session) -> pd.DataFrame:
    """
    Returns DataFrame with columns [Stage, Stage_Name, Prospects_Sent, Emails_To_Be_Sent, Pct_Reached]
    for bar / waterfall charts.
    """
    sql = """
        SELECT
            s1, s2, s3, s4, s5, s6, s7, s8, s9
        FROM (
            SELECT
                COUNT_IF(UPPER(TRIM(WELCOMEJOURNEY_WELCOMEEMAIL_SENT))          = 'TRUE') AS s1,
                COUNT_IF(UPPER(TRIM(WELCOMEJOURNEY_EDUCATIONEMAIL_SENT))         = 'TRUE') AS s2,
                COUNT_IF(UPPER(TRIM(NURTUREJOURNEY_EDUCATIONEMAIL1_SENT))        = 'TRUE') AS s3,
                COUNT_IF(UPPER(TRIM(NURTUREJOURNEY_EDUCATIONEMAIL2_SENT))        = 'TRUE') AS s4,
                COUNT_IF(UPPER(TRIM(NURTUREJOURNEY_PROSPECTSTORYEMAIL_SENT))     = 'TRUE') AS s5,
                COUNT_IF(UPPER(TRIM(HIGHENGAGEMENT_CONVERSIONEMAIL_SENT))        = 'TRUE') AS s6,
                COUNT_IF(UPPER(TRIM(HIGHENGAGEMENT_REMINDEREMAIL_SENT))          = 'TRUE') AS s7,
                COUNT_IF(UPPER(TRIM(LOWENGAGEMENT_REENGAGEMENTEMAIL_SENT))       = 'TRUE') AS s8,
                COUNT_IF(UPPER(TRIM(LOWENGAGEMENTFINALREMINDEREMAIL_SENT))       = 'TRUE') AS s9
            FROM QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_PROSPECT_JOURNEY_DETAILS
        )
    """
    raw = _df(session, sql)
    if raw.empty:
        return pd.DataFrame()

    stage_names = [
        "Stage 01 — Welcome Email",
        "Stage 02 — Education Email",
        "Stage 03 — Education Email 1",
        "Stage 04 — Education Email 2",
        "Stage 05 — Prospect Story Email",
        "Stage 06 — Conversion Email",
        "Stage 07 — Reminder Email",
        "Stage 08 — Re-engagement Email",
        "Stage 09 — Final Reminder Email",
    ]
    cols = ["S1","S2","S3","S4","S5","S6","S7","S8","S9"]
    raw.columns = cols
    counts = [int(raw.iloc[0][c]) for c in cols]
    s1 = counts[0] if counts[0] > 0 else 1

    rows = []
    for i, (name, cnt) in enumerate(zip(stage_names, counts)):
        prior = counts[i - 1] if i > 0 else cnt
        to_send = prior - cnt if i > 0 else 0
        pct = round(cnt / s1 * 100, 1) if s1 > 0 else 0.0
        rows.append({
            "Stage": f"S{i+1:02d}",
            "Stage Name": name,
            "Prospects Sent": cnt,
            "Emails to be Sent": to_send,
            "% Reached": pct,
        })
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Suppression by stage data
# ---------------------------------------------------------------------------

def get_suppression_by_stage_df(session) -> pd.DataFrame:
    """
    Returns DataFrame [Stage_Name, Suppressed_Count] for prospects suppressed at each stage.
    Only counts prospects with SUPPRESSION_FLAG=TRUE.
    """
    sql = """
        SELECT
            CASE
                WHEN UPPER(TRIM(WELCOMEJOURNEY_WELCOMEEMAIL_SENT))        = 'TRUE'
                 AND UPPER(TRIM(WELCOMEJOURNEY_EDUCATIONEMAIL_SENT))      != 'TRUE' THEN 'After Stage 01'
                WHEN UPPER(TRIM(WELCOMEJOURNEY_EDUCATIONEMAIL_SENT))      = 'TRUE'
                 AND UPPER(TRIM(NURTUREJOURNEY_EDUCATIONEMAIL1_SENT))     != 'TRUE' THEN 'After Stage 02'
                WHEN UPPER(TRIM(NURTUREJOURNEY_EDUCATIONEMAIL1_SENT))     = 'TRUE'
                 AND UPPER(TRIM(NURTUREJOURNEY_EDUCATIONEMAIL2_SENT))     != 'TRUE' THEN 'After Stage 03'
                WHEN UPPER(TRIM(NURTUREJOURNEY_EDUCATIONEMAIL2_SENT))     = 'TRUE'
                 AND UPPER(TRIM(NURTUREJOURNEY_PROSPECTSTORYEMAIL_SENT))  != 'TRUE' THEN 'After Stage 04'
                WHEN UPPER(TRIM(NURTUREJOURNEY_PROSPECTSTORYEMAIL_SENT))  = 'TRUE'
                 AND UPPER(TRIM(HIGHENGAGEMENT_CONVERSIONEMAIL_SENT))     != 'TRUE'
                 AND UPPER(TRIM(LOWENGAGEMENT_REENGAGEMENTEMAIL_SENT))    != 'TRUE' THEN 'After Stage 05'
                WHEN UPPER(TRIM(HIGHENGAGEMENT_CONVERSIONEMAIL_SENT))     = 'TRUE'
                 AND UPPER(TRIM(HIGHENGAGEMENT_REMINDEREMAIL_SENT))       != 'TRUE' THEN 'After Stage 06'
                WHEN UPPER(TRIM(HIGHENGAGEMENT_REMINDEREMAIL_SENT))       = 'TRUE' THEN 'After Stage 07'
                WHEN UPPER(TRIM(LOWENGAGEMENT_REENGAGEMENTEMAIL_SENT))    = 'TRUE'
                 AND UPPER(TRIM(LOWENGAGEMENTFINALREMINDEREMAIL_SENT))    != 'TRUE' THEN 'After Stage 08'
                WHEN UPPER(TRIM(LOWENGAGEMENTFINALREMINDEREMAIL_SENT))    = 'TRUE' THEN 'After Stage 09'
                WHEN UPPER(TRIM(WELCOMEJOURNEY_WELCOMEEMAIL_SENT))       != 'TRUE' THEN 'Pre-Journey'
                ELSE 'Unknown'
            END AS suppression_stage,
            COUNT(*) AS suppressed_count
        FROM QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_PROSPECT_JOURNEY_DETAILS
        WHERE UPPER(TRIM(SUPPRESSION_FLAG)) IN ('YES','Y','TRUE','1')
        GROUP BY 1
        ORDER BY 2 DESC
    """
    df = _df(session, sql)
    if df.empty:
        return pd.DataFrame({"Suppression Stage": [], "Suppressed Count": []})
    df.columns = ["Suppression Stage", "Suppressed Count"]
    return df


# ---------------------------------------------------------------------------
# Rejection analysis data
# ---------------------------------------------------------------------------

def get_rejection_trend_df(
    session,
    start_date: str = "2020-01-01",
    end_date: str = "2099-12-31",
    category: str = "intake",
) -> pd.DataFrame:
    """
    Returns top rejection reasons with counts for bar chart.
    category: 'intake' (PHI_PROSPECT_MASTER) | 'sfmc' (FACT_SFMC_ENGAGEMENT) | 'dedup' (SLV_PROSPECT_MASTER)
    """
    table_filter = {
        "intake": "TABLE_NAME = 'PHI_PROSPECT_MASTER'",
        "sfmc": "TABLE_NAME = 'FACT_SFMC_ENGAGEMENT'",
        "dedup": "TABLE_NAME = 'SLV_PROSPECT_MASTER'",
    }.get(category, "TABLE_NAME = 'PHI_PROSPECT_MASTER'")

    sql = f"""
        SELECT
            REJECTION_REASON,
            COUNT(*) AS rejection_count
        FROM QA_FIPSAR_AUDIT.PIPELINE_AUDIT.DQ_REJECTION_LOG
        WHERE {table_filter}
          AND CAST(REJECTED_AT AS DATE) BETWEEN '{start_date}' AND '{end_date}'
        GROUP BY 1
        ORDER BY 2 DESC
        LIMIT 15
    """
    df = _df(session, sql)
    if df.empty:
        return pd.DataFrame({"Rejection Reason": [], "Count": []})
    df.columns = ["Rejection Reason", "Count"]
    return df


# ---------------------------------------------------------------------------
# SFMC engagement breakdown data
# ---------------------------------------------------------------------------

def get_engagement_breakdown_df(
    session,
    start_date: str = "2020-01-01",
    end_date: str = "2099-12-31",
) -> pd.DataFrame:
    """
    Returns SFMC event counts by event type for donut / bar chart.
    """
    sql = f"""
        SELECT
            UPPER(TRIM(EVENT_TYPE)) AS event_type,
            COUNT(*) AS event_count
        FROM QA_FIPSAR_DW.GOLD.FACT_SFMC_ENGAGEMENT
        WHERE DATE(EVENT_TIMESTAMP) BETWEEN '{start_date}' AND '{end_date}'
        GROUP BY 1
        ORDER BY 2 DESC
    """
    df = _df(session, sql)
    if df.empty:
        return pd.DataFrame({"Event Type": [], "Count": []})
    df.columns = ["Event Type", "Count"]
    return df


def get_engagement_trend_df(
    session,
    start_date: str = "2020-01-01",
    end_date: str = "2099-12-31",
) -> pd.DataFrame:
    """
    Returns daily SFMC event counts by type for line chart.
    """
    sql = f"""
        SELECT
            DATE(EVENT_TIMESTAMP) AS event_date,
            UPPER(TRIM(EVENT_TYPE)) AS event_type,
            COUNT(*) AS event_count
        FROM QA_FIPSAR_DW.GOLD.FACT_SFMC_ENGAGEMENT
        WHERE DATE(EVENT_TIMESTAMP) BETWEEN '{start_date}' AND '{end_date}'
        GROUP BY 1, 2
        ORDER BY 1, 2
    """
    df = _df(session, sql)
    if df.empty:
        return pd.DataFrame({"Event Date": [], "Event Type": [], "Count": []})
    df.columns = ["Event Date", "Event Type", "Count"]
    return df


# ---------------------------------------------------------------------------
# Intake volume trend
# ---------------------------------------------------------------------------

def get_intake_trend_df(
    session,
    start_date: str = "2020-01-01",
    end_date: str = "2099-12-31",
) -> pd.DataFrame:
    """
    Daily intake lead volume for line chart.
    """
    sql = f"""
        SELECT
            COALESCE(
                TRY_TO_DATE(FILE_DATE::STRING, 'YYYY-MM-DD'),
                TRY_TO_DATE(FILE_DATE::STRING, 'DD-MM-YYYY')
            ) AS intake_date,
            COUNT(*) AS lead_count
        FROM QA_FIPSAR_PHI_HUB.STAGING.STG_PROSPECT_INTAKE
        WHERE COALESCE(
            TRY_TO_DATE(FILE_DATE::STRING, 'YYYY-MM-DD'),
            TRY_TO_DATE(FILE_DATE::STRING, 'DD-MM-YYYY')
        ) BETWEEN '{start_date}' AND '{end_date}'
        GROUP BY 1
        ORDER BY 1
    """
    df = _df(session, sql)
    if df.empty:
        return pd.DataFrame({"Intake Date": [], "Lead Count": []})
    df.columns = ["Intake Date", "Lead Count"]
    return df


# ---------------------------------------------------------------------------
# Journey pace data
# ---------------------------------------------------------------------------

def get_pace_df(session) -> pd.DataFrame:
    """
    Returns average days between stages compared to expected intervals.
    """
    sql = """
        SELECT
            'S01 → S02' AS transition,
            3 AS expected_days,
            AVG(DATEDIFF('day',
                TRY_TO_TIMESTAMP(WELCOMEJOURNEY_WELCOMEEMAIL_SENTAT),
                TRY_TO_TIMESTAMP(WELCOMEJOURNEY_EDUCATIONEMAIL_SENTAT)
            )) AS avg_actual_days
        FROM QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_PROSPECT_JOURNEY_DETAILS
        WHERE UPPER(TRIM(WELCOMEJOURNEY_WELCOMEEMAIL_SENT))    = 'TRUE'
          AND UPPER(TRIM(WELCOMEJOURNEY_EDUCATIONEMAIL_SENT))  = 'TRUE'
        UNION ALL
        SELECT 'S02 → S03', 5,
            AVG(DATEDIFF('day',
                TRY_TO_TIMESTAMP(WELCOMEJOURNEY_EDUCATIONEMAIL_SENTAT),
                TRY_TO_TIMESTAMP(NURTUREJOURNEY_EDUCATIONEMAIL1_SENTAT)
            ))
        FROM QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_PROSPECT_JOURNEY_DETAILS
        WHERE UPPER(TRIM(WELCOMEJOURNEY_EDUCATIONEMAIL_SENT))    = 'TRUE'
          AND UPPER(TRIM(NURTUREJOURNEY_EDUCATIONEMAIL1_SENT))   = 'TRUE'
        UNION ALL
        SELECT 'S03 → S04', 8,
            AVG(DATEDIFF('day',
                TRY_TO_TIMESTAMP(NURTUREJOURNEY_EDUCATIONEMAIL1_SENTAT),
                TRY_TO_TIMESTAMP(NURTUREJOURNEY_EDUCATIONEMAIL2_SENTAT)
            ))
        FROM QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_PROSPECT_JOURNEY_DETAILS
        WHERE UPPER(TRIM(NURTUREJOURNEY_EDUCATIONEMAIL1_SENT)) = 'TRUE'
          AND UPPER(TRIM(NURTUREJOURNEY_EDUCATIONEMAIL2_SENT)) = 'TRUE'
        UNION ALL
        SELECT 'S04 → S05', 3,
            AVG(DATEDIFF('day',
                TRY_TO_TIMESTAMP(NURTUREJOURNEY_EDUCATIONEMAIL2_SENTAT),
                TRY_TO_TIMESTAMP(NURTUREJOURNEY_PROSPECTSTORYEMAIL_SENTAT)
            ))
        FROM QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_PROSPECT_JOURNEY_DETAILS
        WHERE UPPER(TRIM(NURTUREJOURNEY_EDUCATIONEMAIL2_SENT))    = 'TRUE'
          AND UPPER(TRIM(NURTUREJOURNEY_PROSPECTSTORYEMAIL_SENT)) = 'TRUE'
        ORDER BY 1
    """
    df = _df(session, sql)
    if df.empty:
        return pd.DataFrame({"Transition": [], "Expected Days": [], "Avg Actual Days": []})
    df.columns = ["Transition", "Expected Days", "Avg Actual Days"]
    df["Avg Actual Days"] = df["Avg Actual Days"].round(1)
    return df


# ---------------------------------------------------------------------------
# Recon Analytics — KPI Summary
# ---------------------------------------------------------------------------

def get_recon_kpis(session, start_date: str = "2020-01-01", end_date: str = "2099-12-31") -> dict:
    """
    6 KPIs for the Recon_Analytics tab.
    Sources: STG_PROSPECT_INTAKE, DQ_REJECTION_LOG, PHI_PROSPECT_MASTER, VW_PROSPECT_JOURNEY_ANALYTICS.
    """
    def _safe(df, pos=0):
        try:
            v = df.iloc[0, pos]
            return int(v) if v is not None else 0
        except Exception:
            return 0

    stg_filter = f"""COALESCE(
        TRY_TO_DATE(FILE_DATE::STRING, 'YYYY-MM-DD'),
        TRY_TO_DATE(FILE_DATE::STRING, 'DD-MM-YYYY')
    ) BETWEEN '{start_date}' AND '{end_date}'"""

    total_leads = _safe(_df(session, f"""
        SELECT COUNT(*) FROM QA_FIPSAR_PHI_HUB.STAGING.STG_PROSPECT_INTAKE
        WHERE {stg_filter}
    """))

    invalid_leads = _safe(_df(session, f"""
        SELECT COUNT(*) FROM QA_FIPSAR_AUDIT.PIPELINE_AUDIT.DQ_REJECTION_LOG
        WHERE TABLE_NAME = 'PHI_PROSPECT_MASTER'
          AND CAST(REJECTED_AT AS DATE) BETWEEN '{start_date}' AND '{end_date}'
    """))

    valid_prospects = _safe(_df(session, f"""
        SELECT COUNT(*) FROM QA_FIPSAR_PHI_HUB.PHI_CORE.PHI_PROSPECT_MASTER
        WHERE FILE_DATE BETWEEN '{start_date}' AND '{end_date}'
    """))

    jrn = _df(session, f"""
        SELECT
            SUM(CASE WHEN JOURNEY_STATUS = 'Journey Completed' THEN 1 ELSE 0 END),
            SUM(CASE WHEN JOURNEY_STATUS = 'In Progress'       THEN 1 ELSE 0 END),
            SUM(IS_SUPPRESSED)
        FROM QA_FIPSAR_DW.GOLD.VW_PROSPECT_JOURNEY_ANALYTICS
        WHERE INTAKE_DATE BETWEEN '{start_date}' AND '{end_date}'
    """)

    return {
        "total_leads":     total_leads,
        "invalid_leads":   invalid_leads,
        "valid_prospects": valid_prospects,
        "j_completed":     _safe(jrn, 0),
        "j_in_progress":   _safe(jrn, 1),
        "j_dropped":       _safe(jrn, 2),
    }


# ---------------------------------------------------------------------------
# Recon Analytics — Full Funnel DataFrame (Chart A source)
# ---------------------------------------------------------------------------

def get_recon_funnel_df(
    session,
    start_date: str = "2020-01-01",
    end_date: str   = "2099-12-31",
    journey_status: list = None,
    channel: str = None,
) -> pd.DataFrame:
    """
    Returns one row per funnel segment with count + tooltip breakdown data.
    Segments: Total Leads, Valid Prospects, Deduped, SFMC Prospects, S01-S09.
    """
    def _safe(df, pos=0):
        try:
            v = df.iloc[0, pos]
            return int(v) if v is not None else 0
        except Exception:
            return 0

    stg_filter = f"""COALESCE(
        TRY_TO_DATE(FILE_DATE::STRING, 'YYYY-MM-DD'),
        TRY_TO_DATE(FILE_DATE::STRING, 'DD-MM-YYYY')
    ) BETWEEN '{start_date}' AND '{end_date}'"""

    total_leads = _safe(_df(session, f"""
        SELECT COUNT(*) FROM QA_FIPSAR_PHI_HUB.STAGING.STG_PROSPECT_INTAKE
        WHERE {stg_filter}
    """))

    # Build view WHERE clause
    view_where = [f"INTAKE_DATE BETWEEN '{start_date}' AND '{end_date}'"]
    if journey_status:
        statuses = "', '".join(journey_status)
        view_where.append(f"JOURNEY_STATUS IN ('{statuses}')")
    if channel and channel != "All Channels":
        view_where.append(f"INTAKE_CHANNEL = '{channel}'")
    view_filter = " AND ".join(view_where)

    view_df = _df(session, f"""
        SELECT
            SUM(IN_DIM_PROSPECT)                                                                AS vp,
            SUM(PASSED_DEDUP)                                                                   AS dd,
            SUM(IN_SFMC)                                                                        AS sfmc,
            SUM(STAGE_01_WELCOME_EMAIL_SENT)                                                    AS s01,
            SUM(STAGE_02_EDUCATION_EMAIL_SENT)                                                  AS s02,
            SUM(STAGE_03_EDUCATION_EMAIL_1_SENT)                                                AS s03,
            SUM(STAGE_04_EDUCATION_EMAIL_2_SENT)                                                AS s04,
            SUM(STAGE_05_PROSPECT_STORY_EMAIL_SENT)                                             AS s05,
            SUM(STAGE_06_CONVERSION_EMAIL_SENT)                                                 AS s06,
            SUM(STAGE_07_REMINDER_EMAIL_SENT)                                                   AS s07,
            SUM(STAGE_08_REENGAGEMENT_EMAIL_SENT)                                               AS s08,
            SUM(STAGE_09_FINAL_REMINDER_EMAIL_SENT)                                             AS s09,
            -- Suppressed + awaiting after each stage (for tooltips)
            SUM(CASE WHEN STAGE_01_WELCOME_EMAIL_SENT=1 AND STAGE_02_EDUCATION_EMAIL_SENT=0     AND IS_SUPPRESSED=1 THEN 1 ELSE 0 END) AS s01_sp,
            SUM(CASE WHEN STAGE_01_WELCOME_EMAIL_SENT=1 AND STAGE_02_EDUCATION_EMAIL_SENT=0     AND IS_SUPPRESSED=0 THEN 1 ELSE 0 END) AS s01_wt,
            SUM(CASE WHEN STAGE_02_EDUCATION_EMAIL_SENT=1 AND STAGE_03_EDUCATION_EMAIL_1_SENT=0 AND IS_SUPPRESSED=1 THEN 1 ELSE 0 END) AS s02_sp,
            SUM(CASE WHEN STAGE_02_EDUCATION_EMAIL_SENT=1 AND STAGE_03_EDUCATION_EMAIL_1_SENT=0 AND IS_SUPPRESSED=0 THEN 1 ELSE 0 END) AS s02_wt,
            SUM(CASE WHEN STAGE_03_EDUCATION_EMAIL_1_SENT=1 AND STAGE_04_EDUCATION_EMAIL_2_SENT=0 AND IS_SUPPRESSED=1 THEN 1 ELSE 0 END) AS s03_sp,
            SUM(CASE WHEN STAGE_03_EDUCATION_EMAIL_1_SENT=1 AND STAGE_04_EDUCATION_EMAIL_2_SENT=0 AND IS_SUPPRESSED=0 THEN 1 ELSE 0 END) AS s03_wt,
            SUM(CASE WHEN STAGE_04_EDUCATION_EMAIL_2_SENT=1 AND STAGE_05_PROSPECT_STORY_EMAIL_SENT=0 AND IS_SUPPRESSED=1 THEN 1 ELSE 0 END) AS s04_sp,
            SUM(CASE WHEN STAGE_04_EDUCATION_EMAIL_2_SENT=1 AND STAGE_05_PROSPECT_STORY_EMAIL_SENT=0 AND IS_SUPPRESSED=0 THEN 1 ELSE 0 END) AS s04_wt,
            SUM(CASE WHEN STAGE_05_PROSPECT_STORY_EMAIL_SENT=1 AND STAGE_06_CONVERSION_EMAIL_SENT=0 AND STAGE_08_REENGAGEMENT_EMAIL_SENT=0 AND IS_SUPPRESSED=1 THEN 1 ELSE 0 END) AS s05_sp,
            SUM(CASE WHEN STAGE_05_PROSPECT_STORY_EMAIL_SENT=1 AND STAGE_06_CONVERSION_EMAIL_SENT=0 AND STAGE_08_REENGAGEMENT_EMAIL_SENT=0 AND IS_SUPPRESSED=0 THEN 1 ELSE 0 END) AS s05_wt,
            SUM(CASE WHEN STAGE_06_CONVERSION_EMAIL_SENT=1 AND STAGE_07_REMINDER_EMAIL_SENT=0     AND IS_SUPPRESSED=1 THEN 1 ELSE 0 END) AS s06_sp,
            SUM(CASE WHEN STAGE_06_CONVERSION_EMAIL_SENT=1 AND STAGE_07_REMINDER_EMAIL_SENT=0     AND IS_SUPPRESSED=0 THEN 1 ELSE 0 END) AS s06_wt,
            SUM(CASE WHEN STAGE_07_REMINDER_EMAIL_SENT=1 AND STAGE_08_REENGAGEMENT_EMAIL_SENT=0 AND STAGE_09_FINAL_REMINDER_EMAIL_SENT=0 AND IS_SUPPRESSED=1 THEN 1 ELSE 0 END) AS s07_sp,
            SUM(CASE WHEN STAGE_07_REMINDER_EMAIL_SENT=1 AND STAGE_08_REENGAGEMENT_EMAIL_SENT=0 AND STAGE_09_FINAL_REMINDER_EMAIL_SENT=0 AND IS_SUPPRESSED=0 THEN 1 ELSE 0 END) AS s07_wt,
            SUM(CASE WHEN STAGE_08_REENGAGEMENT_EMAIL_SENT=1 AND STAGE_09_FINAL_REMINDER_EMAIL_SENT=0 AND IS_SUPPRESSED=1 THEN 1 ELSE 0 END) AS s08_sp,
            SUM(CASE WHEN STAGE_08_REENGAGEMENT_EMAIL_SENT=1 AND STAGE_09_FINAL_REMINDER_EMAIL_SENT=0 AND IS_SUPPRESSED=0 THEN 1 ELSE 0 END) AS s08_wt,
            SUM(CASE WHEN STAGE_09_FINAL_REMINDER_EMAIL_SENT=1 THEN 1 ELSE 0 END)              AS s09_done
        FROM QA_FIPSAR_DW.GOLD.VW_PROSPECT_JOURNEY_ANALYTICS
        WHERE {view_filter}
    """)

    if view_df.empty:
        return pd.DataFrame()

    def _v(col):
        try:
            val = view_df.iloc[0][col.upper()]
            return int(val) if val is not None else 0
        except Exception:
            return 0

    # Define funnel segments: (label, count, supp_after, awaiting_after, type, bucket_key)
    segments = [
        ("Total Leads",                    total_leads,   0,          0,          "Pipeline", "total_leads"),
        ("Valid Prospects",                 _v("vp"),      0,          0,          "Pipeline", "valid_prospects"),
        ("Deduped",                        _v("dd"),      0,          0,          "Pipeline", "deduped"),
        ("SFMC Prospects",                 _v("sfmc"),    0,          0,          "SFMC",     "sfmc_prospects"),
        ("Stage 01 — Welcome Email",        _v("s01"),     _v("s01_sp"), _v("s01_wt"), "Stage", "s01"),
        ("Stage 02 — Education Email",      _v("s02"),     _v("s02_sp"), _v("s02_wt"), "Stage", "s02"),
        ("Stage 03 — Education Email 1",    _v("s03"),     _v("s03_sp"), _v("s03_wt"), "Stage", "s03"),
        ("Stage 04 — Education Email 2",    _v("s04"),     _v("s04_sp"), _v("s04_wt"), "Stage", "s04"),
        ("Stage 05 — Prospect Story Email", _v("s05"),     _v("s05_sp"), _v("s05_wt"), "Stage", "s05"),
        ("Stage 06 — Conversion Email",     _v("s06"),     _v("s06_sp"), _v("s06_wt"), "Stage", "s06"),
        ("Stage 07 — Reminder Email",       _v("s07"),     _v("s07_sp"), _v("s07_wt"), "Stage", "s07"),
        ("Stage 08 — Re-engagement Email",  _v("s08"),     _v("s08_sp"), _v("s08_wt"), "Stage", "s08"),
        ("Stage 09 — Final Reminder Email", _v("s09"),     0,          _v("s09_done"), "Stage", "s09"),
    ]

    rows = []
    for i, (label, count, supp, wait, seg_type, bucket) in enumerate(segments):
        # Drop = prospects lost AFTER this stage (forward-looking)
        # This ensures Suppressed + Awaiting = Drop always holds for stage rows
        if i < len(segments) - 1:
            next_count = segments[i + 1][1]
            drop = max(count - next_count, 0)
        else:
            drop = 0
        pct      = round(count / total_leads * 100, 1) if total_leads > 0 else 0.0
        drop_pct = round(drop / count * 100, 1) if count > 0 else 0.0
        rows.append({
            "Segment":     label,
            "Count":       count,
            "Drop":        drop,
            "Drop %":      drop_pct,
            "Suppressed":  supp,
            "Awaiting":    wait,
            "% of Total":  pct,
            "Type":        seg_type,
            "Bucket_Key":  bucket,
            "Sort":        i,
        })

    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Recon Analytics — Drill-down Prospect List
# ---------------------------------------------------------------------------

def get_drill_prospects(
    session,
    bucket_key: str,
    start_date: str = "2020-01-01",
    end_date: str   = "2099-12-31",
    limit: int = 200,
) -> pd.DataFrame:
    """
    Returns prospect-level rows for the selected funnel bucket.
    """
    BUCKET_FILTERS = {
        "valid_prospects":  "IN_DIM_PROSPECT = 1",
        "deduped":          "PASSED_DEDUP = 1",
        "sfmc_prospects":   "IN_SFMC = 1",
        "s01":              "STAGE_01_WELCOME_EMAIL_SENT = 1",
        "s02":              "STAGE_02_EDUCATION_EMAIL_SENT = 1",
        "s03":              "STAGE_03_EDUCATION_EMAIL_1_SENT = 1",
        "s04":              "STAGE_04_EDUCATION_EMAIL_2_SENT = 1",
        "s05":              "STAGE_05_PROSPECT_STORY_EMAIL_SENT = 1",
        "s06":              "STAGE_06_CONVERSION_EMAIL_SENT = 1",
        "s07":              "STAGE_07_REMINDER_EMAIL_SENT = 1",
        "s08":              "STAGE_08_REENGAGEMENT_EMAIL_SENT = 1",
        "s09":              "STAGE_09_FINAL_REMINDER_EMAIL_SENT = 1",
        "suppressed":       "IS_SUPPRESSED = 1",
        "completed":        "JOURNEY_STATUS = 'Journey Completed'",
        "in_progress":      "JOURNEY_STATUS = 'In Progress'",
    }

    if bucket_key == "total_leads":
        stg_filter = f"""COALESCE(
            TRY_TO_DATE(FILE_DATE::STRING, 'YYYY-MM-DD'),
            TRY_TO_DATE(FILE_DATE::STRING, 'DD-MM-YYYY')
        ) BETWEEN '{start_date}' AND '{end_date}'"""
        sql = f"""
            SELECT
                FIRST_NAME, LAST_NAME, EMAIL, PHONE_NUMBER,
                PATIENT_CONSENT,
                CHANNEL AS INTAKE_CHANNEL,
                COALESCE(
                    TRY_TO_DATE(FILE_DATE::STRING, 'YYYY-MM-DD'),
                    TRY_TO_DATE(FILE_DATE::STRING, 'DD-MM-YYYY')
                ) AS INTAKE_DATE
            FROM QA_FIPSAR_PHI_HUB.STAGING.STG_PROSPECT_INTAKE
            WHERE {stg_filter}
            ORDER BY INTAKE_DATE DESC NULLS LAST
            LIMIT {limit}
        """

    elif bucket_key == "invalid_leads":
        sql = f"""
            SELECT
                REJECTION_REASON,
                TABLE_NAME          AS PIPELINE_STAGE,
                COUNT(*)            AS RECORD_COUNT,
                MIN(CAST(REJECTED_AT AS DATE)) AS FIRST_SEEN,
                MAX(CAST(REJECTED_AT AS DATE)) AS LAST_SEEN
            FROM QA_FIPSAR_AUDIT.PIPELINE_AUDIT.DQ_REJECTION_LOG
            WHERE TABLE_NAME = 'PHI_PROSPECT_MASTER'
              AND CAST(REJECTED_AT AS DATE) BETWEEN '{start_date}' AND '{end_date}'
            GROUP BY 1, 2
            ORDER BY RECORD_COUNT DESC
        """

    else:
        where_clause = BUCKET_FILTERS.get(bucket_key, "1=1")
        sql = f"""
            SELECT
                PROSPECT_ID,
                FIRST_NAME,
                LAST_NAME,
                EMAIL,
                INTAKE_DATE,
                INTAKE_CHANNEL,
                JOURNEY_STATUS,
                LAST_COMPLETED_STAGE,
                IS_SUPPRESSED,
                SUPPRESSED_AT_STAGE,
                IS_UNSUBSCRIBED,
                HAS_HARD_BOUNCE,
                DROP_OR_STATUS_REASON
            FROM QA_FIPSAR_DW.GOLD.VW_PROSPECT_JOURNEY_ANALYTICS
            WHERE {where_clause}
              AND INTAKE_DATE BETWEEN '{start_date}' AND '{end_date}'
            ORDER BY PROSPECT_ID
            LIMIT {limit}
        """

    df = _df(session, sql)
    if not df.empty:
        # Normalise column names to Title Case for display
        df.columns = [c.replace("_", " ").title() for c in df.columns]
    return df
