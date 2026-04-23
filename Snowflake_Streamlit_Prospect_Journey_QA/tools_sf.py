"""
tools_sf.py
-----------
SQL data tools for the Snowflake Streamlit app.
All functions accept a Snowpark session and return formatted markdown strings.
No external dependencies — uses only Snowpark SQL execution.
"""

from __future__ import annotations
import textwrap
import pandas as pd


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------

def _df_to_markdown(df: pd.DataFrame) -> str:
    """
    Convert DataFrame to a markdown table without requiring 'tabulate'.
    tabulate is not pre-installed in Snowflake Streamlit.
    """
    cols = list(df.columns)
    header = "| " + " | ".join(str(c) for c in cols) + " |"
    sep    = "| " + " | ".join("---" for _ in cols) + " |"
    rows   = [
        "| " + " | ".join(str(v) if v is not None else "" for v in row) + " |"
        for row in df.itertuples(index=False, name=None)
    ]
    return "\n".join([header, sep] + rows)


def _run(session, sql: str, max_rows: int = 100) -> str:
    """Execute SQL via Snowpark session, return as markdown table string."""
    try:
        df = session.sql(textwrap.dedent(sql).strip()).to_pandas()
        if df is None or df.empty:
            return "_No data returned for this query._"
        if len(df) > max_rows:
            df = df.head(max_rows)
        return _df_to_markdown(df)
    except Exception as e:
        return f"_Query error: {e}_"


def _run_df(session, sql: str, max_rows: int = 100) -> pd.DataFrame:
    """Execute SQL and return raw DataFrame for charting."""
    try:
        df = session.sql(textwrap.dedent(sql).strip()).to_pandas()
        return df.head(max_rows) if df is not None and not df.empty else pd.DataFrame()
    except Exception:
        return pd.DataFrame()


# ---------------------------------------------------------------------------
# Tool 1: Custom SQL
# ---------------------------------------------------------------------------

def run_sql(session, sql: str, max_rows: int = 100) -> str:
    """Execute a custom SELECT statement and return results as markdown."""
    return _run(session, sql, max_rows)


# ---------------------------------------------------------------------------
# Tool 2: Top-Level Funnel Metrics
# ---------------------------------------------------------------------------

def get_funnel_metrics(
    session,
    start_date: str = "2020-01-01",
    end_date: str = "2099-12-31",
) -> str:
    """
    Return full lead-to-engagement funnel summary.
    Default dates cover all data — only filter when user specifies a period.
    """
    stg_filter = f"""COALESCE(
        TRY_TO_DATE(FILE_DATE::STRING,'YYYY-MM-DD'),
        TRY_TO_DATE(FILE_DATE::STRING,'DD-MM-YYYY')
    ) BETWEEN '{start_date}' AND '{end_date}'"""

    sql = f"""
        WITH funnel AS (
            SELECT
                (SELECT COUNT(*) FROM QA_FIPSAR_PHI_HUB.STAGING.STG_PROSPECT_INTAKE
                 WHERE {stg_filter}) AS total_leads,

                (SELECT COUNT(*) FROM QA_FIPSAR_PHI_HUB.STAGING.STG_PROSPECT_INTAKE
                 WHERE {stg_filter}
                   AND UPPER(TRIM(PATIENT_CONSENT)) = 'TRUE') AS consented_leads,

                (SELECT COUNT(*) FROM QA_FIPSAR_DW.GOLD.DIM_PROSPECT
                 WHERE FIRST_INTAKE_DATE BETWEEN '{start_date}' AND '{end_date}') AS valid_prospects,

                (SELECT COUNT(*) FROM QA_FIPSAR_AUDIT.PIPELINE_AUDIT.DQ_REJECTION_LOG
                 WHERE TABLE_NAME = 'PHI_PROSPECT_MASTER'
                   AND CAST(REJECTED_AT AS DATE) BETWEEN '{start_date}' AND '{end_date}') AS invalid_leads,

                (SELECT COUNT(*) FROM QA_FIPSAR_DW.GOLD.FACT_SFMC_ENGAGEMENT
                 WHERE EVENT_TYPE = 'SENT'
                   AND DATE(EVENT_TIMESTAMP) BETWEEN '{start_date}' AND '{end_date}') AS sfmc_sent,

                (SELECT COUNT(*) FROM QA_FIPSAR_DW.GOLD.FACT_SFMC_ENGAGEMENT
                 WHERE EVENT_TYPE IN ('OPEN','OPENED')
                   AND DATE(EVENT_TIMESTAMP) BETWEEN '{start_date}' AND '{end_date}') AS sfmc_opened,

                (SELECT COUNT(*) FROM QA_FIPSAR_DW.GOLD.FACT_SFMC_ENGAGEMENT
                 WHERE EVENT_TYPE IN ('CLICK','CLICKED')
                   AND DATE(EVENT_TIMESTAMP) BETWEEN '{start_date}' AND '{end_date}') AS sfmc_clicked,

                (SELECT COUNT(*) FROM QA_FIPSAR_AUDIT.PIPELINE_AUDIT.DQ_REJECTION_LOG
                 WHERE TABLE_NAME = 'FACT_SFMC_ENGAGEMENT'
                   AND UPPER(REJECTION_REASON) IN ('SUPPRESSED_PROSPECT','FATAL_ERROR')
                   AND CAST(REJECTED_AT AS DATE) BETWEEN '{start_date}' AND '{end_date}') AS sfmc_suppressed
        )
        SELECT
            total_leads                                                               AS "Total Leads",
            consented_leads                                                           AS "Consented Leads",
            valid_prospects                                                           AS "Valid Prospects",
            invalid_leads                                                             AS "Invalid / Rejected Leads",
            ROUND(valid_prospects * 100.0 / NULLIF(total_leads, 0), 1)              AS "Conversion Rate %",
            sfmc_sent                                                                 AS "SFMC Emails Sent",
            sfmc_opened                                                               AS "SFMC Emails Opened",
            sfmc_clicked                                                              AS "SFMC Emails Clicked",
            sfmc_suppressed                                                           AS "SFMC Suppressed",
            ROUND(sfmc_opened * 100.0 / NULLIF(sfmc_sent, 0), 1)                   AS "Open Rate %",
            ROUND(sfmc_clicked * 100.0 / NULLIF(sfmc_sent, 0), 1)                  AS "Click Rate %"
        FROM funnel
    """
    return "### Funnel Summary\n" + _run(session, sql, max_rows=1)


def get_funnel_metrics_df(session, start_date="2020-01-01", end_date="2099-12-31") -> pd.DataFrame:
    """Return funnel data as DataFrame for charting."""
    stg_filter = f"""COALESCE(TRY_TO_DATE(FILE_DATE::STRING,'YYYY-MM-DD'), TRY_TO_DATE(FILE_DATE::STRING,'DD-MM-YYYY')) BETWEEN '{start_date}' AND '{end_date}'"""
    sql = f"""
        SELECT 'Total Leads'       AS stage, COUNT(*) AS count FROM QA_FIPSAR_PHI_HUB.STAGING.STG_PROSPECT_INTAKE WHERE {stg_filter}
        UNION ALL SELECT 'Valid Prospects', COUNT(*) FROM QA_FIPSAR_DW.GOLD.DIM_PROSPECT WHERE FIRST_INTAKE_DATE BETWEEN '{start_date}' AND '{end_date}'
        UNION ALL SELECT 'SFMC Sent', COUNT(*) FROM QA_FIPSAR_DW.GOLD.FACT_SFMC_ENGAGEMENT WHERE EVENT_TYPE='SENT' AND DATE(EVENT_TIMESTAMP) BETWEEN '{start_date}' AND '{end_date}'
        UNION ALL SELECT 'Opened', COUNT(*) FROM QA_FIPSAR_DW.GOLD.FACT_SFMC_ENGAGEMENT WHERE EVENT_TYPE IN ('OPEN','OPENED') AND DATE(EVENT_TIMESTAMP) BETWEEN '{start_date}' AND '{end_date}'
        UNION ALL SELECT 'Clicked', COUNT(*) FROM QA_FIPSAR_DW.GOLD.FACT_SFMC_ENGAGEMENT WHERE EVENT_TYPE IN ('CLICK','CLICKED') AND DATE(EVENT_TIMESTAMP) BETWEEN '{start_date}' AND '{end_date}'
        ORDER BY count DESC
    """
    return _run_df(session, sql)


# ---------------------------------------------------------------------------
# Tool 3: Rejection Analysis
# ---------------------------------------------------------------------------

def get_rejection_analysis(
    session,
    start_date: str = "2020-01-01",
    end_date: str = "2099-12-31",
    category: str = "intake",
    top_n: int = 10,
) -> str:
    """
    Return rejection reason breakdown.
    category: 'intake' (lead→prospect), 'sfmc' (suppression), 'all'
    """
    if category == "intake":
        table_filter = "TABLE_NAME = 'PHI_PROSPECT_MASTER'"
        label = "Lead-to-Prospect Rejection Reasons"
    elif category == "sfmc":
        table_filter = "TABLE_NAME = 'FACT_SFMC_ENGAGEMENT'"
        label = "SFMC Suppression / Send Failure Reasons"
    else:
        table_filter = "1=1"
        label = "All Rejection Reasons"

    sql = f"""
        SELECT
            REJECTION_REASON                                          AS "Rejection Reason",
            TABLE_NAME                                                AS "Pipeline Stage",
            COUNT(*)                                                  AS "Count",
            ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1)      AS "% of Total"
        FROM QA_FIPSAR_AUDIT.PIPELINE_AUDIT.DQ_REJECTION_LOG
        WHERE {table_filter}
          AND CAST(REJECTED_AT AS DATE) BETWEEN '{start_date}' AND '{end_date}'
        GROUP BY 1, 2
        ORDER BY 3 DESC
        LIMIT {top_n}
    """
    return f"### {label}\n" + _run(session, sql)


def get_rejection_df(session, start_date="2020-01-01", end_date="2099-12-31", category="intake") -> pd.DataFrame:
    table_filter = "TABLE_NAME = 'PHI_PROSPECT_MASTER'" if category == "intake" else "TABLE_NAME = 'FACT_SFMC_ENGAGEMENT'" if category == "sfmc" else "1=1"
    sql = f"""
        SELECT REJECTION_REASON AS reason, COUNT(*) AS count
        FROM QA_FIPSAR_AUDIT.PIPELINE_AUDIT.DQ_REJECTION_LOG
        WHERE {table_filter}
          AND CAST(REJECTED_AT AS DATE) BETWEEN '{start_date}' AND '{end_date}'
        GROUP BY 1 ORDER BY 2 DESC LIMIT 10
    """
    return _run_df(session, sql)


# ---------------------------------------------------------------------------
# Tool 4: SFMC Engagement Stats
# ---------------------------------------------------------------------------

def get_sfmc_engagement_stats(
    session,
    start_date: str = "2020-01-01",
    end_date: str = "2099-12-31",
) -> str:
    """Return SFMC engagement event counts by type."""
    sql = f"""
        SELECT
            fe.EVENT_TYPE                                             AS "Event Type",
            COALESCE(j.JOURNEY_TYPE, 'Unknown')                      AS "Journey Phase",
            COUNT(*)                                                  AS "Event Count",
            COUNT(DISTINCT fe.SUBSCRIBER_KEY)                        AS "Unique Prospects"
        FROM QA_FIPSAR_DW.GOLD.FACT_SFMC_ENGAGEMENT fe
        LEFT JOIN QA_FIPSAR_DW.GOLD.DIM_SFMC_JOB j ON fe.JOB_KEY = j.JOB_KEY
        WHERE DATE(fe.EVENT_TIMESTAMP) BETWEEN '{start_date}' AND '{end_date}'
        GROUP BY 1, 2
        ORDER BY 3 DESC
        LIMIT 50
    """
    result = _run(session, sql)
    if "_No data_" in result or "_Query error_" in result:
        # Fallback to raw tables
        sql_raw = f"""
            SELECT event_type, COUNT(*) AS event_count, COUNT(DISTINCT SUBSCRIBER_KEY) AS unique_prospects
            FROM (
                SELECT 'SENT'        AS event_type, SUBSCRIBER_KEY FROM QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_SENT
                UNION ALL SELECT 'OPEN',   SUBSCRIBER_KEY FROM QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_OPENS
                UNION ALL SELECT 'CLICK',  SUBSCRIBER_KEY FROM QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_CLICKS
                UNION ALL SELECT 'BOUNCE', SUBSCRIBER_KEY FROM QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_BOUNCES
                UNION ALL SELECT 'UNSUB',  SUBSCRIBER_KEY FROM QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_UNSUBSCRIBES
            )
            GROUP BY 1 ORDER BY 2 DESC
        """
        result = _run(session, sql_raw)
    return "### SFMC Engagement Events\n" + result


def get_sfmc_engagement_df(session, start_date="2020-01-01", end_date="2099-12-31") -> pd.DataFrame:
    sql = f"""
        SELECT fe.EVENT_TYPE AS event_type, COUNT(*) AS count
        FROM QA_FIPSAR_DW.GOLD.FACT_SFMC_ENGAGEMENT fe
        WHERE DATE(fe.EVENT_TIMESTAMP) BETWEEN '{start_date}' AND '{end_date}'
        GROUP BY 1 ORDER BY 2 DESC
    """
    return _run_df(session, sql)


# ---------------------------------------------------------------------------
# Tool 5: Date-Specific Drop Analysis
# ---------------------------------------------------------------------------

def get_drop_analysis(session, target_date: str) -> str:
    """
    Investigate why volume dropped on a specific date.
    Only call when user specifies a date — never default to today.
    """
    sql = f"""
        SELECT 'Lead Intake' AS signal, COUNT(*) AS count, '{target_date}' AS date
        FROM QA_FIPSAR_PHI_HUB.STAGING.STG_PROSPECT_INTAKE
        WHERE COALESCE(TRY_TO_DATE(FILE_DATE::STRING,'YYYY-MM-DD'), TRY_TO_DATE(FILE_DATE::STRING,'DD-MM-YYYY')) = '{target_date}'
        UNION ALL
        SELECT 'Valid Prospects Mastered', COUNT(*), '{target_date}'
        FROM QA_FIPSAR_DW.GOLD.DIM_PROSPECT WHERE FIRST_INTAKE_DATE = '{target_date}'
        UNION ALL
        SELECT 'Rejections (Intake)', COUNT(*), '{target_date}'
        FROM QA_FIPSAR_AUDIT.PIPELINE_AUDIT.DQ_REJECTION_LOG
        WHERE TABLE_NAME = 'PHI_PROSPECT_MASTER' AND CAST(REJECTED_AT AS DATE) = '{target_date}'
        UNION ALL
        SELECT 'SFMC Emails Sent', COUNT(*), '{target_date}'
        FROM QA_FIPSAR_DW.GOLD.FACT_SFMC_ENGAGEMENT
        WHERE EVENT_TYPE = 'SENT' AND DATE(EVENT_TIMESTAMP) = '{target_date}'
        UNION ALL
        SELECT 'SFMC Suppressed', COUNT(*), '{target_date}'
        FROM QA_FIPSAR_AUDIT.PIPELINE_AUDIT.DQ_REJECTION_LOG
        WHERE TABLE_NAME = 'FACT_SFMC_ENGAGEMENT'
          AND UPPER(REJECTION_REASON) IN ('SUPPRESSED_PROSPECT','FATAL_ERROR')
          AND CAST(REJECTED_AT AS DATE) = '{target_date}'
    """
    top_rejections = f"""
        SELECT REJECTION_REASON AS "Top Rejection Reason", COUNT(*) AS "Count"
        FROM QA_FIPSAR_AUDIT.PIPELINE_AUDIT.DQ_REJECTION_LOG
        WHERE CAST(REJECTED_AT AS DATE) = '{target_date}'
        GROUP BY 1 ORDER BY 2 DESC LIMIT 5
    """
    return (
        f"### Volume Signals for {target_date}\n"
        + _run(session, sql)
        + "\n\n### Top Rejection Reasons on This Date\n"
        + _run(session, top_rejections)
    )


# ---------------------------------------------------------------------------
# Tool 6: Prospect Tracing
# ---------------------------------------------------------------------------

def trace_prospect(session, identifier: str) -> str:
    """Trace a single prospect end-to-end by email or Prospect ID."""
    is_id = identifier.upper().startswith("FIP")
    filter_clause = (
        f"UPPER(TRIM(p.MASTER_PATIENT_ID)) = UPPER(TRIM('{identifier}'))"
        if is_id else
        f"UPPER(TRIM(p.EMAIL_ADDRESS)) = UPPER(TRIM('{identifier}'))"
    )
    sql_profile = f"""
        SELECT
            p.MASTER_PATIENT_ID     AS "Prospect ID",
            p.FIRST_NAME            AS "First Name",
            p.LAST_NAME             AS "Last Name",
            p.EMAIL_ADDRESS         AS "Email",
            p.LEAD_SOURCE           AS "Lead Source",
            p.FIRST_INTAKE_DATE     AS "Intake Date",
            p.PATIENT_CONSENT       AS "Consent"
        FROM QA_FIPSAR_DW.GOLD.DIM_PROSPECT p
        WHERE {filter_clause}
        LIMIT 1
    """
    sql_journey = f"""
        SELECT
            j.WELCOMEJOURNEY_WELCOMEEMAIL_SENT       AS "S01 Welcome",
            j.WELCOMEJOURNEY_EDUCATIONEMAIL_SENT      AS "S02 Education",
            j.NURTUREJOURNEY_EDUCATIONEMAIL1_SENT     AS "S03 Edu 1",
            j.NURTUREJOURNEY_EDUCATIONEMAIL2_SENT     AS "S04 Edu 2",
            j.NURTUREJOURNEY_PROSPECTSTORYEMAIL_SENT  AS "S05 Story",
            j.HIGHENGAGEMENT_CONVERSIONEMAIL_SENT     AS "S06 Conversion",
            j.HIGHENGAGEMENT_REMINDEREMAIL_SENT       AS "S07 Reminder",
            j.LOWENGAGEMENT_REENGAGEMENTEMAIL_SENT    AS "S08 Re-engage",
            j.LOWENGAGEMENTFINALREMINDEREMAIL_SENT    AS "S09 Final",
            j.SUPPRESSION_FLAG                        AS "Suppressed",
            j.WELCOME_JOURNEY_COMPLETE                AS "Welcome Complete",
            j.NURTURE_JOURNEY_COMPLETE                AS "Nurture Complete"
        FROM QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_PROSPECT_JOURNEY_DETAILS j
        WHERE UPPER(TRIM(j.PROSPECT_ID)) = UPPER(TRIM(
            (SELECT MASTER_PATIENT_ID FROM QA_FIPSAR_DW.GOLD.DIM_PROSPECT p WHERE {filter_clause} LIMIT 1)
        ))
        LIMIT 1
    """
    return (
        f"### Prospect Profile — {identifier}\n" + _run(session, sql_profile) +
        "\n\n### Journey Stage History\n" + _run(session, sql_journey)
    )


# ---------------------------------------------------------------------------
# Tool 7: Journey Overview (Q1 pattern)
# ---------------------------------------------------------------------------

def get_journey_overview(session) -> str:
    """Journey health: total, suppressed, active, suppression rate, phase completions."""
    overview_sql = """
        SELECT
            COUNT(*)                                                                                     AS "Total Prospects in Journey",
            COUNT_IF(UPPER(TRIM(SUPPRESSION_FLAG)) IN ('YES','Y','TRUE','1'))                           AS "Suppressed",
            COUNT_IF(UPPER(TRIM(SUPPRESSION_FLAG)) NOT IN ('YES','Y','TRUE','1')
                     OR SUPPRESSION_FLAG IS NULL)                                                        AS "Active (Not Suppressed)",
            ROUND(COUNT_IF(UPPER(TRIM(SUPPRESSION_FLAG)) IN ('YES','Y','TRUE','1'))
                  * 100.0 / COUNT(*), 1)                                                                 AS "Suppression Rate %",
            COUNT_IF(UPPER(TRIM(WELCOME_JOURNEY_COMPLETE)) = 'TRUE')                                    AS "Welcome Phase Complete",
            COUNT_IF(UPPER(TRIM(NURTURE_JOURNEY_COMPLETE)) = 'TRUE')                                    AS "Nurture Phase Complete"
        FROM QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_PROSPECT_JOURNEY_DETAILS
    """
    path_sql = """
        SELECT
            CASE
                WHEN UPPER(TRIM(HIGHENGAGEMENT_CONVERSIONEMAIL_SENT)) = 'TRUE'
                  OR UPPER(TRIM(HIGHENGAGEMENT_REMINDEREMAIL_SENT))   = 'TRUE' THEN 'High Engagement Path'
                WHEN UPPER(TRIM(LOWENGAGEMENT_REENGAGEMENTEMAIL_SENT))     = 'TRUE'
                  OR UPPER(TRIM(LOWENGAGEMENTFINALREMINDEREMAIL_SENT)) = 'TRUE' THEN 'Low Engagement Path'
                ELSE 'Not Yet Branched'
            END                                                                                         AS "Engagement Path",
            COUNT(*)                                                                                    AS "Prospect Count",
            ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1)                                        AS "% of Total",
            COUNT_IF(UPPER(TRIM(SUPPRESSION_FLAG)) IN ('YES','Y','TRUE','1'))                          AS "Suppressed in Path"
        FROM QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_PROSPECT_JOURNEY_DETAILS
        GROUP BY 1 ORDER BY 2 DESC
    """
    zero_sql = """
        SELECT
            COUNT_IF(UPPER(TRIM(SUPPRESSION_FLAG)) IN ('YES','Y','TRUE','1'))                          AS "Zero-Email + Suppressed (pre-journey)",
            COUNT_IF(UPPER(TRIM(SUPPRESSION_FLAG)) NOT IN ('YES','Y','TRUE','1')
                     OR SUPPRESSION_FLAG IS NULL)                                                        AS "Zero-Email + Active (awaiting entry)"
        FROM QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_PROSPECT_JOURNEY_DETAILS
        WHERE COUNT_IF(UPPER(TRIM(WELCOMEJOURNEY_WELCOMEEMAIL_SENT))      = 'TRUE') = 0
          AND COUNT_IF(UPPER(TRIM(WELCOMEJOURNEY_EDUCATIONEMAIL_SENT))    = 'TRUE') = 0
          AND COUNT_IF(UPPER(TRIM(NURTUREJOURNEY_EDUCATIONEMAIL1_SENT))   = 'TRUE') = 0
          AND COUNT_IF(UPPER(TRIM(NURTUREJOURNEY_EDUCATIONEMAIL2_SENT))   = 'TRUE') = 0
          AND COUNT_IF(UPPER(TRIM(NURTUREJOURNEY_PROSPECTSTORYEMAIL_SENT))= 'TRUE') = 0
    """
    # zero_sql has a WHERE using aggregate — rewrite as subquery
    zero_sql = """
        SELECT
            SUM(CASE WHEN UPPER(TRIM(SUPPRESSION_FLAG)) IN ('YES','Y','TRUE','1') THEN 1 ELSE 0 END)   AS "Zero-Email + Suppressed (pre-journey)",
            SUM(CASE WHEN UPPER(TRIM(SUPPRESSION_FLAG)) NOT IN ('YES','Y','TRUE','1')
                       OR SUPPRESSION_FLAG IS NULL THEN 1 ELSE 0 END)                                  AS "Zero-Email + Active (awaiting entry)"
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
    """
    return (
        "### Journey Health Summary\n" + _run(session, overview_sql, max_rows=1)
        + "\n\n### Engagement Path Split\n" + _run(session, path_sql)
        + "\n\n### Zero-Email Prospects\n" + _run(session, zero_sql, max_rows=1)
    )


# ---------------------------------------------------------------------------
# Tool 8: Journey Stage Drop-off (Q4 pattern)
# ---------------------------------------------------------------------------

def get_journey_stage_dropoff(session) -> str:
    """Stage-by-stage email reach + suppression breakdown."""
    progression_sql = """
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
        SELECT sn, stage_name, phase, sent AS "Prospects Sent",
               CASE WHEN sn = 1 THEN NULL ELSE (prev - sent) END AS "Emails to be sent",
               ROUND(sent * 100.0 / NULLIF(s01, 0), 1) AS "% Reached"
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
        ) ORDER BY sn
    """
    suppression_sql = """
        SELECT
            CASE
                WHEN UPPER(TRIM(WELCOMEJOURNEY_WELCOMEEMAIL_SENT))      = 'TRUE'
                 AND UPPER(TRIM(WELCOMEJOURNEY_EDUCATIONEMAIL_SENT))    != 'TRUE'
                THEN 'After Stage 01 — Welcome Email'
                WHEN UPPER(TRIM(WELCOMEJOURNEY_EDUCATIONEMAIL_SENT))    = 'TRUE'
                 AND UPPER(TRIM(NURTUREJOURNEY_EDUCATIONEMAIL1_SENT))   != 'TRUE'
                THEN 'After Stage 02 — Education Email'
                WHEN UPPER(TRIM(NURTUREJOURNEY_EDUCATIONEMAIL1_SENT))   = 'TRUE'
                 AND UPPER(TRIM(NURTUREJOURNEY_EDUCATIONEMAIL2_SENT))   != 'TRUE'
                THEN 'After Stage 03 — Education Email 1'
                WHEN UPPER(TRIM(NURTUREJOURNEY_EDUCATIONEMAIL2_SENT))   = 'TRUE'
                 AND UPPER(TRIM(NURTUREJOURNEY_PROSPECTSTORYEMAIL_SENT))!= 'TRUE'
                THEN 'After Stage 04 — Education Email 2'
                WHEN UPPER(TRIM(NURTUREJOURNEY_PROSPECTSTORYEMAIL_SENT))= 'TRUE'
                 AND UPPER(TRIM(HIGHENGAGEMENT_CONVERSIONEMAIL_SENT))   != 'TRUE'
                 AND UPPER(TRIM(LOWENGAGEMENT_REENGAGEMENTEMAIL_SENT))  != 'TRUE'
                THEN 'After Stage 05 — Prospect Story (pre-branch)'
                WHEN UPPER(TRIM(HIGHENGAGEMENT_CONVERSIONEMAIL_SENT))   = 'TRUE'
                 AND UPPER(TRIM(HIGHENGAGEMENT_REMINDEREMAIL_SENT))     != 'TRUE'
                THEN 'After Stage 06 — Conversion Email'
                WHEN UPPER(TRIM(HIGHENGAGEMENT_REMINDEREMAIL_SENT))     = 'TRUE'
                THEN 'After Stage 07 — Reminder Email'
                WHEN UPPER(TRIM(LOWENGAGEMENT_REENGAGEMENTEMAIL_SENT))  = 'TRUE'
                 AND UPPER(TRIM(LOWENGAGEMENTFINALREMINDEREMAIL_SENT))  != 'TRUE'
                THEN 'After Stage 08 — Re-engagement Email'
                WHEN UPPER(TRIM(LOWENGAGEMENTFINALREMINDEREMAIL_SENT))  = 'TRUE'
                THEN 'After Stage 09 — Final Reminder'
                ELSE 'Before Stage 01 (never entered)'
            END                                                              AS "Suppressed At",
            COUNT(*)                                                         AS "Suppressed Count",
            ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1)             AS "% of All Suppressed"
        FROM QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_PROSPECT_JOURNEY_DETAILS
        WHERE UPPER(TRIM(SUPPRESSION_FLAG)) IN ('YES','Y','TRUE','1')
        GROUP BY 1 ORDER BY 2 DESC
    """
    return (
        "### Stage-by-Stage Email Reach\n"
        "_Note: 'Emails to be sent' includes both suppressed prospects AND those awaiting their next interval._\n\n"
        + _run(session, progression_sql, max_rows=9)
        + "\n\n### Suppression by Journey Stage (SUPPRESSION_FLAG=TRUE only)\n"
        + _run(session, suppression_sql)
    )


def get_journey_stage_dropoff_df(session) -> pd.DataFrame:
    """Return stage progression as DataFrame for charting."""
    sql = """
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
        SELECT stage_name, sent AS prospects_sent
        FROM (
            SELECT 'S01 Welcome'      AS stage_name, s01 AS sent FROM base
            UNION ALL SELECT 'S02 Education',      s02 FROM base
            UNION ALL SELECT 'S03 Edu Email 1',    s03 FROM base
            UNION ALL SELECT 'S04 Edu Email 2',    s04 FROM base
            UNION ALL SELECT 'S05 Story',          s05 FROM base
            UNION ALL SELECT 'S06 Conversion',     s06 FROM base
            UNION ALL SELECT 'S07 Reminder',       s07 FROM base
            UNION ALL SELECT 'S08 Re-engage',      s08 FROM base
            UNION ALL SELECT 'S09 Final',          s09 FROM base
        )
    """
    return _run_df(session, sql)


# ---------------------------------------------------------------------------
# Tool 9: Journey Suppression Linkage (Q3 pattern)
# ---------------------------------------------------------------------------

def get_journey_suppression_linkage(session) -> str:
    """Suppression by stage: where in the journey were suppressed prospects stopped."""
    summary_sql = """
        SELECT
            COUNT(*) AS "Total Prospects",
            COUNT_IF(UPPER(TRIM(SUPPRESSION_FLAG)) IN ('YES','Y','TRUE','1')) AS "Suppressed",
            COUNT_IF(UPPER(TRIM(SUPPRESSION_FLAG)) NOT IN ('YES','Y','TRUE','1') OR SUPPRESSION_FLAG IS NULL) AS "Active",
            ROUND(COUNT_IF(UPPER(TRIM(SUPPRESSION_FLAG)) IN ('YES','Y','TRUE','1')) * 100.0 / COUNT(*), 1) AS "Suppression Rate %",
            COUNT_IF(UPPER(TRIM(WELCOME_JOURNEY_COMPLETE)) = 'TRUE') AS "Welcome Phase Complete",
            COUNT_IF(UPPER(TRIM(NURTURE_JOURNEY_COMPLETE)) = 'TRUE') AS "Nurture Phase Complete"
        FROM QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_PROSPECT_JOURNEY_DETAILS
    """
    stage_sql = """
        SELECT
            CASE
                WHEN UPPER(TRIM(WELCOMEJOURNEY_WELCOMEEMAIL_SENT))      = 'TRUE'
                 AND UPPER(TRIM(WELCOMEJOURNEY_EDUCATIONEMAIL_SENT))    != 'TRUE'
                THEN 'After Stage 01 — Welcome Email'
                WHEN UPPER(TRIM(WELCOMEJOURNEY_EDUCATIONEMAIL_SENT))    = 'TRUE'
                 AND UPPER(TRIM(NURTUREJOURNEY_EDUCATIONEMAIL1_SENT))   != 'TRUE'
                THEN 'After Stage 02 — Education Email'
                WHEN UPPER(TRIM(NURTUREJOURNEY_EDUCATIONEMAIL1_SENT))   = 'TRUE'
                 AND UPPER(TRIM(NURTUREJOURNEY_EDUCATIONEMAIL2_SENT))   != 'TRUE'
                THEN 'After Stage 03 — Education Email 1'
                WHEN UPPER(TRIM(NURTUREJOURNEY_EDUCATIONEMAIL2_SENT))   = 'TRUE'
                 AND UPPER(TRIM(NURTUREJOURNEY_PROSPECTSTORYEMAIL_SENT))!= 'TRUE'
                THEN 'After Stage 04 — Education Email 2'
                WHEN UPPER(TRIM(NURTUREJOURNEY_PROSPECTSTORYEMAIL_SENT))= 'TRUE'
                 AND UPPER(TRIM(HIGHENGAGEMENT_CONVERSIONEMAIL_SENT))   != 'TRUE'
                 AND UPPER(TRIM(LOWENGAGEMENT_REENGAGEMENTEMAIL_SENT))  != 'TRUE'
                THEN 'After Stage 05 — Story (pre-branch)'
                WHEN UPPER(TRIM(HIGHENGAGEMENT_CONVERSIONEMAIL_SENT))   = 'TRUE'
                 AND UPPER(TRIM(HIGHENGAGEMENT_REMINDEREMAIL_SENT))     != 'TRUE'
                THEN 'After Stage 06 — Conversion Email'
                WHEN UPPER(TRIM(HIGHENGAGEMENT_REMINDEREMAIL_SENT))     = 'TRUE'
                THEN 'After Stage 07 — Reminder Email'
                WHEN UPPER(TRIM(LOWENGAGEMENT_REENGAGEMENTEMAIL_SENT))  = 'TRUE'
                 AND UPPER(TRIM(LOWENGAGEMENTFINALREMINDEREMAIL_SENT))  != 'TRUE'
                THEN 'After Stage 08 — Re-engagement Email'
                WHEN UPPER(TRIM(LOWENGAGEMENTFINALREMINDEREMAIL_SENT))  = 'TRUE'
                THEN 'After Stage 09 — Final Reminder'
                ELSE 'Before Stage 01 (never entered)'
            END AS "Suppressed At Stage",
            COUNT(*) AS "Suppressed Count",
            ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1) AS "% of All Suppressed"
        FROM QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_PROSPECT_JOURNEY_DETAILS
        WHERE UPPER(TRIM(SUPPRESSION_FLAG)) IN ('YES','Y','TRUE','1')
        GROUP BY 1 ORDER BY 2 DESC
    """
    return (
        "### Suppression Summary\n" + _run(session, summary_sql, max_rows=1)
        + "\n\n### Suppression by Journey Stage (Q3 — where did suppressed prospects stop?)\n"
        + _run(session, stage_sql)
    )


# ---------------------------------------------------------------------------
# Tool 10: Journey Pace Analysis
# ---------------------------------------------------------------------------

def get_journey_pace_analysis(session) -> str:
    """Average days between stage transitions vs expected intervals."""
    sql = """
        SELECT
            'Stage 01 → Stage 02 (expected: 3d)' AS transition, 3 AS expected_days,
            ROUND(AVG(DATEDIFF('day',
                TRY_TO_TIMESTAMP(WELCOMEJOURNEY_WELCOMEEMAIL_SENT_DATE),
                TRY_TO_TIMESTAMP(WELCOMEJOURNEY_EDUCATIONEMAIL_SENT_DATE)
            )), 1) AS avg_actual_days, COUNT(*) AS prospects_in_transition
        FROM QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_PROSPECT_JOURNEY_DETAILS
        WHERE UPPER(TRIM(WELCOMEJOURNEY_WELCOMEEMAIL_SENT))   = 'TRUE'
          AND UPPER(TRIM(WELCOMEJOURNEY_EDUCATIONEMAIL_SENT)) = 'TRUE'
          AND WELCOMEJOURNEY_WELCOMEEMAIL_SENT_DATE   IS NOT NULL
          AND WELCOMEJOURNEY_EDUCATIONEMAIL_SENT_DATE IS NOT NULL
        UNION ALL
        SELECT 'Stage 02 → Stage 03 (expected: 5d)', 5,
            ROUND(AVG(DATEDIFF('day', TRY_TO_TIMESTAMP(WELCOMEJOURNEY_EDUCATIONEMAIL_SENT_DATE),
                TRY_TO_TIMESTAMP(NURTUREJOURNEY_EDUCATIONEMAIL1_SENT_DATE))), 1), COUNT(*)
        FROM QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_PROSPECT_JOURNEY_DETAILS
        WHERE UPPER(TRIM(WELCOMEJOURNEY_EDUCATIONEMAIL_SENT))  = 'TRUE'
          AND UPPER(TRIM(NURTUREJOURNEY_EDUCATIONEMAIL1_SENT)) = 'TRUE'
          AND WELCOMEJOURNEY_EDUCATIONEMAIL_SENT_DATE IS NOT NULL
          AND NURTUREJOURNEY_EDUCATIONEMAIL1_SENT_DATE IS NOT NULL
        UNION ALL
        SELECT 'Stage 03 → Stage 04 (expected: 8d)', 8,
            ROUND(AVG(DATEDIFF('day', TRY_TO_TIMESTAMP(NURTUREJOURNEY_EDUCATIONEMAIL1_SENT_DATE),
                TRY_TO_TIMESTAMP(NURTUREJOURNEY_EDUCATIONEMAIL2_SENT_DATE))), 1), COUNT(*)
        FROM QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_PROSPECT_JOURNEY_DETAILS
        WHERE UPPER(TRIM(NURTUREJOURNEY_EDUCATIONEMAIL1_SENT)) = 'TRUE'
          AND UPPER(TRIM(NURTUREJOURNEY_EDUCATIONEMAIL2_SENT)) = 'TRUE'
          AND NURTUREJOURNEY_EDUCATIONEMAIL1_SENT_DATE IS NOT NULL
          AND NURTUREJOURNEY_EDUCATIONEMAIL2_SENT_DATE IS NOT NULL
        UNION ALL
        SELECT 'Stage 04 → Stage 05 (expected: 3d)', 3,
            ROUND(AVG(DATEDIFF('day', TRY_TO_TIMESTAMP(NURTUREJOURNEY_EDUCATIONEMAIL2_SENT_DATE),
                TRY_TO_TIMESTAMP(NURTUREJOURNEY_PROSPECTSTORYEMAIL_SENT_DATE))), 1), COUNT(*)
        FROM QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_PROSPECT_JOURNEY_DETAILS
        WHERE UPPER(TRIM(NURTUREJOURNEY_EDUCATIONEMAIL2_SENT))   = 'TRUE'
          AND UPPER(TRIM(NURTUREJOURNEY_PROSPECTSTORYEMAIL_SENT))= 'TRUE'
          AND NURTUREJOURNEY_EDUCATIONEMAIL2_SENT_DATE    IS NOT NULL
          AND NURTUREJOURNEY_PROSPECTSTORYEMAIL_SENT_DATE IS NOT NULL
        ORDER BY transition
    """
    return "### Stage Timing: Average Days Between Transitions vs Expected\n" + _run(session, sql, max_rows=10)
