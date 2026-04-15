"""
charts.py
---------
Plotly chart generators for the FIPSAR Prospect Journey Intelligence platform.

Each named function (funnel_chart, rejection_chart, …) is a purpose-built chart.
`smart_chart` is the generalised engine: give it a SQL query + chart type and it
produces any visualisation — allowing the agent to chart answers to ANY question.

All functions:
  1. Query Snowflake.
  2. Build a Plotly Figure with consistent dark FIPSAR styling.
  3. Push the figure to chart_store (app.py renders it after the agent turn).
  4. Return a plain-text summary string for the LLM.
"""

from __future__ import annotations

import logging
from typing import Optional

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

import chart_store
from snowflake_connector import execute_query

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Helpers — date casting (FILE_DATE may be stored as VARCHAR in Snowflake)
# ---------------------------------------------------------------------------

def _safe_date(col: str) -> str:
    """Wrap a column expression in TRY_TO_DATE so VARCHAR dates don't crash."""
    return f"TRY_TO_DATE({col}::STRING)"


def _date_trunc(trunc: str, col: str) -> str:
    return f"DATE_TRUNC('{trunc}', {_safe_date(col)})::DATE"


def _date_between(col: str, start: str, end: str) -> str:
    return f"{_safe_date(col)} BETWEEN '{start}' AND '{end}'"


# ---------------------------------------------------------------------------
# Colour palette
# ---------------------------------------------------------------------------

_P = {
    "primary":    "#1F6FEB",
    "success":    "#1A7F37",
    "warning":    "#E36209",
    "danger":     "#CF222E",
    "neutral":    "#6E7781",
    "purple":     "#8250DF",
    "teal":       "#0969DA",
    "bg":         "#0D1117",
    "surface":    "#161B22",
    "text":       "#E6EDF3",
    "grid":       "#30363D",
}

_SEQ = [_P["primary"], _P["success"], _P["warning"], _P["danger"],
        _P["purple"], _P["teal"], "#F78166", "#56D364", "#79C0FF", "#D2A8FF"]

_SEGMENT_C = {
    "High Engagement — Conversion Candidate":    _P["success"],
    "Mid Engagement — Nurture Needed":           _P["primary"],
    "Low Engagement — Re-engagement Candidate":  _P["warning"],
    "At Risk — Drop-off Signal":                 _P["danger"],
    "No Activity":                               _P["neutral"],
}

_REJECTION_C = {
    "NULL_EMAIL":       _P["danger"],
    "NO_CONSENT":       _P["warning"],
    "SUPPRESSED":       _P["purple"],
    "FATAL_ERROR":      "#B00020",
    "NULL_FIRST_NAME":  _P["neutral"],
    "NULL_LAST_NAME":   _P["teal"],
    "NULL_PHONE_NUMBER":"#5C6BC0",
}

_EVENT_C = {
    "SENT":        _P["primary"],
    "OPEN":        _P["success"],
    "CLICK":       _P["teal"],
    "BOUNCE":      _P["danger"],
    "UNSUBSCRIBE": _P["warning"],
    "SPAM":        _P["purple"],
    "UNSENT":      _P["neutral"],
}


def _layout(title: str, height: int = 420, **extra) -> dict:
    base = dict(
        title=dict(text=title, font=dict(size=15, color=_P["text"])),
        paper_bgcolor=_P["surface"],
        plot_bgcolor=_P["bg"],
        font=dict(color=_P["text"], size=12),
        height=height,
        margin=dict(l=50, r=30, t=65, b=45),
        legend=dict(bgcolor="rgba(0,0,0,0)", bordercolor=_P["grid"], borderwidth=1),
        xaxis=dict(gridcolor=_P["grid"], zerolinecolor=_P["grid"]),
        yaxis=dict(gridcolor=_P["grid"], zerolinecolor=_P["grid"]),
    )
    base.update(extra)
    return base


def _df(sql: str, max_rows: int = 500) -> pd.DataFrame:
    """Run SQL and return uppercase-column DataFrame. Empty DF on any error."""
    try:
        df = execute_query(sql, max_rows=max_rows)
        if df is not None and not df.empty:
            df.columns = [c.upper() for c in df.columns]
        return df if df is not None else pd.DataFrame()
    except Exception as exc:
        logger.error("Chart SQL error: %s\nSQL: %s", exc, sql)
        return pd.DataFrame()


# ===========================================================================
# 0. GENERALISED SMART CHART  ← the key addition
# ===========================================================================

def smart_chart(
    sql: str,
    chart_type: str = "auto",
    title: str = "Chart",
    x_col: str | None = None,
    y_col: str | None = None,
    color_col: str | None = None,
    orientation: str = "v",
) -> str:
    """
    Universal chart engine. Runs `sql`, maps results to a Plotly figure.

    chart_type options:
      "auto"    — picks bar/line/pie based on column count + data shape
      "bar"     — vertical or horizontal bar (set orientation="h" for horizontal)
      "line"    — line/area time-series
      "pie"     — pie chart (uses first two columns as label + value)
      "donut"   — donut (pie with hole)
      "funnel"  — Plotly funnel
      "scatter" — scatter plot
      "area"    — filled area line chart
    """
    try:
        df = _df(sql, max_rows=500)
        if df.empty:
            return f"Chart '{title}': query returned no rows."

        cols = list(df.columns)

        # Auto-detect columns if not supplied
        if x_col is None:
            x_col = cols[0]
        if y_col is None:
            y_col = cols[1] if len(cols) > 1 else cols[0]

        x_col = x_col.upper()
        y_col = y_col.upper()
        if color_col:
            color_col = color_col.upper()

        # Auto chart type
        if chart_type == "auto":
            n_unique_x = df[x_col].nunique()
            # If x looks like dates → line
            try:
                pd.to_datetime(df[x_col])
                chart_type = "line"
            except Exception:
                pass
            if chart_type == "auto":
                chart_type = "pie" if n_unique_x <= 8 and len(cols) == 2 else "bar"

        fig = go.Figure()

        if chart_type in ("bar",):
            if color_col and color_col in cols:
                for i, val in enumerate(df[color_col].unique()):
                    mask = df[color_col] == val
                    fig.add_trace(go.Bar(
                        name=str(val),
                        x=df.loc[mask, x_col] if orientation == "v" else df.loc[mask, y_col],
                        y=df.loc[mask, y_col] if orientation == "v" else df.loc[mask, x_col],
                        marker_color=_SEQ[i % len(_SEQ)],
                        orientation=orientation,
                        text=df.loc[mask, y_col] if orientation == "v" else df.loc[mask, x_col],
                        textposition="outside",
                    ))
                fig.update_layout(barmode="group")
            else:
                colour_list = _SEQ[:len(df)] if len(df) <= len(_SEQ) else _SEQ * (len(df) // len(_SEQ) + 1)
                fig.add_trace(go.Bar(
                    x=df[x_col] if orientation == "v" else df[y_col],
                    y=df[y_col] if orientation == "v" else df[x_col],
                    marker_color=colour_list[:len(df)],
                    orientation=orientation,
                    text=df[y_col] if orientation == "v" else df[x_col],
                    textposition="outside",
                ))

        elif chart_type in ("line", "area"):
            fill = "tozeroy" if chart_type == "area" else None
            if color_col and color_col in cols:
                for i, val in enumerate(df[color_col].unique()):
                    mask = df[color_col] == val
                    fig.add_trace(go.Scatter(
                        name=str(val), x=df.loc[mask, x_col], y=df.loc[mask, y_col],
                        mode="lines+markers", fill=fill,
                        line=dict(color=_SEQ[i % len(_SEQ)], width=2),
                        marker=dict(size=5),
                    ))
            else:
                # If there are multiple numeric columns, plot each as a line
                numeric_cols = [c for c in cols if c != x_col and pd.api.types.is_numeric_dtype(df[c])]
                if not numeric_cols:
                    numeric_cols = [y_col]
                for i, yc in enumerate(numeric_cols):
                    fig.add_trace(go.Scatter(
                        name=yc.replace("_", " ").title(),
                        x=df[x_col], y=df[yc],
                        mode="lines+markers", fill=fill,
                        line=dict(color=_SEQ[i % len(_SEQ)], width=2),
                        marker=dict(size=5),
                    ))

        elif chart_type in ("pie", "donut"):
            hole = 0.45 if chart_type == "donut" else 0
            colours = [_SEQ[i % len(_SEQ)] for i in range(len(df))]
            fig.add_trace(go.Pie(
                labels=df[x_col], values=df[y_col], hole=hole,
                marker=dict(colors=colours, line=dict(color=_P["bg"], width=2)),
                textinfo="label+percent+value",
            ))
            fig.update_layout(showlegend=True)

        elif chart_type == "funnel":
            fig.add_trace(go.Funnel(
                y=df[x_col].astype(str).tolist(),
                x=df[y_col].tolist(),
                textinfo="value+percent initial",
                marker=dict(color=_SEQ[:len(df)]),
            ))

        elif chart_type == "scatter":
            z_col = cols[2] if len(cols) > 2 else None
            fig.add_trace(go.Scatter(
                x=df[x_col], y=df[y_col],
                mode="markers",
                marker=dict(
                    size=10,
                    color=df[z_col] if z_col else _P["primary"],
                    colorscale="Blues" if z_col else None,
                    showscale=bool(z_col),
                ),
                text=df[x_col],
            ))

        fig.update_layout(**_layout(title, height=440))
        chart_store.push(fig)
        return f"Chart '{title}' generated successfully ({chart_type}, {len(df)} data points)."

    except Exception as exc:
        logger.error("smart_chart error: %s", exc, exc_info=True)
        return f"Could not generate chart '{title}': {exc}"


# ===========================================================================
# 1. Funnel chart
# ===========================================================================

def funnel_chart(start_date: str = "2020-01-01", end_date: str = "2099-12-31") -> str:
    try:
        sql = f"""
            SELECT 'F01 Lead Intake' AS stage, COUNT(*) AS cnt
            FROM QA_FIPSAR_PHI_HUB.STAGING.STG_PROSPECT_INTAKE
            WHERE {_date_between('FILE_DATE', start_date, end_date)}
            UNION ALL
            SELECT 'F02 Valid Prospects', COUNT(*)
            FROM QA_FIPSAR_PHI_HUB.PHI_CORE.PHI_PROSPECT_MASTER
            WHERE {_date_between('FILE_DATE', start_date, end_date)}
            UNION ALL
            SELECT 'F04 SFMC Sent', COUNT(*)
            FROM QA_FIPSAR_DW.GOLD.FACT_SFMC_ENGAGEMENT
            WHERE EVENT_TYPE = 'SENT'
              AND {_date_between('EVENT_TIMESTAMP', start_date, end_date)}
            UNION ALL
            SELECT 'F06 Opened', COUNT(*)
            FROM QA_FIPSAR_DW.GOLD.FACT_SFMC_ENGAGEMENT
            WHERE EVENT_TYPE = 'OPEN'
              AND {_date_between('EVENT_TIMESTAMP', start_date, end_date)}
            UNION ALL
            SELECT 'F06 Clicked', COUNT(*)
            FROM QA_FIPSAR_DW.GOLD.FACT_SFMC_ENGAGEMENT
            WHERE EVENT_TYPE = 'CLICK'
              AND {_date_between('EVENT_TIMESTAMP', start_date, end_date)}
        """
        df = _df(sql, max_rows=10)
        if df.empty:
            return "No funnel data available for the selected period."

        stage_order = ["F01 Lead Intake", "F02 Valid Prospects",
                       "F04 SFMC Sent", "F06 Opened", "F06 Clicked"]
        df["STAGE"] = pd.Categorical(df["STAGE"], categories=stage_order, ordered=True)
        df = df.sort_values("STAGE").dropna(subset=["STAGE"])

        fig = go.Figure(go.Funnel(
            y=df["STAGE"].astype(str).tolist(),
            x=df["CNT"].tolist(),
            textinfo="value+percent initial",
            marker=dict(color=_SEQ[:len(df)]),
            connector=dict(line=dict(color=_P["grid"], width=2)),
        ))
        fig.update_layout(**_layout(
            f"Prospect Journey Funnel  |  {start_date} → {end_date}", height=460
        ))
        chart_store.push(fig)
        summary = "  →  ".join(f"{r['STAGE']}: {r['CNT']:,}" for _, r in df.iterrows())
        return f"Funnel chart generated. {summary}"

    except Exception as exc:
        logger.error("funnel_chart: %s", exc, exc_info=True)
        return f"Could not generate funnel chart: {exc}"


# ===========================================================================
# 2. Rejection reasons donut
# ===========================================================================

def rejection_chart(
    start_date: str = "2020-01-01",
    end_date: str = "2099-12-31",
    rejection_category: str = "all",
) -> str:
    try:
        if rejection_category == "intake":
            cat_f = "AND UPPER(REJECTION_REASON) NOT IN ('SUPPRESSED','FATAL_ERROR')"
            title_s = "Lead Mastering Rejections"
        elif rejection_category == "sfmc":
            cat_f = "AND UPPER(REJECTION_REASON) IN ('SUPPRESSED','FATAL_ERROR')"
            title_s = "SFMC Suppression Signals"
        else:
            cat_f = ""
            title_s = "All Rejection Reasons"

        sql = f"""
            SELECT REJECTION_REASON, COUNT(*) AS cnt
            FROM QA_FIPSAR_AUDIT.PIPELINE_AUDIT.DQ_REJECTION_LOG
            WHERE (
                TRY_TO_DATE(TRY_PARSE_JSON(REJECTED_RECORD):FILE_DATE::STRING)
                    BETWEEN '{start_date}' AND '{end_date}'
                OR (
                    TRY_TO_DATE(TRY_PARSE_JSON(REJECTED_RECORD):FILE_DATE::STRING) IS NULL
                    AND {_date_between('REJECTED_AT', start_date, end_date)}
                )
            )
            {cat_f}
            GROUP BY 1 ORDER BY 2 DESC
        """
        df = _df(sql, max_rows=20)
        if df.empty:
            return "No rejection data found for the selected period."

        colours = [_REJECTION_C.get(r, _P["neutral"]) for r in df["REJECTION_REASON"]]
        fig = go.Figure(go.Pie(
            labels=df["REJECTION_REASON"], values=df["CNT"], hole=0.45,
            marker=dict(colors=colours, line=dict(color=_P["bg"], width=2)),
            textinfo="label+percent+value", textfont=dict(size=12),
        ))
        fig.update_layout(**_layout(f"{title_s}  |  {start_date} → {end_date}"))
        chart_store.push(fig)
        top = df.iloc[0]
        return (f"Rejection chart generated. Top: {top['REJECTION_REASON']} "
                f"({top['CNT']:,} records). {len(df)} reason(s) shown.")

    except Exception as exc:
        logger.error("rejection_chart: %s", exc, exc_info=True)
        return f"Could not generate rejection chart: {exc}"


# ===========================================================================
# 3. SFMC engagement grouped bar
# ===========================================================================

def engagement_chart(
    start_date: str = "2020-01-01",
    end_date: str = "2099-12-31",
    journey_type: str | None = None,
) -> str:
    try:
        jf = (f"AND UPPER(j.JOURNEY_TYPE) LIKE '%{journey_type.upper()}%'"
              if journey_type else "")

        sql = f"""
            SELECT j.JOURNEY_TYPE, fe.EVENT_TYPE, COUNT(*) AS cnt
            FROM QA_FIPSAR_DW.GOLD.FACT_SFMC_ENGAGEMENT fe
            JOIN QA_FIPSAR_DW.GOLD.DIM_SFMC_JOB j ON fe.JOB_KEY = j.JOB_KEY
            WHERE {_date_between('fe.EVENT_TIMESTAMP', start_date, end_date)} {jf}
            GROUP BY 1, 2 ORDER BY 1, 2
        """
        df = _df(sql, max_rows=300)

        if df.empty:
            # Fallback: raw SFMC tables
            sql_raw = """
                SELECT 'SENT' AS event_type, COUNT(*) AS cnt
                FROM QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_SENT
                UNION ALL SELECT 'OPEN',        COUNT(*) FROM QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_OPENS
                UNION ALL SELECT 'CLICK',       COUNT(*) FROM QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_CLICKS
                UNION ALL SELECT 'BOUNCE',      COUNT(*) FROM QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_BOUNCES
                UNION ALL SELECT 'UNSUBSCRIBE', COUNT(*) FROM QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_UNSUBSCRIBES
                UNION ALL SELECT 'SPAM',        COUNT(*) FROM QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_SPAM
            """
            df_r = _df(sql_raw, max_rows=10)
            if df_r.empty:
                return "No SFMC engagement data available."

            fig = go.Figure([
                go.Bar(
                    x=[row["EVENT_TYPE"]], y=[row["CNT"]],
                    name=row["EVENT_TYPE"],
                    marker_color=_EVENT_C.get(row["EVENT_TYPE"], _P["neutral"]),
                    text=[f"{int(row['CNT']):,}"], textposition="outside",
                )
                for _, row in df_r.iterrows()
            ])
            fig.update_layout(**_layout("SFMC Event Volume (Raw Events)", height=420),
                              barmode="group", showlegend=False)
            chart_store.push(fig)
            return "SFMC engagement chart generated from raw event tables."

        pivot = df.pivot_table(index="EVENT_TYPE", columns="JOURNEY_TYPE",
                               values="CNT", fill_value=0)
        fig = go.Figure([
            go.Bar(
                name=journey,
                x=pivot.index.tolist(),
                y=pivot[journey].tolist(),
                marker_color=_SEQ[i % len(_SEQ)],
                text=[f"{int(v):,}" for v in pivot[journey]],
                textposition="outside",
            )
            for i, journey in enumerate(pivot.columns)
        ])
        fig.update_layout(
            **_layout(f"SFMC Engagement by Journey & Event  |  {start_date} → {end_date}",
                      height=460),
            barmode="group",
        )
        chart_store.push(fig)
        return f"Engagement chart generated. Journeys: {', '.join(pivot.columns.tolist())}."

    except Exception as exc:
        logger.error("engagement_chart: %s", exc, exc_info=True)
        return f"Could not generate engagement chart: {exc}"


# ===========================================================================
# 4. Conversion / segment dual donut
# ===========================================================================

def conversion_segment_chart(
    start_date: str = "2020-01-01",
    end_date: str = "2099-12-31",
) -> str:
    try:
        sql_seg = f"""
            WITH pe AS (
                SELECT x.MASTER_PATIENT_ID,
                    COUNT(CASE WHEN fe.EVENT_TYPE='CLICK'       THEN 1 END) AS clicks,
                    COUNT(CASE WHEN fe.EVENT_TYPE='OPEN'        THEN 1 END) AS opens,
                    COUNT(CASE WHEN fe.EVENT_TYPE='BOUNCE'      THEN 1 END) AS bounces,
                    COUNT(CASE WHEN fe.EVENT_TYPE='UNSUBSCRIBE' THEN 1 END) AS unsubscribes,
                    COUNT(CASE WHEN fe.EVENT_TYPE='SENT'        THEN 1 END) AS sends
                FROM QA_FIPSAR_PHI_HUB.PHI_CORE.PHI_PROSPECT_MASTER p
                JOIN QA_FIPSAR_PHI_HUB.PHI_CORE.PATIENT_IDENTITY_XREF x
                     ON p.MASTER_PATIENT_ID = x.MASTER_PATIENT_ID
                JOIN QA_FIPSAR_DW.GOLD.FACT_SFMC_ENGAGEMENT fe
                     ON x.IDENTITY_KEY = fe.SUBSCRIBER_KEY
                WHERE {_date_between('fe.EVENT_TIMESTAMP', start_date, end_date)}
                  AND p.IS_ACTIVE = TRUE
                GROUP BY 1
            )
            SELECT
                CASE WHEN clicks>0 THEN 'High Engagement'
                     WHEN opens>0 AND clicks=0 THEN 'Mid Engagement'
                     WHEN bounces>0 OR unsubscribes>0 THEN 'At Risk'
                     WHEN sends>0 AND opens=0 THEN 'Low Engagement'
                     ELSE 'No Activity' END AS segment,
                COUNT(*) AS cnt
            FROM pe GROUP BY 1
        """
        sql_act = f"""
            SELECT CASE WHEN IS_ACTIVE THEN 'Active' ELSE 'Inactive / Dropped' END AS status,
                   COUNT(*) AS cnt
            FROM QA_FIPSAR_PHI_HUB.PHI_CORE.PHI_PROSPECT_MASTER
            WHERE {_date_between('FILE_DATE', start_date, end_date)}
            GROUP BY 1
        """
        df_seg = _df(sql_seg, max_rows=10)
        df_act = _df(sql_act, max_rows=5)

        if df_seg.empty and df_act.empty:
            return "No conversion/segment data available."

        has_seg = not df_seg.empty
        fig = make_subplots(
            rows=1, cols=2 if has_seg else 1,
            specs=[[{"type": "pie"}, {"type": "pie"}]] if has_seg
                  else [[{"type": "pie"}]],
            subplot_titles=(["Engagement Segments", "Active vs Dropped"] if has_seg
                            else ["Active vs Dropped"]),
        )

        seg_label_map = {
            "High Engagement":    "High Engagement — Conversion Candidate",
            "Mid Engagement":     "Mid Engagement — Nurture Needed",
            "At Risk":            "At Risk — Drop-off Signal",
            "Low Engagement":     "Low Engagement — Re-engagement Candidate",
            "No Activity":        "No Activity",
        }

        if has_seg:
            seg_colours = [_SEGMENT_C.get(seg_label_map.get(s, s), _P["neutral"])
                           for s in df_seg["SEGMENT"]]
            fig.add_trace(go.Pie(
                labels=df_seg["SEGMENT"], values=df_seg["CNT"], hole=0.45,
                marker=dict(colors=seg_colours, line=dict(color=_P["bg"], width=2)),
                textinfo="percent+value", name="Segments",
            ), row=1, col=1)

        if not df_act.empty:
            act_c = [_P["success"] if s == "Active" else _P["danger"]
                     for s in df_act["STATUS"]]
            fig.add_trace(go.Pie(
                labels=df_act["STATUS"], values=df_act["CNT"], hole=0.45,
                marker=dict(colors=act_c, line=dict(color=_P["bg"], width=2)),
                textinfo="percent+value", name="Active/Dropped",
            ), row=1, col=2 if has_seg else 1)

        layout = _layout(
            f"Conversion & Drop-off Overview  |  {start_date} → {end_date}", height=460
        )
        layout.pop("xaxis", None)
        layout.pop("yaxis", None)
        fig.update_layout(**layout)
        chart_store.push(fig)

        if has_seg:
            top = df_seg.sort_values("CNT", ascending=False).iloc[0]
            return (f"Conversion chart generated. Largest segment: "
                    f"{top['SEGMENT']} ({top['CNT']:,} prospects).")
        return "Conversion chart generated (active/inactive breakdown)."

    except Exception as exc:
        logger.error("conversion_segment_chart: %s", exc, exc_info=True)
        return f"Could not generate conversion chart: {exc}"


# ===========================================================================
# 5. SFMC stage gap / fishbone chart
# ===========================================================================

def sfmc_stage_fishbone_chart(
    target_date: str,
    prospect_id: str | None = None,
) -> str:
    try:
        prospect_filter = f"AND jd.PROSPECT_ID = '{prospect_id}'" if prospect_id else ""
        sql = f"""
            WITH base AS (
                SELECT *
                FROM QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_PROSPECT_JOURNEY_DETAILS jd
                WHERE 1=1 {prospect_filter}
            )
            SELECT * FROM (
                SELECT 2 AS stage_order, 'Stage 2 - Education' AS stage,
                    COUNT(*) AS expected_count,
                    SUM(CASE WHEN UPPER(TRIM(WELCOMEJOURNEY_EDUCATIONEMAIL_SENT)) = 'TRUE'
                              AND TRY_TO_DATE(WELCOMEJOURNEY_EDUCATIONEMAIL_SENT_DATE::STRING) = '{target_date}' THEN 1 ELSE 0 END) AS sent,
                    SUM(CASE WHEN UPPER(TRIM(WELCOMEJOURNEY_EDUCATIONEMAIL_SENT)) != 'TRUE'
                              AND UPPER(TRIM(SUPPRESSION_FLAG)) IN ('YES','Y','TRUE','1') THEN 1 ELSE 0 END) AS suppressed,
                    SUM(CASE WHEN UPPER(TRIM(WELCOMEJOURNEY_EDUCATIONEMAIL_SENT)) != 'TRUE'
                              AND UPPER(TRIM(SUPPRESSION_FLAG)) NOT IN ('YES','Y','TRUE','1') THEN 1 ELSE 0 END) AS unsent
                FROM base
                WHERE UPPER(TRIM(WELCOMEJOURNEY_WELCOMEEMAIL_SENT)) = 'TRUE'
                  AND DATEADD(DAY, 3, TRY_TO_DATE(WELCOMEJOURNEY_WELCOMEEMAIL_SENT_DATE::STRING)) = '{target_date}'
                UNION ALL
                SELECT 3, 'Stage 3 - Nurture Edu 1',
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
                SELECT 4, 'Stage 4 - Nurture Edu 2',
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
                SELECT 5, 'Stage 5 - Prospect Story',
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
                SELECT 6, 'Stage 6 - Conversion',
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
                SELECT 7, 'Stage 7 - Reminder',
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
                SELECT 8, 'Stage 8 - Re-engagement',
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
                SELECT 9, 'Stage 9 - Final Reminder',
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
        """
        df = _df(sql, max_rows=20)
        if df.empty:
            return f"No stage-level expected send records were found for {target_date}."

        fig = go.Figure()
        fig.add_trace(go.Bar(
            y=df["STAGE"],
            x=df["SENT"],
            name="Sent",
            orientation="h",
            marker_color=_P["success"],
            text=df["SENT"],
            textposition="inside",
        ))
        fig.add_trace(go.Bar(
            y=df["STAGE"],
            x=df["SUPPRESSED"],
            name="Suppressed",
            orientation="h",
            marker_color=_P["danger"],
            text=df["SUPPRESSED"],
            textposition="inside",
        ))
        fig.add_trace(go.Bar(
            y=df["STAGE"],
            x=df["UNSENT"],
            name="Unsent Gap",
            orientation="h",
            marker_color=_P["neutral"],
            text=df["UNSENT"],
            textposition="inside",
        ))
        fig.update_layout(
            **_layout(f"SFMC Stage Fishbone | {target_date}", height=520),
            barmode="stack",
            xaxis=dict(title="Expected = Sent + Suppressed + Unsent Gap", gridcolor=_P["grid"]),
            yaxis=dict(autorange="reversed"),
        )
        chart_store.push(fig)

        total_expected = int(df["EXPECTED_COUNT"].sum()) if "EXPECTED_COUNT" in df.columns else int((df["SENT"] + df["SUPPRESSED"] + df["UNSENT"]).sum())
        total_sent = int(df["SENT"].sum())
        total_suppressed = int(df["SUPPRESSED"].sum())
        return (
            f"SFMC stage fishbone chart generated for {target_date}. "
            f"Expected: {total_expected:,}, Sent: {total_sent:,}, Suppressed: {total_suppressed:,}."
        )
    except Exception as exc:
        logger.error("sfmc_stage_fishbone_chart: %s", exc, exc_info=True)
        return f"Could not generate SFMC stage fishbone chart: {exc}"


# ===========================================================================
# 6. Intake trend (time series) — fixed DATE_TRUNC
# ===========================================================================

def intake_trend_chart(
    start_date: str = "2020-01-01",
    end_date: str = "2099-12-31",
    group_by: str = "month",
) -> str:
    try:
        trunc = {"day": "DAY", "week": "WEEK", "month": "MONTH"}.get(
            group_by.lower(), "MONTH"
        )
        # TRY_TO_DATE wrapping fixes VARCHAR FILE_DATE columns
        sql_l = f"""
            SELECT {_date_trunc(trunc, 'FILE_DATE')} AS period, COUNT(*) AS leads
            FROM QA_FIPSAR_PHI_HUB.STAGING.STG_PROSPECT_INTAKE
            WHERE {_date_between('FILE_DATE', start_date, end_date)}
            GROUP BY 1 ORDER BY 1
        """
        sql_p = f"""
            SELECT {_date_trunc(trunc, 'FILE_DATE')} AS period, COUNT(*) AS prospects
            FROM QA_FIPSAR_PHI_HUB.PHI_CORE.PHI_PROSPECT_MASTER
            WHERE {_date_between('FILE_DATE', start_date, end_date)}
            GROUP BY 1 ORDER BY 1
        """
        df_l = _df(sql_l, max_rows=500)
        df_p = _df(sql_p, max_rows=500)

        if df_l.empty:
            return "No intake trend data available for the selected period."

        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=df_l["PERIOD"], y=df_l["LEADS"],
            name="Lead Intake", mode="lines+markers",
            line=dict(color=_P["primary"], width=2), marker=dict(size=6),
        ))
        if not df_p.empty:
            fig.add_trace(go.Scatter(
                x=df_p["PERIOD"], y=df_p["PROSPECTS"],
                name="Valid Prospects", mode="lines+markers",
                line=dict(color=_P["success"], width=2, dash="dot"),
                marker=dict(size=6),
            ))

        # Add a light shaded gap area to show the rejection band
        if not df_p.empty and len(df_l) == len(df_p):
            fig.add_trace(go.Scatter(
                x=pd.concat([df_l["PERIOD"], df_l["PERIOD"][::-1]]),
                y=pd.concat([df_l["LEADS"], df_p["PROSPECTS"][::-1]]),
                fill="toself",
                fillcolor="rgba(207,34,46,0.10)",
                line=dict(color="rgba(0,0,0,0)"),
                showlegend=True,
                name="Rejected / Gap",
            ))

        fig.update_layout(**_layout(
            f"Lead & Prospect Intake Trend ({group_by.capitalize()})  "
            f"|  {start_date} → {end_date}",
            height=430,
        ))
        chart_store.push(fig)
        return f"Intake trend chart generated by {group_by}. {len(df_l)} period(s) plotted."

    except Exception as exc:
        logger.error("intake_trend_chart: %s", exc, exc_info=True)
        return f"Could not generate intake trend chart: {exc}"


# ===========================================================================
# 7. Bounce analysis — Hard vs Soft by journey
# ===========================================================================

def bounce_analysis_chart(
    start_date: str = "2020-01-01",
    end_date: str = "2099-12-31",
) -> str:
    """Grouped bar: Hard vs Soft bounce counts per journey."""
    try:
        sql = f"""
            SELECT
                COALESCE(j.JOURNEY_TYPE, 'Unknown') AS journey,
                COALESCE(UPPER(b.BOUNCE_CATEGORY), 'UNKNOWN') AS bounce_type,
                COUNT(*) AS cnt
            FROM QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_BOUNCES b
            LEFT JOIN QA_FIPSAR_DW.GOLD.DIM_SFMC_JOB j ON b.JOB_ID = j.JOB_ID
            WHERE TRY_TO_DATE(SPLIT(b.EVENT_DATE, ' ')[0]::STRING, 'MM/DD/YYYY')
                  BETWEEN '{start_date}' AND '{end_date}'
            GROUP BY 1, 2
            ORDER BY 1, 2
        """
        df = _df(sql, max_rows=100)
        if df.empty:
            return "No bounce data found for the selected period."

        bounce_colors = {"HARD": _P["danger"], "SOFT": _P["warning"], "UNKNOWN": _P["neutral"]}
        pivot = df.pivot_table(index="JOURNEY", columns="BOUNCE_TYPE", values="CNT", fill_value=0)

        fig = go.Figure()
        for btype in pivot.columns:
            fig.add_trace(go.Bar(
                name=f"{btype} Bounce",
                x=pivot.index.tolist(),
                y=pivot[btype].tolist(),
                marker_color=bounce_colors.get(btype, _P["neutral"]),
                text=[f"{int(v):,}" for v in pivot[btype]],
                textposition="outside",
            ))

        fig.update_layout(
            **_layout(f"Bounce Analysis — Hard vs Soft by Journey  |  {start_date} → {end_date}", height=440),
            barmode="group",
        )
        chart_store.push(fig)
        total = int(df["CNT"].sum())
        return f"Bounce analysis chart generated. Total bounces: {total:,} across {df['JOURNEY'].nunique()} journey(s)."

    except Exception as exc:
        logger.error("bounce_analysis_chart: %s", exc, exc_info=True)
        return f"Could not generate bounce analysis chart: {exc}"


# ===========================================================================
# 8. Email KPI scorecard — open/click/bounce/unsub rates as horizontal bars
# ===========================================================================

def email_kpi_scorecard_chart(
    start_date: str = "2020-01-01",
    end_date: str = "2099-12-31",
) -> str:
    """Horizontal bar KPI scorecard showing open rate, click rate, bounce rate, unsub rate."""
    try:
        sql = f"""
            SELECT
                SUM(CASE WHEN EVENT_TYPE = 'SENT'        THEN 1 ELSE 0 END) AS sent,
                SUM(CASE WHEN EVENT_TYPE = 'OPEN'        THEN 1 ELSE 0 END) AS opens,
                SUM(CASE WHEN EVENT_TYPE = 'CLICK'       THEN 1 ELSE 0 END) AS clicks,
                SUM(CASE WHEN EVENT_TYPE = 'BOUNCE'      THEN 1 ELSE 0 END) AS bounces,
                SUM(CASE WHEN EVENT_TYPE = 'UNSUBSCRIBE' THEN 1 ELSE 0 END) AS unsubscribes,
                SUM(CASE WHEN EVENT_TYPE = 'SPAM'        THEN 1 ELSE 0 END) AS spam
            FROM QA_FIPSAR_DW.GOLD.FACT_SFMC_ENGAGEMENT
            WHERE DATE(EVENT_TIMESTAMP) BETWEEN '{start_date}' AND '{end_date}'
        """
        df = _df(sql, max_rows=1)
        if df.empty or df["SENT"].iloc[0] == 0:
            return "No SFMC email data found for scorecard."

        row = df.iloc[0]
        sent = max(int(row["SENT"]), 1)
        metrics = {
            "Open Rate":        round(int(row["OPENS"])        / sent * 100, 1),
            "Click Rate":       round(int(row["CLICKS"])       / sent * 100, 1),
            "Bounce Rate":      round(int(row["BOUNCES"])      / sent * 100, 1),
            "Unsubscribe Rate": round(int(row["UNSUBSCRIBES"]) / sent * 100, 1),
            "Spam Rate":        round(int(row["SPAM"])         / sent * 100, 1),
        }
        metric_colors = {
            "Open Rate":        _P["success"],
            "Click Rate":       _P["primary"],
            "Bounce Rate":      _P["danger"],
            "Unsubscribe Rate": _P["warning"],
            "Spam Rate":        _P["purple"],
        }

        labels = list(metrics.keys())
        values = list(metrics.values())
        colors = [metric_colors[l] for l in labels]

        fig = go.Figure(go.Bar(
            y=labels,
            x=values,
            orientation="h",
            marker_color=colors,
            text=[f"{v:.1f}%" for v in values],
            textposition="outside",
            textfont=dict(size=13, color=_P["text"]),
        ))
        fig.update_layout(
            **_layout(f"Email KPI Scorecard  |  {int(row['SENT']):,} emails sent  |  {start_date} → {end_date}", height=380),
            xaxis=dict(title="Rate (%)", gridcolor=_P["grid"], range=[0, max(values) * 1.35]),
            yaxis=dict(gridcolor="rgba(0,0,0,0)"),
            showlegend=False,
        )
        chart_store.push(fig)
        return (
            f"Email KPI scorecard generated. Sent: {int(row['SENT']):,} | "
            f"Open Rate: {metrics['Open Rate']}% | Click Rate: {metrics['Click Rate']}% | "
            f"Bounce Rate: {metrics['Bounce Rate']}% | Unsub Rate: {metrics['Unsubscribe Rate']}%"
        )

    except Exception as exc:
        logger.error("email_kpi_scorecard_chart: %s", exc, exc_info=True)
        return f"Could not generate email KPI scorecard: {exc}"


# ===========================================================================
# 9. Journey stage progression — how many prospects reached each stage
# ===========================================================================

def journey_stage_progression_chart() -> str:
    """Horizontal bar showing how many prospects had each of the 9 stages sent (TRUE)."""
    try:
        sql = """
            SELECT
                'S1 Welcome Email'        AS stage, 1 AS ord,
                SUM(CASE WHEN UPPER(TRIM(WELCOMEJOURNEY_WELCOMEEMAIL_SENT))          = 'TRUE' THEN 1 ELSE 0 END) AS reached
            FROM QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_PROSPECT_JOURNEY_DETAILS
            UNION ALL
            SELECT 'S2 Education Email',       2,
                SUM(CASE WHEN UPPER(TRIM(WELCOMEJOURNEY_EDUCATIONEMAIL_SENT))        = 'TRUE' THEN 1 ELSE 0 END)
            FROM QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_PROSPECT_JOURNEY_DETAILS
            UNION ALL
            SELECT 'S3 Nurture Edu 1',         3,
                SUM(CASE WHEN UPPER(TRIM(NURTUREJOURNEY_EDUCATIONEMAIL1_SENT))       = 'TRUE' THEN 1 ELSE 0 END)
            FROM QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_PROSPECT_JOURNEY_DETAILS
            UNION ALL
            SELECT 'S4 Nurture Edu 2',         4,
                SUM(CASE WHEN UPPER(TRIM(NURTUREJOURNEY_EDUCATIONEMAIL2_SENT))       = 'TRUE' THEN 1 ELSE 0 END)
            FROM QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_PROSPECT_JOURNEY_DETAILS
            UNION ALL
            SELECT 'S5 Prospect Story',        5,
                SUM(CASE WHEN UPPER(TRIM(NURTUREJOURNEY_PROSPECTSTORYEMAIL_SENT))    = 'TRUE' THEN 1 ELSE 0 END)
            FROM QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_PROSPECT_JOURNEY_DETAILS
            UNION ALL
            SELECT 'S6 Conversion Email',      6,
                SUM(CASE WHEN UPPER(TRIM(HIGHENGAGEMENT_CONVERSIONEMAIL_SENT))       = 'TRUE' THEN 1 ELSE 0 END)
            FROM QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_PROSPECT_JOURNEY_DETAILS
            UNION ALL
            SELECT 'S7 Reminder Email',        7,
                SUM(CASE WHEN UPPER(TRIM(HIGHENGAGEMENT_REMINDEREMAIL_SENT))         = 'TRUE' THEN 1 ELSE 0 END)
            FROM QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_PROSPECT_JOURNEY_DETAILS
            UNION ALL
            SELECT 'S8 Re-engagement Email',   8,
                SUM(CASE WHEN UPPER(TRIM(LOWENGAGEMENT_REENGAGEMENTEMAIL_SENT))      = 'TRUE' THEN 1 ELSE 0 END)
            FROM QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_PROSPECT_JOURNEY_DETAILS
            UNION ALL
            SELECT 'S9 Final Reminder',        9,
                SUM(CASE WHEN UPPER(TRIM(LOWENGAGEMENTFINALREMINDEREMAIL_SENT))      = 'TRUE' THEN 1 ELSE 0 END)
            FROM QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_PROSPECT_JOURNEY_DETAILS
            ORDER BY ord
        """
        df = _df(sql, max_rows=9)
        if df.empty:
            return "No journey progression data found."

        # Gradient colours from strong to faded as stages progress
        stage_colors = [_SEQ[i % len(_SEQ)] for i in range(len(df))]

        fig = go.Figure(go.Bar(
            y=df["STAGE"].tolist(),
            x=df["REACHED"].tolist(),
            orientation="h",
            marker_color=stage_colors,
            text=[f"{int(v):,}" for v in df["REACHED"]],
            textposition="outside",
            textfont=dict(size=12),
        ))
        fig.update_layout(
            **_layout("SFMC Journey Stage Progression — Prospects Reached per Stage", height=480),
            xaxis=dict(title="Prospects Reached", gridcolor=_P["grid"]),
            yaxis=dict(autorange="reversed", gridcolor="rgba(0,0,0,0)"),
            showlegend=False,
        )
        chart_store.push(fig)
        top_stage = df.sort_values("REACHED", ascending=False).iloc[0]
        return (
            f"Journey stage progression chart generated. "
            f"Highest reach: {top_stage['STAGE']} ({int(top_stage['REACHED']):,} prospects). "
            f"9 stages shown."
        )

    except Exception as exc:
        logger.error("journey_stage_progression_chart: %s", exc, exc_info=True)
        return f"Could not generate journey stage progression chart: {exc}"


# ===========================================================================
# 10. Daily engagement trend — multi-line OPEN / CLICK / SENT over time
# ===========================================================================

def daily_engagement_trend_chart(
    start_date: str = "2020-01-01",
    end_date: str = "2099-12-31",
    event_types: str = "SENT,OPEN,CLICK",
) -> str:
    """Multi-line daily trend of selected SFMC engagement event types."""
    try:
        events = [e.strip().upper() for e in event_types.split(",") if e.strip()]
        placeholders = ", ".join(f"'{e}'" for e in events)
        sql = f"""
            SELECT
                DATE(EVENT_TIMESTAMP)   AS event_date,
                EVENT_TYPE,
                COUNT(*)                AS cnt
            FROM QA_FIPSAR_DW.GOLD.FACT_SFMC_ENGAGEMENT
            WHERE DATE(EVENT_TIMESTAMP) BETWEEN '{start_date}' AND '{end_date}'
              AND EVENT_TYPE IN ({placeholders})
            GROUP BY 1, 2
            ORDER BY 1, 2
        """
        df = _df(sql, max_rows=5000)
        if df.empty:
            return "No daily engagement data found for the selected period and event types."

        fig = go.Figure()
        for i, etype in enumerate(df["EVENT_TYPE"].unique()):
            sub = df[df["EVENT_TYPE"] == etype].sort_values("EVENT_DATE")
            fig.add_trace(go.Scatter(
                name=etype,
                x=sub["EVENT_DATE"],
                y=sub["CNT"],
                mode="lines+markers",
                line=dict(color=_EVENT_C.get(etype, _SEQ[i % len(_SEQ)]), width=2),
                marker=dict(size=5),
                fill="tozeroy" if etype == "SENT" else None,
                fillcolor=f"rgba(31,111,235,0.07)" if etype == "SENT" else None,
            ))

        fig.update_layout(
            **_layout(f"Daily SFMC Engagement Trend  |  {start_date} → {end_date}", height=450),
            xaxis=dict(title="Date", gridcolor=_P["grid"]),
            yaxis=dict(title="Event Count", gridcolor=_P["grid"]),
            hovermode="x unified",
        )
        chart_store.push(fig)
        total = int(df["CNT"].sum())
        return (
            f"Daily engagement trend chart generated. "
            f"Events: {', '.join(df['EVENT_TYPE'].unique().tolist())} | "
            f"Total events: {total:,} across {df['EVENT_DATE'].nunique()} day(s)."
        )

    except Exception as exc:
        logger.error("daily_engagement_trend_chart: %s", exc, exc_info=True)
        return f"Could not generate daily engagement trend chart: {exc}"


# ===========================================================================
# 11. Prospect channel mix — donut by lead source channel
# ===========================================================================

def prospect_channel_mix_chart(
    start_date: str = "2020-01-01",
    end_date: str = "2099-12-31",
) -> str:
    """Donut chart of prospects by source channel."""
    try:
        sql = f"""
            SELECT
                COALESCE(c.CHANNEL_NAME, 'Unknown') AS channel,
                COUNT(DISTINCT fi.PROSPECT_KEY)     AS prospects
            FROM QA_FIPSAR_DW.GOLD.FACT_PROSPECT_INTAKE fi
            LEFT JOIN QA_FIPSAR_DW.GOLD.DIM_CHANNEL c ON fi.CHANNEL_KEY = c.CHANNEL_KEY
            WHERE fi.FILE_DATE BETWEEN '{start_date}' AND '{end_date}'
            GROUP BY 1
            ORDER BY 2 DESC
        """
        df = _df(sql, max_rows=20)
        if df.empty:
            # Fallback: PHI layer
            sql_phi = f"""
                SELECT
                    COALESCE(LEAD_SOURCE, 'Unknown') AS channel,
                    COUNT(*) AS prospects
                FROM QA_FIPSAR_PHI_HUB.PHI_CORE.PHI_PROSPECT_MASTER
                WHERE FILE_DATE BETWEEN '{start_date}' AND '{end_date}'
                GROUP BY 1
                ORDER BY 2 DESC
            """
            df = _df(sql_phi, max_rows=20)
            if df.empty:
                return "No channel data available for the selected period."
            df.columns = ["CHANNEL", "PROSPECTS"]

        colours = [_SEQ[i % len(_SEQ)] for i in range(len(df))]
        fig = go.Figure(go.Pie(
            labels=df["CHANNEL"],
            values=df["PROSPECTS"],
            hole=0.48,
            marker=dict(colors=colours, line=dict(color=_P["bg"], width=2)),
            textinfo="label+percent+value",
            textfont=dict(size=12),
        ))
        fig.update_layout(
            **_layout(f"Prospect Channel Mix  |  {start_date} → {end_date}", height=440),
        )
        layout_patch = dict(paper_bgcolor=_P["surface"], plot_bgcolor=_P["bg"])
        fig.update_layout(**layout_patch)
        chart_store.push(fig)
        top = df.iloc[0]
        return (
            f"Channel mix chart generated. Top channel: {top['CHANNEL']} "
            f"({int(top['PROSPECTS']):,} prospects). {len(df)} channel(s) shown."
        )

    except Exception as exc:
        logger.error("prospect_channel_mix_chart: %s", exc, exc_info=True)
        return f"Could not generate channel mix chart: {exc}"


# ===========================================================================
# 12. Funnel waterfall — loss at each stage (Lead → Click)
# ===========================================================================

def funnel_waterfall_chart(
    start_date: str = "2020-01-01",
    end_date: str = "2099-12-31",
) -> str:
    """Waterfall chart showing absolute volume and drop-off loss at each funnel stage."""
    try:
        sql = f"""
            SELECT 'F01 Lead Intake' AS stage, 1 AS ord, COUNT(*) AS cnt
            FROM QA_FIPSAR_PHI_HUB.STAGING.STG_PROSPECT_INTAKE
            WHERE {_date_between('FILE_DATE', start_date, end_date)}
            UNION ALL
            SELECT 'F02 Valid Prospects', 2, COUNT(*)
            FROM QA_FIPSAR_PHI_HUB.PHI_CORE.PHI_PROSPECT_MASTER
            WHERE {_date_between('FILE_DATE', start_date, end_date)}
            UNION ALL
            SELECT 'F04 SFMC Sent', 3, COUNT(*)
            FROM QA_FIPSAR_DW.GOLD.FACT_SFMC_ENGAGEMENT
            WHERE EVENT_TYPE = 'SENT'
              AND {_date_between('EVENT_TIMESTAMP', start_date, end_date)}
            UNION ALL
            SELECT 'F05 Opened', 4, COUNT(*)
            FROM QA_FIPSAR_DW.GOLD.FACT_SFMC_ENGAGEMENT
            WHERE EVENT_TYPE = 'OPEN'
              AND {_date_between('EVENT_TIMESTAMP', start_date, end_date)}
            UNION ALL
            SELECT 'F06 Clicked', 5, COUNT(*)
            FROM QA_FIPSAR_DW.GOLD.FACT_SFMC_ENGAGEMENT
            WHERE EVENT_TYPE = 'CLICK'
              AND {_date_between('EVENT_TIMESTAMP', start_date, end_date)}
            ORDER BY ord
        """
        df = _df(sql, max_rows=10)
        if df.empty:
            return "No funnel data available for waterfall chart."

        df = df.sort_values("ORD")
        stages = df["STAGE"].tolist()
        counts = df["CNT"].astype(int).tolist()

        # Build waterfall: first bar is absolute, rest are relative losses
        measure = ["absolute"] + ["relative"] * (len(counts) - 1)
        y_vals = [counts[0]] + [counts[i] - counts[i - 1] for i in range(1, len(counts))]
        text_vals = [
            f"{counts[i]:,}" if i == 0
            else f"{counts[i]:,}  ({counts[i]-counts[i-1]:+,})"
            for i in range(len(counts))
        ]

        fig = go.Figure(go.Waterfall(
            orientation="v",
            measure=measure,
            x=stages,
            y=y_vals,
            text=text_vals,
            textposition="outside",
            connector=dict(line=dict(color=_P["grid"], width=1, dash="dot")),
            increasing=dict(marker=dict(color=_P["success"])),
            decreasing=dict(marker=dict(color=_P["danger"])),
            totals=dict(marker=dict(color=_P["primary"])),
        ))
        fig.update_layout(
            **_layout(f"Funnel Loss Waterfall  |  {start_date} → {end_date}", height=460),
            showlegend=False,
        )
        chart_store.push(fig)
        drop = counts[0] - counts[-1]
        drop_pct = round(drop / max(counts[0], 1) * 100, 1)
        return (
            f"Funnel waterfall chart generated. "
            f"Lead Intake: {counts[0]:,} → Final Clicked: {counts[-1]:,} "
            f"(total funnel loss: {drop:,} = {drop_pct}% drop-off)."
        )

    except Exception as exc:
        logger.error("funnel_waterfall_chart: %s", exc, exc_info=True)
        return f"Could not generate funnel waterfall chart: {exc}"
