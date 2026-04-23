"""
streamlit_app.py
----------------
FIPSAR Prospect Journey Intelligence — Snowflake Streamlit (Snowsight) app.
Copy this entire folder into Snowflake Streamlit and run.

Dependencies (all pre-installed in Snowflake Streamlit):
  streamlit, pandas, altair, snowflake-snowpark-python

Compatibility notes:
  - Uses st.experimental_rerun() (works on Streamlit <1.27 used in Snowsight)
  - No st.divider() (uses st.markdown("---") instead)
  - No altair xOffset / alt.Gradient (Altair 4.x safe)
  - No tabulate, no plotly
"""

import re
import streamlit as st
import pandas as pd
import altair as alt
from datetime import date, timedelta
from snowflake.snowpark.context import get_active_session

import agent_sf as agent
import analytics_sf as analytics
import tools_sf as T


# ── Compatibility shim ────────────────────────────────────────────────────────

def _rerun():
    if hasattr(st, "rerun"):
        st.rerun()
    else:
        st.experimental_rerun()


# ── Page config ───────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="FIPSAR Intelligence",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="expanded",
)


# ── CSS ───────────────────────────────────────────────────────────────────────

st.markdown("""
<style>
/* ── Fixed nav: header + tab bar stay locked to viewport top ── */
/* Use position:fixed so Streamlit's nested scroll containers don't break it */
#fipsar-header {
    position: fixed !important;
    top: 0 !important;
    left: 0 !important;
    right: 0 !important;
    z-index: 9999 !important;
    background: #ffffff !important;
    border-bottom: 1px solid #e2e8f0 !important;
    box-shadow: 0 2px 8px rgba(0,0,0,0.08) !important;
    padding: 13px 24px !important;
    display: flex !important;
    align-items: center !important;
    gap: 12px !important;
}
.stTabs [data-baseweb="tab-list"] {
    position: fixed !important;
    top: 57px !important;
    left: 0 !important;
    right: 0 !important;
    z-index: 9998 !important;
    box-shadow: 0 2px 8px rgba(0,0,0,0.06) !important;
}
/* Push tab content below the two fixed bars (57px header + 46px tab bar ≈ 103px) */
.stTabs [data-baseweb="tab-panel"] {
    padding-top: 50px !important;
}
/* Push the entire block container down so fixed header doesn't overlap content */
div.block-container {
    padding-top: 110px !important;
    padding-bottom: 40px !important;
}

/* ── Global light theme ── */
[data-testid="stAppViewContainer"],
[data-testid="stMain"],
section.main > div {
    background: #f0f4f8 !important;
    color: #1a1f36;
}

/* ── Sidebar ── */
[data-testid="stSidebar"] {
    background: #1a2035 !important;
    border-right: none !important;
}
[data-testid="stSidebar"] * { color: #c9d1e0 !important; }
[data-testid="stSidebar"] .stSelectbox > div > div {
    background: #232b42 !important;
    border: 1px solid #2e3a55 !important;
    color: #e0e6f0 !important;
    border-radius: 8px !important;
}
[data-testid="stSidebar"] .stButton > button {
    background: #2e3a55 !important;
    color: #c9d1e0 !important;
    border: 1px solid #3a4a6a !important;
    border-radius: 8px !important;
}

/* ── Tab bar ── */
.stTabs [data-baseweb="tab-list"] {
    background: #ffffff !important;
    border-radius: 0 !important;
    border-bottom: 2px solid #e2e8f0 !important;
    gap: 0 !important;
    padding: 0 12px !important;
    margin-bottom: 0 !important;
}
.stTabs [data-baseweb="tab"] {
    background: transparent !important;
    border-radius: 0 !important;
    color: #64748b !important;
    font-weight: 600 !important;
    padding: 12px 26px !important;
    font-size: 14px !important;
    border-bottom: 3px solid transparent !important;
    margin-bottom: -2px !important;
}
.stTabs [aria-selected="true"] {
    background: transparent !important;
    color: #1a56db !important;
    border-bottom: 3px solid #1a56db !important;
    border-radius: 0 !important;
}

/* ── KPI cards (colored gradient) ── */
.kpi-card {
    border-radius: 14px;
    padding: 18px 12px;
    text-align: center;
    margin-bottom: 4px;
}
.kpi-blue   { background: linear-gradient(135deg,#1a56db 0%,#0f3fa8 100%); }
.kpi-purple { background: linear-gradient(135deg,#7c3aed 0%,#5b21b6 100%); }
.kpi-orange { background: linear-gradient(135deg,#f59e0b 0%,#d97706 100%); }
.kpi-red    { background: linear-gradient(135deg,#ef4444 0%,#b91c1c 100%); }
.kpi-teal   { background: linear-gradient(135deg,#0d9488 0%,#0f766e 100%); }
.kpi-indigo { background: linear-gradient(135deg,#4f46e5 0%,#3730a3 100%); }
.kpi-cyan   { background: linear-gradient(135deg,#0891b2 0%,#0e7490 100%); }
.kpi-green  { background: linear-gradient(135deg,#10b981 0%,#059669 100%); }
.kpi-label  {
    font-size: 10px;
    text-transform: uppercase;
    letter-spacing: 1px;
    color: rgba(255,255,255,0.82);
    font-weight: 700;
    margin-bottom: 6px;
}
.kpi-value {
    font-size: 26px;
    font-weight: 800;
    color: #ffffff;
    line-height: 1.1;
}
.kpi-sub {
    font-size: 11px;
    color: rgba(255,255,255,0.68);
    margin-top: 3px;
}

/* ── Section header ── */
.sec-hdr {
    font-size: 14px;
    font-weight: 700;
    color: #1a1f36;
    margin: 0 0 12px 0;
    padding-bottom: 8px;
    border-bottom: 2px solid #e2e8f0;
}

/* ── Buttons (main) ── */
.stButton > button {
    background: linear-gradient(135deg,#1a56db,#0f3fa8) !important;
    color: #ffffff !important;
    border: none !important;
    border-radius: 8px !important;
    font-weight: 600 !important;
    font-size: 13px !important;
    padding: 6px 16px !important;
}

/* ── Inputs ── */
.stTextInput input, .stTextArea textarea {
    background: #ffffff !important;
    border: 1px solid #d1d9e6 !important;
    border-radius: 10px !important;
    color: #1a1f36 !important;
    font-size: 14px !important;
}
.stSelectbox > div > div {
    background: #ffffff !important;
    border: 1px solid #d1d9e6 !important;
    border-radius: 10px !important;
    color: #1a1f36 !important;
}
.stDateInput > div > div > input {
    background: #ffffff !important;
    border: 1px solid #d1d9e6 !important;
    border-radius: 10px !important;
    color: #1a1f36 !important;
}

/* ── Input labels (main content area) ── */
.stDateInput label,
.stSelectbox label,
.stMultiSelect label,
[data-testid="stDateInput"] label,
[data-testid="stSelectbox"] label,
[data-testid="stMultiSelect"] label {
    color: #1a1f36 !important;
    font-weight: 600 !important;
    font-size: 12px !important;
}

/* ── Multiselect (main content area — not sidebar) ── */
.stMultiSelect > div > div,
[data-testid="stMultiSelect"] > div > div,
.stMultiSelect [data-baseweb="select"] > div:first-child {
    background: #ffffff !important;
    border: 1px solid #d1d9e6 !important;
    border-radius: 10px !important;
    color: #1a1f36 !important;
}
.stMultiSelect [data-baseweb="tag"] {
    background: #dbeafe !important;
    color: #1a1f36 !important;
    border-radius: 6px !important;
}
.stMultiSelect [data-baseweb="tag"] span {
    color: #1a1f36 !important;
}
.stMultiSelect input {
    color: #1a1f36 !important;
}

/* ── Chat panel ── */
.chat-panel {
    background: #ffffff;
    border-radius: 14px;
    border: 1px solid #e2e8f0;
    padding: 14px 12px;
    height: 640px;
    overflow-y: auto;
}
.cat-title {
    font-size: 10px;
    font-weight: 800;
    text-transform: uppercase;
    letter-spacing: 0.9px;
    color: #1a56db;
    padding: 10px 0 4px 0;
    margin-top: 4px;
}
.cat-rule { height:1px; background:#e2e8f0; margin:2px 0 6px 0; }

/* ── Chat messages ── */
.chat-wrap {
    background: #f8fafc;
    border-radius: 14px;
    border: 1px solid #e2e8f0;
    padding: 20px;
    min-height: 420px;
    max-height: 460px;
    overflow-y: auto;
    margin-bottom: 10px;
}
.msg-user {
    text-align: right;
    margin-bottom: 10px;
}
.msg-user-bub {
    display: inline-block;
    background: linear-gradient(135deg,#1a56db,#0f3fa8);
    color: #ffffff;
    border-radius: 16px 16px 4px 16px;
    padding: 10px 16px;
    max-width: 72%;
    font-size: 13px;
    line-height: 1.5;
    text-align: left;
    word-wrap: break-word;
}
.msg-ai {
    text-align: left;
    margin-bottom: 10px;
}
.msg-ai-bub {
    display: inline-block;
    background: #ffffff;
    color: #1a1f36;
    border: 1px solid #e2e8f0;
    border-radius: 16px 16px 16px 4px;
    padding: 12px 16px;
    max-width: 88%;
    font-size: 13px;
    line-height: 1.6;
    box-shadow: 0 1px 3px rgba(0,0,0,0.06);
    text-align: left;
    word-wrap: break-word;
}
.chat-hint {
    text-align: center;
    color: #94a3b8;
    font-style: italic;
    font-size: 13px;
    padding: 16px 0;
}

/* ── Chat welcome ── */
.chat-welcome {
    text-align: center;
    padding: 36px 20px 20px 20px;
}
.chat-welcome h2 {
    font-size: 22px;
    font-weight: 800;
    color: #1a1f36;
    margin: 12px 0 6px 0;
}
.chat-welcome p {
    font-size: 13px;
    color: #64748b;
    margin-bottom: 24px;
}

/* ── DataFrame ── */
[data-testid="stDataFrame"] {
    background: #ffffff !important;
    border-radius: 10px !important;
    border: 1px solid #e2e8f0 !important;
}
hr { border-color: #e2e8f0 !important; }

/* ── Expander headers (Snowflake Streamlit compatibility) ── */
.streamlit-expanderHeader,
.streamlit-expanderHeader p,
[data-testid="stExpander"] summary,
[data-testid="stExpander"] summary p,
[data-testid="stExpander"] > details > summary {
    color: #1a1f36 !important;
    font-weight: 600 !important;
    font-size: 13px !important;
}
.streamlit-expanderHeader svg,
[data-testid="stExpander"] summary svg {
    fill: #1a1f36 !important;
    color: #1a1f36 !important;
}

/* ── Date inputs in all contexts ── */
.stDateInput input,
.stDateInput > div > div > input,
.stDateInput > label + div input {
    background: #ffffff !important;
    color: #1a1f36 !important;
    border: 1px solid #d1d9e6 !important;
    border-radius: 8px !important;
}

/* ── Info boxes ── */
[data-testid="stAlert"] { border-radius: 10px !important; }

/* ── Markdown content (chat AI responses + general) ── */
.stMarkdown h1 { color: #1a1f36 !important; font-size: 20px !important; font-weight: 800 !important; margin: 16px 0 8px !important; }
.stMarkdown h2 { color: #1a1f36 !important; font-size: 16px !important; font-weight: 700 !important; margin: 14px 0 6px !important; border-bottom: 2px solid #e2e8f0; padding-bottom: 4px; }
.stMarkdown h3 { color: #1a56db !important; font-size: 14px !important; font-weight: 700 !important; margin: 12px 0 6px !important; }
.stMarkdown h4, .stMarkdown h5 { color: #374151 !important; font-size: 13px !important; font-weight: 600 !important; }
.stMarkdown p  { color: #374151 !important; font-size: 13px !important; line-height: 1.6 !important; }
.stMarkdown li { color: #374151 !important; font-size: 13px !important; line-height: 1.6 !important; }
.stMarkdown strong { color: #1a1f36 !important; }
.stMarkdown em { color: #64748b !important; }
.stMarkdown code { background: #f0f4f8 !important; color: #1a56db !important; border-radius: 4px !important; padding: 1px 5px !important; font-size: 12px !important; }
.stMarkdown pre  { background: #f0f4f8 !important; border-radius: 8px !important; padding: 12px !important; }
.stMarkdown pre code { background: transparent !important; color: #1a1f36 !important; }

/* ── Markdown tables ── */
.stMarkdown table { width: 100% !important; border-collapse: collapse !important; font-size: 12px !important; margin: 10px 0 !important; }
.stMarkdown th { background: #1a56db !important; color: #ffffff !important; padding: 8px 12px !important; text-align: left !important; font-weight: 700 !important; font-size: 11px !important; text-transform: uppercase !important; letter-spacing: 0.5px !important; }
.stMarkdown td { color: #374151 !important; padding: 7px 12px !important; border-bottom: 1px solid #e2e8f0 !important; }
.stMarkdown tr:nth-child(even) td { background: #f8fafc !important; }
.stMarkdown tr:hover td { background: #f0f4f8 !important; }
</style>
""", unsafe_allow_html=True)


# ── Chart theme constants (Altair 4.x, light bg) ──────────────────────────────

CHART_BG   = "#ffffff"
CHART_GRID = "#f0f4f8"
CHART_TEXT = "#374151"
BLUE_MAIN  = "#1a56db"
BLUE_LIGHT = "#3b82f6"
BLUE_MID   = "#93c5fd"
RED_ACCENT = "#ef4444"
AMBER      = "#f59e0b"
TEAL       = "#0d9488"
PURPLE     = "#7c3aed"
GREEN      = "#10b981"


def _base(chart, height=300):
    return (
        chart
        .properties(height=height, background=CHART_BG)
        .configure_axis(
            gridColor=CHART_GRID,
            labelColor=CHART_TEXT,
            titleColor=CHART_TEXT,
            domainColor="#e2e8f0",
            tickColor="#e2e8f0",
        )
        .configure_legend(labelColor=CHART_TEXT, titleColor=CHART_TEXT)
        .configure_view(strokeWidth=0)
    )


# ── Auto-chart helpers for chat responses ─────────────────────────────────────

def _md_block_to_df(lines: list) -> pd.DataFrame:
    """Convert a list of raw markdown table lines into a DataFrame."""
    # Remove separator lines (| --- | :---: | etc.)
    data_lines = [l for l in lines if not re.match(r"^\|[\s\-:|]+\|$", l.replace(" ", ""))]
    if len(data_lines) < 2:
        return pd.DataFrame()

    headers = [h.strip() for h in data_lines[0].split("|")[1:-1]]
    rows = []
    for line in data_lines[1:]:
        cells = [c.strip() for c in line.split("|")[1:-1]]
        if len(cells) == len(headers):
            rows.append(cells)

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows, columns=headers)

    # Coerce numeric-looking columns (strip commas + %)
    for col in df.columns:
        cleaned = df[col].str.replace(",", "").str.replace("%", "").str.strip()
        numeric = pd.to_numeric(cleaned, errors="coerce")
        if numeric.notna().sum() >= max(1, len(df) * 0.6):
            df[col] = numeric

    return df


def _parse_md_tables(text: str) -> list:
    """
    Extract all markdown tables from response text.
    Returns list of (chart_type, DataFrame) tuples.
    chart_type: 'bar-h', 'bar-v', 'line', 'donut', 'none', 'auto'

    Handles [CHART:xxx] tags that appear on their own line before the table,
    including cases where the LLM wraps the tag across two lines.
    """
    # First, normalise multi-line chart tags into single-line form:
    #   [CHART:bar-v\n] → [CHART:bar-v]
    normalised = re.sub(
        r"\[CHART\s*:\s*(bar-h|bar-v|line|donut|none|auto)\s*\]",
        r"[CHART:\1]",
        text,
        flags=re.IGNORECASE | re.DOTALL,
    )

    results, block = [], []
    pending_chart_type = "auto"

    for line in normalised.split("\n"):
        stripped = line.strip()

        # Detect chart tag on its own line
        chart_match = re.match(r"^\[CHART:(bar-h|bar-v|line|donut|none|auto)\]$", stripped, re.I)
        if chart_match:
            pending_chart_type = chart_match.group(1).lower()
            continue

        if stripped.startswith("|"):
            block.append(stripped)
        else:
            if block:
                df = _md_block_to_df(block)
                if not df.empty and len(df) >= 2:
                    results.append((pending_chart_type, df))
                block = []
                pending_chart_type = "auto"

    if block:
        df = _md_block_to_df(block)
        if not df.empty and len(df) >= 2:
            results.append((pending_chart_type, df))

    return results


def _chat_bar_h(df, label_col, value_col):
    """Horizontal sorted bar chart."""
    return (
        alt.Chart(df)
        .mark_bar(color=BLUE_MAIN)
        .encode(
            x=alt.X(f"{value_col}:Q", title=""),
            y=alt.Y(f"{label_col}:N", sort="-x", title=""),
            tooltip=[f"{label_col}:N", alt.Tooltip(f"{value_col}:Q", format=",")],
        )
    )


def _chat_bar_v(df, label_col, value_col):
    """Vertical bar chart (good for stages in order)."""
    return (
        alt.Chart(df)
        .mark_bar(color=BLUE_MAIN)
        .encode(
            x=alt.X(f"{label_col}:N", sort=None, title=""),
            y=alt.Y(f"{value_col}:Q", title=value_col),
            tooltip=[f"{label_col}:N", alt.Tooltip(f"{value_col}:Q", format=",")],
        )
    )


def _render_response_charts(text: str) -> None:
    """
    Parse markdown tables + [CHART:type] hints from an AI response.
    Renders at most 3 charts. Skips [CHART:none] and tables with no numeric column.
    """
    tables = _parse_md_tables(text)
    if not tables:
        return

    rendered = 0
    for chart_type, df in tables[:3]:
        if chart_type == "none":
            continue

        cols = list(df.columns)
        numeric_cols = [c for c in cols if pd.api.types.is_numeric_dtype(df[c])]
        text_cols    = [c for c in cols if c not in numeric_cols]

        if not numeric_cols or not text_cols:
            continue

        # ── Pick label column ─────────────────────────────────────────────
        label_col = None
        for candidate in [
            "Stage", "Stage Name", "Suppression Stage", "Rejection Reason",
            "Event Type", "Transition", "Reason", "Category", "Table Name",
            "Month", "Date", "Week", "Phase",
        ]:
            if candidate in text_cols:
                label_col = candidate
                break
        if label_col is None:
            label_col = text_cols[0]

        # ── Pick value column ─────────────────────────────────────────────
        value_col = None
        for candidate in [
            "Prospects Sent", "Count", "Rejection Count", "Suppressed Count",
            "Event Count", "Lead Count", "rejection_count", "event_count",
        ]:
            if candidate in numeric_cols:
                value_col = candidate
                break
        if value_col is None:
            value_col = numeric_cols[0]

        # ── Build plot DataFrame ──────────────────────────────────────────
        plot_df = df[[label_col, value_col]].copy().dropna()
        plot_df = plot_df[plot_df[value_col] != 0]
        if plot_df.empty or len(plot_df) < 2:
            continue

        # ── Resolve chart type: use tag hint, fall back to auto-detection ─
        if chart_type == "auto":
            label_lower = label_col.lower()
            if any(w in label_lower for w in ["date", "month", "week", "year"]):
                chart_type = "line"
            elif "stage" in label_lower or "transition" in label_lower:
                chart_type = "bar-v"
            elif len(plot_df) <= 6:
                chart_type = "donut"
            else:
                chart_type = "bar-h"

        # ── Build Altair chart ────────────────────────────────────────────
        chart_icon = "📊"
        try:
            if chart_type == "line":
                chart_icon = "📈"
                plot_df[label_col] = pd.to_datetime(plot_df[label_col])
                chart = (
                    alt.Chart(plot_df)
                    .mark_line(point=True, color=BLUE_MAIN, strokeWidth=2)
                    .encode(
                        x=alt.X(f"{label_col}:T", title=""),
                        y=alt.Y(f"{value_col}:Q", title=value_col),
                        tooltip=[f"{label_col}:T", alt.Tooltip(f"{value_col}:Q", format=",")],
                    )
                )
            elif chart_type == "bar-v":
                chart = _chat_bar_v(plot_df, label_col, value_col)
            elif chart_type == "donut":
                chart_icon = "🥧"
                chart = (
                    alt.Chart(plot_df)
                    .mark_arc(innerRadius=48, outerRadius=88)
                    .encode(
                        theta=alt.Theta(f"{value_col}:Q"),
                        color=alt.Color(
                            f"{label_col}:N",
                            scale=alt.Scale(scheme="tableau10"),
                            legend=alt.Legend(orient="right"),
                        ),
                        tooltip=[f"{label_col}:N", alt.Tooltip(f"{value_col}:Q", format=",")],
                    )
                )
            else:  # bar-h (default)
                chart = _chat_bar_h(plot_df, label_col, value_col)
        except Exception:
            chart = _chat_bar_h(plot_df, label_col, value_col)

        # ── Render ────────────────────────────────────────────────────────
        st.markdown(
            f"<div style='font-size:11px;font-weight:700;color:#64748b;"
            f"text-transform:uppercase;letter-spacing:0.6px;margin:14px 0 4px;"
            f"padding:6px 10px;background:#f8fafc;border-left:3px solid #1a56db;"
            f"border-radius:0 6px 6px 0;'>"
            f"{chart_icon} {label_col} — {value_col}</div>",
            unsafe_allow_html=True,
        )
        st.altair_chart(_base(chart, height=220), use_container_width=True)
        rendered += 1


# ── Snowpark session ──────────────────────────────────────────────────────────

@st.cache_resource
def get_session():
    return get_active_session()

session = get_session()


# ── Session state ─────────────────────────────────────────────────────────────

if "chat_history" not in st.session_state:
    st.session_state.chat_history = []


# ── Sidebar ───────────────────────────────────────────────────────────────────

with st.sidebar:
    st.markdown(
        "<div style='padding:10px 0 6px;'>"
        "<span style='font-size:22px;font-weight:900;color:#4da3ff;letter-spacing:-0.5px;'>FIPSAR</span><br>"
        "<span style='font-size:10px;color:#6b7897;letter-spacing:2px;font-weight:600;'>INTELLIGENCE</span>"
        "</div>",
        unsafe_allow_html=True,
    )
    st.markdown("---")

    selected_model = st.selectbox(
        "Cortex Model",
        ["mistral-large2", "llama3.1-70b", "llama3.1-8b", "mistral-large", "mixtral-8x7b"],
        index=0,
        help="Snowflake Cortex LLM for AI responses.",
    )

    st.markdown("---")
    if st.button("🗑️  Clear Chat", use_container_width=True):
        st.session_state.chat_history = []
        _rerun()

    st.markdown("---")
    st.markdown(
        "<small style='color:#4a5568;'>Snowflake Cortex · FIPSAR QA Platform</small>",
        unsafe_allow_html=True,
    )


# ── Page header ───────────────────────────────────────────────────────────────

st.markdown(
    "<div id='fipsar-header'>"
    "<span style='font-size:22px;font-weight:900;color:#1a1f36;letter-spacing:-0.3px;'>"
    "FIPSAR Prospect Journey Intelligence</span>"
    "<span style='font-size:12px;color:#64748b;margin-left:8px;border-left:2px solid #e2e8f0;"
    "padding-left:12px;'>"
    "Snowflake Cortex &nbsp;·&nbsp; Real-time Analytics &nbsp;·&nbsp; Journey QA Platform</span>"
    "</div>",
    unsafe_allow_html=True,
)


# ── Tabs ──────────────────────────────────────────────────────────────────────

tab_recon, tab_chat, tab_journey, tab_analytics = st.tabs([
    "🔁   Recon Analytics",
    "💬   Chat",
    "🗺️   Journey Intelligence",
    "📊   Event Analytics",
])


# ══════════════════════════════════════════════════════════════════════════════
# TAB 1 — Analytics Dashboard
# ══════════════════════════════════════════════════════════════════════════════

with tab_analytics:

    # ── Collapsible filter bar ─────────────────────────────────────────────
    today = date.today()
    with st.expander("🔎  Filters", expanded=True):
        f1, f2, f3, f4, f5 = st.columns([2, 2, 2, 2, 1])
        with f1:
            ana_start = st.date_input("Start Date", value=date(2020, 1, 1), key="ana_start")
        with f2:
            ana_end   = st.date_input("End Date", value=today, key="ana_end")
        with f3:
            st.selectbox("Journey", ["All Journeys", "Prospect Journey"], key="ana_journey")
        with f4:
            st.selectbox(
                "Stage",
                ["All Stages","S01 Welcome","S02 Education","S03 Edu Email 1","S04 Edu Email 2",
                 "S05 Prospect Story","S06 Conversion","S07 Reminder","S08 Re-engagement","S09 Final"],
                key="ana_stage",
            )
        with f5:
            st.markdown("<br>", unsafe_allow_html=True)
            st.button("🔄 Refresh", use_container_width=True, key="ana_refresh")

    filter_start = ana_start.isoformat()
    filter_end   = ana_end.isoformat()

    # ── KPI cards ─────────────────────────────────────────────────────────
    with st.spinner("Loading KPIs..."):
        kpis = analytics.get_kpi_summary(session, filter_start, filter_end)

    kpi_cols = st.columns(8)
    kpi_items = [
        ("Total Leads",       f"{kpis['total_leads']:,}",          "Intake received",   "kpi-blue"),
        ("Valid Prospects",   f"{kpis['valid_prospects']:,}",       "Mastered",          "kpi-purple"),
        ("Rejection Rate",    f"{kpis['rejection_rate']}%",         "Intake → Prospect", "kpi-orange"),
        ("Suppressed",        f"{kpis['suppressed_prospects']:,}",  "Journey exited",    "kpi-red"),
        ("Active in Journey", f"{kpis['active_in_journey']:,}",     "Still progressing", "kpi-teal"),
        ("SFMC Events",       f"{kpis['total_sfmc_events']:,}",     "Total recorded",    "kpi-indigo"),
        ("Open Rate",         f"{kpis['open_rate']}%",              "Opens / sent",      "kpi-cyan"),
        ("Click Rate",        f"{kpis['click_rate']}%",             "Clicks / sent",     "kpi-green"),
    ]
    for col, (label, value, sub, cls) in zip(kpi_cols, kpi_items):
        with col:
            st.markdown(
                f"<div class='kpi-card {cls}'>"
                f"<div class='kpi-label'>{label}</div>"
                f"<div class='kpi-value'>{value}</div>"
                f"<div class='kpi-sub'>{sub}</div>"
                f"</div>",
                unsafe_allow_html=True,
            )

    st.markdown("<br>", unsafe_allow_html=True)

    # ── Row 1: Funnel + Rejection ──────────────────────────────────────────
    col_funnel, col_rej = st.columns(2)

    with col_funnel:
        st.markdown(
            "<div style='background:#fff;border-radius:14px;border:1px solid #e2e8f0;"
            "padding:18px 18px 8px 18px;box-shadow:0 2px 8px rgba(0,0,0,0.05);'>",
            unsafe_allow_html=True,
        )
        st.markdown("<div class='sec-hdr'>Leads to Prospect Conversion</div>", unsafe_allow_html=True)
        with st.spinner(""):
            funnel_df = analytics.get_funnel_df(session, filter_start, filter_end)
        if not funnel_df.empty:
            funnel_df["pct"]   = (funnel_df["Count"] / funnel_df["Count"].iloc[0] * 100).round(1)
            funnel_df["label"] = funnel_df.apply(lambda r: f"{r['Count']:,}  ({r['pct']}%)", axis=1)
            colors = [BLUE_MAIN, PURPLE, BLUE_LIGHT, RED_ACCENT]
            chart = (
                alt.Chart(funnel_df)
                .mark_bar()
                .encode(
                    x=alt.X("Count:Q", title=""),
                    y=alt.Y("Stage:N", sort=None, title=""),
                    color=alt.Color("Stage:N", scale=alt.Scale(range=colors), legend=None),
                    tooltip=["Stage:N", "Count:Q", "pct:Q"],
                )
            )
            text = chart.mark_text(align="left", dx=6, color=CHART_TEXT, fontSize=11).encode(
                text="label:N"
            )
            st.altair_chart(_base(chart + text, height=220), use_container_width=True)
        else:
            st.info("No funnel data for this period.")
        st.markdown("</div>", unsafe_allow_html=True)

    with col_rej:
        st.markdown(
            "<div style='background:#fff;border-radius:14px;border:1px solid #e2e8f0;"
            "padding:18px 18px 8px 18px;box-shadow:0 2px 8px rgba(0,0,0,0.05);'>",
            unsafe_allow_html=True,
        )
        st.markdown("<div class='sec-hdr'>Top Rejection Reasons</div>", unsafe_allow_html=True)
        rej_cat = st.selectbox(
            "Category",
            ["Intake (PHI_PROSPECT_MASTER)", "Dedup (SLV_PROSPECT_MASTER)", "SFMC (FACT_SFMC_ENGAGEMENT)"],
            key="rej_cat",
            label_visibility="collapsed",
        )
        cat_map = {
            "Intake (PHI_PROSPECT_MASTER)": "intake",
            "Dedup (SLV_PROSPECT_MASTER)":  "dedup",
            "SFMC (FACT_SFMC_ENGAGEMENT)":  "sfmc",
        }
        with st.spinner(""):
            rej_df = analytics.get_rejection_trend_df(
                session, filter_start, filter_end, category=cat_map[rej_cat]
            )
        if not rej_df.empty:
            chart = (
                alt.Chart(rej_df)
                .mark_bar(color=BLUE_MAIN)
                .encode(
                    x=alt.X("Count:Q", title=""),
                    y=alt.Y("Rejection Reason:N", sort="-x", title=""),
                    tooltip=["Rejection Reason:N", "Count:Q"],
                )
            )
            text = chart.mark_text(align="left", dx=4, color=CHART_TEXT, fontSize=11).encode(
                text=alt.Text("Count:Q", format=",")
            )
            st.altair_chart(_base(chart + text, height=240), use_container_width=True)
        else:
            st.info("No rejection data for this period.")
        st.markdown("</div>", unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    # ── Row 2: SFMC Event Mix + Trend ─────────────────────────────────────
    col_donut, col_trend = st.columns([1, 2])

    with col_donut:
        st.markdown(
            "<div style='background:#fff;border-radius:14px;border:1px solid #e2e8f0;"
            "padding:18px 18px 8px 18px;box-shadow:0 2px 8px rgba(0,0,0,0.05);'>",
            unsafe_allow_html=True,
        )
        st.markdown("<div class='sec-hdr'>Engagement Overview</div>", unsafe_allow_html=True)
        with st.spinner(""):
            eng_df = analytics.get_engagement_breakdown_df(session, filter_start, filter_end)
        if not eng_df.empty:
            chart = (
                alt.Chart(eng_df)
                .mark_arc(innerRadius=52)
                .encode(
                    theta=alt.Theta("Count:Q"),
                    color=alt.Color("Event Type:N", scale=alt.Scale(scheme="tableau10")),
                    tooltip=["Event Type:N", alt.Tooltip("Count:Q", format=",")],
                )
            )
            st.altair_chart(_base(chart, height=240), use_container_width=True)
        else:
            st.info("No engagement data for this period.")
        st.markdown("</div>", unsafe_allow_html=True)

    with col_trend:
        st.markdown(
            "<div style='background:#fff;border-radius:14px;border:1px solid #e2e8f0;"
            "padding:18px 18px 8px 18px;box-shadow:0 2px 8px rgba(0,0,0,0.05);'>",
            unsafe_allow_html=True,
        )
        st.markdown("<div class='sec-hdr'>SFMC Event Trend</div>", unsafe_allow_html=True)
        with st.spinner(""):
            trend_df = analytics.get_engagement_trend_df(session, filter_start, filter_end)
        if not trend_df.empty:
            trend_df["Event Date"] = pd.to_datetime(trend_df["Event Date"])
            chart = (
                alt.Chart(trend_df)
                .mark_line(point=True)
                .encode(
                    x=alt.X("Event Date:T", title=""),
                    y=alt.Y("Count:Q", title="Events"),
                    color=alt.Color("Event Type:N", scale=alt.Scale(scheme="tableau10")),
                    tooltip=["Event Date:T", "Event Type:N", alt.Tooltip("Count:Q", format=",")],
                )
            )
            st.altair_chart(_base(chart, height=240), use_container_width=True)
        else:
            st.info("No engagement trend data for this period.")
        st.markdown("</div>", unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    # ── Row 3: Intake volume ───────────────────────────────────────────────
    st.markdown(
        "<div style='background:#fff;border-radius:14px;border:1px solid #e2e8f0;"
        "padding:18px 18px 8px 18px;box-shadow:0 2px 8px rgba(0,0,0,0.05);'>",
        unsafe_allow_html=True,
    )
    st.markdown("<div class='sec-hdr'>Daily Lead Intake Volume</div>", unsafe_allow_html=True)
    with st.spinner(""):
        intake_df = analytics.get_intake_trend_df(session, filter_start, filter_end)
    if not intake_df.empty:
        intake_df["Intake Date"] = pd.to_datetime(intake_df["Intake Date"])
        chart = (
            alt.Chart(intake_df)
            .mark_area(
                line={"color": BLUE_MAIN, "strokeWidth": 2},
                color=BLUE_MAIN,
                opacity=0.12,
            )
            .encode(
                x=alt.X("Intake Date:T", title=""),
                y=alt.Y("Lead Count:Q", title="Leads"),
                tooltip=["Intake Date:T", alt.Tooltip("Lead Count:Q", format=",")],
            )
        )
        st.altair_chart(_base(chart, height=180), use_container_width=True)
    else:
        st.info("No intake trend data for this period.")
    st.markdown("</div>", unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════════════════════
# TAB 2 — Chat Assistant
# ══════════════════════════════════════════════════════════════════════════════

CHAT_CATEGORIES = {
    "Funnel & Drop Analysis": [
        "Give me a full funnel summary — leads to prospects to engagement.",
        "Show me the funnel chart for all time.",
        "Why is there a volume drop? What are the top rejection reasons?",
        "Show me the lead-to-prospect conversion rate.",
    ],
    "Rejections & DQ": [
        "What are the top intake rejection reasons?",
        "Show me SFMC suppression counts by stage.",
        "Are there any dedup rejections in the silver layer?",
        "Show me all rejection categories with counts.",
    ],
    "SFMC Journey & Events": [
        "How is the Prospect Journey performing?",
        "Which stage has the highest suppression?",
        "Show me SFMC engagement breakdown by event type.",
        "Are emails going out on schedule?",
    ],
    "Prospect Trace": [
        "Trace prospect FIP001234.",
        "How do I find a prospect by email?",
    ],
    "AI & Scores": [
        "What are the AI conversion signals?",
        "Show me open rate and click rate trends.",
    ],
    "Trends": [
        "Show me monthly intake trend for 2026.",
        "What is the SFMC engagement trend over time?",
    ],
    "Observability": [
        "Are there any data quality issues today?",
        "Show me the pipeline health summary.",
    ],
}

with tab_chat:
    left_col, right_col = st.columns([1.7, 4])

    # ── Left panel: categorized prompts ───────────────────────────────────
    with left_col:
        st.markdown(
            "<div style='background:#ffffff;border-radius:14px;border:1px solid #e2e8f0;"
            "box-shadow:0 2px 8px rgba(0,0,0,0.05);padding:14px 12px;"
            "overflow-y:auto;max-height:660px;'>",
            unsafe_allow_html=True,
        )
        clicked_cat_prompt = None
        for cat_name, prompts in CHAT_CATEGORIES.items():
            st.markdown(
                f"<div class='cat-title'>{cat_name}</div>"
                f"<div class='cat-rule'></div>",
                unsafe_allow_html=True,
            )
            for i, prompt in enumerate(prompts):
                safe_key = f"cat_{cat_name[:6].replace(' ','_')}_{i}"
                display  = prompt if len(prompt) <= 50 else prompt[:48] + "…"
                if st.button(display, key=safe_key, use_container_width=True):
                    clicked_cat_prompt = prompt
        st.markdown("</div>", unsafe_allow_html=True)

    # ── Right panel: chat area ─────────────────────────────────────────────
    with right_col:

        # ── Chat history ───────────────────────────────────────────────────
        # User messages: HTML bubbles (simple text, no markdown needed)
        # AI messages: st.markdown() — renders tables, headers, bold correctly
        if not st.session_state.chat_history:
            st.markdown(
                "<div style='text-align:center;padding:48px 20px 20px;'>"
                "<div style='font-size:48px;'>🔍</div>"
                "<div style='font-size:22px;font-weight:800;color:#1a1f36;margin:14px 0 8px;'>"
                "FIPSAR Intelligence</div>"
                "<div style='font-size:13px;color:#64748b;'>"
                "Ask about leads, journeys, engagement, or data quality.</div>"
                "</div>",
                unsafe_allow_html=True,
            )
        else:
            for turn in st.session_state.chat_history:
                if turn["role"] == "user":
                    # Escape only for safe HTML embedding — no markdown escaping
                    safe = (
                        turn["content"]
                        .replace("&", "&amp;")
                        .replace("<", "&lt;")
                        .replace(">", "&gt;")
                        .replace("\n", "<br>")
                    )
                    st.markdown(
                        f"<div style='text-align:right;margin:6px 0 12px;'>"
                        f"<div style='display:inline-block;"
                        f"background:linear-gradient(135deg,#1a56db,#0f3fa8);"
                        f"color:#fff;border-radius:16px 16px 4px 16px;"
                        f"padding:10px 16px;max-width:72%;font-size:13px;"
                        f"line-height:1.5;text-align:left;word-wrap:break-word;'>"
                        f"{safe}</div></div>",
                        unsafe_allow_html=True,
                    )
                else:
                    # AI label badge
                    st.markdown(
                        "<div style='margin:4px 0 4px;'>"
                        "<span style='font-size:10px;font-weight:800;color:#1a56db;"
                        "text-transform:uppercase;letter-spacing:0.8px;'>"
                        "🔍 FIPSAR Intelligence</span></div>",
                        unsafe_allow_html=True,
                    )
                    # Strip [CHART:xxx] tags before rendering — the LLM sometimes
                    # wraps them across two lines (e.g. "[CHART:bar-v\n]"), so
                    # we use DOTALL to match across the newline, then remove them.
                    clean = re.sub(
                        r"\[CHART\s*:\s*(?:bar-h|bar-v|line|donut|none|auto)\s*\]",
                        "",
                        turn["content"],
                        flags=re.IGNORECASE | re.DOTALL,
                    )
                    # Render response — markdown tables, headers, bold all work here
                    st.markdown(clean)
                    # Auto-render chart if response contains tabular/numeric data
                    _render_response_charts(turn["content"])
                    # Thin divider between turns
                    st.markdown(
                        "<div style='height:1px;background:#e8ecf4;margin:10px 0 18px;'></div>",
                        unsafe_allow_html=True,
                    )

        # Quick action pills — only when chat is empty
        clicked_qa = None
        if not st.session_state.chat_history:
            quick_actions = [
                "Give me a full funnel summary",
                "Show me rejection reasons",
                "SFMC engagement by journey",
                "Conversion probability chart",
                "Monthly intake trend 2026",
                "Data quality issues",
            ]
            qa_cols = st.columns(3)
            for i, qa in enumerate(quick_actions):
                with qa_cols[i % 3]:
                    if st.button(qa, key=f"qa_{i}", use_container_width=True):
                        clicked_qa = qa

        # Chat input form
        with st.form("chat_form", clear_on_submit=True):
            col_input, col_send = st.columns([8, 1])
            with col_input:
                user_input = st.text_input(
                    "Message",
                    placeholder="Ask about leads, journeys, engagement, or data quality...",
                    label_visibility="collapsed",
                )
            with col_send:
                send_btn = st.form_submit_button("➤", use_container_width=True)

        # Determine which message to send
        message_to_send = (
            clicked_cat_prompt
            or clicked_qa
            or (user_input.strip() if (send_btn and user_input) else None)
        )

        if message_to_send:
            st.session_state.chat_history.append({"role": "user", "content": message_to_send})
            with st.spinner("Analyzing..."):
                prior    = st.session_state.chat_history[:-1]
                response = agent.chat(
                    session=session,
                    model=selected_model,
                    user_message=message_to_send,
                    conversation_history=prior,
                )
            st.session_state.chat_history.append({"role": "assistant", "content": response})
            _rerun()


# ══════════════════════════════════════════════════════════════════════════════
# TAB 3 — Journey Intelligence
# ══════════════════════════════════════════════════════════════════════════════

with tab_journey:
    st.markdown(
        "<div style='background:#ffffff;border-radius:14px;padding:16px 22px;"
        "margin-bottom:18px;box-shadow:0 1px 4px rgba(0,0,0,0.06);border:1px solid #e2e8f0;'>"
        "<span style='font-size:15px;font-weight:800;color:#1a1f36;'>"
        "Prospect Journey — Stage Intelligence</span><br>"
        "<span style='font-size:12px;color:#64748b;'>"
        "One journey &nbsp;·&nbsp; 9 stages &nbsp;·&nbsp; 4 phases. &nbsp;"
        "Lower stage counts = timing (not attrition). &nbsp;Suppression = sole permanent exit."
        "</span></div>",
        unsafe_allow_html=True,
    )

    # ── Stage reach ───────────────────────────────────────────────────────
    with st.spinner("Loading stage reach data..."):
        stage_df = analytics.get_stage_reach_df(session)

    if not stage_df.empty:
        col_chart, col_table = st.columns([3, 2])

        with col_chart:
            st.markdown(
                "<div style='background:#fff;border-radius:14px;border:1px solid #e2e8f0;"
                "padding:18px 18px 8px 18px;box-shadow:0 2px 8px rgba(0,0,0,0.05);'>",
                unsafe_allow_html=True,
            )
            st.markdown("<div class='sec-hdr'>Email Reach by Stage</div>", unsafe_allow_html=True)
            melted = stage_df[["Stage", "Prospects Sent", "Emails to be Sent"]].melt(
                id_vars="Stage", var_name="Category", value_name="Count"
            )
            color_scale = alt.Scale(
                domain=["Prospects Sent", "Emails to be Sent"],
                range=[BLUE_MAIN, BLUE_MID],
            )
            chart = (
                alt.Chart(melted)
                .mark_bar()
                .encode(
                    x=alt.X("Stage:N", sort=None, title=""),
                    y=alt.Y("Count:Q", title="Prospects", stack="zero"),
                    color=alt.Color("Category:N", scale=color_scale),
                    tooltip=["Stage:N", "Category:N", alt.Tooltip("Count:Q", format=",")],
                )
            )
            st.altair_chart(_base(chart, height=320), use_container_width=True)
            st.markdown("</div>", unsafe_allow_html=True)

        with col_table:
            st.markdown(
                "<div style='background:#fff;border-radius:14px;border:1px solid #e2e8f0;"
                "padding:18px 18px 8px 18px;box-shadow:0 2px 8px rgba(0,0,0,0.05);'>",
                unsafe_allow_html=True,
            )
            st.markdown("<div class='sec-hdr'>Stage Summary</div>", unsafe_allow_html=True)
            disp = stage_df[["Stage", "Stage Name", "Prospects Sent", "% Reached"]].copy()
            disp["Prospects Sent"] = disp["Prospects Sent"].apply(lambda x: f"{x:,}")
            disp["% Reached"]      = disp["% Reached"].apply(lambda x: f"{x:.1f}%")
            st.dataframe(disp.reset_index(drop=True), use_container_width=True, height=320)
            st.markdown("</div>", unsafe_allow_html=True)
    else:
        st.info("No stage reach data available.")

    st.markdown("<br>", unsafe_allow_html=True)

    # ── Suppression by stage ──────────────────────────────────────────────
    st.markdown(
        "<div style='background:#fff;border-radius:14px;border:1px solid #e2e8f0;"
        "padding:18px 18px 8px 18px;box-shadow:0 2px 8px rgba(0,0,0,0.05);margin-bottom:4px;'>",
        unsafe_allow_html=True,
    )
    st.markdown(
        "<div class='sec-hdr'>Suppression by Stage (Permanent Journey Exits)</div>",
        unsafe_allow_html=True,
    )
    with st.spinner("Loading suppression data..."):
        supp_df = analytics.get_suppression_by_stage_df(session)

    if not supp_df.empty:
        col_supp, col_supp_kpi = st.columns([2, 1])

        with col_supp:
            chart = (
                alt.Chart(supp_df)
                .mark_bar()
                .encode(
                    x=alt.X("Suppressed Count:Q", title=""),
                    y=alt.Y("Suppression Stage:N", sort="-x", title=""),
                    color=alt.Color(
                        "Suppressed Count:Q",
                        scale=alt.Scale(range=[BLUE_LIGHT, RED_ACCENT]),
                        legend=None,
                    ),
                    tooltip=["Suppression Stage:N", alt.Tooltip("Suppressed Count:Q", format=",")],
                )
            )
            text = chart.mark_text(align="left", dx=4, color=CHART_TEXT, fontSize=11).encode(
                text=alt.Text("Suppressed Count:Q", format=",")
            )
            st.altair_chart(_base(chart + text, height=280), use_container_width=True)

        with col_supp_kpi:
            total_supp = int(supp_df["Suppressed Count"].sum())
            top_stage  = supp_df.sort_values("Suppressed Count", ascending=False).iloc[0]
            pct_top    = round(top_stage["Suppressed Count"] / total_supp * 100, 1) if total_supp > 0 else 0

            st.markdown(
                f"<div class='kpi-card kpi-red' style='margin-bottom:12px;'>"
                f"<div class='kpi-label'>Total Suppressed</div>"
                f"<div class='kpi-value'>{total_supp:,}</div>"
                f"<div class='kpi-sub'>All journey stages</div>"
                f"</div>",
                unsafe_allow_html=True,
            )
            st.markdown(
                f"<div class='kpi-card kpi-orange' style='margin-bottom:12px;'>"
                f"<div class='kpi-label'>Highest Exit Point</div>"
                f"<div class='kpi-value' style='font-size:15px;'>{top_stage['Suppression Stage']}</div>"
                f"<div class='kpi-sub'>{int(top_stage['Suppressed Count']):,} prospects ({pct_top}%)</div>"
                f"</div>",
                unsafe_allow_html=True,
            )
            st.markdown(
                "<div style='background:#f8fafc;border:1px solid #e2e8f0;border-radius:10px;"
                "padding:14px;font-size:12px;color:#374151;'>"
                "<b style='color:#1a56db;'>Suppression Rule</b><br><br>"
                "SUPPRESSION_FLAG = TRUE means the prospect's journey was permanently ended. "
                "No further emails will be sent.<br><br>"
                "NULL stage columns after the last TRUE stage are intentional cutoffs, not missing data."
                "</div>",
                unsafe_allow_html=True,
            )
    else:
        st.info("No suppression data available.")
    st.markdown("</div>", unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    # ── Journey pace ──────────────────────────────────────────────────────
    st.markdown(
        "<div style='background:#fff;border-radius:14px;border:1px solid #e2e8f0;"
        "padding:18px 18px 8px 18px;box-shadow:0 2px 8px rgba(0,0,0,0.05);margin-bottom:4px;'>",
        unsafe_allow_html=True,
    )
    st.markdown(
        "<div class='sec-hdr'>Journey Pace — Actual vs Expected Days</div>",
        unsafe_allow_html=True,
    )
    with st.spinner("Loading pace data..."):
        pace_df = analytics.get_pace_df(session)

    if not pace_df.empty:
        col_pace, col_pace_tbl = st.columns([2, 1])

        with col_pace:
            bars = (
                alt.Chart(pace_df)
                .mark_bar(color=BLUE_MAIN, opacity=0.85)
                .encode(
                    x=alt.X("Transition:N", sort=None, title=""),
                    y=alt.Y("Avg Actual Days:Q", title="Days"),
                    tooltip=["Transition:N", alt.Tooltip("Avg Actual Days:Q", format=".1f")],
                )
            )
            line = (
                alt.Chart(pace_df)
                .mark_line(color=AMBER, strokeWidth=2, strokeDash=[4, 3])
                .encode(
                    x=alt.X("Transition:N", sort=None),
                    y=alt.Y("Expected Days:Q"),
                    tooltip=["Transition:N", "Expected Days:Q"],
                )
            )
            points = (
                alt.Chart(pace_df)
                .mark_point(color=AMBER, size=60, filled=True)
                .encode(
                    x=alt.X("Transition:N", sort=None),
                    y=alt.Y("Expected Days:Q"),
                )
            )
            st.altair_chart(_base(bars + line + points, height=260), use_container_width=True)

        with col_pace_tbl:
            pace_disp = pace_df.copy()
            pace_disp["Delta"]  = (pace_disp["Avg Actual Days"] - pace_disp["Expected Days"]).round(1)
            pace_disp["Status"] = pace_disp["Delta"].apply(
                lambda d: "On Time" if abs(d) <= 1 else ("Slow" if d > 1 else "Fast")
            )
            st.dataframe(
                pace_disp[["Transition", "Expected Days", "Avg Actual Days", "Status"]].reset_index(drop=True),
                use_container_width=True,
                height=260,
            )
    else:
        st.info("No pace data available (timestamp columns may not be populated).")
    st.markdown("</div>", unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    # ── Prospect trace ────────────────────────────────────────────────────
    st.markdown(
        "<div style='background:#fff;border-radius:14px;border:1px solid #e2e8f0;"
        "padding:20px 22px;box-shadow:0 2px 8px rgba(0,0,0,0.05);'>",
        unsafe_allow_html=True,
    )
    st.markdown("<div class='sec-hdr'>Journey Detail Query</div>", unsafe_allow_html=True)
    st.caption("Trace a specific prospect end-to-end, or use the Chat tab for AI-powered analysis.")

    with st.form("journey_lookup_form"):
        lookup_id  = st.text_input(
            "Prospect ID or Email",
            placeholder="e.g. FIP001234  or  user@example.com",
        )
        lookup_btn = st.form_submit_button("Trace Prospect")

    if lookup_btn and lookup_id.strip():
        with st.spinner(f"Tracing {lookup_id}..."):
            result = T.trace_prospect(session, lookup_id.strip())
        st.markdown(result)
    st.markdown("</div>", unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════════════════════
# TAB 4 — Recon Analytics
# ══════════════════════════════════════════════════════════════════════════════

with tab_recon:

    # ── Filters (collapsible expander — same pattern as Analytics tab) ──────
    with st.expander("🔎  Filters", expanded=True):
        fc1, fc2, fc3, fc4, fc5 = st.columns([2, 2, 2, 2, 1])
        with fc1:
            rc_start = st.date_input("Start Date", value=date(2020, 1, 1), key="rc_start")
        with fc2:
            rc_end   = st.date_input("End Date", value=date.today(), key="rc_end")
        with fc3:
            rc_status = st.multiselect(
                "Journey Status",
                options=[
                    "In Progress",
                    "Suppressed",
                    "Journey Completed",
                    "Awaiting Journey Start",
                    "Not Exported to SFMC",
                ],
                default=[],
                key="rc_status",
            )
        with fc4:
            rc_channel = st.selectbox(
                "Channel",
                ["All Channels", "Email", "Web", "Phone", "Referral", "Campaign"],
                key="rc_channel",
            )
        with fc5:
            st.markdown("<div style='height:26px;'></div>", unsafe_allow_html=True)
            st.button("🔄 Apply", use_container_width=True, key="rc_apply")
    st.caption(
        "⚠ Date filter applies to all segments. "
        "Journey Status / Channel apply to Deduped → Stage 09 only."
    )

    # ── Read filter values ──────────────────────────────────────────────────
    rc_start_str = st.session_state.get("rc_start", date(2020, 1, 1)).isoformat()
    rc_end_str   = st.session_state.get("rc_end",   date.today()).isoformat()
    rc_status_v  = st.session_state.get("rc_status", [])
    rc_channel_v = st.session_state.get("rc_channel", "All Channels")

    # ── KPI Bar ────────────────────────────────────────────────────────────
    with st.spinner("Loading KPIs…"):
        rc_kpis = analytics.get_recon_kpis(session, rc_start_str, rc_end_str)

    kpi_defs = [
        ("Total Leads",         f"{rc_kpis['total_leads']:,}",     "Intake received",    "kpi-blue"),
        ("Invalid Leads",       f"{rc_kpis['invalid_leads']:,}",   "Intake rejections",  "kpi-red"),
        ("Valid Prospects",     f"{rc_kpis['valid_prospects']:,}", "Mastered prospects", "kpi-purple"),
        ("Completed Journeys",  f"{rc_kpis['j_completed']:,}",    "All 9 stages sent",  "kpi-green"),
        ("In Progress",         f"{rc_kpis['j_in_progress']:,}",  "Active in journey",  "kpi-teal"),
        ("Dropped / Suppressed",f"{rc_kpis['j_dropped']:,}",      "Permanent exits",    "kpi-orange"),
    ]
    kpi_cols = st.columns(6)
    for col_kpi, (lbl, val, sub, cls) in zip(kpi_cols, kpi_defs):
        with col_kpi:
            st.markdown(
                f"<div class='kpi-card {cls}'>"
                f"<div class='kpi-label'>{lbl}</div>"
                f"<div class='kpi-value'>{val}</div>"
                f"<div class='kpi-sub'>{sub}</div>"
                f"</div>",
                unsafe_allow_html=True,
            )

    st.markdown("<br>", unsafe_allow_html=True)

    # ── Load funnel data ───────────────────────────────────────────────────
    with st.spinner("Loading funnel data…"):
        funnel_df = analytics.get_recon_funnel_df(
            session, rc_start_str, rc_end_str,
            journey_status=rc_status_v if rc_status_v else None,
            channel=rc_channel_v,
        )

    if funnel_df.empty:
        st.info("No data for the selected filters.")
    else:
        # ── Chart A + Chart B side by side ──────────────────────────────
        col_a, col_b = st.columns(2)

        # ── Chart A: Funnel Bar Chart ────────────────────────────────────
        with col_a:
            st.markdown(
                "<div style='background:#fff;border-radius:14px;border:1px solid #e2e8f0;"
                "padding:18px 18px 12px 18px;box-shadow:0 2px 8px rgba(0,0,0,0.05);'>"
                "<div class='sec-hdr'>Prospect Journey Snapshot</div>"
                "</div>",
                unsafe_allow_html=True,
            )

            type_colors = {
                "Pipeline": BLUE_MAIN,
                "SFMC":     TEAL,
                "Stage":    PURPLE,
            }

            # Explicit Y-axis order: ascending Sort (0→12) = first item at top in Altair
            # Total Leads (Sort=0) → top, Stage 09 (Sort=12) → bottom
            y_sort_order = funnel_df.sort_values("Sort", ascending=True)["Segment"].tolist()

            chart_a = (
                alt.Chart(funnel_df)
                .mark_bar(cornerRadiusTopRight=4, cornerRadiusBottomRight=4)
                .encode(
                    x=alt.X(
                        "Count:Q",
                        title="Prospects",
                        axis=alt.Axis(format=",d", grid=True),
                    ),
                    y=alt.Y(
                        "Segment:N",
                        sort=y_sort_order,
                        title="",
                        axis=alt.Axis(labelFontSize=11, labelLimit=220),
                    ),
                    color=alt.Color(
                        "Type:N",
                        scale=alt.Scale(
                            domain=["Pipeline", "SFMC", "Stage"],
                            range=[BLUE_MAIN, TEAL, PURPLE],
                        ),
                        legend=alt.Legend(title="Segment Type", orient="bottom"),
                    ),
                    tooltip=[
                        alt.Tooltip("Segment:N",    title="Stage"),
                        alt.Tooltip("Count:Q",      title="Count",           format=","),
                        alt.Tooltip("Drop:Q",       title="Drop to Next",    format=","),
                        alt.Tooltip("Drop %:Q",     title="Drop %",          format=".1f"),
                        alt.Tooltip("% of Total:Q", title="% of All Leads",  format=".1f"),
                        alt.Tooltip("Suppressed:Q", title="Suppressed After",format=","),
                        alt.Tooltip("Awaiting:Q",   title="In Progress",     format=","),
                    ],
                )
            )

            labels_a = chart_a.mark_text(
                align="left", dx=5, color=CHART_TEXT, fontSize=10, fontWeight=600,
            ).encode(
                text=alt.Text("Count:Q", format=","),
            )

            st.altair_chart(
                _base(chart_a + labels_a, height=480),
                use_container_width=True,
            )

        # ── Chart B: Detailed Pivot Table ────────────────────────────────
        with col_b:
            st.markdown(
                "<div style='background:#fff;border-radius:14px;border:1px solid #e2e8f0;"
                "padding:18px 18px 12px 18px;box-shadow:0 2px 8px rgba(0,0,0,0.05);'>"
                "<div class='sec-hdr'>Detailed Funnel Pivot</div>"
                "</div>",
                unsafe_allow_html=True,
            )

            pivot_display = funnel_df[[
                "Segment", "Count", "Drop", "Drop %", "Suppressed", "Awaiting", "% of Total"
            ]].copy()
            pivot_display["Count"]      = pivot_display["Count"].apply(lambda x: f"{x:,}")
            pivot_display["Drop"]       = pivot_display["Drop"].apply(
                lambda x: f"▼ {x:,}" if x > 0 else "—"
            )
            pivot_display["Drop %"]     = pivot_display["Drop %"].apply(
                lambda x: f"{x:.1f}%" if x > 0 else "—"
            )
            pivot_display["Suppressed"] = pivot_display["Suppressed"].apply(
                lambda x: f"{x:,}" if x > 0 else "—"
            )
            pivot_display["Awaiting"]   = pivot_display["Awaiting"].apply(
                lambda x: f"{x:,}" if x > 0 else "—"
            )
            pivot_display["% of Total"] = pivot_display["% of Total"].apply(
                lambda x: f"{x:.1f}%"
            )
            pivot_display = pivot_display.rename(columns={
                "Drop":       "Lost After",
                "Drop %":     "Lost %",
                "% of Total": "% Leads",
                "Suppressed": "Supp.",
                "Awaiting":   "Awaiting",
            })

            st.dataframe(
                pivot_display.reset_index(drop=True),
                use_container_width=True,
                height=480,
            )

    # ── Drill-down section ─────────────────────────────────────────────────
    st.markdown("---")
    st.markdown(
        "<span style='font-size:13px;font-weight:700;color:#1a1f36;'>"
        "🔍 Drill Down to Prospects</span>",
        unsafe_allow_html=True,
    )
    st.caption("Select a funnel segment to view individual prospect records.")

    bucket_labels = {
        "total_leads":     "Total Leads",
        "valid_prospects": "Valid Prospects",
        "deduped":         "Deduped",
        "sfmc_prospects":  "SFMC Prospects",
        "s01": "Stage 01 — Welcome Email",
        "s02": "Stage 02 — Education Email",
        "s03": "Stage 03 — Education Email 1",
        "s04": "Stage 04 — Education Email 2",
        "s05": "Stage 05 — Prospect Story Email",
        "s06": "Stage 06 — Conversion Email",
        "s07": "Stage 07 — Reminder Email",
        "s08": "Stage 08 — Re-engagement Email",
        "s09": "Stage 09 — Final Reminder Email",
        "invalid_leads":   "Invalid Leads (Rejections)",
        "suppressed":      "Suppressed Prospects",
        "in_progress":     "In Progress",
        "completed":       "Journey Completed",
    }

    # Selectbox outside form so its value is always captured in session state
    # immediately on change — avoids older Streamlit form capture bug
    drill_col1, drill_col2 = st.columns([5, 1])
    with drill_col1:
        st.selectbox(
            "Select Segment",
            options=list(bucket_labels.keys()),
            format_func=lambda k: bucket_labels[k],
            key="rc_drill_select",
        )
    with drill_col2:
        st.markdown("<div style='height:26px;'></div>", unsafe_allow_html=True)
        drill_btn = st.button("🔍 Load", use_container_width=True, key="rc_drill_btn")

    if drill_btn:
        # Read current selectbox value directly from session state — always correct
        st.session_state["rc_drill_key"] = st.session_state.get("rc_drill_select", "valid_prospects")
        st.session_state["rc_drill_loaded"] = True

    if st.session_state.get("rc_drill_loaded", False):
        drill_key = st.session_state.get("rc_drill_key", "valid_prospects")
        with st.spinner(f"Loading {bucket_labels.get(drill_key, drill_key)}…"):
            drill_df = analytics.get_drill_prospects(
                session, drill_key, rc_start_str, rc_end_str, limit=200
            )

        if not drill_df.empty:
            rec_count = len(drill_df)
            st.caption(
                f"Showing up to 200 records · {rec_count} loaded for "
                f"**{bucket_labels.get(drill_key)}**"
            )
            st.dataframe(
                drill_df.reset_index(drop=True),
                use_container_width=True,
                height=300,
            )
        else:
            st.info("No records found for the selected segment and date range.")

    # ── Stage-level suppression breakdown ──────────────────────────────────
    st.markdown("<br>", unsafe_allow_html=True)

    with st.expander("📊  Stage-by-Stage Suppression Breakdown", expanded=False):
        st.caption(
            "For each stage, shows how many prospects were suppressed after receiving that email "
            "vs. how many are awaiting the next send interval."
        )
        if not funnel_df.empty:
            stage_rows = funnel_df[funnel_df["Type"] == "Stage"].copy()
            if not stage_rows.empty:
                supp_chart_df = stage_rows[["Segment", "Suppressed", "Awaiting"]].copy()
                supp_melted = supp_chart_df.melt(
                    id_vars="Segment", var_name="Category", value_name="Count"
                )
                supp_color = alt.Scale(
                    domain=["Suppressed", "Awaiting"],
                    range=[RED_ACCENT, AMBER],
                )
                supp_chart = (
                    alt.Chart(supp_melted)
                    .mark_bar()
                    .encode(
                        x=alt.X("Segment:N", sort=None, title="",
                                axis=alt.Axis(labelAngle=-30, labelFontSize=10)),
                        y=alt.Y("Count:Q", title="Prospects", stack=True),
                        color=alt.Color("Category:N", scale=supp_color),
                        tooltip=[
                            "Segment:N",
                            "Category:N",
                            alt.Tooltip("Count:Q", format=","),
                        ],
                    )
                )
                st.altair_chart(_base(supp_chart, height=240), use_container_width=True)

                stage_rows_disp = stage_rows[
                    ["Segment", "Count", "Drop", "Suppressed", "Awaiting", "% of Total"]
                ].copy()
                stage_rows_disp["Count"]      = stage_rows_disp["Count"].apply(lambda x: f"{x:,}")
                stage_rows_disp["Drop"]       = stage_rows_disp["Drop"].apply(lambda x: f"{x:,}")
                stage_rows_disp["Suppressed"] = stage_rows_disp["Suppressed"].apply(lambda x: f"{x:,}")
                stage_rows_disp["Awaiting"]   = stage_rows_disp["Awaiting"].apply(lambda x: f"{x:,}")
                stage_rows_disp["% of Total"] = stage_rows_disp["% of Total"].apply(
                    lambda x: f"{x:.1f}%"
                )
                st.dataframe(
                    stage_rows_disp.reset_index(drop=True),
                    use_container_width=True,
                    height=280,
                )
        else:
            st.info("No stage data available.")

    # ── Invalid Lead Rejection Breakdown ───────────────────────────────────
    with st.expander("❌  Invalid Lead Rejection Analysis", expanded=False):
        st.caption(
            "Breakdown of why leads failed mastering — "
            "NULL fields, invalid dates, and duplicate records."
        )
        with st.spinner(""):
            rej_detail_df = analytics.get_rejection_trend_df(
                session, rc_start_str, rc_end_str, category="intake"
            )
        if not rej_detail_df.empty:
            rc_rej_cols = st.columns([2, 1])
            with rc_rej_cols[0]:
                rej_chart = (
                    alt.Chart(rej_detail_df)
                    .mark_bar(color=RED_ACCENT)
                    .encode(
                        x=alt.X("Count:Q", title="Rejections"),
                        y=alt.Y("Rejection Reason:N", sort="-x", title=""),
                        tooltip=["Rejection Reason:N", alt.Tooltip("Count:Q", format=",")],
                    )
                )
                rej_text = rej_chart.mark_text(
                    align="left", dx=4, color=CHART_TEXT, fontSize=11
                ).encode(text=alt.Text("Count:Q", format=","))
                st.altair_chart(_base(rej_chart + rej_text, height=200), use_container_width=True)
            with rc_rej_cols[1]:
                rej_disp = rej_detail_df.copy()
                rej_disp["Count"] = rej_disp["Count"].apply(lambda x: f"{x:,}")
                st.dataframe(rej_disp.reset_index(drop=True), use_container_width=True, height=200)
        else:
            st.info("No rejection data for this period.")
