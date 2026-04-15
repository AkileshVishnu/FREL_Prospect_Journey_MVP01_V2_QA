"""
server.py
---------
FastAPI backend for FIPSAR Intelligence.
Exposes all agent, analytics, voice and email capabilities as REST endpoints
consumed by the React frontend.

Run with:
    python server.py
  or
    uvicorn server:app --host 0.0.0.0 --port 8000 --reload
"""

from __future__ import annotations

import io
import logging
import re
from datetime import date, datetime
from typing import Any, Optional

import uvicorn
from fastapi import FastAPI, File, HTTPException, Query, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

# ── Backend imports ────────────────────────────────────────────────────────
from snowflake_connector import test_connection
from agent import chat, reset_session
from frel_agent import frel_chat, reset_frel_session
import chart_store
from voice_assistant import transcribe_audio, text_to_speech
from config import email_config
from email_sender import test_email_connection, send_email
from analytics_dashboard import (
    _fetch_funnel_kpis,
    _fetch_email_kpis,
    _fetch_conversion_segments,
    _fetch_prospect_segments,
    _fetch_daily_trend,
    _fetch_filter_options,
    _chart_lead_funnel,
    _chart_email_comparison,
    _chart_conversion_probability,
    _chart_prospect_segments,
    _chart_daily_trend,
    _journey_code,
)

logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger(__name__)

# ── App ────────────────────────────────────────────────────────────────────
app = FastAPI(title="FIPSAR Intelligence API", version="2.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ══════════════════════════════════════════════════════════════════════════════
# HEALTH
# ══════════════════════════════════════════════════════════════════════════════

@app.get("/api/health")
async def health():
    return {"status": "ok", "timestamp": datetime.now().isoformat()}


# ══════════════════════════════════════════════════════════════════════════════
# STATUS
# ══════════════════════════════════════════════════════════════════════════════

@app.get("/api/status/snowflake")
async def snowflake_status():
    ok = test_connection()
    return {"connected": ok}


@app.get("/api/status/email")
async def email_status():
    return {
        "configured": email_config.is_configured,
        "to_address": email_config.to_address,
    }


@app.post("/api/email/test-smtp")
async def test_smtp():
    result = test_email_connection()
    return result


@app.post("/api/email/send-test")
async def send_test_email():
    result = send_email(
        subject="FIPSAR Intelligence — Email Test",
        report_markdown=(
            "## Connection Test\n\nThis is a test email from FIPSAR Intelligence.\n\n"
            "Your email configuration is working correctly.\n\n"
            f"- **SMTP Host:** {email_config.smtp_host}\n"
            f"- **To:** {email_config.to_address}"
        ),
    )
    return result


# ══════════════════════════════════════════════════════════════════════════════
# ANALYTICS
# ══════════════════════════════════════════════════════════════════════════════

@app.get("/api/analytics/filters")
async def get_filters():
    """Available channel + journey options for filter dropdowns."""
    return _fetch_filter_options()


@app.get("/api/analytics/kpis")
async def get_kpis(
    start_date: str = Query(...),
    end_date: str = Query(...),
    channel: str = Query("All"),
    journey: str = Query("All"),
):
    """All 7 KPI values for the dashboard strip."""
    s = date.fromisoformat(start_date)
    e = date.fromisoformat(end_date)
    funnel = _fetch_funnel_kpis(s, e, channel)
    email_kpis = _fetch_email_kpis(s, e, journey)
    conv_rate = (
        round(funnel["prospects"] / funnel["leads"] * 100, 1)
        if funnel["leads"] > 0 else 0.0
    )
    return {
        "leads": funnel["leads"],
        "prospects": funnel["prospects"],
        "invalid": funnel["invalid"],
        "sent": email_kpis["sent"],
        "opened": email_kpis["opened"],
        "clicked": email_kpis["clicked"],
        "unsubscribed": email_kpis["unsubscribed"],
        "conversion_rate": conv_rate,
    }


@app.get("/api/analytics/charts")
async def get_charts(
    start_date: str = Query(...),
    end_date: str = Query(...),
    channel: str = Query("All"),
    journey: str = Query("All"),
):
    """All chart data as Plotly JSON strings."""
    s = date.fromisoformat(start_date)
    e = date.fromisoformat(end_date)

    funnel    = _fetch_funnel_kpis(s, e, channel)
    email_k   = _fetch_email_kpis(s, e, journey)
    conv      = _fetch_conversion_segments(s, e, journey)
    segs      = _fetch_prospect_segments(s, e, journey)
    trend_df  = _fetch_daily_trend(s, e, channel)

    charts: dict[str, str | None] = {}

    charts["funnel"] = _chart_lead_funnel(
        funnel["leads"],
        funnel["invalid"],
        funnel["prospects"],
        funnel["dq_passed"],
        funnel["sfmc_load"],
    ).to_json()

    charts["email"] = _chart_email_comparison(
        email_k["sent"], email_k["opened"],
        email_k["clicked"], email_k["unsubscribed"]
    ).to_json()

    charts["conversion"] = (
        _chart_conversion_probability(conv).to_json()
        if sum(conv.values()) > 0 else None
    )

    charts["segments"] = (
        _chart_prospect_segments(segs).to_json()
        if sum(segs.values()) > 0 else None
    )

    trend_fig = _chart_daily_trend(trend_df)
    charts["trend"] = trend_fig.to_json() if trend_fig else None

    return charts


# ══════════════════════════════════════════════════════════════════════════════
# CHAT
# ══════════════════════════════════════════════════════════════════════════════

class ChatRequest(BaseModel):
    session_id: str
    message: str


@app.post("/api/chat")
async def chat_endpoint(req: ChatRequest):
    chart_store.set_session(req.session_id)
    response = chat(req.session_id, req.message)
    charts = chart_store.pop_all(req.session_id)
    return {
        "response": response,
        "charts": [fig.to_json() for fig in charts],
    }


@app.post("/api/chat/reset")
async def reset_chat_endpoint(session_id: str = Query(...)):
    reset_session(session_id)
    return {"status": "ok"}


# ══════════════════════════════════════════════════════════════════════════════
# FREL AGENT
# ══════════════════════════════════════════════════════════════════════════════

class FRELRequest(BaseModel):
    session_id: str
    message: str


@app.post("/api/frel")
async def frel_endpoint(req: FRELRequest):
    chart_store.set_session(req.session_id)
    response = frel_chat(req.session_id, req.message)
    charts = chart_store.pop_all(req.session_id)

    email_sent = (
        "✅ Email sent successfully" in response
        or "email sent" in response.lower()
    )
    email_meta: Optional[dict] = None
    if email_sent:
        sm = re.search(r"Subject:\s*['\"]?([^\n'\"]+)", response)
        tm = re.search(r"To:\s*([^\s\n]+@[^\s\n]+)", response)
        cm = re.search(r"(\d+)\s+chart", response)
        email_meta = {
            "to": tm.group(1) if tm else email_config.to_address,
            "subject": sm.group(1).strip() if sm else "FIPSAR Report",
            "sent_at": datetime.now().strftime("%B %d, %Y  %I:%M %p"),
            "charts_attached": int(cm.group(1)) if cm else 0,
        }

    return {
        "response": response,
        "charts": [fig.to_json() for fig in charts],
        "email_sent": email_sent,
        "email_meta": email_meta,
    }


@app.post("/api/frel/reset")
async def reset_frel_endpoint(session_id: str = Query(...)):
    reset_frel_session(session_id)
    return {"status": "ok"}


# ══════════════════════════════════════════════════════════════════════════════
# VOICE
# ══════════════════════════════════════════════════════════════════════════════

class VoiceChatRequest(BaseModel):
    session_id: str
    transcript: str
    voice: str = "alloy"
    speed: float = 1.0


@app.post("/api/voice/transcribe")
async def transcribe_endpoint(audio: UploadFile = File(...)):
    audio_bytes = await audio.read()
    transcript = transcribe_audio(audio_bytes)
    if not transcript:
        raise HTTPException(status_code=400, detail="Could not transcribe audio")
    return {"transcript": transcript}


@app.post("/api/voice/chat")
async def voice_chat_endpoint(req: VoiceChatRequest):
    """Run transcript through the agent and return response + audio."""
    chart_store.set_session(req.session_id)
    response = chat(req.session_id, req.transcript)
    charts = chart_store.pop_all(req.session_id)
    audio_bytes = text_to_speech(response, voice=req.voice, speed=req.speed)
    audio_b64 = None
    if audio_bytes:
        import base64
        audio_b64 = base64.b64encode(audio_bytes).decode()
    return {
        "response": response,
        "audio_b64": audio_b64,
        "charts": [fig.to_json() for fig in charts],
    }


# ══════════════════════════════════════════════════════════════════════════════
# Serve built React frontend (production)
# ══════════════════════════════════════════════════════════════════════════════

import os
_DIST = os.path.join(os.path.dirname(__file__), "frontend", "dist")
if os.path.isdir(_DIST):
    app.mount("/", StaticFiles(directory=_DIST, html=True), name="static")


# ══════════════════════════════════════════════════════════════════════════════
# Entry point
# ══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    uvicorn.run("server:app", host="0.0.0.0", port=8001, reload=True)
