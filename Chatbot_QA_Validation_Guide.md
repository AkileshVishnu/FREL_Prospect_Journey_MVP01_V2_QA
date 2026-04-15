# FIPSAR Prospect Journey Intelligence — Chatbot QA Validation Guide

> **Purpose:** Business-facing test script to validate that the AI chatbot is responding correctly across all major analytical areas.  
> **How to use:** Ask each question exactly as written in the **Question** field. Compare what the chatbot returns against the **Expected Response** criteria. Mark Pass / Fail in the last column.  
> **Date of guide:** 12 April 2026 — update `[TODAY]` placeholders if running on a different date.

---

## How to Score Each Question

| Rating | Criteria |
|--------|----------|
| ✅ Pass | Response hits all "Must Include" points and generates the expected chart(s) |
| ⚠️ Partial | Response answers the question but misses one chart or one key metric |
| ❌ Fail | Response is wrong, generic, or returns an error |

---

## PART 1 — Current Day Analysis (10 Questions)

> All questions in this section refer to **today: 12 April 2026**.  
> The chatbot must use live Snowflake data, not cached or historical numbers.

---

### Q1 — Today's Lead Intake Snapshot

**Question to ask:**
```
How many leads came in today and how many became valid prospects?
```

**What the agent should do:**
- Call `get_funnel_metrics(start_date="2026-04-12", end_date="2026-04-12")`
- Auto-generate `chart_funnel` or `chart_funnel_waterfall` for today

**Expected Response — Must Include:**
- A count of total leads ingested today from `STG_PROSPECT_INTAKE` (FILE_DATE = today)
- A count of valid prospects from `PHI_PROSPECT_MASTER` (FILE_DATE = today)
- A **lead-to-prospect conversion rate** shown as a percentage with 1 decimal (e.g., `94.3%`)
- A count of invalid/rejected leads (arithmetic: leads − prospects)
- At least one chart (funnel bar or waterfall)
- A contextual callout: e.g., *"That's a strong 94.3% conversion rate"* or a flag if the rate is low

**What it should NOT do:**
- Return `0` for both counts without explanation (means date filter is broken)
- Use yesterday's or all-time numbers

**Pass Criteria:** Conversion rate displayed + at least 1 chart rendered ✅

---

### Q2 — Today's Rejection Breakdown

**Question to ask:**
```
Show me why leads were rejected today. Give me a breakdown by reason.
```

**What the agent should do:**
- Call `get_rejection_analysis(start_date="2026-04-12", end_date="2026-04-12", rejection_category="intake")`
- Call `chart_rejections` for today's date range

**Expected Response — Must Include:**
- A markdown table with columns: `REJECTION_REASON | COUNT`
- Reasons should be from the intake category ONLY: `NULL_EMAIL`, `NULL_FIRST_NAME`, `NULL_LAST_NAME`, `NULL_PHONE_NUMBER`, `INVALID_FILE_DATE`
- `SUPPRESSED_PROSPECT` and `FATAL_ERROR` must NOT appear in this table (those are SFMC-layer, not intake)
- Donut/pie chart of rejection reasons
- A sentence explaining what each reason means in plain English

**What it should NOT do:**
- Mix SFMC suppression reasons (`SUPPRESSED_PROSPECT`) with intake rejection reasons
- Return an empty table without trying both `FILE_DATE` parsing formats

**Pass Criteria:** Table shows only intake rejection reasons + donut chart rendered ✅

---

### Q3 — SFMC Stage Emails Expected Today

**Question to ask:**
```
Which SFMC stage emails were expected to go out today (12 April 2026)? 
How many were actually sent, suppressed, or missed?
```

**What the agent should do:**
- Call `get_sfmc_stage_suppression(target_date="2026-04-12")`
- Call `chart_sfmc_stage_fishbone(target_date="2026-04-12")`

**Expected Response — Must Include:**
- A table with columns: `STAGE | EXPECTED | SENT | SUPPRESSED | NOT SENT`
- Only stages where emails were expected today (based on interval logic) should appear — stages with 0 expected may be omitted
- The fishbone/stacked bar chart showing Sent vs Suppressed vs Unsent per stage
- A callout if suppression count is > 0: *"X prospects were suppressed at Stage Y today"*

**What it should NOT do:**
- Return "no data" without trying the stage interval calculation
- Show all 9 stages with 0 values without explanation

**Pass Criteria:** At least one stage row returned + fishbone chart rendered ✅

---

### Q4 — Today's SFMC Suppression Check

**Question to ask:**
```
Were any prospects suppressed from receiving SFMC emails today?
Show me who was suppressed and why.
```

**What the agent should do:**
- Call `get_rejection_analysis(start_date="2026-04-12", end_date="2026-04-12", rejection_category="sfmc")`
- Optionally call `get_sfmc_stage_suppression(target_date="2026-04-12")` for stage detail

**Expected Response — Must Include:**
- Count of suppressed prospects today from `DQ_REJECTION_LOG` where `REJECTION_REASON = 'SUPPRESSED_PROSPECT'` and `TABLE_NAME = 'FACT_SFMC_ENGAGEMENT'`
- If suppressions exist: a breakdown of which stage they were suppressed at
- If zero suppressions: a clear confirmation — *"No suppressions recorded today"* (not just silence)
- Reference to `SUPPRESSION_FLAG` in `RAW_SFMC_PROSPECT_JOURNEY_DETAILS` as the source signal

**What it should NOT do:**
- Confuse intake rejections with SFMC suppressions
- Return generic "no data" without checking both `DQ_REJECTION_LOG` and `FACT_SFMC_ENGAGEMENT`

**Pass Criteria:** Explicit suppression count returned (0 or more) + source table cited ✅

---

### Q5 — Today's SFMC Engagement Events

**Question to ask:**
```
How many SFMC emails were sent, opened, and clicked today?
Show me a breakdown by journey.
```

**What the agent should do:**
- Call `get_sfmc_engagement_stats(start_date="2026-04-12", end_date="2026-04-12")`
- Call `chart_engagement(start_date="2026-04-12", end_date="2026-04-12")`
- If FACT table returns empty, fall back to raw SFMC tables automatically

**Expected Response — Must Include:**
- Counts for `SENT`, `OPEN`, `CLICK` at minimum
- Breakdown by journey (`J01_Welcome`, `J02_Nurture`, `J03_Conversion`, `J04_ReEngagement`)
- Open rate and click rate calculated inline (e.g., *"63.6% open rate on 280 sent"*)
- Grouped bar chart by journey and event type
- If FACT data is empty: must try raw tables (RAW_SFMC_SENT, etc.) and still return numbers

**What it should NOT do:**
- Say "no SFMC data available" without trying the raw table fallback
- Filter using `DATE_KEY → DIM_DATE` join (known broken — must use `DATE(EVENT_TIMESTAMP)`)

**Pass Criteria:** Event counts returned with journey breakdown + engagement chart rendered ✅

---

### Q6 — Today's Email KPI Rates

**Question to ask:**
```
Give me the email performance KPI scorecard for today. 
I want to see open rate, click rate, bounce rate, and unsubscribe rate.
```

**What the agent should do:**
- Call `chart_email_kpi_scorecard(start_date="2026-04-12", end_date="2026-04-12")`

**Expected Response — Must Include:**
- Horizontal bar chart with % rates for: Open Rate, Click Rate, Bounce Rate, Unsubscribe Rate, Spam Rate
- Each rate expressed as a percentage of `SENT` (e.g., Opens ÷ Sent × 100)
- Contextual benchmarks called out: 
  - Open rate > 40% → positive callout
  - Bounce rate > 5% → warning callout
  - Unsub rate > 2% → flag
- The total sent count used as the denominator stated clearly

**What it should NOT do:**
- Show raw counts instead of rates
- Skip the chart and only return a table

**Pass Criteria:** KPI scorecard chart rendered with % labels visible ✅

---

### Q7 — Pipeline Health Check for Today

**Question to ask:**
```
Did the data pipeline run successfully today? 
Any data quality issues or failed pipeline steps?
```

**What the agent should do:**
- Call `get_pipeline_observability()`
- Filter for today's runs (PIPELINE_RUN_LOG WHERE CAST(RUN_DATE AS DATE) = today)

**Expected Response — Must Include:**
- A list of pipeline runs today with their `STATUS` (SUCCESS / FAILED / RUNNING)
- Row counts processed per pipeline step (e.g., `ROWS_PROCESSED: 335`)
- Any DQ signal counts from `DQ_REJECTION_LOG` for today
- If all runs succeeded: *"All pipeline steps completed successfully today"*
- If a failure: the failed step name, error type, and affected row count

**What it should NOT do:**
- Return all-time pipeline history without filtering to today
- Ignore the `DQ_REJECTION_LOG` counts

**Pass Criteria:** Today's pipeline status (success or failure) explicitly stated ✅

---

### Q8 — Are All Active Prospects in SFMC Right Now?

**Question to ask:**
```
Are all of today's newly added prospects loaded into SFMC? 
Show me if there are any prospects in our system that haven't reached SFMC yet.
```

**What the agent should do:**
- Call `get_sfmc_prospect_outbound_match()`

**Expected Response — Must Include:**
- Count of prospects in `DIM_PROSPECT` (active = TRUE) that are NOT in `RAW_SFMC_PROSPECT_C`
- Count of prospects in `RAW_SFMC_PROSPECT_C` with no matching `DIM_PROSPECT` record (data integrity flag)
- Count of successfully matched prospects
- A clear statement: *"X prospects in DIM_PROSPECT have not yet been exported to SFMC"*
- Explanation that `VW_SFMC_PROSPECT_OUTBOUND` is the activation view

**What it should NOT do:**
- Claim "all prospects are in SFMC" without actually running the reconciliation query
- Confuse `DIM_PROSPECT.MASTER_PATIENT_ID` with `RAW_SFMC_PROSPECT_C.PROSPECT_ID` as a mismatch

**Pass Criteria:** Matched vs unmatched counts returned + export gap (if any) flagged ✅

---

### Q9 — Today's Bounce Check

**Question to ask:**
```
Did any emails bounce today? 
Show me the bounce analysis for today with hard vs soft bounce breakdown.
```

**What the agent should do:**
- Call `chart_bounce_analysis(start_date="2026-04-12", end_date="2026-04-12")`
- Optionally call `run_sql` for count breakdown by `BOUNCE_CATEGORY`

**Expected Response — Must Include:**
- Total bounce count for today
- Split between `Hard` bounce and `Soft` bounce counts
- Breakdown by journey (which journey had most bounces)
- If zero bounces: *"No bounces recorded today"* — not silence
- Bounce chart rendered (grouped bar)
- If Hard bounce count > 5: a callout about list hygiene risk

**What it should NOT do:**
- Return bounce data without distinguishing Hard vs Soft
- Skip the chart entirely

**Pass Criteria:** Hard/Soft split returned + bounce chart rendered ✅

---

### Q10 — End-of-Day Summary (Today)

**Question to ask:**
```
Give me a full end-of-day summary for today across the entire prospect journey — 
from leads in to SFMC engagement.
```

**What the agent should do:**
- Call `get_funnel_metrics(start_date="2026-04-12", end_date="2026-04-12")`
- Call `get_sfmc_engagement_stats(start_date="2026-04-12", end_date="2026-04-12")`
- Call `get_rejection_analysis` for both intake and SFMC categories
- Generate multiple charts: `chart_funnel`, `chart_engagement`, `chart_email_kpi_scorecard`

**Expected Response — Must Include:**
- `## Overview` section with headline metrics (leads, prospects, conversion rate)
- `## SFMC Activity` section: sent, opened, clicked, bounced, unsubscribed
- `## Data Quality` section: rejection counts by category, any suppressions
- `## Pipeline Health` section: run status
- At least 2 charts (funnel + engagement minimum)
- A **Key Takeaways** or **TL;DR** section at the end
- **Suggested follow-up questions** (Dig Deeper)

**What it should NOT do:**
- Return a single table with no narrative
- Miss any of the 4 major sections
- Generate only 1 chart for a comprehensive summary request

**Pass Criteria:** All 4 sections present + 2+ charts rendered + follow-ups suggested ✅

---

## PART 2 — Historical Analysis (10 Questions)

> These questions span date ranges across the full pipeline history.  
> The chatbot should handle broad date windows efficiently and show trends over time.

---

### Q11 — All-Time Funnel Overview

**Question to ask:**
```
Show me the overall funnel from the very beginning — all leads that ever came in, 
how many became prospects, and how that converted through SFMC.
```

**What the agent should do:**
- Call `get_funnel_metrics()` (no date filter = all time)
- Call `chart_funnel()` or `chart_funnel_waterfall()`

**Expected Response — Must Include:**
- F01: Total all-time lead count
- F02: Total valid prospects (and conversion rate from leads)
- F04: Total SFMC sent
- F06: Opens, clicks, unsubscribes
- Overall funnel drop-off: percentage lost at each stage
- A funnel chart AND/OR waterfall chart
- A contextual insight: e.g., *"The biggest drop occurs between prospects and SFMC sent, suggesting suppression or outbound timing issues"*

**What it should NOT do:**
- Return only today's data when no date filter is specified
- Miss the funnel waterfall showing where volume is lost

**Pass Criteria:** All-time counts at all funnel stages + chart rendered ✅

---

### Q12 — Rejection Reason Trends Over All Time

**Question to ask:**
```
What are the top reasons leads have been rejected historically? 
Show me the all-time rejection breakdown and which reason accounts for the most volume.
```

**What the agent should do:**
- Call `get_rejection_analysis(rejection_category="intake")`
- Call `chart_rejections(rejection_category="intake")`

**Expected Response — Must Include:**
- Ranked table: `REJECTION_REASON | COUNT | % of Total Rejections`
- Only intake-layer reasons (`NULL_EMAIL`, `NULL_FIRST_NAME`, `NULL_LAST_NAME`, `NULL_PHONE_NUMBER`, `INVALID_FILE_DATE`)
- The #1 rejection reason called out explicitly
- A donut chart of rejection reasons
- A business interpretation: *"NULL_EMAIL is the primary rejection driver — this suggests the intake form or API is accepting leads without a valid email field"*

**What it should NOT do:**
- Include `SUPPRESSED_PROSPECT` or `DUPLICATE_RECORD_ID` in this count
- Return percentages that don't add up to 100%

**Pass Criteria:** Top rejection reason identified + donut chart rendered + business interpretation given ✅

---

### Q13 — SFMC Journey Performance Comparison

**Question to ask:**
```
Compare the performance of all 4 SFMC journeys — Welcome, Nurture, High Engagement, 
and Re-engagement. Which journey has the best open rate and click rate?
```

**What the agent should do:**
- Call `get_sfmc_engagement_stats()` (all time, no journey filter)
- Call `chart_engagement()`
- Calculate open rate and click rate per journey inline

**Expected Response — Must Include:**
- A table with columns: `JOURNEY | SENT | OPENS | CLICKS | OPEN_RATE | CLICK_RATE`
- Open rate = Opens ÷ Sent × 100 (per journey)
- Click rate = Clicks ÷ Sent × 100 (per journey)
- The top-performing journey highlighted
- Grouped bar chart by journey × event type
- A narrative: *"J03 High Engagement shows the strongest click rate at X% — this aligns with the conversion-intent targeting of that journey"*

**What it should NOT do:**
- Return event counts without calculating rates
- Omit any of the 4 journeys without explanation

**Pass Criteria:** Rates calculated per journey + engagement chart rendered + best journey called out ✅

---

### Q14 — Lead Intake Trend Over the Last 3 Months

**Question to ask:**
```
Show me how lead intake and prospect volumes have trended month by month 
from January 2026 to April 2026.
```

**What the agent should do:**
- Call `chart_intake_trend(start_date="2026-01-01", end_date="2026-04-12", group_by="month")`
- Optionally call `get_funnel_metrics` per month for table detail

**Expected Response — Must Include:**
- A multi-line time series chart showing: Lead Intake volume per month + Valid Prospect volume per month
- A shaded gap band representing the rejection volume between leads and prospects
- A table: `MONTH | LEADS | PROSPECTS | CONVERSION_RATE`
- Month-over-month trend insight: *"February had the highest intake volume at X leads"* or *"Conversion rate dipped in March — worth investigating"*

**What it should NOT do:**
- Use `_LOADED_AT` instead of `FILE_DATE` for the business date
- Fail to parse mixed `FILE_DATE` formats (YYYY-MM-DD vs DD-MM-YYYY)

**Pass Criteria:** Monthly trend chart rendered with both Lead and Prospect lines + gap band ✅

---

### Q15 — All-Time Suppression Analysis

**Question to ask:**
```
How many prospects have ever been suppressed from SFMC emails? 
Break it down by reason and show which stage the suppression happened at.
```

**What the agent should do:**
- Call `get_rejection_analysis(rejection_category="sfmc")`
- Call `get_sfmc_stage_suppression()` (no target_date = all-time summary)
- Call `chart_rejections(rejection_category="sfmc")`

**Expected Response — Must Include:**
- Total suppression count from `DQ_REJECTION_LOG` (REJECTION_REASON = 'SUPPRESSED_PROSPECT', TABLE_NAME = 'FACT_SFMC_ENGAGEMENT')
- Breakdown by stage: which stage had the most suppressions
- The suppression source: `RAW_SFMC_PROSPECT_JOURNEY_DETAILS.SUPPRESSION_FLAG`
- A donut or bar chart of suppressions
- Explanation of what suppression means: *"A suppressed prospect is a valid, mastered prospect whose SFMC email send was blocked — they did not receive the email even though they were eligible"*

**What it should NOT do:**
- Mix suppression count with intake rejection count
- Use `REJECTION_REASON = 'SUPPRESSED'` only (must also check `'SUPPRESSED_PROSPECT'`)

**Pass Criteria:** Suppression count from SFMC layer returned + stage breakdown + chart rendered ✅

---

### Q16 — Journey Stage Progression Chart

**Question to ask:**
```
Show me how many prospects have progressed through each of the 9 SFMC journey stages.
Which stage has the most drop-off?
```

**What the agent should do:**
- Call `chart_journey_stage_progression()`
- Optionally call `get_sfmc_stage_suppression()` for context

**Expected Response — Must Include:**
- A horizontal bar chart showing prospect reach counts for all 9 stages (S1 through S9)
- Stage counts decreasing from S1 → S9 (natural funnel shape expected)
- The stage with the largest absolute drop-off identified
- Percentage drop between each stage pair called out for the biggest gap
- Business interpretation: *"The sharpest drop is between Stage X and Stage Y — this is where most prospects exit the journey"*

**What it should NOT do:**
- Return only a table without the chart
- Skip stages with 0 counts without acknowledging them

**Pass Criteria:** All 9 stages shown in horizontal bar chart + biggest drop-off stage identified ✅

---

### Q17 — Bounce Analysis Across All Time

**Question to ask:**
```
Give me a historical bounce analysis — how many hard bounces vs soft bounces have 
we had, and which journey is responsible for the most bounces?
```

**What the agent should do:**
- Call `chart_bounce_analysis()` (all time)
- Call `run_sql` or `get_sfmc_engagement_stats` for total bounce count

**Expected Response — Must Include:**
- Total bounce count (all time)
- Hard bounce count and Soft bounce count and their split %
- Journey-level breakdown: which journey contributed most bounces
- Grouped bar chart: Hard vs Soft by journey
- A risk callout: if hard bounce rate > 5% of total sent, flag it as a list hygiene issue
- Business explanation: *"Hard bounces (permanent delivery failures) should ideally be <2% of total sends. Soft bounces (temporary failures) are acceptable in small volumes."*

**What it should NOT do:**
- Return bounce counts without Hard/Soft distinction
- Use `FACT_SFMC_ENGAGEMENT` without checking `RAW_SFMC_BOUNCES` for category detail

**Pass Criteria:** Hard/Soft split + journey breakdown + bounce chart rendered ✅

---

### Q18 — Prospect Channel Mix (Historical)

**Question to ask:**
```
Where have our prospects historically come from? 
Show me the channel mix — which lead source channels drive the most volume?
```

**What the agent should do:**
- Call `chart_prospect_channel_mix()` (all time)
- Call `run_sql` against `FACT_PROSPECT_INTAKE + DIM_CHANNEL` for table detail

**Expected Response — Must Include:**
- A donut chart showing prospect count by channel
- A table: `CHANNEL | PROSPECT_COUNT | % of Total`
- The top channel called out: *"Channel X contributes Y% of all prospects"*
- If channel data is sparse, the fallback to `PHI_PROSPECT_MASTER.LEAD_SOURCE` should be used

**What it should NOT do:**
- Return "Unknown" for all channels without attempting the `DIM_CHANNEL` join
- Fail silently if `FACT_PROSPECT_INTAKE` has no channel data

**Pass Criteria:** Channel donut chart rendered + top channel identified by name ✅

---

### Q19 — Trace a Prospect End-to-End

**Question to ask:**
```
Trace prospect FIP000001 through the entire journey — 
from when they were first ingested to what SFMC emails they received.
```

> *(Replace FIP000001 with a real MASTER_PATIENT_ID from your data. Run `SELECT TOP 1 MASTER_PATIENT_ID FROM QA_FIPSAR_PHI_HUB.PHI_CORE.PHI_PROSPECT_MASTER` to get a valid ID.)*

**What the agent should do:**
- Call `trace_prospect(email_or_id="FIP000001")`
- Optionally call `get_sfmc_stage_suppression(prospect_id="FIP000001")`

**Expected Response — Must Include:**
- Prospect identity: name (masked/initials), email domain, intake date
- Intake status: valid prospect, first intake date
- SFMC journey progress as a timeline:
  - Stage 1: ✅ Sent on [date] or ❌ Not sent
  - Stage 2: ✅ Sent on [date] or ❌ Not sent
  - ... through Stage 9
- Engagement events: how many times opened, clicked
- If suppressed: which stage, suppression date, reason
- If unsubscribed: date and reason
- Closing: *"This prospect is currently at Stage X in Journey J0Y"* or *"Journey complete"*

**What it should NOT do:**
- Return raw column names like `MASTER_PATIENT_ID` in the response (should translate to "Prospect ID")
- Join via `PATIENT_IDENTITY_XREF` for the SFMC event lookup (must use `SUBSCRIBER_KEY = MASTER_PATIENT_ID` directly)

**Pass Criteria:** Complete stage-by-stage timeline returned + current journey status stated ✅

---

### Q20 — Full Historical Executive Report

**Question to ask:**
```
Give me the complete historical intelligence picture for the FIPSAR prospect programme 
from 1 January 2026 to today. I want everything — funnel, rejections, SFMC engagement, 
bounce analysis, and key recommendations.
```

**What the agent should do:**
- Call `get_funnel_metrics(start_date="2026-01-01", end_date="2026-04-12")`
- Call `get_rejection_analysis(start_date="2026-01-01", end_date="2026-04-12")`
- Call `get_sfmc_engagement_stats(start_date="2026-01-01", end_date="2026-04-12")`
- Call `chart_funnel_waterfall` + `chart_engagement` + `chart_email_kpi_scorecard` (3 charts minimum)

**Expected Response — Must Include:**
All five sections with headers:

1. **`## Funnel Overview`** — Lead → Prospect → Sent → Opened → Clicked with rates
2. **`## Rejection Analysis`** — Top rejection reasons with counts and business interpretation
3. **`## SFMC Journey Performance`** — Event counts and rates per journey
4. **`## Bounce & Suppression`** — Bounce split + suppression count
5. **`## Key Recommendations`** — 3–5 actionable bullets based on what the data shows

Plus:
- At least 3 charts rendered
- A **TL;DR** line at the top summarising the single most important finding
- A **Dig Deeper** section with 3 follow-up questions at the end

**What it should NOT do:**
- Produce a single wall of text with no section headers
- Generate only 1 chart for a comprehensive executive request
- Skip the recommendations section

**Pass Criteria:** All 5 sections present + 3 charts rendered + recommendations included ✅

---

## Quick Reference: Tool → Question Mapping

| Tool Called | Questions That Should Trigger It |
|---|---|
| `get_funnel_metrics` | Q1, Q10, Q11, Q20 |
| `get_rejection_analysis` (intake) | Q2, Q12 |
| `get_rejection_analysis` (sfmc) | Q4, Q15 |
| `get_sfmc_engagement_stats` | Q5, Q13, Q20 |
| `get_sfmc_stage_suppression` | Q3, Q4, Q16 |
| `get_sfmc_prospect_outbound_match` | Q8 |
| `get_pipeline_observability` | Q7 |
| `trace_prospect` | Q19 |
| `chart_funnel` | Q1, Q10, Q11 |
| `chart_funnel_waterfall` | Q11, Q20 |
| `chart_rejections` | Q2, Q12, Q15 |
| `chart_engagement` | Q5, Q13, Q20 |
| `chart_email_kpi_scorecard` | Q6, Q10, Q20 |
| `chart_bounce_analysis` | Q9, Q17 |
| `chart_daily_engagement_trend` | Q5 (alt) |
| `chart_journey_stage_progression` | Q16 |
| `chart_sfmc_stage_fishbone` | Q3 |
| `chart_intake_trend` | Q14 |
| `chart_prospect_channel_mix` | Q18 |
| `chart_conversion_segments` | Q10 (alt) |

---

## Common Failure Patterns to Watch For

| Symptom | Likely Cause | What to Report |
|---|---|---|
| All SFMC counts return 0 | Agent used `DATE_KEY → DIM_DATE` join instead of `DATE(EVENT_TIMESTAMP)` | Report as ❌ Fail — SQL generation bug |
| Today's lead count = 0 | `FILE_DATE` parsed with wrong format (`DD-MM-YYYY` not handled) | Report as ❌ Fail — date parsing bug |
| Rejection table includes `SUPPRESSED_PROSPECT` | Agent mixing SFMC suppressions into intake rejections | Report as ❌ Fail — rejection category bug |
| Response has no chart | Agent returned table only without calling a chart tool | Report as ⚠️ Partial |
| Response very short (1–2 sentences) | Agent hit token limit or prompt not triggered correctly | Report as ⚠️ Partial |
| "I encountered an error" | Snowflake connection issue or bad SQL generated | Report as ❌ Fail — note the exact error |
| Prospect trace returns wrong prospect | `PATIENT_IDENTITY_XREF` used instead of direct `SUBSCRIBER_KEY = MASTER_PATIENT_ID` join | Report as ❌ Fail — identity join bug |
| No follow-up questions in response | Footer not generated | Report as ⚠️ Partial |

---

## Validation Sign-Off

| Tester Name | Date Tested | Questions Passed | Questions Failed | Sign-Off |
|---|---|---|---|---|
| | | / 20 | / 20 | |
| | | / 20 | / 20 | |

---

*Document version: 1.0 — Generated 12 April 2026*  
*Maintained by: FIPSAR Data Engineering Team*
