-- ═══════════════════════════════════════════════════════════════════════════════
-- VIEW : QA_FIPSAR_DW.GOLD.VW_PROSPECT_JOURNEY_ANALYTICS
-- PURPOSE : One row per prospect — full pipeline funnel with suppression analysis
-- GRAIN   : MASTER_PATIENT_ID (unique per prospect)
-- PIPELINE: Bronze → Silver (dedup) → Gold DIM_PROSPECT → SFMC Export
--           → Journey Details (9 stages) → Unsubscribe / Bounce signals
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW QA_FIPSAR_DW.GOLD.VW_PROSPECT_JOURNEY_ANALYTICS
COMMENT = 'Prospect funnel analytics: one row per prospect from Bronze intake through 9-stage SFMC journey. Includes pipeline stage flags, journey stage sent flags (0/1), suppression analysis, unsubscribe and bounce signals, and a consolidated DROP_OR_STATUS_REASON label.'
AS

WITH

-- ─────────────────────────────────────────────────────────────────────────────
-- 1. BASE: Bronze Prospect Master — starting point of the pipeline
--    Grain: one row per unique MASTER_PATIENT_ID (latest FILE_DATE wins)
--    Note: BRZ may have multi-row for re-submitted prospects; QUALIFY deduplicates
-- ─────────────────────────────────────────────────────────────────────────────
brz AS (
    SELECT
        MASTER_PATIENT_ID,
        FIRST_NAME,
        LAST_NAME,
        EMAIL,
        PHONE_NUMBER,
        PATIENT_CONSENT,
        CHANNEL,
        FILE_DATE                    AS INTAKE_DATE
    FROM QA_FIPSAR_DW.BRONZE.BRZ_PROSPECT_MASTER
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY MASTER_PATIENT_ID
        ORDER BY FILE_DATE DESC
    ) = 1
),

-- ─────────────────────────────────────────────────────────────────────────────
-- 2. SILVER: Deduplication outcome
--    DQ_PASSED = TRUE  → survived dedup, version promoted forward
--    DQ_PASSED = FALSE → dropped as duplicate (dedup key = RECORD_ID)
--    IS_CURRENT = TRUE → the latest active SCD2 version
-- ─────────────────────────────────────────────────────────────────────────────
slv AS (
    SELECT
        MASTER_PATIENT_ID,
        MAX(CASE WHEN IS_CURRENT = TRUE AND DQ_PASSED = TRUE THEN 1 ELSE 0 END) AS PASSED_DEDUP,
        MAX(VERSION_NUM)                                                          AS SLV_VERSION
    FROM QA_FIPSAR_DW.SILVER.SLV_PROSPECT_MASTER
    GROUP BY MASTER_PATIENT_ID
),

-- ─────────────────────────────────────────────────────────────────────────────
-- 3. GOLD DIM_PROSPECT: validated, deduped prospect attributes
--    Presence here = gold-quality, journey-eligible prospect
-- ─────────────────────────────────────────────────────────────────────────────
dim_p AS (
    SELECT
        MASTER_PATIENT_ID,
        FIRST_INTAKE_DATE,
        LAST_INTAKE_DATE,
        INTAKE_COUNT,
        AGE,
        AGE_GROUP,
        CITY,
        STATE,
        ZIP_CODE,
        PRIMARY_CHANNEL
    FROM QA_FIPSAR_DW.GOLD.DIM_PROSPECT
),

-- ─────────────────────────────────────────────────────────────────────────────
-- 4. SFMC CURRENT SNAPSHOT: was this prospect successfully exported to SFMC?
--    Join: RAW_SFMC_PROSPECT_C.PROSPECT_ID = MASTER_PATIENT_ID (FIP... format)
--    Missing row here = prospect went outbound but was NOT loaded in SFMC
-- ─────────────────────────────────────────────────────────────────────────────
sfmc_snap AS (
    SELECT
        PROSPECT_ID,
        MARKETING_CONSENT    AS SFMC_MARKETING_CONSENT,
        HIGH_ENGAGEMENT      AS SFMC_HIGH_ENGAGEMENT,
        REGISTRATION_DATE    AS SFMC_REGISTRATION_DATE,
        LAST_UPDATED         AS SFMC_LAST_UPDATED
    FROM QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_PROSPECT_C
),

-- ─────────────────────────────────────────────────────────────────────────────
-- 5. JOURNEY DETAILS: 9-stage send flags (0/1) and sent dates
--    Source: RAW_SFMC_PROSPECT_JOURNEY_DETAILS — one row per prospect (wide table)
--    Flag rule: UPPER(TRIM(col)) = 'TRUE'  (values are VARCHAR 'True'/'False')
--    Date rule: TRY_TO_DATE() — raw date strings, safe cast
--    NULL after last TRUE stage = intentional suppression cutoff (NOT missing data)
-- ─────────────────────────────────────────────────────────────────────────────
journey AS (
    SELECT
        PROSPECT_ID,

        -- Stage 01: Welcome Email — Welcome Phase
        CASE WHEN UPPER(TRIM(WELCOMEJOURNEY_WELCOMEEMAIL_SENT))          = 'TRUE' THEN 1 ELSE 0 END  AS S01_SENT,
        TRY_TO_DATE(WELCOMEJOURNEY_WELCOMEEMAIL_SENT_DATE)                                            AS S01_SENT_DATE,

        -- Stage 02: Education Email — Welcome Phase
        CASE WHEN UPPER(TRIM(WELCOMEJOURNEY_EDUCATIONEMAIL_SENT))         = 'TRUE' THEN 1 ELSE 0 END  AS S02_SENT,
        TRY_TO_DATE(WELCOMEJOURNEY_EDUCATIONEMAIL_SENT_DATE)                                           AS S02_SENT_DATE,

        -- Stage 03: Education Email 1 — Nurture Phase
        CASE WHEN UPPER(TRIM(NURTUREJOURNEY_EDUCATIONEMAIL1_SENT))        = 'TRUE' THEN 1 ELSE 0 END  AS S03_SENT,
        TRY_TO_DATE(NURTUREJOURNEY_EDUCATIONEMAIL1_SENT_DATE)                                          AS S03_SENT_DATE,

        -- Stage 04: Education Email 2 — Nurture Phase
        CASE WHEN UPPER(TRIM(NURTUREJOURNEY_EDUCATIONEMAIL2_SENT))        = 'TRUE' THEN 1 ELSE 0 END  AS S04_SENT,
        TRY_TO_DATE(NURTUREJOURNEY_EDUCATIONEMAIL2_SENT_DATE)                                          AS S04_SENT_DATE,

        -- Stage 05: Prospect Story Email — Nurture Phase
        CASE WHEN UPPER(TRIM(NURTUREJOURNEY_PROSPECTSTORYEMAIL_SENT))     = 'TRUE' THEN 1 ELSE 0 END  AS S05_SENT,
        TRY_TO_DATE(NURTUREJOURNEY_PROSPECTSTORYEMAIL_SENT_DATE)                                       AS S05_SENT_DATE,

        -- Stage 06: Conversion Email — High Engagement Phase
        CASE WHEN UPPER(TRIM(HIGHENGAGEMENT_CONVERSIONEMAIL_SENT))        = 'TRUE' THEN 1 ELSE 0 END  AS S06_SENT,
        TRY_TO_DATE(HIGHENGAGEMENT_CONVERSIONEMAIL_SENT_DATE)                                          AS S06_SENT_DATE,

        -- Stage 07: Reminder Email — High Engagement Phase
        CASE WHEN UPPER(TRIM(HIGHENGAGEMENT_REMINDEREMAIL_SENT))          = 'TRUE' THEN 1 ELSE 0 END  AS S07_SENT,
        TRY_TO_DATE(HIGHENGAGEMENT_REMINDEREMAIL_SENT_DATE)                                            AS S07_SENT_DATE,

        -- Stage 08: Re-engagement Email — Low Engagement Phase
        CASE WHEN UPPER(TRIM(LOWENGAGEMENT_REENGAGEMENTEMAIL_SENT))       = 'TRUE' THEN 1 ELSE 0 END  AS S08_SENT,
        TRY_TO_DATE(LOWENGAGEMENT_REENGAGEMENTEMAIL_SENT_DATE)                                         AS S08_SENT_DATE,

        -- Stage 09: Final Reminder Email — Low Engagement Phase
        CASE WHEN UPPER(TRIM(LOWENGAGEMENTFINALREMINDEREMAIL_SENT))       = 'TRUE' THEN 1 ELSE 0 END  AS S09_SENT,
        TRY_TO_DATE(LOWENGAGEMENTFINALREMINDEREMAIL_SENT_DATE)                                         AS S09_SENT_DATE,

        -- Suppression: SUPPRESSION_FLAG='YES/Y/TRUE/1' → prospect permanently exited
        CASE WHEN UPPER(TRIM(SUPPRESSION_FLAG)) IN ('YES','Y','TRUE','1') THEN 1 ELSE 0 END           AS IS_SUPPRESSED,

        -- Phase completion flags
        CASE WHEN UPPER(TRIM(WELCOME_JOURNEY_COMPLETE)) = 'TRUE' THEN 1 ELSE 0 END                    AS WELCOME_PHASE_COMPLETE,
        CASE WHEN UPPER(TRIM(NURTURE_JOURNEY_COMPLETE)) = 'TRUE' THEN 1 ELSE 0 END                    AS NURTURE_PHASE_COMPLETE

    FROM QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_PROSPECT_JOURNEY_DETAILS
),

-- ─────────────────────────────────────────────────────────────────────────────
-- 6. UNSUBSCRIBES: first opt-out date per prospect
--    EVENT_DATE is VARCHAR stored as 'MM/DD/YYYY HH:MM:SS AM/PM'
--    Must parse with TRY_TO_DATE(SPLIT(EVENT_DATE,' ')[0], 'MM/DD/YYYY')
--    Taking MIN = the earliest unsubscribe (first time the prospect opted out)
-- ─────────────────────────────────────────────────────────────────────────────
unsub AS (
    SELECT
        SUBSCRIBER_KEY,
        MIN(TRY_TO_DATE(SPLIT(EVENT_DATE, ' ')[0]::STRING, 'MM/DD/YYYY')) AS UNSUBSCRIBED_DATE,
        COUNT(*)                                                            AS UNSUB_EVENT_COUNT
    FROM QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_UNSUBSCRIBES
    GROUP BY SUBSCRIBER_KEY
),

-- ─────────────────────────────────────────────────────────────────────────────
-- 7. SFMC SUPPRESSION REASON: most recent suppression event from engagement fact
--    IS_SUPPRESSED = TRUE on FACT_SFMC_ENGAGEMENT = suppressed send event
--    SUPPRESSION_REASON holds human-readable cause (e.g. 'Unsubscribed', 'Hard Bounce')
-- ─────────────────────────────────────────────────────────────────────────────
sfmc_supp AS (
    SELECT
        SUBSCRIBER_KEY,
        SUPPRESSION_REASON,
        CAST(MAX(EVENT_TIMESTAMP) AS DATE)   AS SUPPRESSION_DATE
    FROM QA_FIPSAR_DW.GOLD.FACT_SFMC_ENGAGEMENT
    WHERE IS_SUPPRESSED = TRUE
    GROUP BY SUBSCRIBER_KEY, SUPPRESSION_REASON
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY SUBSCRIBER_KEY
        ORDER BY MAX(EVENT_TIMESTAMP) DESC
    ) = 1
),

-- ─────────────────────────────────────────────────────────────────────────────
-- 8. BOUNCES: hard / soft bounce signals per prospect
--    Hard bounce = permanent delivery failure (invalid email / ISP block)
--    Soft bounce = transient failure (mailbox full, server timeout)
--    Both can trigger suppression; Hard bounce is the more critical signal
-- ─────────────────────────────────────────────────────────────────────────────
bounces AS (
    SELECT
        SUBSCRIBER_KEY,
        MAX(CASE WHEN UPPER(BOUNCE_CATEGORY) = 'HARD' THEN 1 ELSE 0 END)   AS HAS_HARD_BOUNCE,
        MAX(CASE WHEN UPPER(BOUNCE_CATEGORY) = 'SOFT' THEN 1 ELSE 0 END)   AS HAS_SOFT_BOUNCE,
        MIN(TRY_TO_DATE(SPLIT(EVENT_DATE, ' ')[0]::STRING, 'MM/DD/YYYY'))  AS FIRST_BOUNCE_DATE,
        COUNT(*)                                                             AS TOTAL_BOUNCE_EVENTS
    FROM QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_BOUNCES
    GROUP BY SUBSCRIBER_KEY
)


-- ═══════════════════════════════════════════════════════════════════════════════
-- FINAL SELECT — One row per prospect, full pipeline visibility
-- Sections: A=Identity | B=Pipeline Flags | C=SFMC Attrs | D=Journey Stages
--           E=Derived Metrics | F=Journey Status | G=Suppression Detail
--           H=Unsubscribe | I=Bounce | J=Drop Reason
-- ═══════════════════════════════════════════════════════════════════════════════
SELECT

    -- ── A. PROSPECT IDENTITY ─────────────────────────────────────────────────
    b.MASTER_PATIENT_ID                                                     AS PROSPECT_ID,
    b.FIRST_NAME,
    b.LAST_NAME,
    b.EMAIL,
    b.PHONE_NUMBER,
    b.PATIENT_CONSENT,
    b.CHANNEL                                                               AS INTAKE_CHANNEL,
    b.INTAKE_DATE,
    d.AGE,
    d.AGE_GROUP,
    d.CITY,
    d.STATE,
    d.ZIP_CODE,
    d.FIRST_INTAKE_DATE,
    d.LAST_INTAKE_DATE,
    d.INTAKE_COUNT,
    d.PRIMARY_CHANNEL,

    -- ── B. PIPELINE STAGE FLAGS (0 = dropped at/before this layer, 1 = present) ──
    -- Allows you to see exactly where in the pipeline each prospect fell off
    1                                                                       AS IN_BRONZE,
    COALESCE(s.PASSED_DEDUP, 0)                                            AS PASSED_DEDUP,
    CASE WHEN d.MASTER_PATIENT_ID IS NOT NULL THEN 1 ELSE 0 END            AS IN_DIM_PROSPECT,
    CASE WHEN sf.PROSPECT_ID      IS NOT NULL THEN 1 ELSE 0 END            AS IN_SFMC,
    CASE WHEN j.PROSPECT_ID       IS NOT NULL THEN 1 ELSE 0 END            AS HAS_JOURNEY_RECORD,

    -- ── C. SFMC PROSPECT ATTRIBUTES ──────────────────────────────────────────
    sf.SFMC_MARKETING_CONSENT,
    sf.SFMC_HIGH_ENGAGEMENT,
    sf.SFMC_REGISTRATION_DATE,
    sf.SFMC_LAST_UPDATED,

    -- ── D. JOURNEY STAGE FLAGS (0/1) + SENT DATES ───────────────────────────
    -- 0 = not sent (either suppressed, awaiting interval, or not yet reached)
    -- NULL date = stage was not reached (flag = 0)
    COALESCE(j.S01_SENT, 0)                                                AS STAGE_01_WELCOME_EMAIL_SENT,
    j.S01_SENT_DATE                                                         AS STAGE_01_WELCOME_EMAIL_SENT_DATE,

    COALESCE(j.S02_SENT, 0)                                                AS STAGE_02_EDUCATION_EMAIL_SENT,
    j.S02_SENT_DATE                                                         AS STAGE_02_EDUCATION_EMAIL_SENT_DATE,

    COALESCE(j.S03_SENT, 0)                                                AS STAGE_03_EDUCATION_EMAIL_1_SENT,
    j.S03_SENT_DATE                                                         AS STAGE_03_EDUCATION_EMAIL_1_SENT_DATE,

    COALESCE(j.S04_SENT, 0)                                                AS STAGE_04_EDUCATION_EMAIL_2_SENT,
    j.S04_SENT_DATE                                                         AS STAGE_04_EDUCATION_EMAIL_2_SENT_DATE,

    COALESCE(j.S05_SENT, 0)                                                AS STAGE_05_PROSPECT_STORY_EMAIL_SENT,
    j.S05_SENT_DATE                                                         AS STAGE_05_PROSPECT_STORY_EMAIL_SENT_DATE,

    COALESCE(j.S06_SENT, 0)                                                AS STAGE_06_CONVERSION_EMAIL_SENT,
    j.S06_SENT_DATE                                                         AS STAGE_06_CONVERSION_EMAIL_SENT_DATE,

    COALESCE(j.S07_SENT, 0)                                                AS STAGE_07_REMINDER_EMAIL_SENT,
    j.S07_SENT_DATE                                                         AS STAGE_07_REMINDER_EMAIL_SENT_DATE,

    COALESCE(j.S08_SENT, 0)                                                AS STAGE_08_REENGAGEMENT_EMAIL_SENT,
    j.S08_SENT_DATE                                                         AS STAGE_08_REENGAGEMENT_EMAIL_SENT_DATE,

    COALESCE(j.S09_SENT, 0)                                                AS STAGE_09_FINAL_REMINDER_EMAIL_SENT,
    j.S09_SENT_DATE                                                         AS STAGE_09_FINAL_REMINDER_EMAIL_SENT_DATE,

    -- ── E. DERIVED JOURNEY METRICS ───────────────────────────────────────────
    -- Total stages where an email was actually sent to this prospect
    (
        COALESCE(j.S01_SENT, 0) + COALESCE(j.S02_SENT, 0) +
        COALESCE(j.S03_SENT, 0) + COALESCE(j.S04_SENT, 0) +
        COALESCE(j.S05_SENT, 0) + COALESCE(j.S06_SENT, 0) +
        COALESCE(j.S07_SENT, 0) + COALESCE(j.S08_SENT, 0) +
        COALESCE(j.S09_SENT, 0)
    )                                                                       AS STAGES_COMPLETED_COUNT,

    COALESCE(j.WELCOME_PHASE_COMPLETE, 0)                                  AS WELCOME_PHASE_COMPLETE,
    COALESCE(j.NURTURE_PHASE_COMPLETE, 0)                                  AS NURTURE_PHASE_COMPLETE,
    COALESCE(j.IS_SUPPRESSED, 0)                                           AS IS_SUPPRESSED,

    -- ── F. JOURNEY STATUS (authoritative business label) ─────────────────────
    CASE
        WHEN j.PROSPECT_ID IS NULL AND sf.PROSPECT_ID IS NOT NULL THEN 'Awaiting Journey Start'
        WHEN j.PROSPECT_ID IS NULL AND sf.PROSPECT_ID IS NULL     THEN 'Not Exported to SFMC'
        WHEN d.MASTER_PATIENT_ID   IS NULL                        THEN 'Pipeline Drop — Pre-Gold'
        WHEN j.IS_SUPPRESSED = 1                                  THEN 'Suppressed'
        WHEN j.S09_SENT      = 1                                  THEN 'Journey Completed'
        ELSE                                                           'In Progress'
    END                                                                     AS JOURNEY_STATUS,

    -- ── G. STAGE-LEVEL SUPPRESSION ANALYSIS ──────────────────────────────────
    -- Last stage where an email was confirmed sent (scan S09 → S01)
    CASE
        WHEN j.S09_SENT = 1 THEN 'Stage 09 — Final Reminder Email'
        WHEN j.S08_SENT = 1 THEN 'Stage 08 — Re-engagement Email'
        WHEN j.S07_SENT = 1 THEN 'Stage 07 — Reminder Email'
        WHEN j.S06_SENT = 1 THEN 'Stage 06 — Conversion Email'
        WHEN j.S05_SENT = 1 THEN 'Stage 05 — Prospect Story Email'
        WHEN j.S04_SENT = 1 THEN 'Stage 04 — Education Email 2'
        WHEN j.S03_SENT = 1 THEN 'Stage 03 — Education Email 1'
        WHEN j.S02_SENT = 1 THEN 'Stage 02 — Education Email'
        WHEN j.S01_SENT = 1 THEN 'Stage 01 — Welcome Email'
        ELSE                     'None — Not Started'
    END                                                                     AS LAST_COMPLETED_STAGE,

    -- Stage at which suppression cut this prospect off (only populated when IS_SUPPRESSED=1)
    -- Logic: first stage after LAST_COMPLETED_STAGE where flag = 0 and IS_SUPPRESSED = 1
    CASE WHEN j.IS_SUPPRESSED = 1 THEN
        CASE
            WHEN j.S01_SENT = 0                             THEN 'Stage 01 — Welcome Email (Never Started)'
            WHEN j.S01_SENT = 1 AND j.S02_SENT = 0         THEN 'Stage 02 — Education Email'
            WHEN j.S02_SENT = 1 AND j.S03_SENT = 0         THEN 'Stage 03 — Education Email 1'
            WHEN j.S03_SENT = 1 AND j.S04_SENT = 0         THEN 'Stage 04 — Education Email 2'
            WHEN j.S04_SENT = 1 AND j.S05_SENT = 0         THEN 'Stage 05 — Prospect Story Email'
            WHEN j.S05_SENT = 1 AND j.S06_SENT = 0         THEN 'Stage 06 — Conversion Email'
            WHEN j.S06_SENT = 1 AND j.S07_SENT = 0         THEN 'Stage 07 — Reminder Email'
            WHEN j.S07_SENT = 1 AND j.S08_SENT = 0         THEN 'Stage 08 — Re-engagement Email'
            WHEN j.S08_SENT = 1 AND j.S09_SENT = 0         THEN 'Stage 09 — Final Reminder Email'
            ELSE                                                 'Suppressed After All Stages'
        END
    ELSE NULL END                                                           AS SUPPRESSED_AT_STAGE,

    -- Suppression reason text from the engagement fact (e.g. 'Unsubscribed', 'Hard Bounce')
    ss.SUPPRESSION_REASON,
    ss.SUPPRESSION_DATE,

    -- ── H. UNSUBSCRIBE SIGNAL ────────────────────────────────────────────────
    -- IS_UNSUBSCRIBED = 1 means prospect opted out via email unsubscribe link
    -- Unsubscribed prospects should have SUPPRESSION_FLAG=TRUE in journey table
    CASE WHEN u.SUBSCRIBER_KEY IS NOT NULL THEN 1 ELSE 0 END               AS IS_UNSUBSCRIBED,
    u.UNSUBSCRIBED_DATE,
    u.UNSUB_EVENT_COUNT,

    -- ── I. BOUNCE SIGNALS ────────────────────────────────────────────────────
    -- Hard bounce = permanent failure (triggers suppression)
    -- Soft bounce = transient failure (may retry)
    COALESCE(bou.HAS_HARD_BOUNCE, 0)                                       AS HAS_HARD_BOUNCE,
    COALESCE(bou.HAS_SOFT_BOUNCE, 0)                                       AS HAS_SOFT_BOUNCE,
    bou.FIRST_BOUNCE_DATE,
    COALESCE(bou.TOTAL_BOUNCE_EVENTS, 0)                                   AS TOTAL_BOUNCE_EVENTS,

    -- ── J. DROP OR STATUS REASON (consolidated funnel label) ─────────────────
    -- Tells analytics exactly WHERE and WHY this prospect stopped progressing.
    -- Used for funnel drop-off reporting and root cause grouping.
    CASE
        WHEN d.MASTER_PATIENT_ID IS NULL
          AND COALESCE(s.PASSED_DEDUP, 0) = 0              THEN 'Duplicate — Filtered at Dedup'
        WHEN d.MASTER_PATIENT_ID IS NULL                   THEN 'Pipeline Gap — Did Not Reach Gold'
        WHEN sf.PROSPECT_ID IS NULL                        THEN 'Not Exported to SFMC'
        WHEN j.PROSPECT_ID  IS NULL                        THEN 'In SFMC — Awaiting Journey Start'
        WHEN j.IS_SUPPRESSED = 1
         AND u.SUBSCRIBER_KEY IS NOT NULL                  THEN 'Suppressed — Unsubscribed'
        WHEN j.IS_SUPPRESSED = 1
         AND COALESCE(bou.HAS_HARD_BOUNCE, 0) = 1         THEN 'Suppressed — Hard Bounce'
        WHEN j.IS_SUPPRESSED = 1                           THEN 'Suppressed — Other Reason'
        WHEN j.S09_SENT = 1                                THEN 'Journey Completed'
        ELSE                                                    'In Progress'
    END                                                                     AS DROP_OR_STATUS_REASON

FROM                 brz    b
LEFT JOIN            slv    s   ON b.MASTER_PATIENT_ID = s.MASTER_PATIENT_ID
LEFT JOIN          dim_p    d   ON b.MASTER_PATIENT_ID = d.MASTER_PATIENT_ID
LEFT JOIN      sfmc_snap    sf  ON b.MASTER_PATIENT_ID = sf.PROSPECT_ID
LEFT JOIN        journey    j   ON b.MASTER_PATIENT_ID = j.PROSPECT_ID
LEFT JOIN          unsub    u   ON b.MASTER_PATIENT_ID = u.SUBSCRIBER_KEY
LEFT JOIN      sfmc_supp    ss  ON b.MASTER_PATIENT_ID = ss.SUBSCRIBER_KEY
LEFT JOIN        bounces    bou ON b.MASTER_PATIENT_ID = bou.SUBSCRIBER_KEY
;


-- ═══════════════════════════════════════════════════════════════════════════════
-- QUICK VALIDATION QUERIES — run after CREATE VIEW to verify data quality
-- ═══════════════════════════════════════════════════════════════════════════════

-- 1. Row count and basic sanity (should equal DIM_PROSPECT count for gold-quality)
-- SELECT COUNT(*), COUNT(DISTINCT PROSPECT_ID) FROM QA_FIPSAR_DW.GOLD.VW_PROSPECT_JOURNEY_ANALYTICS;

-- 2. Pipeline funnel summary — how many prospects at each stage?
-- SELECT
--     COUNT(*)                                          AS TOTAL_PROSPECTS_IN_BRONZE,
--     SUM(PASSED_DEDUP)                                 AS PASSED_DEDUP,
--     SUM(IN_DIM_PROSPECT)                              AS IN_GOLD_DIM_PROSPECT,
--     SUM(IN_SFMC)                                      AS EXPORTED_TO_SFMC,
--     SUM(HAS_JOURNEY_RECORD)                           AS HAS_JOURNEY_RECORD,
--     SUM(STAGE_01_WELCOME_EMAIL_SENT)                  AS STAGE_01_SENT,
--     SUM(STAGE_02_EDUCATION_EMAIL_SENT)                AS STAGE_02_SENT,
--     SUM(STAGE_03_EDUCATION_EMAIL_1_SENT)              AS STAGE_03_SENT,
--     SUM(STAGE_04_EDUCATION_EMAIL_2_SENT)              AS STAGE_04_SENT,
--     SUM(STAGE_05_PROSPECT_STORY_EMAIL_SENT)           AS STAGE_05_SENT,
--     SUM(STAGE_06_CONVERSION_EMAIL_SENT)               AS STAGE_06_SENT,
--     SUM(STAGE_07_REMINDER_EMAIL_SENT)                 AS STAGE_07_SENT,
--     SUM(STAGE_08_REENGAGEMENT_EMAIL_SENT)             AS STAGE_08_SENT,
--     SUM(STAGE_09_FINAL_REMINDER_EMAIL_SENT)           AS STAGE_09_SENT,
--     SUM(IS_SUPPRESSED)                                AS SUPPRESSED,
--     SUM(IS_UNSUBSCRIBED)                              AS UNSUBSCRIBED,
--     SUM(HAS_HARD_BOUNCE)                              AS HARD_BOUNCED
-- FROM QA_FIPSAR_DW.GOLD.VW_PROSPECT_JOURNEY_ANALYTICS;

-- 3. Journey status breakdown
-- SELECT JOURNEY_STATUS, COUNT(*) AS PROSPECTS
-- FROM QA_FIPSAR_DW.GOLD.VW_PROSPECT_JOURNEY_ANALYTICS
-- GROUP BY 1 ORDER BY 2 DESC;

-- 4. Suppression stage distribution (where are prospects dropping out?)
-- SELECT SUPPRESSED_AT_STAGE, COUNT(*) AS SUPPRESSED_COUNT
-- FROM QA_FIPSAR_DW.GOLD.VW_PROSPECT_JOURNEY_ANALYTICS
-- WHERE IS_SUPPRESSED = 1
-- GROUP BY 1 ORDER BY 2 DESC;

-- 5. Drop reason funnel
-- SELECT DROP_OR_STATUS_REASON, COUNT(*) AS PROSPECTS
-- FROM QA_FIPSAR_DW.GOLD.VW_PROSPECT_JOURNEY_ANALYTICS
-- GROUP BY 1 ORDER BY 2 DESC;

-- 6. Spot-check a specific prospect end-to-end
-- SELECT * FROM QA_FIPSAR_DW.GOLD.VW_PROSPECT_JOURNEY_ANALYTICS
-- WHERE PROSPECT_ID = 'FIP001234';
