-- ============================================================================
-- FILE 4: 04_Orchestration_DAGs.sql
-- TASK DAG: PHI → Bronze → Silver → Email Notification → Gold → SFMC Outbound
--
-- CORRECTIONS APPLIED:
--   FIX-01: Added TASK_SILVER_DQ_NOTIFICATION (was referenced but never created)
--   FIX-02: TASK_GOLD_LOAD now correctly chains AFTER TASK_SILVER_DQ_NOTIFICATION
--           (was incorrectly chaining AFTER TASK_SILVER_LOAD)
--   FIX-03: TASK_GOLD_LOAD now calls all 3 missing dimension SPs before fact load:
--           SP_LOAD_GOLD_DIM_CHANNEL, SP_LOAD_GOLD_DIM_DATE, SP_LOAD_GOLD_DIM_GEOGRAPHY
--   FIX-04: RESUME block now includes TASK_SILVER_DQ_NOTIFICATION (was commented out)
--   FIX-05: SUSPEND block is now fully commented out — running this file end-to-end
--           will NO LONGER suspend tasks immediately after resuming them.
--           Use the SUSPEND section only when you intentionally want to stop the pipeline.
--
-- FINAL DAG ORDER (child → parent resume sequence):
--   TASK_SFMC_OUTBOUND_EXPORT
--   TASK_GOLD_LOAD
--   TASK_SILVER_DQ_NOTIFICATION
--   TASK_SILVER_LOAD
--   TASK_BRONZE_LOAD
--   TASK_PHI_IDENTITY_LOAD
--   TASK_PHI_ORCHESTRATOR  ← ROOT (always resume last)
--   TASK_SFMC_EVENTS_LOAD  ← separate root
-- ============================================================================

USE DATABASE QA_FIPSAR_PHI_HUB;
USE SCHEMA PHI_CORE;

-- ============================================================================
-- TASK DEFINITIONS
-- ============================================================================

-- ROOT: Every 5 minutes. Loads S3 CSVs into PHI staging.
CREATE OR REPLACE TASK TASK_PHI_ORCHESTRATOR
    WAREHOUSE = COMPUTE_WH
    SCHEDULE  = '2 MINUTE'
    COMMENT   = 'ROOT: Load S3 CSVs into PHI staging'
AS
    CALL QA_FIPSAR_PHI_HUB.PHI_CORE.SP_LOAD_STAGING_FROM_S3();

-- Chain 1: PHI identity resolution + load
CREATE OR REPLACE TASK TASK_PHI_IDENTITY_LOAD
    WAREHOUSE = COMPUTE_WH
    COMMENT   = 'Assign MASTER_PATIENT_ID and load PHI table'
    AFTER     TASK_PHI_ORCHESTRATOR
AS
    CALL QA_FIPSAR_PHI_HUB.PHI_CORE.SP_LOAD_PHI_PROSPECT();

-- Chain 2: Bronze raw copy
CREATE OR REPLACE TASK TASK_BRONZE_LOAD
    WAREHOUSE = COMPUTE_WH
    COMMENT   = 'Raw copy PHI → Bronze'
    AFTER     TASK_PHI_IDENTITY_LOAD
AS
    CALL QA_FIPSAR_DW.BRONZE.SP_LOAD_BRONZE_PROSPECT();

-- Chain 3: Silver DQ gates (SCD2)
CREATE OR REPLACE TASK TASK_SILVER_LOAD
    WAREHOUSE = COMPUTE_WH
    COMMENT   = 'DQ gates: null rejection + dedup + SCD2 → Silver'
    AFTER     TASK_BRONZE_LOAD
AS
    CALL QA_FIPSAR_DW.SILVER.SP_LOAD_SILVER_PROSPECT();

-- Chain 4a: Email status notification after Silver (FIX-01: task now created)
CREATE OR REPLACE TASK TASK_SILVER_DQ_NOTIFICATION
    WAREHOUSE = COMPUTE_WH
    COMMENT   = 'Send pipeline status + DQ summary email after Silver load'
    AFTER     TASK_SILVER_LOAD
AS
    CALL QA_FIPSAR_AUDIT.PIPELINE_AUDIT.SP_SEND_SILVER_DQ_STATUS_EMAIL();

-- Chain 4b: Gold dimensions + facts (FIX-02: chains AFTER email, not AFTER Silver)
--           (FIX-03: all 3 dimension SPs added before fact load)
CREATE OR REPLACE TASK TASK_GOLD_LOAD
    WAREHOUSE = COMPUTE_WH
    COMMENT   = 'Load all Gold dimensions then fact table'
    AFTER     TASK_SILVER_DQ_NOTIFICATION
AS
BEGIN
    -- Dimensions must be populated before fact FK resolution
    CALL QA_FIPSAR_DW.GOLD.SP_LOAD_GOLD_DIM_CHANNEL();
    CALL QA_FIPSAR_DW.GOLD.SP_LOAD_GOLD_DIM_DATE();
    CALL QA_FIPSAR_DW.GOLD.SP_LOAD_GOLD_DIM_GEOGRAPHY();
    CALL QA_FIPSAR_DW.GOLD.SP_LOAD_GOLD_DIM_PROSPECT();
    -- Fact load (FK joins DIM_CHANNEL, DIM_DATE, DIM_GEOGRAPHY, DIM_PROSPECT)
    CALL QA_FIPSAR_DW.GOLD.SP_LOAD_GOLD_FACT_INTAKE();
END;

-- Chain 5: SFMC outbound export
CREATE OR REPLACE TASK TASK_SFMC_OUTBOUND_EXPORT
    WAREHOUSE = COMPUTE_WH
    COMMENT   = 'Export prospect delta CSV to S3 for SFMC (Prospect_c_delta_YYYYMMDD_HHMM.csv)'
    AFTER     TASK_GOLD_LOAD
AS
    CALL QA_FIPSAR_DW.GOLD.SP_EXPORT_SFMC_OUTBOUND();


-- Separate root: SFMC event ingestion (every 4 hours)
CREATE OR REPLACE TASK QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.TASK_SFMC_EVENTS_LOAD
    WAREHOUSE = COMPUTE_WH
    SCHEDULE  = 'USING CRON 0 */4 * * * UTC'
    COMMENT   = 'Load SFMC event files from dedicated S3 folders every 4 hours'
AS
    CALL QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.SP_RUN_SFMC_EVENTS_PIPELINE();


-- ============================================================================
-- STEP: RESUME ALL TASKS
-- IMPORTANT: Resume child tasks BEFORE parent. Root task last.
-- FIX-04: TASK_SILVER_DQ_NOTIFICATION is now included (was commented out before)
-- ============================================================================

USE DATABASE QA_FIPSAR_PHI_HUB;
USE SCHEMA PHI_CORE;

ALTER TASK TASK_SFMC_OUTBOUND_EXPORT                             RESUME;
ALTER TASK TASK_GOLD_LOAD                                        RESUME;
ALTER TASK TASK_SILVER_DQ_NOTIFICATION                           RESUME;
ALTER TASK TASK_SILVER_LOAD                                      RESUME;
ALTER TASK TASK_BRONZE_LOAD                                      RESUME;
ALTER TASK TASK_PHI_IDENTITY_LOAD                                RESUME;
ALTER TASK TASK_PHI_ORCHESTRATOR                                 RESUME;  -- Root last
ALTER TASK QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.TASK_SFMC_EVENTS_LOAD  RESUME;  -- Separate root


-- ============================================================================
-- VERIFY TASKS
-- ============================================================================
SHOW TASKS IN SCHEMA QA_FIPSAR_PHI_HUB.PHI_CORE;
SHOW TASKS IN SCHEMA QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS;


-- ============================================================================
-- SUSPEND TASKS
-- FIX-05: This entire block is commented out so running the file end-to-end
--         does NOT immediately suspend all tasks after resuming them.
--         Uncomment and run ONLY when you intentionally want to pause the pipeline.
-- ============================================================================

-- USE DATABASE QA_FIPSAR_PHI_HUB;
-- USE SCHEMA PHI_CORE;

-- ALTER TASK TASK_PHI_ORCHESTRATOR                                SUSPEND;  -- Root first
-- ALTER TASK TASK_PHI_IDENTITY_LOAD                               SUSPEND;
-- ALTER TASK TASK_BRONZE_LOAD                                     SUSPEND;
-- ALTER TASK TASK_SILVER_LOAD                                     SUSPEND;
-- ALTER TASK TASK_SILVER_DQ_NOTIFICATION                          SUSPEND;
-- ALTER TASK TASK_GOLD_LOAD                                       SUSPEND;
-- ALTER TASK TASK_SFMC_OUTBOUND_EXPORT                            SUSPEND;
-- ALTER TASK QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.TASK_SFMC_EVENTS_LOAD SUSPEND;
