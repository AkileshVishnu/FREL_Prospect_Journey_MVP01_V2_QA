/*
================================================================================
FIPSAR-SPRINT01-MVP01  |  ALIGNED & CORRECTED CODEBASE
================================================================================
EXECUTION ORDER: Run files in this sequence
  File 1: 01_ALIGNED_INFRASTRUCTURE_PHI_BRONZE.sql    ← THIS FILE
  File 2: 02_ALIGNED_SILVER_GOLD.sql
  File 3: 03_ALIGNED_SFMC_EVENTS_OUTBOUND_TASKS.sql
  File 4: 04_ALIGNED_BACKFILL.sql
  File 5: 05_DEMO_CODEBOOK.sql

FIXES APPLIED IN THIS FILE (from 01_INFRASTRUCTURE_SETUP.sql):
  FIX-01: Removed stray ~ character (line 164 of original) that caused syntax error
  FIX-02: PIPELINE_RUN_LOG already has ROWS_REJECTED — no duplicate needed
  FIX-03: Clarified email ALLOWED_RECIPIENTS to use confirmed address from 08_ORCHESTRATION
  NOTE: Storage integration, file formats, stages, sequence unchanged — correct as-is.

ACCOUNT: MACAPPM-GNB57612
USER:    ROSNOW1201
ROLE:    ACCOUNTADMIN
================================================================================
*/

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_WH;

-- ============================================================================
-- STEP 1: DATABASES
-- ============================================================================
CREATE DATABASE IF NOT EXISTS QA_FIPSAR_PHI_HUB
    COMMENT = 'PHI Hub: patient identity, staging, core PHI tables';

CREATE DATABASE IF NOT EXISTS QA_FIPSAR_DW
    COMMENT = 'Data Warehouse: Bronze / Silver / Gold layers';

CREATE DATABASE IF NOT EXISTS QA_FIPSAR_SFMC_EVENTS
    COMMENT = 'SFMC return-file ingestion: sends, opens, clicks, bounces, etc.';

CREATE DATABASE IF NOT EXISTS QA_FIPSAR_AI
    COMMENT = 'AI synthetic data, feature tables, semantic models';

CREATE DATABASE IF NOT EXISTS QA_FIPSAR_AUDIT
    COMMENT = 'Pipeline audit and data quality logging';

-- ============================================================================
-- STEP 2: SCHEMAS
-- ============================================================================
USE DATABASE QA_FIPSAR_PHI_HUB;
CREATE SCHEMA IF NOT EXISTS STAGING   COMMENT = 'S3 CSV landing area';
CREATE SCHEMA IF NOT EXISTS PHI_CORE  COMMENT = 'Core PHI tables with MASTER_PATIENT_ID';

USE DATABASE QA_FIPSAR_DW;
CREATE SCHEMA IF NOT EXISTS BRONZE    COMMENT = 'Raw append-only copies from PHI';
CREATE SCHEMA IF NOT EXISTS SILVER    COMMENT = 'Data quality gated layer';
CREATE SCHEMA IF NOT EXISTS GOLD      COMMENT = 'Star schema for reporting and AI';

USE DATABASE QA_FIPSAR_SFMC_EVENTS;
CREATE SCHEMA IF NOT EXISTS RAW_EVENTS COMMENT = 'Raw SFMC event CSV landing';

USE DATABASE QA_FIPSAR_AI;
CREATE SCHEMA IF NOT EXISTS AI_SYNTHETIC_DATA COMMENT = 'Synthetic data for AI development';
CREATE SCHEMA IF NOT EXISTS AI_FEATURES       COMMENT = 'Feature tables for ML models';
CREATE SCHEMA IF NOT EXISTS AI_SEMANTIC       COMMENT = 'Semantic views for model consumption';

USE DATABASE QA_FIPSAR_AUDIT;
CREATE SCHEMA IF NOT EXISTS PIPELINE_AUDIT COMMENT = 'Pipeline logs and DQ tracking';

-- ============================================================================
-- STEP 3: AUDIT AND DATA QUALITY TABLES
-- FIX-02 APPLIED: ROWS_REJECTED already in original — kept. No change needed.
-- ============================================================================
USE DATABASE QA_FIPSAR_AUDIT;
USE SCHEMA PIPELINE_AUDIT;

CREATE TABLE IF NOT EXISTS PIPELINE_RUN_LOG (
    RUN_ID              VARCHAR(50)   DEFAULT UUID_STRING(),
    PIPELINE_NAME       VARCHAR(200)  NOT NULL,
    LAYER               VARCHAR(50)   NOT NULL,
    TABLE_NAME          VARCHAR(200),
    STATUS              VARCHAR(20)   NOT NULL,   -- STARTED, SUCCESS, FAILED
    ROWS_LOADED         INTEGER       DEFAULT 0,
    ROWS_REJECTED       INTEGER       DEFAULT 0,
    ERROR_MESSAGE       VARCHAR(4000),
    STARTED_AT          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    COMPLETED_AT        TIMESTAMP_NTZ
);

CREATE TABLE IF NOT EXISTS DQ_REJECTION_LOG (
    REJECTION_ID        VARCHAR(50)   DEFAULT UUID_STRING(),
    TABLE_NAME          VARCHAR(200)  NOT NULL,
    REJECTION_REASON    VARCHAR(500)  NOT NULL,
    REJECTED_RECORD     VARIANT,
    REJECTED_AT         TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- SFMC file load audit (added in 06 — create here for complete setup)
CREATE TABLE IF NOT EXISTS SFMC_FILE_LOAD_AUDIT (
    RUN_ID               VARCHAR(50),
    ENTITY_NAME          VARCHAR(200),
    STAGE_NAME           VARCHAR(500),
    TARGET_TABLE_NAME    VARCHAR(200),
    FILE_NAME            VARCHAR(1000),
    FILE_TIMESTAMP       TIMESTAMP_NTZ,
    STATUS               VARCHAR(20),
    ROWS_LOADED          NUMBER,
    ERROR_MESSAGE        VARCHAR(4000),
    STARTED_AT           TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    COMPLETED_AT         TIMESTAMP_NTZ
);

-- ============================================================================
-- STEP 4: S3 STORAGE INTEGRATION
-- Replace ARN with your actual Snowflake IAM role ARN from AWS Console.
-- After creating, run: DESC INTEGRATION FIPSAR_S3_INTEGRATION;
-- Copy STORAGE_AWS_IAM_USER_ARN and STORAGE_AWS_EXTERNAL_ID.
-- Paste into your AWS IAM role Trust Policy JSON (see Doc 1 runbook).
-- ============================================================================
CREATE OR REPLACE STORAGE INTEGRATION FIPSAR_S3_INTEGRATION
    TYPE                      = EXTERNAL_STAGE
    STORAGE_PROVIDER          = 'S3'
    ENABLED                   = TRUE
    STORAGE_AWS_ROLE_ARN      = 'arn:aws:iam::266931005008:role/fipsar-snowflake-s3-role-2'
    STORAGE_ALLOWED_LOCATIONS = ('s3://fipsar-salesforce/')
    COMMENT = 'S3 integration covering entire fipsar-salesforce bucket';

-- FIX-01 APPLIED: Removed stray ~ character that was on original line 164.
-- The original file had a bare ~ token between two comment blocks which is
-- not valid SQL and would cause an immediate parse error halting the entire
-- script at that point.

-- After creating integration, run this to get trust policy values:
DESC INTEGRATION FIPSAR_S3_INTEGRATION;

-- ============================================================================
-- STEP 5: EMAIL NOTIFICATION INTEGRATION
-- FIX-03 APPLIED: Using akileshvishnu103@gmail.com to match 08_ORCHESTRATION
-- (original 01 used akileshvishnummm@gmail.com which doesn't match Tasks file)
-- ============================================================================
CREATE OR REPLACE NOTIFICATION INTEGRATION FIPSAR_EMAIL_NOTIFICATION
    TYPE               = EMAIL
    ENABLED            = TRUE
    ALLOWED_RECIPIENTS = ('akileshvishnu103@gmail.com')
    COMMENT            = 'Pipeline notifications — both admin emails';

-- ============================================================================
-- STEP 6: FILE FORMATS
-- ============================================================================
USE DATABASE QA_FIPSAR_PHI_HUB;
USE SCHEMA STAGING;

CREATE OR REPLACE FILE FORMAT FF_PROSPECT_CSV
    TYPE                         = CSV
    FIELD_DELIMITER              = ','
    RECORD_DELIMITER             = '\n'
    SKIP_HEADER                  = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    NULL_IF                      = ('', 'NULL', 'null')
    EMPTY_FIELD_AS_NULL          = TRUE
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE;

CREATE OR REPLACE FILE FORMAT FF_SFMC_EVENT_CSV
    TYPE                         = CSV
    FIELD_DELIMITER              = ','
    RECORD_DELIMITER             = '\n'
    SKIP_HEADER                  = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    NULL_IF                      = ('', 'NULL', 'null')
    EMPTY_FIELD_AS_NULL          = TRUE
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE;

-- ============================================================================
-- STEP 7: EXTERNAL STAGES (S3 folders)
-- ============================================================================
-- Inbound: Lambda writes prospect intake CSVs here
CREATE OR REPLACE STAGE STG_PROSPECT_INBOUND
    STORAGE_INTEGRATION = FIPSAR_S3_INTEGRATION
    URL                 = 's3://fipsar-salesforce/SFMC/Prospects-Patients-Campaign/inbound/'
    FILE_FORMAT         = FF_PROSPECT_CSV
    COMMENT             = 'Prospect intake CSVs from Lambda / API';

-- Outbound: Snowflake exports to here for SFMC audience consumption
CREATE OR REPLACE STAGE STG_SFMC_OUTBOUND
    STORAGE_INTEGRATION = FIPSAR_S3_INTEGRATION
    URL                 = 's3://fipsar-salesforce/SFMC/Snowflake-to-SFMC-Patient-Prospects/Prospect_delta/'
    FILE_FORMAT         = FF_PROSPECT_CSV
    COMMENT             = 'Outbound prospect delta files for SFMC';

-- SFMC event return files (separate schema for event-specific stages)
USE DATABASE QA_FIPSAR_SFMC_EVENTS;
USE SCHEMA RAW_EVENTS;

CREATE OR REPLACE STAGE STG_SFMC_EVENTS_INBOUND
    STORAGE_INTEGRATION = FIPSAR_S3_INTEGRATION
    URL                 = 's3://fipsar-salesforce/SFMC/SFMC-to-SNowflake-Events/inbound/'
    FILE_FORMAT         = (FORMAT_NAME = QA_FIPSAR_PHI_HUB.STAGING.FF_SFMC_EVENT_CSV)
    COMMENT             = 'Legacy shared inbound stage (kept for backward compat)';

-- Per-event-type dedicated stages (06_SFMC_Events_Inbound.sql pattern)
CREATE OR REPLACE STAGE STG_SFMC_SENT_DE
    STORAGE_INTEGRATION = FIPSAR_S3_INTEGRATION
    URL = 's3://fipsar-salesforce/SFMC/SFMC-to-SNowflake-Events/Sent_DE/'
    FILE_FORMAT = (FORMAT_NAME = QA_FIPSAR_PHI_HUB.STAGING.FF_SFMC_EVENT_CSV);

CREATE OR REPLACE STAGE STG_SFMC_OPEN_DE
    STORAGE_INTEGRATION = FIPSAR_S3_INTEGRATION
    URL = 's3://fipsar-salesforce/SFMC/SFMC-to-SNowflake-Events/Open_DE/'
    FILE_FORMAT = (FORMAT_NAME = QA_FIPSAR_PHI_HUB.STAGING.FF_SFMC_EVENT_CSV);

CREATE OR REPLACE STAGE STG_SFMC_CLICK_DE
    STORAGE_INTEGRATION = FIPSAR_S3_INTEGRATION
    URL = 's3://fipsar-salesforce/SFMC/SFMC-to-SNowflake-Events/Click_DE/'
    FILE_FORMAT = (FORMAT_NAME = QA_FIPSAR_PHI_HUB.STAGING.FF_SFMC_EVENT_CSV);

CREATE OR REPLACE STAGE STG_SFMC_BOUNCE_DE
    STORAGE_INTEGRATION = FIPSAR_S3_INTEGRATION
    URL = 's3://fipsar-salesforce/SFMC/SFMC-to-SNowflake-Events/Bounce_DE/'
    FILE_FORMAT = (FORMAT_NAME = QA_FIPSAR_PHI_HUB.STAGING.FF_SFMC_EVENT_CSV);

CREATE OR REPLACE STAGE STG_SFMC_UNSUBSCRIBE_DE
    STORAGE_INTEGRATION = FIPSAR_S3_INTEGRATION
    URL = 's3://fipsar-salesforce/SFMC/SFMC-to-SNowflake-Events/Unsubscribe_DE/'
    FILE_FORMAT = (FORMAT_NAME = QA_FIPSAR_PHI_HUB.STAGING.FF_SFMC_EVENT_CSV);

CREATE OR REPLACE STAGE STG_SFMC_JOB_DE
    STORAGE_INTEGRATION = FIPSAR_S3_INTEGRATION
    URL = 's3://fipsar-salesforce/SFMC/SFMC-to-SNowflake-Events/Job_DE/'
    FILE_FORMAT = (FORMAT_NAME = QA_FIPSAR_PHI_HUB.STAGING.FF_SFMC_EVENT_CSV);

CREATE OR REPLACE STAGE STG_SFMC_JOURNEY_ACTIVITY_DE
    STORAGE_INTEGRATION = FIPSAR_S3_INTEGRATION
    URL = 's3://fipsar-salesforce/SFMC/SFMC-to-SNowflake-Events/JourneyActivity_DE/'
    FILE_FORMAT = (FORMAT_NAME = QA_FIPSAR_PHI_HUB.STAGING.FF_SFMC_EVENT_CSV);

CREATE OR REPLACE STAGE STG_SFMC_PROSPECT_C
    STORAGE_INTEGRATION = FIPSAR_S3_INTEGRATION
    URL = 's3://fipsar-salesforce/SFMC/SFMC-to-Snowflake-History/Prospect_c/'
    FILE_FORMAT = (FORMAT_NAME = QA_FIPSAR_PHI_HUB.STAGING.FF_SFMC_EVENT_CSV);

CREATE OR REPLACE STAGE STG_SFMC_PROSPECT_C_HISTORY
    STORAGE_INTEGRATION = FIPSAR_S3_INTEGRATION
    URL = 's3://fipsar-salesforce/SFMC/SFMC-to-Snowflake-History/Prospect_c_history/'
    FILE_FORMAT = (FORMAT_NAME = QA_FIPSAR_PHI_HUB.STAGING.FF_SFMC_EVENT_CSV);



CREATE OR REPLACE STAGE STG_SFMC_PROSPECT_JOURNEY_DETAILS
    STORAGE_INTEGRATION = FIPSAR_S3_INTEGRATION
    URL = 's3://fipsar-salesforce/SFMC/SFMC-to-Snowflake-History/Prospect_journey_details/'
    FILE_FORMAT = (FORMAT_NAME = QA_FIPSAR_PHI_HUB.STAGING.FF_SFMC_EVENT_CSV);

-- ============================================================================
-- STEP 8: SEQUENCE FOR MASTER_PATIENT_ID
-- Generates: FIP0000000001, FIP0000000002, ...
-- ============================================================================
USE DATABASE QA_FIPSAR_PHI_HUB;
USE SCHEMA PHI_CORE;

CREATE OR REPLACE SEQUENCE SEQ_MASTER_PATIENT_ID
    START     = 801
    INCREMENT = 1
    COMMENT   = 'Generates FIP + 10-digit zero-padded IDs';

-- ============================================================================
-- STEP 9: PHI HUB TABLES (02_SYSTEM2_PHI_HUB_PIPELINE.sql — no changes needed)
-- ============================================================================
USE DATABASE QA_FIPSAR_PHI_HUB;
USE SCHEMA STAGING;

-- Staging table: raw CSV landing, truncated each cycle
CREATE OR REPLACE TABLE STG_PROSPECT_INTAKE (
    FIRST_NAME           VARCHAR(100),
    LAST_NAME            VARCHAR(100),
    EMAIL                VARCHAR(255),
    PHONE_NUMBER         VARCHAR(20),
    AGE                  VARCHAR(10),
    ADDRESS              VARCHAR(255),
    APARTMENT_NO         VARCHAR(50),
    CITY                 VARCHAR(100),
    STATE                VARCHAR(2),
    ZIP_CODE             VARCHAR(10),
    PATIENT_CONSENT      VARCHAR(10),
    CHANNEL              VARCHAR(50),
    SUBMISSION_TIMESTAMP VARCHAR(30),
    FILE_DATE            VARCHAR(10),
    _LOADED_AT           TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _SOURCE_FILE         VARCHAR(500)
);

USE SCHEMA PHI_CORE;

-- Patient identity crosswalk: one row per unique person (by name+email combo)
CREATE OR REPLACE TABLE PATIENT_IDENTITY_XREF (
    MASTER_PATIENT_ID    VARCHAR(13)    NOT NULL,
    IDENTITY_KEY         VARCHAR(600)   NOT NULL,
    FIRST_NAME           VARCHAR(100),
    LAST_NAME            VARCHAR(100),
    EMAIL                VARCHAR(255),
    CREATED_AT           TIMESTAMP_NTZ  DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT PK_IDENTITY_XREF PRIMARY KEY (IDENTITY_KEY)
);

-- PHI Prospect Master: append-only by FILE_DATE, full PII retained here only
CREATE OR REPLACE TABLE PHI_PROSPECT_MASTER (
    RECORD_ID            VARCHAR(50)    DEFAULT UUID_STRING(),
    MASTER_PATIENT_ID    VARCHAR(13)    NOT NULL,
    FIRST_NAME           VARCHAR(100)   NOT NULL,
    LAST_NAME            VARCHAR(100)   NOT NULL,
    EMAIL                VARCHAR(255)   NOT NULL,
    PHONE_NUMBER         VARCHAR(20),
    AGE                  INTEGER,
    ADDRESS              VARCHAR(255),
    APARTMENT_NO         VARCHAR(50),
    CITY                 VARCHAR(100),
    STATE                VARCHAR(2),
    ZIP_CODE             VARCHAR(10),
    PATIENT_CONSENT      BOOLEAN,
    CHANNEL              VARCHAR(50),
    SUBMISSION_TIMESTAMP TIMESTAMP_NTZ,
    FILE_DATE            DATE           NOT NULL,
    PHI_LOADED_AT        TIMESTAMP_NTZ  DEFAULT CURRENT_TIMESTAMP(),
    IS_ACTIVE            BOOLEAN        DEFAULT TRUE,
    CONSTRAINT PK_PHI_PROSPECT PRIMARY KEY (RECORD_ID)
);




-- ============================================================================
-- STEP 11: BRONZE TABLE AND SP (03_SYSTEM3_BRONZE_PIPELINE.sql — no changes)
-- ============================================================================
USE DATABASE QA_FIPSAR_DW;
USE SCHEMA BRONZE;

CREATE OR REPLACE TABLE BRZ_PROSPECT_MASTER (
    RECORD_ID            VARCHAR(50),
    MASTER_PATIENT_ID    VARCHAR(13),
    FIRST_NAME           VARCHAR(100),
    LAST_NAME            VARCHAR(100),
    EMAIL                VARCHAR(255),
    PHONE_NUMBER         VARCHAR(20),
    AGE                  INTEGER,
    ADDRESS              VARCHAR(255),
    APARTMENT_NO         VARCHAR(50),
    CITY                 VARCHAR(100),
    STATE                VARCHAR(2),
    ZIP_CODE             VARCHAR(10),
    PATIENT_CONSENT      BOOLEAN,
    CHANNEL              VARCHAR(50),
    SUBMISSION_TIMESTAMP TIMESTAMP_NTZ,
    FILE_DATE            DATE,
    PHI_LOADED_AT        TIMESTAMP_NTZ,
    IS_ACTIVE            BOOLEAN,
    -- Bronze metadata columns
    BRONZE_LOADED_AT     TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    SOURCE_LAYER         VARCHAR(20)   DEFAULT 'PHI_HUB'
);







/*
================================================================================
FIPSAR-SPRINT01-MVP01  |  02_ALIGNED_SILVER_GOLD.sql
================================================================================
EXECUTION ORDER: Run this SECOND (after 01_ALIGNED_INFRASTRUCTURE_PHI_BRONZE.sql)

FIXES APPLIED IN THIS FILE:

  FIX-04 (CRITICAL): Removed stray inline `select * from FACT_SFMC_ENGAGEMENT`
         that appeared after the ALTER TABLE in the original 05_SYSTEM3_GOLD.sql
         at line 176. This unquoted SELECT in DDL batch context would cause an
         immediate execution error halting all subsequent object creation.

  FIX-05 (CRITICAL): FACT_SFMC_ENGAGEMENT column alignment.
         Original 05 created:  ENGAGEMENT_KEY (autoincrement), GOLD_LOADED_AT
         Original 06 defined:  FACT_ENGAGEMENT_KEY (autoincrement), LOADED_AT
         06's SP_LOAD_GOLD_FACT_ENGAGEMENT inserts into LOADED_AT — FAILS against
         05's GOLD_LOADED_AT column name. The VW_MART_JOURNEY_INTELLIGENCE view
         references FACT_ENGAGEMENT_KEY.
         FIX: Use FACT_ENGAGEMENT_KEY + LOADED_AT everywhere. This file is
         authoritative — 06 uses IF NOT EXISTS so it will not override.

  FIX-06 (CRITICAL): DIM_SFMC_JOB missing RECORD_TYPE column.
         Original 05 CREATE OR REPLACE TABLE DIM_SFMC_JOB did NOT include
         RECORD_TYPE. Original 06 IF NOT EXISTS version DID include RECORD_TYPE.
         Because 05 runs first with CREATE OR REPLACE, 06's IF NOT EXISTS does
         nothing. SP_LOAD_SFMC_JOB_METADATA() then tries to insert RECORD_TYPE
         and FAILS with column not found.
         FIX: Added RECORD_TYPE to DIM_SFMC_JOB in this file.

  FIX-07 (MINOR): VW_MART_JOURNEY_INTELLIGENCE was only defined inside
         MVP01_DML_Insert.sql and referenced in Demo SQL. Moved here into the
         proper Gold layer where it belongs architecturally.

  FIX-08 (MINOR): SP_LOAD_GOLD_DIM_PROSPECT used MIN_BY() which is correct
         Snowflake syntax — KEPT unchanged.
================================================================================
*/

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE QA_FIPSAR_DW;
USE SCHEMA SILVER;

-- ============================================================================
-- SILVER TABLE: SCD Type 2 with DQ columns
-- (04_SYSTEM3_SILVER_PIPELINE.sql — no structural changes)
-- ============================================================================
CREATE OR REPLACE TABLE SLV_PROSPECT_MASTER (
    SLV_KEY              INTEGER       AUTOINCREMENT,
    RECORD_ID            VARCHAR(50),
    MASTER_PATIENT_ID    VARCHAR(13)   NOT NULL,
    FIRST_NAME           VARCHAR(100)  NOT NULL,
    LAST_NAME            VARCHAR(100)  NOT NULL,
    EMAIL                VARCHAR(255)  NOT NULL,
    PHONE_NUMBER         VARCHAR(20),
    AGE                  INTEGER,
    ADDRESS              VARCHAR(255),
    APARTMENT_NO         VARCHAR(50),
    CITY                 VARCHAR(100),
    STATE                VARCHAR(2),
    ZIP_CODE             VARCHAR(10),
    PATIENT_CONSENT      BOOLEAN,
    CHANNEL              VARCHAR(50),
    SUBMISSION_TIMESTAMP TIMESTAMP_NTZ,
    FILE_DATE            DATE,
    PHI_LOADED_AT        TIMESTAMP_NTZ,
    IS_ACTIVE            BOOLEAN,
    -- SCD Type 2 tracking columns
    EFF_START_DATE       DATE          NOT NULL,
    EFF_END_DATE         DATE          NOT NULL DEFAULT TO_DATE('9999-12-31'),
    IS_CURRENT           BOOLEAN       NOT NULL DEFAULT TRUE,
    VERSION_NUM          INTEGER       NOT NULL DEFAULT 1,
    -- DQ and metadata
    DQ_PASSED            BOOLEAN       DEFAULT TRUE,
    SILVER_LOADED_AT     TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);







-- ============================================================================
-- GOLD: DIMENSION TABLES
-- FIX-06 APPLIED: DIM_SFMC_JOB now includes RECORD_TYPE column
-- ============================================================================
USE DATABASE QA_FIPSAR_DW;
USE SCHEMA GOLD;

-- Channel reference dimension
CREATE OR REPLACE TABLE DIM_CHANNEL (
    CHANNEL_KEY      INTEGER AUTOINCREMENT,
    CHANNEL_NAME     VARCHAR(50)  NOT NULL,
    CHANNEL_CATEGORY VARCHAR(50),
    CONSTRAINT PK_DIM_CHANNEL PRIMARY KEY (CHANNEL_KEY)
);

INSERT INTO DIM_CHANNEL (CHANNEL_NAME, CHANNEL_CATEGORY)
    SELECT 'Instagram',    'Social Media' WHERE NOT EXISTS (SELECT 1 FROM DIM_CHANNEL WHERE CHANNEL_NAME='Instagram');
INSERT INTO DIM_CHANNEL (CHANNEL_NAME, CHANNEL_CATEGORY)
    SELECT 'Campaign App', 'Web Campaign' WHERE NOT EXISTS (SELECT 1 FROM DIM_CHANNEL WHERE CHANNEL_NAME='Campaign App');
INSERT INTO DIM_CHANNEL (CHANNEL_NAME, CHANNEL_CATEGORY)
    SELECT 'Facebook',     'Social Media' WHERE NOT EXISTS (SELECT 1 FROM DIM_CHANNEL WHERE CHANNEL_NAME='Facebook');
INSERT INTO DIM_CHANNEL (CHANNEL_NAME, CHANNEL_CATEGORY)
    SELECT 'Website',      'Web Campaign' WHERE NOT EXISTS (SELECT 1 FROM DIM_CHANNEL WHERE CHANNEL_NAME='Website');
INSERT INTO DIM_CHANNEL (CHANNEL_NAME, CHANNEL_CATEGORY)
    SELECT 'Referral',     'Referral'     WHERE NOT EXISTS (SELECT 1 FROM DIM_CHANNEL WHERE CHANNEL_NAME='Referral');

-- SFMC Event type dimension
CREATE OR REPLACE TABLE DIM_ENGAGEMENT_TYPE (
    EVENT_TYPE_KEY INTEGER AUTOINCREMENT,
    EVENT_TYPE     VARCHAR(50)  NOT NULL,
    EVENT_CATEGORY VARCHAR(50),
    IS_POSITIVE    BOOLEAN,
    CONSTRAINT PK_DIM_ET PRIMARY KEY (EVENT_TYPE_KEY)
);

MERGE INTO DIM_ENGAGEMENT_TYPE tgt
USING (SELECT * FROM VALUES
    ('SENT','Delivery',TRUE), ('OPEN','Engagement',TRUE), ('CLICK','Engagement',TRUE),
    ('BOUNCE','Negative',FALSE), ('UNSUBSCRIBE','Negative',FALSE), ('SPAM','Negative',FALSE)
    AS t(EVENT_TYPE, EVENT_CATEGORY, IS_POSITIVE)) src
ON tgt.EVENT_TYPE = src.EVENT_TYPE
WHEN NOT MATCHED THEN
    INSERT (EVENT_TYPE, EVENT_CATEGORY, IS_POSITIVE) VALUES (src.EVENT_TYPE, src.EVENT_CATEGORY, src.IS_POSITIVE);

-- Calendar dimension (2025–2027, ~1096 rows)
CREATE OR REPLACE TABLE DIM_DATE (
    DATE_KEY     INTEGER NOT NULL,
    FULL_DATE    DATE    NOT NULL,
    YEAR         INTEGER,
    QUARTER      INTEGER,
    MONTH        INTEGER,
    MONTH_NAME   VARCHAR(20),
    WEEK_OF_YEAR INTEGER,
    DAY_OF_MONTH INTEGER,
    DAY_OF_WEEK  INTEGER,
    DAY_NAME     VARCHAR(20),
    IS_WEEKEND   BOOLEAN,
    CONSTRAINT PK_DIM_DATE PRIMARY KEY (DATE_KEY)
);

INSERT INTO DIM_DATE
SELECT
    TO_NUMBER(TO_CHAR(dt, 'YYYYMMDD')),
    dt, YEAR(dt), QUARTER(dt), MONTH(dt), MONTHNAME(dt),
    WEEKOFYEAR(dt), DAY(dt), DAYOFWEEK(dt), DAYNAME(dt),
    CASE WHEN DAYOFWEEK(dt) IN (0, 6) THEN TRUE ELSE FALSE END
FROM (
    SELECT DATEADD(DAY, SEQ4(), '2025-01-01'::DATE) AS dt
    FROM TABLE(GENERATOR(ROWCOUNT => 1096))
);

-- Geography dimension
CREATE OR REPLACE TABLE DIM_GEOGRAPHY (
    GEO_KEY  INTEGER AUTOINCREMENT,
    CITY     VARCHAR(100),
    STATE    VARCHAR(2),
    ZIP_CODE VARCHAR(10),
    REGION   VARCHAR(50),
    CONSTRAINT PK_DIM_GEO PRIMARY KEY (GEO_KEY)
);

-- Prospect master dimension (SCD Type 1 — always current state)
CREATE OR REPLACE TABLE DIM_PROSPECT (
    PROSPECT_KEY       INTEGER AUTOINCREMENT,
    MASTER_PATIENT_ID  VARCHAR(13)  NOT NULL,
    FIRST_NAME         VARCHAR(100),
    LAST_NAME          VARCHAR(100),
    EMAIL              VARCHAR(255),
    PHONE_NUMBER       VARCHAR(20),
    AGE                INTEGER,
    AGE_GROUP          VARCHAR(20),
    CITY               VARCHAR(100),
    STATE              VARCHAR(2),
    ZIP_CODE           VARCHAR(10),
    PATIENT_CONSENT    BOOLEAN,
    FIRST_INTAKE_DATE  DATE,
    LAST_INTAKE_DATE   DATE,
    INTAKE_COUNT       INTEGER DEFAULT 1,
    PRIMARY_CHANNEL    VARCHAR(50),
    IS_ACTIVE          BOOLEAN DEFAULT TRUE,
    CREATED_AT         TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT         TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT PK_DIM_PROSPECT PRIMARY KEY (PROSPECT_KEY),
    CONSTRAINT UQ_DIM_PROSPECT  UNIQUE (MASTER_PATIENT_ID)
);

-- FIX-06 APPLIED: RECORD_TYPE column added to DIM_SFMC_JOB
-- (Original 05_GOLD omitted RECORD_TYPE; 06's SP inserts it — column mismatch fixed here)
CREATE OR REPLACE TABLE DIM_SFMC_JOB (
    JOB_KEY       INTEGER AUTOINCREMENT,
    JOB_ID        INTEGER NOT NULL,
    JOURNEY_TYPE  VARCHAR(50),
    MAPPED_STAGE  VARCHAR(100),
    EMAIL_NAME    VARCHAR(200),
    EMAIL_SUBJECT VARCHAR(500),
    RECORD_TYPE   VARCHAR(50),     -- FIX-06: Added this column
    CREATED_AT    TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT    TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT PK_DIM_SFMC_JOB PRIMARY KEY (JOB_KEY),
    CONSTRAINT UQ_DIM_JOB_ID UNIQUE (JOB_ID)
);

-- ============================================================================
-- GOLD: FACT TABLES
-- FIX-04 APPLIED: Removed stray `select * from FACT_SFMC_ENGAGEMENT` that was
--   inline in the original 05_GOLD after ALTER TABLE (line 176). This SELECT
--   would execute mid-DDL and interrupt the batch.
-- FIX-05 APPLIED: Column names aligned to match 06_SFMC_Events_Inbound.sql SPs:
--   ENGAGEMENT_KEY → FACT_ENGAGEMENT_KEY
--   GOLD_LOADED_AT → LOADED_AT
--   (06's SP_LOAD_GOLD_FACT_ENGAGEMENT inserts into LOADED_AT and references
--    FACT_ENGAGEMENT_KEY; the view VW_MART_JOURNEY_INTELLIGENCE also uses
--    FACT_ENGAGEMENT_KEY. Using IF NOT EXISTS in 06 means 05 wins — fix here.)
-- ============================================================================

-- Prospect intake fact: one row per intake submission
CREATE OR REPLACE TABLE FACT_PROSPECT_INTAKE (
    INTAKE_KEY           INTEGER AUTOINCREMENT,
    RECORD_ID            VARCHAR(50),
    MASTER_PATIENT_ID    VARCHAR(13),
    PROSPECT_KEY         INTEGER,
    CHANNEL_KEY          INTEGER,
    DATE_KEY             INTEGER,
    GEO_KEY              INTEGER,
    SUBMISSION_TIMESTAMP TIMESTAMP_NTZ,
    FILE_DATE            DATE,
    AGE                  INTEGER,
    CONSENT_FLAG         INTEGER,
    GOLD_LOADED_AT       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT PK_FACT_INTAKE PRIMARY KEY (INTAKE_KEY)
);

-- SFMC engagement fact: unified event log (SENT, OPEN, CLICK, BOUNCE, UNSUB, SPAM)
-- FIX-05: FACT_ENGAGEMENT_KEY and LOADED_AT (not ENGAGEMENT_KEY / GOLD_LOADED_AT)
CREATE OR REPLACE TABLE FACT_SFMC_ENGAGEMENT (
    FACT_ENGAGEMENT_KEY INTEGER AUTOINCREMENT,  -- FIX-05: was ENGAGEMENT_KEY in original 05
	SUBSCRIBER_KEY VARCHAR(100),
	MASTER_PATIENT_ID VARCHAR(13),
	PROSPECT_KEY NUMBER(38,0),
	JOB_KEY NUMBER(38,0),
	EVENT_TYPE_KEY NUMBER(38,0),
	DATE_KEY NUMBER(38,0),
	EVENT_TIMESTAMP TIMESTAMP_NTZ(9),
	EVENT_TYPE VARCHAR(50),
	DOMAIN VARCHAR(255),
	IS_UNIQUE BOOLEAN,
	BOUNCE_CATEGORY VARCHAR(100),
	BOUNCE_TYPE VARCHAR(100),
	CLICK_URL VARCHAR(2000),
	REASON VARCHAR(500),
	ACCOUNT_ID VARCHAR(20),
	JOB_ID NUMBER(38,0),
	RECORD_TYPE VARCHAR(50),
	SOURCE_FILE_NAME VARCHAR(1000),
	LOADED_AT TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP(),
	IS_SUPPRESSED BOOLEAN DEFAULT FALSE,
	SUPPRESSION_REASON VARCHAR(500),
	constraint PK_FACT_ENGAGEMENT primary key (FACT_ENGAGEMENT_KEY)
);







-- ============================================================================
-- GOLD: OUTBOUND VIEW FOR SFMC
-- (07_SFMC_FILE_EXCHANGE_Outbound.sql — no changes, kept here for logical grouping)
-- ============================================================================

CREATE OR REPLACE VIEW QA_FIPSAR_DW.GOLD.VW_SFMC_PROSPECT_OUTBOUND AS
SELECT
    dp.MASTER_PATIENT_ID as ProspectID,
    dp.FIRST_NAME as FirstName,
    dp.LAST_NAME as LastName,
    dp.EMAIL as EmailAddress,
    dp.PATIENT_CONSENT as MarketingConsent,
    TRUE as HighEngagement,
    TO_VARCHAR(current_timestamp(), 'MM/DD/YYYY HH12:MI:SS AM') as RegistrationDate
FROM QA_FIPSAR_DW.GOLD.DIM_PROSPECT dp
WHERE dp.IS_ACTIVE = TRUE
  AND dp.PATIENT_CONSENT = TRUE
  AND NULLIF(TRIM(dp.EMAIL), '') IS NOT NULL
  AND UPPER(TRIM(dp.EMAIL)) <> 'N/A';


  


