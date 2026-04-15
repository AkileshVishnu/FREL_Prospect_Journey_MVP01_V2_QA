
-- =====================================================================================
-- FIPSAR SFMC EVENTS END-TO-END PIPELINE
-- =====================================================================================
-- PURPOSE:
--   1) Dedicated external stages per S3 folder (replaces shared inbound + PATTERN).
--   2) Load only unseen delta files, ordered by suffix timestamp in filename.
--   3) Preserve existing RAW/GOLD downstream contracts.
--   4) Additional RAW landing tables for detailed SFMC datasets.
--
-- RAW tables feeding GOLD:
--   RAW_SFMC_SENT, RAW_SFMC_OPENS, RAW_SFMC_CLICKS, RAW_SFMC_BOUNCES,
--   RAW_SFMC_UNSUBSCRIBES, RAW_SFMC_JOB_METADATA, RAW_SFMC_SPAM
--
-- Additional detailed RAW tables:
--   RAW_SFMC_JOB_DE_DETAIL, RAW_SFMC_UNSUBSCRIBE_DE_DETAIL,
--   RAW_SFMC_PROSPECT_C, RAW_SFMC_PROSPECT_C_HISTORY,
--   RAW_SFMC_PROSPECT_JOURNEY_DETAILS, RAW_SFMC_JOURNEY_ACTIVITY_GENERIC
--
-- CSV encoding: UTF-16 (primary), UTF-8 (alternate).
-- Delta logic: LIST @stage -> parse latest filename timestamp -> COPY new files.
-- Idempotent: uses IF NOT EXISTS for DDL, MERGE for seeds, NOT EXISTS for inserts.
-- =====================================================================================

-- =====================================================================================
-- PART 1: SESSION CONTEXT
-- =====================================================================================

USE DATABASE QA_FIPSAR_SFMC_EVENTS;
USE SCHEMA RAW_EVENTS;

-- =====================================================================================
-- PART 2: FILE FORMATS
-- =====================================================================================

CREATE OR REPLACE FILE FORMAT QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.FF_SFMC_CSV_UTF16
  TYPE = CSV
  FIELD_DELIMITER = ','
  SKIP_HEADER = 1
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  TRIM_SPACE = TRUE
  EMPTY_FIELD_AS_NULL = TRUE
  NULL_IF = ('NULL', 'null', '')
  ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
  ESCAPE_UNENCLOSED_FIELD = NONE
  ENCODING = 'UTF16LE'
  SKIP_BYTE_ORDER_MARK = TRUE;

CREATE OR REPLACE FILE FORMAT QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.FF_SFMC_CSV_UTF8
  TYPE = CSV
  FIELD_DELIMITER = ','
  SKIP_HEADER = 1
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  TRIM_SPACE = TRUE
  EMPTY_FIELD_AS_NULL = TRUE
  NULL_IF = ('NULL', 'null', '')
  ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
  ESCAPE_UNENCLOSED_FIELD = NONE
  ENCODING = 'UTF8';

-- =====================================================================================
-- PART 3: EXTERNAL STAGES
-- =====================================================================================

CREATE OR REPLACE STAGE QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.STG_SFMC_SENT_DE
  STORAGE_INTEGRATION = FIPSAR_S3_INTEGRATION
  URL = 's3://fipsar-salesforce/SFMC/SFMC-to-SNowflake-Events/Sent_DE/'
  FILE_FORMAT = QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.FF_SFMC_CSV_UTF16
  COMMENT = 'SFMC Sent event files';

  list @QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.STG_SFMC_SENT_DE;

CREATE OR REPLACE STAGE QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.STG_SFMC_UNSUBSCRIBE_DE
  STORAGE_INTEGRATION = FIPSAR_S3_INTEGRATION
  URL = 's3://fipsar-salesforce/SFMC/SFMC-to-SNowflake-Events/Unsubscribe_DE/'
  FILE_FORMAT = QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.FF_SFMC_CSV_UTF16
  COMMENT = 'SFMC Unsubscribe event files';

CREATE OR REPLACE STAGE QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.STG_SFMC_JOURNEY_ACTIVITY_DE
  STORAGE_INTEGRATION = FIPSAR_S3_INTEGRATION
  URL = 's3://fipsar-salesforce/SFMC/SFMC-to-SNowflake-Events/JourneyActivity_DE/'
  FILE_FORMAT = QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.FF_SFMC_CSV_UTF16
  COMMENT = 'SFMC Journey Activity export files';

CREATE OR REPLACE STAGE QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.STG_SFMC_OPEN_DE
  STORAGE_INTEGRATION = FIPSAR_S3_INTEGRATION
  URL = 's3://fipsar-salesforce/SFMC/SFMC-to-SNowflake-Events/Open_DE/'
  FILE_FORMAT = QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.FF_SFMC_CSV_UTF16
  COMMENT = 'SFMC Open event files';

CREATE OR REPLACE STAGE QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.STG_SFMC_CLICK_DE
  STORAGE_INTEGRATION = FIPSAR_S3_INTEGRATION
  URL = 's3://fipsar-salesforce/SFMC/SFMC-to-SNowflake-Events/Click_DE/'
  FILE_FORMAT = QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.FF_SFMC_CSV_UTF16
  COMMENT = 'SFMC Click event files';


CREATE OR REPLACE STAGE QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.STG_SFMC_BOUNCE_DE
  STORAGE_INTEGRATION = FIPSAR_S3_INTEGRATION
  URL = 's3://fipsar-salesforce/SFMC/SFMC-to-SNowflake-Events/Bounce_DE/'
  FILE_FORMAT = QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.FF_SFMC_CSV_UTF16
  COMMENT = 'SFMC Bounce event files';

CREATE OR REPLACE STAGE QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.STG_SFMC_JOB_DE
  STORAGE_INTEGRATION = FIPSAR_S3_INTEGRATION
  URL = 's3://fipsar-salesforce/SFMC/SFMC-to-SNowflake-Events/Job_DE/'
  FILE_FORMAT = QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.FF_SFMC_CSV_UTF16
  COMMENT = 'SFMC Job export files';

CREATE OR REPLACE STAGE QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.STG_SFMC_PROSPECT_C_HISTORY
  STORAGE_INTEGRATION = FIPSAR_S3_INTEGRATION
  URL = 's3://fipsar-salesforce/SFMC/SFMC-to-Snowflake-History/Prospect_c_history/'
  FILE_FORMAT = QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.FF_SFMC_CSV_UTF16
  COMMENT = 'SFMC Prospect_c_history files';

CREATE OR REPLACE STAGE QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.STG_SFMC_PROSPECT_C
  STORAGE_INTEGRATION = FIPSAR_S3_INTEGRATION
  URL = 's3://fipsar-salesforce/SFMC/SFMC-to-Snowflake-History/Prospect_c/'
  FILE_FORMAT = QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.FF_SFMC_CSV_UTF16
  COMMENT = 'SFMC Prospect_c current snapshot files';

CREATE OR REPLACE STAGE QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.STG_SFMC_PROSPECT_JOURNEY_DETAILS
  STORAGE_INTEGRATION = FIPSAR_S3_INTEGRATION
  URL = 's3://fipsar-salesforce/SFMC/SFMC-to-Snowflake-History/Prospect_journey_details/'
  FILE_FORMAT = QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.FF_SFMC_CSV_UTF16
  COMMENT = 'SFMC Prospect journey details files';

-- =====================================================================================
-- PART 4: AUDIT TABLES
-- =====================================================================================

CREATE TABLE IF NOT EXISTS QA_FIPSAR_AUDIT.PIPELINE_AUDIT.PIPELINE_RUN_LOG (
    RUN_ID           VARCHAR,
    PIPELINE_NAME    VARCHAR,
    LAYER            VARCHAR,
    TABLE_NAME       VARCHAR,
    STATUS           VARCHAR,
    ROWS_LOADED      NUMBER,
    ROWS_REJECTED    NUMBER    DEFAULT 0,
    ERROR_MESSAGE    STRING,
    STARTED_AT       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    COMPLETED_AT     TIMESTAMP_NTZ
);

CREATE TABLE IF NOT EXISTS QA_FIPSAR_AUDIT.PIPELINE_AUDIT.SFMC_FILE_LOAD_AUDIT (
    RUN_ID               VARCHAR,
    ENTITY_NAME          VARCHAR,
    STAGE_NAME           VARCHAR,
    TARGET_TABLE_NAME    VARCHAR,
    FILE_NAME            VARCHAR,
    FILE_TIMESTAMP       TIMESTAMP_NTZ,
    STATUS               VARCHAR,
    ROWS_LOADED          NUMBER,
    ERROR_MESSAGE        STRING,
    STARTED_AT           TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    COMPLETED_AT         TIMESTAMP_NTZ
);

CREATE TABLE IF NOT EXISTS QA_FIPSAR_AUDIT.PIPELINE_AUDIT.DQ_REJECTION_LOG (
    REJECTION_ID      VARCHAR(50) DEFAULT UUID_STRING(),
    TABLE_NAME        VARCHAR(200) NOT NULL,
    REJECTION_REASON  VARCHAR(500) NOT NULL,
    REJECTED_RECORD   VARIANT,
    REJECTED_AT       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- =====================================================================================
-- PART 5: RAW TABLES
-- =====================================================================================

CREATE TABLE IF NOT EXISTS QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_SENT (
    ACCOUNT_ID                    VARCHAR(50),
    OYB_ACCOUNT_ID                VARCHAR(50),
    JOB_ID                        INTEGER,
    LIST_ID                       INTEGER,
    BATCH_ID                      INTEGER,
    SUBSCRIBER_ID                 INTEGER,
    SUBSCRIBER_KEY                VARCHAR(100),
    EVENT_DATE                    VARCHAR(100),
    DOMAIN                        VARCHAR(255),
    TRIGGERED_SEND_CUSTOMER_KEY   VARCHAR(255),
    TRIGGERED_SEND_DEF_OBJECT_ID  VARCHAR(255),
    RECORD_TYPE                   VARCHAR(50),
    _SOURCE_FILE_NAME             VARCHAR(1000),
    _SOURCE_ROW_NUMBER            NUMBER,
    _LOADED_AT                    TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_OPENS (
    ACCOUNT_ID                    VARCHAR(50),
    OYB_ACCOUNT_ID                VARCHAR(50),
    JOB_ID                        INTEGER,
    LIST_ID                       INTEGER,
    BATCH_ID                      INTEGER,
    SUBSCRIBER_ID                 INTEGER,
    SUBSCRIBER_KEY                VARCHAR(100),
    EVENT_DATE                    VARCHAR(100),
    DOMAIN                        VARCHAR(255),
    IS_UNIQUE                     VARCHAR(20),
    TRIGGERED_SEND_CUSTOMER_KEY   VARCHAR(255),
    TRIGGERED_SEND_DEF_OBJECT_ID  VARCHAR(255),
    RECORD_TYPE                   VARCHAR(50),
    _SOURCE_FILE_NAME             VARCHAR(1000),
    _SOURCE_ROW_NUMBER            NUMBER,
    _LOADED_AT                    TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_CLICKS (
    ACCOUNT_ID                    VARCHAR(50),
    OYB_ACCOUNT_ID                VARCHAR(50),
    JOB_ID                        INTEGER,
    LIST_ID                       INTEGER,
    BATCH_ID                      INTEGER,
    SUBSCRIBER_ID                 INTEGER,
    SUBSCRIBER_KEY                VARCHAR(100),
    EVENT_DATE                    VARCHAR(100),
    DOMAIN                        VARCHAR(255),
    URL                           VARCHAR(2000),
    LINK_NAME                     VARCHAR(1000),
    LINK_CONTENT                  VARCHAR(2000),
    IS_UNIQUE                     VARCHAR(20),
    TRIGGERED_SEND_DEF_OBJECT_ID  VARCHAR(255),
    TRIGGERED_SEND_CUSTOMER_KEY   VARCHAR(255),
    RECORD_TYPE                   VARCHAR(50),
    _SOURCE_FILE_NAME             VARCHAR(1000),
    _SOURCE_ROW_NUMBER            NUMBER,
    _LOADED_AT                    TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_BOUNCES (
    ACCOUNT_ID                    VARCHAR(50),
    OYB_ACCOUNT_ID                VARCHAR(50),
    JOB_ID                        INTEGER,
    LIST_ID                       INTEGER,
    BATCH_ID                      INTEGER,
    SUBSCRIBER_ID                 INTEGER,
    SUBSCRIBER_KEY                VARCHAR(100),
    EVENT_DATE                    VARCHAR(100),
    DOMAIN                        VARCHAR(255),
    BOUNCE_CATEGORY               VARCHAR(255),
    BOUNCE_TYPE                   VARCHAR(255),
    SMTP_BOUNCE_REASON            VARCHAR(2000),
    TRIGGERED_SEND_CUSTOMER_KEY   VARCHAR(255),
    TRIGGERED_SEND_DEF_OBJECT_ID  VARCHAR(255),
    RECORD_TYPE                   VARCHAR(50),
    _SOURCE_FILE_NAME             VARCHAR(1000),
    _SOURCE_ROW_NUMBER            NUMBER,
    _LOADED_AT                    TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_UNSUBSCRIBES (
    ACCOUNT_ID                    VARCHAR(50),
    SUBSCRIBER_KEY                VARCHAR(100),
    JOB_ID                        INTEGER,
    EVENT_DATE                    VARCHAR(100),
    REASON                        VARCHAR(2000),
    RECORD_TYPE                   VARCHAR(50),
    _SOURCE_FILE_NAME             VARCHAR(1000),
    _SOURCE_ROW_NUMBER            NUMBER,
    _LOADED_AT                    TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_SPAM (
    ACCOUNT_ID                    VARCHAR(50),
    SUBSCRIBER_KEY                VARCHAR(100),
    JOB_ID                        INTEGER,
    EVENT_DATE                    VARCHAR(100),
    COMPLAINT_TYPE                VARCHAR(255),
    RECORD_TYPE                   VARCHAR(50),
    _SOURCE_FILE_NAME             VARCHAR(1000),
    _SOURCE_ROW_NUMBER            NUMBER,
    _LOADED_AT                    TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_JOB_METADATA (
    JOB_ID                        INTEGER,
    JOURNEY_TYPE                  VARCHAR(255),
    MAPPED_STAGE                  VARCHAR(255),
    EMAIL_NAME                    VARCHAR(500),
    EMAIL_SUBJECT                 VARCHAR(1000),
    RECORD_TYPE                   VARCHAR(50),
    _SOURCE_FILE_NAME             VARCHAR(1000),
    _SOURCE_ROW_NUMBER            NUMBER,
    _LOADED_AT                    TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_JOB_DE_DETAIL (
    JOB_ID                              VARCHAR,
    EMAIL_ID                            VARCHAR,
    ACCOUNT_ID                          VARCHAR,
    ACCOUNT_USER_ID                     VARCHAR,
    FROM_NAME                           VARCHAR,
    FROM_EMAIL                          VARCHAR,
    SCHED_TIME                          VARCHAR,
    PICKUP_TIME                         VARCHAR,
    DELIVERED_TIME                      VARCHAR,
    EVENT_ID                            VARCHAR,
    IS_MULTIPART                        VARCHAR,
    JOB_TYPE                            VARCHAR,
    JOB_STATUS                          VARCHAR,
    MODIFIED_BY                         VARCHAR,
    MODIFIED_DATE                       VARCHAR,
    EMAIL_NAME                          VARCHAR,
    EMAIL_SUBJECT                       VARCHAR,
    IS_WRAPPED                          VARCHAR,
    TEST_EMAIL_ADDR                     VARCHAR,
    CATEGORY                            VARCHAR,
    BCC_EMAIL                           VARCHAR,
    ORIGINAL_SCHED_TIME                 VARCHAR,
    CREATED_DATE                        VARCHAR,
    CHARACTER_SET                       VARCHAR,
    IP_ADDRESS                          VARCHAR,
    SALESFORCE_TOTAL_SUBSCRIBER_COUNT   VARCHAR,
    SALESFORCE_ERROR_SUBSCRIBER_COUNT   VARCHAR,
    SEND_TYPE                           VARCHAR,
    DYNAMIC_EMAIL_SUBJECT               VARCHAR,
    SUPPRESS_TRACKING                   VARCHAR,
    SEND_CLASSIFICATION_TYPE            VARCHAR,
    SEND_CLASSIFICATION                 VARCHAR,
    RESOLVE_LINKS_WITH_CURRENT_DATA     VARCHAR,
    EMAIL_SEND_DEFINITION               VARCHAR,
    DEDUPLICATE_BY_EMAIL                VARCHAR,
    TRIGGERER_SEND_DEFINITION_OBJECT_ID VARCHAR,
    TRIGGERED_SEND_CUSTOMER_KEY         VARCHAR,
    CAMPAIGN_NAME                       VARCHAR,
    CAMPAIGN_PURPOSE                    VARCHAR,
    PRODUCT_THERAPY_AREA                VARCHAR,
    CARE_PROGRAM                        VARCHAR,
    AUDIENCE_TYPE                       VARCHAR,
    _SOURCE_FILE_NAME                   VARCHAR(1000),
    _SOURCE_ROW_NUMBER                  NUMBER,
    _LOADED_AT                          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_UNSUBSCRIBE_DE_DETAIL (
    ACCOUNT_ID           VARCHAR,
    OYB_ACCOUNT_ID       VARCHAR,
    JOB_ID               VARCHAR,
    LIST_ID              VARCHAR,
    BATCH_ID             VARCHAR,
    SUBSCRIBER_ID        VARCHAR,
    SUBSCRIBER_KEY       VARCHAR,
    EVENT_DATE           VARCHAR,
    IS_UNIQUE            VARCHAR,
    DOMAIN               VARCHAR,
    _SOURCE_FILE_NAME    VARCHAR(1000),
    _SOURCE_ROW_NUMBER   NUMBER,
    _LOADED_AT           TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_PROSPECT_C (
    PROSPECT_ID          VARCHAR,
    FIRST_NAME           VARCHAR,
    LAST_NAME            VARCHAR,
    EMAIL_ADDRESS        VARCHAR,
    MARKETING_CONSENT    VARCHAR,
    HIGH_ENGAGEMENT      VARCHAR,
    REGISTRATION_DATE    VARCHAR,
    LAST_UPDATED         VARCHAR,
    _SOURCE_FILE_NAME    VARCHAR(1000),
    _SOURCE_ROW_NUMBER   NUMBER,
    _LOADED_AT           TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_PROSPECT_C_HISTORY (
    PROSPECT_ID          VARCHAR,
    FIRST_NAME           VARCHAR,
    LAST_NAME            VARCHAR,
    EMAIL_ADDRESS        VARCHAR,
    MARKETING_CONSENT    VARCHAR,
    HIGH_ENGAGEMENT      VARCHAR,
    REGISTRATION_DATE    VARCHAR,
    BATCH_ID             VARCHAR,
    JOB_ID               VARCHAR,
    LAST_UPDATED         VARCHAR,
    _SOURCE_FILE_NAME    VARCHAR(1000),
    _SOURCE_ROW_NUMBER   NUMBER,
    _LOADED_AT           TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_PROSPECT_JOURNEY_DETAILS (
    PROSPECT_ID                                  VARCHAR,
    WELCOME_JOURNEY_COMPLETE                     VARCHAR,
    NURTURE_JOURNEY_COMPLETE                     VARCHAR,
    SUPPRESSION_FLAG                             VARCHAR,
    WELCOMEJOURNEY_WELCOMEEMAIL_SENT             VARCHAR,
    WELCOMEJOURNEY_WELCOMEEMAIL_SENT_DATE        VARCHAR,
    WELCOMEJOURNEY_EDUCATIONEMAIL_SENT           VARCHAR,
    WELCOMEJOURNEY_EDUCATIONEMAIL_SENT_DATE      VARCHAR,
    NURTUREJOURNEY_EDUCATIONEMAIL1_SENT          VARCHAR,
    NURTUREJOURNEY_EDUCATIONEMAIL1_SENT_DATE     VARCHAR,
    NURTUREJOURNEY_EDUCATIONEMAIL2_SENT          VARCHAR,
    NURTUREJOURNEY_EDUCATIONEMAIL2_SENT_DATE     VARCHAR,
    NURTUREJOURNEY_PROSPECTSTORYEMAIL_SENT       VARCHAR,
    NURTUREJOURNEY_PROSPECTSTORYEMAIL_SENT_DATE  VARCHAR,
    HIGHENGAGEMENT_CONVERSIONEMAIL_SENT          VARCHAR,
    HIGHENGAGEMENT_CONVERSIONEMAIL_SENT_DATE     VARCHAR,
    HIGHENGAGEMENT_REMINDEREMAIL_SENT            VARCHAR,
    HIGHENGAGEMENT_REMINDEREMAIL_SENT_DATE       VARCHAR,
    LOWENGAGEMENT_REENGAGEMENTEMAIL_SENT         VARCHAR,
    LOWENGAGEMENT_REENGAGEMENTEMAIL_SENT_DATE    VARCHAR,
    LOWENGAGEMENTFINALREMINDEREMAIL_SENT         VARCHAR,
    LOWENGAGEMENTFINALREMINDEREMAIL_SENT_DATE    VARCHAR,
    _SOURCE_FILE_NAME                            VARCHAR(1000),
    _SOURCE_ROW_NUMBER                           NUMBER,
    _LOADED_AT                                   TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_JOURNEY_ACTIVITY_GENERIC (
    RAW_ROW               VARIANT,
    _SOURCE_FILE_NAME     VARCHAR(1000),
    _SOURCE_ROW_NUMBER    NUMBER,
    _LOADED_AT            TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- =====================================================================================
-- PART 6: GOLD TABLES (complete DDLs — no ALTER TABLE needed)
-- =====================================================================================

CREATE TABLE IF NOT EXISTS QA_FIPSAR_DW.GOLD.DIM_ENGAGEMENT_TYPE (
    EVENT_TYPE_KEY   NUMBER AUTOINCREMENT START 1 INCREMENT 1,
    EVENT_TYPE       VARCHAR(50) NOT NULL,
    EVENT_CATEGORY   VARCHAR(50),
    IS_POSITIVE      BOOLEAN,
    CREATED_AT       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS QA_FIPSAR_DW.GOLD.DIM_SFMC_JOB (
    JOB_KEY          NUMBER AUTOINCREMENT START 1 INCREMENT 1,
    JOB_ID           NUMBER NOT NULL,
    JOURNEY_TYPE     VARCHAR(50),
    MAPPED_STAGE     VARCHAR(100),
    EMAIL_NAME       VARCHAR(200),
    EMAIL_SUBJECT    VARCHAR(500),
    RECORD_TYPE      VARCHAR(50),
    CREATED_AT       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS QA_FIPSAR_DW.GOLD.FACT_SFMC_ENGAGEMENT (
    FACT_ENGAGEMENT_KEY NUMBER AUTOINCREMENT START 1 INCREMENT 1,
    SUBSCRIBER_KEY      VARCHAR(100),
    MASTER_PATIENT_ID   VARCHAR(13),
    PROSPECT_KEY        NUMBER,
    JOB_KEY             NUMBER,
    EVENT_TYPE_KEY      NUMBER,
    DATE_KEY            NUMBER,
    EVENT_TIMESTAMP     TIMESTAMP_NTZ,
    EVENT_TYPE          VARCHAR(50),
    DOMAIN              VARCHAR(255),
    IS_UNIQUE           BOOLEAN,
    CLICK_URL           VARCHAR(2000),
    BOUNCE_CATEGORY     VARCHAR(100),
    BOUNCE_TYPE         VARCHAR(100),
    REASON              VARCHAR(500),
    ACCOUNT_ID          VARCHAR(20),
    JOB_ID              NUMBER,
    RECORD_TYPE         VARCHAR(50),
    SOURCE_FILE_NAME    VARCHAR(1000),
    LOADED_AT           TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    IS_SUPPRESSED       BOOLEAN DEFAULT FALSE,
    SUPPRESSION_REASON  VARCHAR(500),
    SOURCE_ROW_NUMBER   NUMBER
);

CREATE TABLE IF NOT EXISTS QA_FIPSAR_DW.GOLD.REF_SFMC_STAGE_INTERVALS (
    FROM_STAGE_NUM NUMBER,
    TO_STAGE_NUM   NUMBER,
    WAIT_DAYS      NUMBER,
    IS_ACTIVE      BOOLEAN DEFAULT TRUE,
    CREATED_AT     TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- =====================================================================================
-- PART 7: SEED DATA
-- =====================================================================================

MERGE INTO QA_FIPSAR_DW.GOLD.DIM_ENGAGEMENT_TYPE tgt
USING (
    SELECT 'SENT' AS EVENT_TYPE UNION ALL
    SELECT 'OPEN' UNION ALL
    SELECT 'CLICK' UNION ALL
    SELECT 'BOUNCE' UNION ALL
    SELECT 'UNSUBSCRIBE' UNION ALL
    SELECT 'SPAM' UNION ALL
    SELECT 'SUPPRESSED'
) src
ON tgt.EVENT_TYPE = src.EVENT_TYPE
WHEN NOT MATCHED THEN
    INSERT (EVENT_TYPE) VALUES (src.EVENT_TYPE);

MERGE INTO QA_FIPSAR_DW.GOLD.REF_SFMC_STAGE_INTERVALS tgt
USING (
    SELECT 1 AS FROM_STAGE_NUM, 2 AS TO_STAGE_NUM, 3 AS WAIT_DAYS UNION ALL
    SELECT 2, 3, 5 UNION ALL
    SELECT 3, 4, 8 UNION ALL
    SELECT 4, 5, 3 UNION ALL
    SELECT 5, 6, 2 UNION ALL
    SELECT 6, 7, 2 UNION ALL
    SELECT 7, 8, 2 UNION ALL
    SELECT 8, 9, 2
) src
ON  tgt.FROM_STAGE_NUM = src.FROM_STAGE_NUM
AND tgt.TO_STAGE_NUM   = src.TO_STAGE_NUM
WHEN MATCHED THEN UPDATE SET
    tgt.WAIT_DAYS = src.WAIT_DAYS,
    tgt.IS_ACTIVE = TRUE
WHEN NOT MATCHED THEN
    INSERT (FROM_STAGE_NUM, TO_STAGE_NUM, WAIT_DAYS)
    VALUES (src.FROM_STAGE_NUM, src.TO_STAGE_NUM, src.WAIT_DAYS);

-- =====================================================================================
-- PART 8: GOLD SEED PROCEDURE — DIM_ENGAGEMENT_TYPE
-- =====================================================================================

CREATE OR REPLACE PROCEDURE QA_FIPSAR_DW.GOLD.SP_LOAD_GOLD_DIM_ENGAGEMENT_TYPE()
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    v_run_id      VARCHAR DEFAULT UUID_STRING();
    v_rows_loaded NUMBER DEFAULT 0;
BEGIN
    INSERT INTO QA_FIPSAR_AUDIT.PIPELINE_AUDIT.PIPELINE_RUN_LOG
    (
        RUN_ID, PIPELINE_NAME, LAYER, TABLE_NAME, STATUS
    )
    VALUES
    (
        :v_run_id, 'SP_LOAD_GOLD_DIM_ENGAGEMENT_TYPE', 'GOLD', 'DIM_ENGAGEMENT_TYPE', 'STARTED'
    );

    MERGE INTO QA_FIPSAR_DW.GOLD.DIM_ENGAGEMENT_TYPE tgt
    USING (
        SELECT * FROM VALUES
            ('SENT',        'Delivery',    TRUE),
            ('OPEN',        'Engagement',  TRUE),
            ('CLICK',       'Engagement',  TRUE),
            ('BOUNCE',      'Negative',    FALSE),
            ('UNSUBSCRIBE', 'Negative',    FALSE),
            ('SPAM',        'Negative',    FALSE),
            ('SUPPRESSED',  'Suppression', FALSE)
        AS t(EVENT_TYPE, EVENT_CATEGORY, IS_POSITIVE)
    ) src
    ON tgt.EVENT_TYPE = src.EVENT_TYPE
    WHEN MATCHED THEN UPDATE SET
        tgt.EVENT_CATEGORY = src.EVENT_CATEGORY,
        tgt.IS_POSITIVE    = src.IS_POSITIVE
    WHEN NOT MATCHED THEN
        INSERT (EVENT_TYPE, EVENT_CATEGORY, IS_POSITIVE)
        VALUES (src.EVENT_TYPE, src.EVENT_CATEGORY, src.IS_POSITIVE);

    v_rows_loaded := SQLROWCOUNT;

    UPDATE QA_FIPSAR_AUDIT.PIPELINE_AUDIT.PIPELINE_RUN_LOG
    SET STATUS = 'SUCCESS',
        ROWS_LOADED = :v_rows_loaded,
        ROWS_REJECTED = 0,
        ERROR_MESSAGE = NULL,
        COMPLETED_AT = CURRENT_TIMESTAMP()
    WHERE RUN_ID = :v_run_id;

    RETURN OBJECT_CONSTRUCT(
        'status','SUCCESS',
        'run_id',:v_run_id,
        'rows_loaded',:v_rows_loaded
    );

EXCEPTION
    WHEN OTHER THEN
        UPDATE QA_FIPSAR_AUDIT.PIPELINE_AUDIT.PIPELINE_RUN_LOG
        SET STATUS = 'FAILED',
            ERROR_MESSAGE = 'SP_LOAD_GOLD_DIM_ENGAGEMENT_TYPE failed',
            COMPLETED_AT = CURRENT_TIMESTAMP()
        WHERE RUN_ID = :v_run_id;

        RETURN OBJECT_CONSTRUCT(
            'status','FAILED',
            'run_id',:v_run_id,
            'error','SP_LOAD_GOLD_DIM_ENGAGEMENT_TYPE failed'
        );
END;
$$;

-- =====================================================================================
-- PART 9: GENERIC INCREMENTAL RAW LOADER
-- =====================================================================================

CREATE OR REPLACE PROCEDURE QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.SP_COPY_NEW_FILES(
    P_ENTITY_NAME      STRING,
    P_STAGE_NAME       STRING,
    P_TARGET_TABLE     STRING,
    P_FILE_FORMAT      STRING,
    P_SELECT_SQL       STRING
)
RETURNS VARIANT
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
function getUuid() {
  const rs = snowflake.createStatement({sqlText: `SELECT UUID_STRING()`}).execute();
  rs.next();
  return rs.getColumnValue(1);
}

function parseTimestampFromFileName(fileName) {
  const baseName = String(fileName).split('/').pop();

  let m = baseName.match(/(\d{8}_\d{6})(?=\.[^.]+$)/);
  if (m) {
    const s = m[1];
    return new Date(
      s.substring(0,4) + '-' + s.substring(4,6) + '-' + s.substring(6,8) + 'T' +
      s.substring(9,11) + ':' + s.substring(11,13) + ':' + s.substring(13,15) + 'Z'
    );
  }

  m = baseName.match(/(\d{8}_\d{4})(?=\.[^.]+$)/);
  if (m) {
    const s = m[1];
    return new Date(
      s.substring(0,4) + '-' + s.substring(4,6) + '-' + s.substring(6,8) + 'T' +
      s.substring(9,11) + ':' + s.substring(11,13) + ':00Z'
    );
  }

  return null;
}

function exec(sqlText, binds) {
  return snowflake.createStatement({sqlText: sqlText, binds: binds || []}).execute();
}

const pipelineRunId = getUuid();
let totalRowsLoaded = 0;
let filesLoaded = 0;
let filesSkipped = 0;
let fileCount = 0;
let currentFile = null;

try {
  exec(`
    INSERT INTO QA_FIPSAR_AUDIT.PIPELINE_AUDIT.PIPELINE_RUN_LOG
    (RUN_ID, PIPELINE_NAME, LAYER, TABLE_NAME, STATUS, STARTED_AT)
    VALUES (?, ?, 'RAW', ?, 'STARTED', CURRENT_TIMESTAMP())
  `, [pipelineRunId, 'SP_COPY_NEW_FILES_' + P_ENTITY_NAME, P_TARGET_TABLE]);

  const files = [];
  const listRs = exec(`LIST @${P_STAGE_NAME}`);

  while (listRs.next()) {
    const fullName = listRs.getColumnValue(1);
    const lastModified = listRs.getColumnValue(4);
    const baseName = String(fullName).split('/').pop();
    const parsedTs = parseTimestampFromFileName(fullName);
    files.push({
      fullName: fullName,
      baseName: baseName,
      lastModified: String(lastModified),
      parsedTs: parsedTs
    });
  }

  if (files.length === 0) {
    exec(`
      UPDATE QA_FIPSAR_AUDIT.PIPELINE_AUDIT.PIPELINE_RUN_LOG
      SET STATUS = 'SUCCESS',
          ROWS_LOADED = 0,
          ROWS_REJECTED = 0,
          ERROR_MESSAGE = NULL,
          COMPLETED_AT = CURRENT_TIMESTAMP()
      WHERE RUN_ID = ?
    `, [pipelineRunId]);

    return {
      status: 'SUCCESS',
      run_id: pipelineRunId,
      entity: P_ENTITY_NAME,
      rows_loaded: 0,
      files_loaded: 0,
      files_skipped: 0,
      message: 'No files found in stage'
    };
  }

  files.sort(function(a, b) {
    const aTs = a.parsedTs ? a.parsedTs.getTime() : null;
    const bTs = b.parsedTs ? b.parsedTs.getTime() : null;

    if (aTs !== null && bTs !== null && aTs !== bTs) return aTs - bTs;
    if (aTs === null && bTs !== null) return -1;
    if (aTs !== null && bTs === null) return 1;
    if (a.lastModified !== b.lastModified) return a.lastModified.localeCompare(b.lastModified);
    return a.baseName.localeCompare(b.baseName);
  });

  fileCount = files.length;

  for (let i = 0; i < files.length; i++) {
    currentFile = files[i];
    const fileRunId = getUuid();
    const fileTimestampString = currentFile.parsedTs
      ? currentFile.parsedTs.toISOString().replace('T', ' ').replace('Z', '')
      : null;

    const chkRs = exec(`
      SELECT COUNT(*)
      FROM QA_FIPSAR_AUDIT.PIPELINE_AUDIT.SFMC_FILE_LOAD_AUDIT
      WHERE ENTITY_NAME = ?
        AND STAGE_NAME = ?
        AND TARGET_TABLE_NAME = ?
        AND FILE_NAME = ?
        AND STATUS = 'SUCCESS'
    `, [P_ENTITY_NAME, P_STAGE_NAME, P_TARGET_TABLE, currentFile.baseName]);
    chkRs.next();
    const alreadyLoaded = Number(chkRs.getColumnValue(1));

    if (alreadyLoaded > 0) {
      filesSkipped++;

      exec(`
        INSERT INTO QA_FIPSAR_AUDIT.PIPELINE_AUDIT.SFMC_FILE_LOAD_AUDIT
        (RUN_ID, ENTITY_NAME, STAGE_NAME, TARGET_TABLE_NAME, FILE_NAME, FILE_TIMESTAMP,
         STATUS, ROWS_LOADED, ERROR_MESSAGE, STARTED_AT, COMPLETED_AT)
        VALUES (?, ?, ?, ?, ?, TRY_TO_TIMESTAMP_NTZ(?),
                'SKIPPED', 0, 'File already loaded', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP())
      `, [fileRunId, P_ENTITY_NAME, P_STAGE_NAME, P_TARGET_TABLE, currentFile.baseName, fileTimestampString]);

      continue;
    }

    exec(`
      INSERT INTO QA_FIPSAR_AUDIT.PIPELINE_AUDIT.SFMC_FILE_LOAD_AUDIT
      (RUN_ID, ENTITY_NAME, STAGE_NAME, TARGET_TABLE_NAME, FILE_NAME, FILE_TIMESTAMP,
       STATUS, ROWS_LOADED, STARTED_AT)
      VALUES (?, ?, ?, ?, ?, TRY_TO_TIMESTAMP_NTZ(?),
              'STARTED', 0, CURRENT_TIMESTAMP())
    `, [fileRunId, P_ENTITY_NAME, P_STAGE_NAME, P_TARGET_TABLE, currentFile.baseName, fileTimestampString]);

    const escapedBaseName = currentFile.baseName.replace(/'/g, "''");
    const copySql = `
      COPY INTO ${P_TARGET_TABLE}
      FROM (
        ${P_SELECT_SQL}
        FROM @${P_STAGE_NAME}
      )
      FILES = ('${escapedBaseName}')
      FILE_FORMAT = (FORMAT_NAME = ${P_FILE_FORMAT})
      ON_ERROR = 'ABORT_STATEMENT'
    `;

    exec(copySql);

    const cntRs = exec(
      `SELECT COUNT(*) FROM ${P_TARGET_TABLE} WHERE _SOURCE_FILE_NAME ILIKE ?`,
      ['%' + currentFile.baseName]
    );
    cntRs.next();
    const rowsForFile = Number(cntRs.getColumnValue(1));

    totalRowsLoaded += rowsForFile;
    filesLoaded++;

    exec(`
      UPDATE QA_FIPSAR_AUDIT.PIPELINE_AUDIT.SFMC_FILE_LOAD_AUDIT
      SET STATUS = 'SUCCESS',
          ROWS_LOADED = ?,
          ERROR_MESSAGE = NULL,
          COMPLETED_AT = CURRENT_TIMESTAMP()
      WHERE RUN_ID = ?
    `, [rowsForFile, fileRunId]);
  }

  exec(`
    UPDATE QA_FIPSAR_AUDIT.PIPELINE_AUDIT.PIPELINE_RUN_LOG
    SET STATUS = 'SUCCESS',
        ROWS_LOADED = ?,
        ROWS_REJECTED = 0,
        ERROR_MESSAGE = NULL,
        COMPLETED_AT = CURRENT_TIMESTAMP()
    WHERE RUN_ID = ?
  `, [totalRowsLoaded, pipelineRunId]);

  return {
    status: 'SUCCESS',
    run_id: pipelineRunId,
    entity: P_ENTITY_NAME,
    files_seen: fileCount,
    files_loaded: filesLoaded,
    files_skipped: filesSkipped,
    rows_loaded: totalRowsLoaded
  };

} catch (err) {
  exec(`
    UPDATE QA_FIPSAR_AUDIT.PIPELINE_AUDIT.PIPELINE_RUN_LOG
    SET STATUS = 'FAILED',
        ERROR_MESSAGE = ?,
        COMPLETED_AT = CURRENT_TIMESTAMP()
    WHERE RUN_ID = ?
  `, [String(err), pipelineRunId]);

  if (currentFile !== null) {
    exec(`
      INSERT INTO QA_FIPSAR_AUDIT.PIPELINE_AUDIT.SFMC_FILE_LOAD_AUDIT
      (RUN_ID, ENTITY_NAME, STAGE_NAME, TARGET_TABLE_NAME, FILE_NAME, FILE_TIMESTAMP,
       STATUS, ROWS_LOADED, ERROR_MESSAGE, STARTED_AT, COMPLETED_AT)
      VALUES (?, ?, ?, ?, ?, TRY_TO_TIMESTAMP_NTZ(?),
              'FAILED', 0, ?, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP())
    `, [
      getUuid(),
      P_ENTITY_NAME,
      P_STAGE_NAME,
      P_TARGET_TABLE,
      currentFile.baseName,
      currentFile.parsedTs ? currentFile.parsedTs.toISOString().replace('T',' ').replace('Z','') : null,
      String(err)
    ]);
  }

  return {
    status: 'FAILED',
    run_id: pipelineRunId,
    entity: P_ENTITY_NAME,
    error: String(err)
  };
}
$$;

-- =====================================================================================
-- PART 10: RAW LOAD WRAPPERS
-- =====================================================================================

CREATE OR REPLACE PROCEDURE QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.SP_LOAD_RAW_SFMC_SENT()
RETURNS VARIANT
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
const rs = snowflake.createStatement({
  sqlText: `CALL QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.SP_COPY_NEW_FILES(?,?,?,?,?)`,
  binds: [
    'RAW_SFMC_SENT',
    'QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.STG_SFMC_SENT_DE',
    'QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_SENT',
    'QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.FF_SFMC_CSV_UTF16',
    `SELECT
        $1,   -- ACCOUNT_ID
        $7,   -- OYB_ACCOUNT_ID
        $5,   -- JOB_ID
        $6,   -- LIST_ID
        $2,   -- BATCH_ID
        $8,   -- SUBSCRIBER_ID
        $9,   -- SUBSCRIBER_KEY
        $4,   -- EVENT_DATE
        $3,   -- DOMAIN
        $10,  -- TRIGGERED_SEND_CUSTOMER_KEY
        $11,  -- TRIGGERED_SEND_DEF_OBJECT_ID
        'SENT',
        METADATA$FILENAME,
        METADATA$FILE_ROW_NUMBER,
        CURRENT_TIMESTAMP()`
  ]
}).execute();
rs.next();
return rs.getColumnValue(1);
$$;

CREATE OR REPLACE PROCEDURE QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.SP_LOAD_RAW_SFMC_OPENS()
RETURNS VARIANT
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
const rs = snowflake.createStatement({
  sqlText: `CALL QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.SP_COPY_NEW_FILES(?,?,?,?,?)`,
  binds: [
    'RAW_SFMC_OPENS',
    'QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.STG_SFMC_OPEN_DE',
    'QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_OPENS',
    'QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.FF_SFMC_CSV_UTF16',
    `SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, 'OPEN',
            METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, CURRENT_TIMESTAMP()`
  ]
}).execute();
rs.next();
return rs.getColumnValue(1);
$$;

CREATE OR REPLACE PROCEDURE QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.SP_LOAD_RAW_SFMC_CLICKS()
RETURNS VARIANT
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
const rs = snowflake.createStatement({
  sqlText: `CALL QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.SP_COPY_NEW_FILES(?,?,?,?,?)`,
  binds: [
    'RAW_SFMC_CLICKS',
    'QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.STG_SFMC_CLICK_DE',
    'QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_CLICKS',
    'QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.FF_SFMC_CSV_UTF16',
    `SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, 'CLICK',
            METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, CURRENT_TIMESTAMP()`
  ]
}).execute();
rs.next();
return rs.getColumnValue(1);
$$;

CREATE OR REPLACE PROCEDURE QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.SP_LOAD_RAW_SFMC_BOUNCES()
RETURNS VARIANT
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
const rs = snowflake.createStatement({
  sqlText: `CALL QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.SP_COPY_NEW_FILES(?,?,?,?,?)`,
  binds: [
    'RAW_SFMC_BOUNCES',
    'QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.STG_SFMC_BOUNCE_DE',
    'QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_BOUNCES',
    'QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.FF_SFMC_CSV_UTF16',
    `SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, 'BOUNCE',
            METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, CURRENT_TIMESTAMP()`
  ]
}).execute();
rs.next();
return rs.getColumnValue(1);
$$;

CREATE OR REPLACE PROCEDURE QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.SP_LOAD_RAW_SFMC_UNSUBSCRIBES()
RETURNS VARIANT
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
const rs = snowflake.createStatement({
  sqlText: `CALL QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.SP_COPY_NEW_FILES(?,?,?,?,?)`,
  binds: [
    'RAW_SFMC_UNSUBSCRIBES',
    'QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.STG_SFMC_UNSUBSCRIBE_DE',
    'QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_UNSUBSCRIBES',
    'QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.FF_SFMC_CSV_UTF16',
    `SELECT $1, $7, $3, $8, NULL, 'UNSUBSCRIBE',
            METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, CURRENT_TIMESTAMP()`
  ]
}).execute();
rs.next();
return rs.getColumnValue(1);
$$;

CREATE OR REPLACE PROCEDURE QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.SP_LOAD_RAW_SFMC_SPAM()
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    v_run_id VARCHAR DEFAULT UUID_STRING();
BEGIN
    INSERT INTO QA_FIPSAR_AUDIT.PIPELINE_AUDIT.PIPELINE_RUN_LOG
    (
        RUN_ID, PIPELINE_NAME, LAYER, TABLE_NAME, STATUS, STARTED_AT
    )
    VALUES
    (
        :v_run_id, 'SP_LOAD_RAW_SFMC_SPAM', 'RAW', 'RAW_SFMC_SPAM', 'STARTED', CURRENT_TIMESTAMP()
    );

    UPDATE QA_FIPSAR_AUDIT.PIPELINE_AUDIT.PIPELINE_RUN_LOG
    SET STATUS = 'SUCCESS',
        ROWS_LOADED = 0,
        ROWS_REJECTED = 0,
        ERROR_MESSAGE = NULL,
        COMPLETED_AT = CURRENT_TIMESTAMP()
    WHERE RUN_ID = :v_run_id;

    RETURN OBJECT_CONSTRUCT(
        'status','SUCCESS',
        'run_id',:v_run_id,
        'rows_loaded',0,
        'message','No Spam source stage configured'
    );
END;
$$;

CREATE OR REPLACE PROCEDURE QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.SP_LOAD_RAW_SFMC_JOB_METADATA()
RETURNS VARIANT
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
const rs = snowflake.createStatement({
  sqlText: `CALL QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.SP_COPY_NEW_FILES(?,?,?,?,?)`,
  binds: [
    'RAW_SFMC_JOB_METADATA',
    'QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.STG_SFMC_JOB_DE',
    'QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_JOB_METADATA',
    'QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.FF_SFMC_CSV_UTF16',
    `SELECT $1, NULL, NULL, $16, RTRIM($17), 'JOB',
            METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, CURRENT_TIMESTAMP()`
  ]
}).execute();
rs.next();
return rs.getColumnValue(1);
$$;

CREATE OR REPLACE PROCEDURE QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.SP_LOAD_RAW_SFMC_JOB_DE_DETAIL()
RETURNS VARIANT
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
const rs = snowflake.createStatement({
  sqlText: `CALL QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.SP_COPY_NEW_FILES(?,?,?,?,?)`,
  binds: [
    'RAW_SFMC_JOB_DE_DETAIL',
    'QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.STG_SFMC_JOB_DE',
    'QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_JOB_DE_DETAIL',
    'QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.FF_SFMC_CSV_UTF16',
    `SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, RTRIM($17),
            $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, $31, $32, $33, $34,
            $35, $36, $37, $38, $39, $40, $41, $42,
            METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, CURRENT_TIMESTAMP()`
  ]
}).execute();
rs.next();
return rs.getColumnValue(1);
$$;

CREATE OR REPLACE PROCEDURE QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.SP_LOAD_RAW_SFMC_UNSUBSCRIBE_DE_DETAIL()
RETURNS VARIANT
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
const rs = snowflake.createStatement({
  sqlText: `CALL QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.SP_COPY_NEW_FILES(?,?,?,?,?)`,
  binds: [
    'RAW_SFMC_UNSUBSCRIBE_DE_DETAIL',
    'QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.STG_SFMC_UNSUBSCRIBE_DE',
    'QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_UNSUBSCRIBE_DE_DETAIL',
    'QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.FF_SFMC_CSV_UTF16',
    `SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
            METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, CURRENT_TIMESTAMP()`
  ]
}).execute();
rs.next();
return rs.getColumnValue(1);
$$;

CREATE OR REPLACE PROCEDURE QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.SP_LOAD_RAW_SFMC_PROSPECT_C()
RETURNS VARIANT
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
const rs = snowflake.createStatement({
  sqlText: `CALL QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.SP_COPY_NEW_FILES(?,?,?,?,?)`,
  binds: [
    'RAW_SFMC_PROSPECT_C',
    'QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.STG_SFMC_PROSPECT_C',
    'QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_PROSPECT_C',
    'QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.FF_SFMC_CSV_UTF16',
    `SELECT $1, $2, $3, $4, $5, $6, $7, $8,
            METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, CURRENT_TIMESTAMP()`
  ]
}).execute();
rs.next();
return rs.getColumnValue(1);
$$;

CREATE OR REPLACE PROCEDURE QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.SP_LOAD_RAW_SFMC_PROSPECT_C_HISTORY()
RETURNS VARIANT
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
const rs = snowflake.createStatement({
  sqlText: `CALL QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.SP_COPY_NEW_FILES(?,?,?,?,?)`,
  binds: [
    'RAW_SFMC_PROSPECT_C_HISTORY',
    'QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.STG_SFMC_PROSPECT_C_HISTORY',
    'QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_PROSPECT_C_HISTORY',
    'QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.FF_SFMC_CSV_UTF16',
    `SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
            METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, CURRENT_TIMESTAMP()`
  ]
}).execute();
rs.next();
return rs.getColumnValue(1);
$$;

CREATE OR REPLACE PROCEDURE QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.SP_LOAD_RAW_SFMC_PROSPECT_JOURNEY_DETAILS()
RETURNS VARIANT
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
const rs = snowflake.createStatement({
  sqlText: `CALL QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.SP_COPY_NEW_FILES(?,?,?,?,?)`,
  binds: [
    'RAW_SFMC_PROSPECT_JOURNEY_DETAILS',
    'QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.STG_SFMC_PROSPECT_JOURNEY_DETAILS',
    'QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_PROSPECT_JOURNEY_DETAILS',
    'QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.FF_SFMC_CSV_UTF16',
    `SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22,
            METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, CURRENT_TIMESTAMP()`
  ]
}).execute();
rs.next();
return rs.getColumnValue(1);
$$;

CREATE OR REPLACE PROCEDURE QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.SP_LOAD_RAW_SFMC_JOURNEY_ACTIVITY_GENERIC()
RETURNS VARIANT
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
const rs = snowflake.createStatement({
  sqlText: `CALL QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.SP_COPY_NEW_FILES(?,?,?,?,?)`,
  binds: [
    'RAW_SFMC_JOURNEY_ACTIVITY_GENERIC',
    'QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.STG_SFMC_JOURNEY_ACTIVITY_DE',
    'QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_JOURNEY_ACTIVITY_GENERIC',
    'QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.FF_SFMC_CSV_UTF16',
    `SELECT ARRAY_CONSTRUCT($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28,$29,$30,$31,$32,$33,$34,$35,$36,$37,$38,$39,$40,$41,$42,$43,$44,$45,$46,$47,$48,$49,$50),
            METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, CURRENT_TIMESTAMP()`
  ]
}).execute();
rs.next();
return rs.getColumnValue(1);
$$;

-- =====================================================================================
-- PART 11: GOLD LOADERS
-- =====================================================================================

CREATE OR REPLACE PROCEDURE QA_FIPSAR_DW.GOLD.SP_LOAD_GOLD_DIM_SFMC_JOB()
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    v_run_id      VARCHAR DEFAULT UUID_STRING();
    v_rows_loaded NUMBER DEFAULT 0;
BEGIN
    INSERT INTO QA_FIPSAR_AUDIT.PIPELINE_AUDIT.PIPELINE_RUN_LOG
    (
        RUN_ID, PIPELINE_NAME, LAYER, TABLE_NAME, STATUS, STARTED_AT
    )
    VALUES
    (
        :v_run_id, 'SP_LOAD_GOLD_DIM_SFMC_JOB', 'GOLD', 'DIM_SFMC_JOB', 'STARTED', CURRENT_TIMESTAMP()
    );

    MERGE INTO QA_FIPSAR_DW.GOLD.DIM_SFMC_JOB tgt
    USING (
        WITH meta AS (
            SELECT JOB_ID, JOURNEY_TYPE, MAPPED_STAGE, EMAIL_NAME, EMAIL_SUBJECT, RECORD_TYPE
            FROM (
                SELECT
                    JOB_ID, JOURNEY_TYPE, MAPPED_STAGE, EMAIL_NAME, EMAIL_SUBJECT, RECORD_TYPE,
                    ROW_NUMBER() OVER (
                        PARTITION BY JOB_ID
                        ORDER BY _LOADED_AT DESC, _SOURCE_ROW_NUMBER DESC
                    ) AS rn
                FROM QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_JOB_METADATA
                WHERE JOB_ID IS NOT NULL
            )
            WHERE rn = 1
        ),
        detail AS (
            SELECT
                TRY_TO_NUMBER(JOB_ID) AS JOB_ID,
                EMAIL_NAME,
                RTRIM(EMAIL_SUBJECT) AS EMAIL_SUBJECT
            FROM (
                SELECT
                    JOB_ID,
                    EMAIL_NAME,
                    EMAIL_SUBJECT,
                    ROW_NUMBER() OVER (
                        PARTITION BY TRY_TO_NUMBER(JOB_ID)
                        ORDER BY _LOADED_AT DESC, _SOURCE_ROW_NUMBER DESC
                    ) AS rn
                FROM QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_JOB_DE_DETAIL
                WHERE TRY_TO_NUMBER(JOB_ID) IS NOT NULL
            )
            WHERE rn = 1
        )
        SELECT
            COALESCE(m.JOB_ID, d.JOB_ID) AS JOB_ID,
            m.JOURNEY_TYPE,
            m.MAPPED_STAGE,
            COALESCE(NULLIF(m.EMAIL_NAME, ''), d.EMAIL_NAME) AS EMAIL_NAME,
            COALESCE(NULLIF(m.EMAIL_SUBJECT, ''), d.EMAIL_SUBJECT) AS EMAIL_SUBJECT,
            COALESCE(m.RECORD_TYPE, 'JOB') AS RECORD_TYPE
        FROM meta m
        FULL OUTER JOIN detail d
          ON m.JOB_ID = d.JOB_ID
        WHERE COALESCE(m.JOB_ID, d.JOB_ID) IS NOT NULL
    ) src
    ON tgt.JOB_ID = src.JOB_ID
    WHEN MATCHED THEN UPDATE SET
        tgt.JOURNEY_TYPE  = src.JOURNEY_TYPE,
        tgt.MAPPED_STAGE  = src.MAPPED_STAGE,
        tgt.EMAIL_NAME    = src.EMAIL_NAME,
        tgt.EMAIL_SUBJECT = src.EMAIL_SUBJECT,
        tgt.RECORD_TYPE   = src.RECORD_TYPE,
        tgt.UPDATED_AT    = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN
        INSERT (JOB_ID, JOURNEY_TYPE, MAPPED_STAGE, EMAIL_NAME, EMAIL_SUBJECT, RECORD_TYPE)
        VALUES (src.JOB_ID, src.JOURNEY_TYPE, src.MAPPED_STAGE, src.EMAIL_NAME, src.EMAIL_SUBJECT, src.RECORD_TYPE);

    v_rows_loaded := SQLROWCOUNT;

    UPDATE QA_FIPSAR_AUDIT.PIPELINE_AUDIT.PIPELINE_RUN_LOG
    SET STATUS = 'SUCCESS',
        ROWS_LOADED = :v_rows_loaded,
        ROWS_REJECTED = 0,
        ERROR_MESSAGE = NULL,
        COMPLETED_AT = CURRENT_TIMESTAMP()
    WHERE RUN_ID = :v_run_id;

    RETURN OBJECT_CONSTRUCT(
        'status','SUCCESS',
        'run_id',:v_run_id,
        'rows_loaded',:v_rows_loaded
    );

EXCEPTION
    WHEN OTHER THEN
        UPDATE QA_FIPSAR_AUDIT.PIPELINE_AUDIT.PIPELINE_RUN_LOG
        SET STATUS = 'FAILED',
            ERROR_MESSAGE = 'SP_LOAD_GOLD_DIM_SFMC_JOB failed',
            COMPLETED_AT = CURRENT_TIMESTAMP()
        WHERE RUN_ID = :v_run_id;

        RETURN OBJECT_CONSTRUCT(
            'status','FAILED',
            'run_id',:v_run_id,
            'error','SP_LOAD_GOLD_DIM_SFMC_JOB failed'
        );
END;
$$;

CREATE OR REPLACE PROCEDURE QA_FIPSAR_DW.GOLD.SP_LOG_SFMC_SUPPRESSIONS()
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    v_run_id      VARCHAR DEFAULT UUID_STRING();
    v_rows_logged NUMBER DEFAULT 0;
BEGIN
    INSERT INTO QA_FIPSAR_AUDIT.PIPELINE_AUDIT.PIPELINE_RUN_LOG
    (
        RUN_ID, PIPELINE_NAME, LAYER, TABLE_NAME, STATUS, STARTED_AT
    )
    VALUES
    (
        :v_run_id, 'SP_LOG_SFMC_SUPPRESSIONS', 'GOLD', 'DQ_REJECTION_LOG', 'STARTED', CURRENT_TIMESTAMP()
    );

    INSERT INTO QA_FIPSAR_AUDIT.PIPELINE_AUDIT.DQ_REJECTION_LOG
    (
        TABLE_NAME,
        REJECTION_REASON,
        REJECTED_RECORD,
        REJECTED_AT
    )
    SELECT
        'FACT_SFMC_ENGAGEMENT' AS TABLE_NAME,
        'SUPPRESSED_PROSPECT' AS REJECTION_REASON,
        OBJECT_CONSTRUCT(
            'PROSPECT_ID', s.PROSPECT_ID,
            'SUPPRESSION_FLAG', 'YES',
            'STAGE', 'SFMC_SUPPRESSION_DETECTION',
            '_SOURCE_FILE_NAME', s._SOURCE_FILE_NAME,
            '_SOURCE_ROW_NUMBER', s._SOURCE_ROW_NUMBER
        ) AS REJECTED_RECORD,
        CURRENT_TIMESTAMP() AS REJECTED_AT
    FROM (
        SELECT DISTINCT
            PROSPECT_ID,
            _SOURCE_FILE_NAME,
            _SOURCE_ROW_NUMBER
        FROM QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_PROSPECT_JOURNEY_DETAILS
        WHERE UPPER(TRIM(SUPPRESSION_FLAG)) IN ('YES','Y','TRUE','1')
          AND NULLIF(TRIM(PROSPECT_ID), '') IS NOT NULL
    ) s
    WHERE NOT EXISTS (
        SELECT 1
        FROM QA_FIPSAR_AUDIT.PIPELINE_AUDIT.DQ_REJECTION_LOG d
        WHERE d.TABLE_NAME = 'FACT_SFMC_ENGAGEMENT'
          AND d.REJECTION_REASON = 'SUPPRESSED_PROSPECT'
          AND d.REJECTED_RECORD = OBJECT_CONSTRUCT(
                'PROSPECT_ID', s.PROSPECT_ID,
                'SUPPRESSION_FLAG', 'YES',
                'STAGE', 'SFMC_SUPPRESSION_DETECTION',
                '_SOURCE_FILE_NAME', s._SOURCE_FILE_NAME,
                '_SOURCE_ROW_NUMBER', s._SOURCE_ROW_NUMBER
          )
    );

    v_rows_logged := SQLROWCOUNT;

    UPDATE QA_FIPSAR_AUDIT.PIPELINE_AUDIT.PIPELINE_RUN_LOG
    SET STATUS = 'SUCCESS',
        ROWS_LOADED = 0,
        ROWS_REJECTED = :v_rows_logged,
        ERROR_MESSAGE = NULL,
        COMPLETED_AT = CURRENT_TIMESTAMP()
    WHERE RUN_ID = :v_run_id;

    RETURN OBJECT_CONSTRUCT(
        'status','SUCCESS',
        'run_id',:v_run_id,
        'rows_logged',:v_rows_logged
    );

EXCEPTION
    WHEN OTHER THEN
        UPDATE QA_FIPSAR_AUDIT.PIPELINE_AUDIT.PIPELINE_RUN_LOG
        SET STATUS = 'FAILED',
            ERROR_MESSAGE = 'SP_LOG_SFMC_SUPPRESSIONS failed',
            COMPLETED_AT = CURRENT_TIMESTAMP()
        WHERE RUN_ID = :v_run_id;

        RETURN OBJECT_CONSTRUCT(
            'status','FAILED',
            'run_id',:v_run_id,
            'error','SP_LOG_SFMC_SUPPRESSIONS failed'
        );
END;
$$;

CREATE OR REPLACE PROCEDURE QA_FIPSAR_DW.GOLD.SP_LOAD_GOLD_FACT_SFMC_ENGAGEMENT()
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    v_run_id      VARCHAR DEFAULT UUID_STRING();
    v_rows_loaded NUMBER DEFAULT 0;
BEGIN
    INSERT INTO QA_FIPSAR_AUDIT.PIPELINE_AUDIT.PIPELINE_RUN_LOG
    (
        RUN_ID, PIPELINE_NAME, LAYER, TABLE_NAME, STATUS, STARTED_AT
    )
    VALUES
    (
        :v_run_id, 'SP_LOAD_GOLD_FACT_SFMC_ENGAGEMENT', 'GOLD', 'FACT_SFMC_ENGAGEMENT', 'STARTED', CURRENT_TIMESTAMP()
    );

    -- SENT
    INSERT INTO QA_FIPSAR_DW.GOLD.FACT_SFMC_ENGAGEMENT
    (
        SUBSCRIBER_KEY, MASTER_PATIENT_ID, PROSPECT_KEY, JOB_KEY, EVENT_TYPE_KEY, DATE_KEY,
        EVENT_TIMESTAMP, EVENT_TYPE, DOMAIN, ACCOUNT_ID, JOB_ID, RECORD_TYPE,
        SOURCE_FILE_NAME, LOADED_AT, IS_SUPPRESSED, SUPPRESSION_REASON
    )
    SELECT
        s.SUBSCRIBER_KEY,
        dp.MASTER_PATIENT_ID,
        dp.PROSPECT_KEY,
        j.JOB_KEY,
        et.EVENT_TYPE_KEY,
        TO_NUMBER(TO_CHAR(TRY_TO_TIMESTAMP_NTZ(s.EVENT_DATE), 'YYYYMMDD')),
        TRY_TO_TIMESTAMP_NTZ(s.EVENT_DATE),
        'SENT',
        s.DOMAIN,
        s.ACCOUNT_ID,
        s.JOB_ID,
        s.RECORD_TYPE,
        s._SOURCE_FILE_NAME,
        CURRENT_TIMESTAMP(),
        COALESCE(sp.SUPPRESSION_FLAG_VAL, FALSE),
        sp.SUPPRESSION_REASON_VAL
    FROM QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_SENT s
    LEFT JOIN QA_FIPSAR_DW.GOLD.DIM_SFMC_JOB j
        ON s.JOB_ID = j.JOB_ID
    LEFT JOIN QA_FIPSAR_DW.GOLD.DIM_ENGAGEMENT_TYPE et
        ON et.EVENT_TYPE = 'SENT'
    LEFT JOIN QA_FIPSAR_DW.GOLD.DIM_PROSPECT dp
        ON dp.MASTER_PATIENT_ID = s.SUBSCRIBER_KEY
    LEFT JOIN (
        SELECT DISTINCT
            PROSPECT_ID,
            TRUE AS SUPPRESSION_FLAG_VAL,
            'SUPPRESSION_FLAG=YES' AS SUPPRESSION_REASON_VAL
        FROM QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_PROSPECT_JOURNEY_DETAILS
        WHERE UPPER(TRIM(SUPPRESSION_FLAG)) IN ('YES','Y','TRUE','1')
          AND NULLIF(TRIM(PROSPECT_ID), '') IS NOT NULL
    ) sp
        ON sp.PROSPECT_ID = COALESCE(dp.MASTER_PATIENT_ID, s.SUBSCRIBER_KEY)
    WHERE NOT EXISTS (
        SELECT 1
        FROM QA_FIPSAR_DW.GOLD.FACT_SFMC_ENGAGEMENT f
        WHERE f.EVENT_TYPE = 'SENT'
          AND f.SOURCE_FILE_NAME = s._SOURCE_FILE_NAME
          AND COALESCE(f.SUBSCRIBER_KEY,'~') = COALESCE(s.SUBSCRIBER_KEY,'~')
          AND COALESCE(f.JOB_ID,-1) = COALESCE(s.JOB_ID,-1)
          AND COALESCE(f.EVENT_TIMESTAMP,TO_TIMESTAMP_NTZ('1900-01-01')) =
              COALESCE(TRY_TO_TIMESTAMP_NTZ(s.EVENT_DATE),TO_TIMESTAMP_NTZ('1900-01-01'))
    );

    v_rows_loaded := v_rows_loaded + SQLROWCOUNT;

    -- OPEN
    INSERT INTO QA_FIPSAR_DW.GOLD.FACT_SFMC_ENGAGEMENT
    (
        SUBSCRIBER_KEY, MASTER_PATIENT_ID, PROSPECT_KEY, JOB_KEY, EVENT_TYPE_KEY, DATE_KEY,
        EVENT_TIMESTAMP, EVENT_TYPE, DOMAIN, IS_UNIQUE, ACCOUNT_ID, JOB_ID, RECORD_TYPE,
        SOURCE_FILE_NAME, LOADED_AT, IS_SUPPRESSED, SUPPRESSION_REASON
    )
    SELECT
        o.SUBSCRIBER_KEY,
        dp.MASTER_PATIENT_ID,
        dp.PROSPECT_KEY,
        j.JOB_KEY,
        et.EVENT_TYPE_KEY,
        TO_NUMBER(TO_CHAR(TRY_TO_TIMESTAMP_NTZ(o.EVENT_DATE), 'YYYYMMDD')),
        TRY_TO_TIMESTAMP_NTZ(o.EVENT_DATE),
        'OPEN',
        o.DOMAIN,
        CASE WHEN UPPER(o.IS_UNIQUE) IN ('TRUE','1','YES','Y') THEN TRUE ELSE FALSE END,
        o.ACCOUNT_ID,
        o.JOB_ID,
        o.RECORD_TYPE,
        o._SOURCE_FILE_NAME,
        CURRENT_TIMESTAMP(),
        COALESCE(sp.SUPPRESSION_FLAG_VAL, FALSE),
        sp.SUPPRESSION_REASON_VAL
    FROM QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_OPENS o
    LEFT JOIN QA_FIPSAR_DW.GOLD.DIM_SFMC_JOB j
        ON o.JOB_ID = j.JOB_ID
    LEFT JOIN QA_FIPSAR_DW.GOLD.DIM_ENGAGEMENT_TYPE et
        ON et.EVENT_TYPE = 'OPEN'
    LEFT JOIN QA_FIPSAR_DW.GOLD.DIM_PROSPECT dp
        ON dp.MASTER_PATIENT_ID = o.SUBSCRIBER_KEY
    LEFT JOIN (
        SELECT DISTINCT PROSPECT_ID, TRUE AS SUPPRESSION_FLAG_VAL, 'SUPPRESSION_FLAG=YES' AS SUPPRESSION_REASON_VAL
        FROM QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_PROSPECT_JOURNEY_DETAILS
        WHERE UPPER(TRIM(SUPPRESSION_FLAG)) IN ('YES','Y','TRUE','1')
          AND NULLIF(TRIM(PROSPECT_ID), '') IS NOT NULL
    ) sp
        ON sp.PROSPECT_ID = COALESCE(dp.MASTER_PATIENT_ID, o.SUBSCRIBER_KEY)
    WHERE NOT EXISTS (
        SELECT 1
        FROM QA_FIPSAR_DW.GOLD.FACT_SFMC_ENGAGEMENT f
        WHERE f.EVENT_TYPE = 'OPEN'
          AND f.SOURCE_FILE_NAME = o._SOURCE_FILE_NAME
          AND COALESCE(f.SUBSCRIBER_KEY,'~') = COALESCE(o.SUBSCRIBER_KEY,'~')
          AND COALESCE(f.JOB_ID,-1) = COALESCE(o.JOB_ID,-1)
          AND COALESCE(f.EVENT_TIMESTAMP,TO_TIMESTAMP_NTZ('1900-01-01')) =
              COALESCE(TRY_TO_TIMESTAMP_NTZ(o.EVENT_DATE),TO_TIMESTAMP_NTZ('1900-01-01'))
    );

    v_rows_loaded := v_rows_loaded + SQLROWCOUNT;

    -- CLICK
    INSERT INTO QA_FIPSAR_DW.GOLD.FACT_SFMC_ENGAGEMENT
    (
        SUBSCRIBER_KEY, MASTER_PATIENT_ID, PROSPECT_KEY, JOB_KEY, EVENT_TYPE_KEY, DATE_KEY,
        EVENT_TIMESTAMP, EVENT_TYPE, DOMAIN, IS_UNIQUE, CLICK_URL, ACCOUNT_ID, JOB_ID, RECORD_TYPE,
        SOURCE_FILE_NAME, LOADED_AT, IS_SUPPRESSED, SUPPRESSION_REASON
    )
    SELECT
        c.SUBSCRIBER_KEY,
        dp.MASTER_PATIENT_ID,
        dp.PROSPECT_KEY,
        j.JOB_KEY,
        et.EVENT_TYPE_KEY,
        TO_NUMBER(TO_CHAR(TRY_TO_TIMESTAMP_NTZ(c.EVENT_DATE), 'YYYYMMDD')),
        TRY_TO_TIMESTAMP_NTZ(c.EVENT_DATE),
        'CLICK',
        c.DOMAIN,
        CASE WHEN UPPER(c.IS_UNIQUE) IN ('TRUE','1','YES','Y') THEN TRUE ELSE FALSE END,
        c.URL,
        c.ACCOUNT_ID,
        c.JOB_ID,
        c.RECORD_TYPE,
        c._SOURCE_FILE_NAME,
        CURRENT_TIMESTAMP(),
        COALESCE(sp.SUPPRESSION_FLAG_VAL, FALSE),
        sp.SUPPRESSION_REASON_VAL
    FROM QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_CLICKS c
    LEFT JOIN QA_FIPSAR_DW.GOLD.DIM_SFMC_JOB j
        ON c.JOB_ID = j.JOB_ID
    LEFT JOIN QA_FIPSAR_DW.GOLD.DIM_ENGAGEMENT_TYPE et
        ON et.EVENT_TYPE = 'CLICK'
    LEFT JOIN QA_FIPSAR_DW.GOLD.DIM_PROSPECT dp
        ON dp.MASTER_PATIENT_ID = c.SUBSCRIBER_KEY
    LEFT JOIN (
        SELECT DISTINCT PROSPECT_ID, TRUE AS SUPPRESSION_FLAG_VAL, 'SUPPRESSION_FLAG=YES' AS SUPPRESSION_REASON_VAL
        FROM QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_PROSPECT_JOURNEY_DETAILS
        WHERE UPPER(TRIM(SUPPRESSION_FLAG)) IN ('YES','Y','TRUE','1')
          AND NULLIF(TRIM(PROSPECT_ID), '') IS NOT NULL
    ) sp
        ON sp.PROSPECT_ID = COALESCE(dp.MASTER_PATIENT_ID, c.SUBSCRIBER_KEY)
    WHERE NOT EXISTS (
        SELECT 1
        FROM QA_FIPSAR_DW.GOLD.FACT_SFMC_ENGAGEMENT f
        WHERE f.EVENT_TYPE = 'CLICK'
          AND f.SOURCE_FILE_NAME = c._SOURCE_FILE_NAME
          AND COALESCE(f.SUBSCRIBER_KEY,'~') = COALESCE(c.SUBSCRIBER_KEY,'~')
          AND COALESCE(f.JOB_ID,-1) = COALESCE(c.JOB_ID,-1)
          AND COALESCE(f.EVENT_TIMESTAMP,TO_TIMESTAMP_NTZ('1900-01-01')) =
              COALESCE(TRY_TO_TIMESTAMP_NTZ(c.EVENT_DATE),TO_TIMESTAMP_NTZ('1900-01-01'))
          AND COALESCE(f.CLICK_URL,'~') = COALESCE(c.URL,'~')
    );

    v_rows_loaded := v_rows_loaded + SQLROWCOUNT;

    -- BOUNCE
    INSERT INTO QA_FIPSAR_DW.GOLD.FACT_SFMC_ENGAGEMENT
    (
        SUBSCRIBER_KEY, MASTER_PATIENT_ID, PROSPECT_KEY, JOB_KEY, EVENT_TYPE_KEY, DATE_KEY,
        EVENT_TIMESTAMP, EVENT_TYPE, DOMAIN, BOUNCE_CATEGORY, BOUNCE_TYPE, ACCOUNT_ID, JOB_ID, RECORD_TYPE,
        SOURCE_FILE_NAME, LOADED_AT, IS_SUPPRESSED, SUPPRESSION_REASON
    )
    SELECT
        b.SUBSCRIBER_KEY,
        dp.MASTER_PATIENT_ID,
        dp.PROSPECT_KEY,
        j.JOB_KEY,
        et.EVENT_TYPE_KEY,
        TO_NUMBER(TO_CHAR(TRY_TO_TIMESTAMP_NTZ(b.EVENT_DATE), 'YYYYMMDD')),
        TRY_TO_TIMESTAMP_NTZ(b.EVENT_DATE),
        'BOUNCE',
        b.DOMAIN,
        b.BOUNCE_CATEGORY,
        b.BOUNCE_TYPE,
        b.ACCOUNT_ID,
        b.JOB_ID,
        b.RECORD_TYPE,
        b._SOURCE_FILE_NAME,
        CURRENT_TIMESTAMP(),
        COALESCE(sp.SUPPRESSION_FLAG_VAL, FALSE),
        sp.SUPPRESSION_REASON_VAL
    FROM QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_BOUNCES b
    LEFT JOIN QA_FIPSAR_DW.GOLD.DIM_SFMC_JOB j
        ON b.JOB_ID = j.JOB_ID
    LEFT JOIN QA_FIPSAR_DW.GOLD.DIM_ENGAGEMENT_TYPE et
        ON et.EVENT_TYPE = 'BOUNCE'
    LEFT JOIN QA_FIPSAR_DW.GOLD.DIM_PROSPECT dp
        ON dp.MASTER_PATIENT_ID = b.SUBSCRIBER_KEY
    LEFT JOIN (
        SELECT DISTINCT PROSPECT_ID, TRUE AS SUPPRESSION_FLAG_VAL, 'SUPPRESSION_FLAG=YES' AS SUPPRESSION_REASON_VAL
        FROM QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_PROSPECT_JOURNEY_DETAILS
        WHERE UPPER(TRIM(SUPPRESSION_FLAG)) IN ('YES','Y','TRUE','1')
          AND NULLIF(TRIM(PROSPECT_ID), '') IS NOT NULL
    ) sp
        ON sp.PROSPECT_ID = COALESCE(dp.MASTER_PATIENT_ID, b.SUBSCRIBER_KEY)
    WHERE NOT EXISTS (
        SELECT 1
        FROM QA_FIPSAR_DW.GOLD.FACT_SFMC_ENGAGEMENT f
        WHERE f.EVENT_TYPE = 'BOUNCE'
          AND f.SOURCE_FILE_NAME = b._SOURCE_FILE_NAME
          AND COALESCE(f.SUBSCRIBER_KEY,'~') = COALESCE(b.SUBSCRIBER_KEY,'~')
          AND COALESCE(f.JOB_ID,-1) = COALESCE(b.JOB_ID,-1)
          AND COALESCE(f.EVENT_TIMESTAMP,TO_TIMESTAMP_NTZ('1900-01-01')) =
              COALESCE(TRY_TO_TIMESTAMP_NTZ(b.EVENT_DATE),TO_TIMESTAMP_NTZ('1900-01-01'))
          AND COALESCE(f.BOUNCE_CATEGORY,'~') = COALESCE(b.BOUNCE_CATEGORY,'~')
          AND COALESCE(f.BOUNCE_TYPE,'~') = COALESCE(b.BOUNCE_TYPE,'~')
    );

    v_rows_loaded := v_rows_loaded + SQLROWCOUNT;

    -- UNSUBSCRIBE
    INSERT INTO QA_FIPSAR_DW.GOLD.FACT_SFMC_ENGAGEMENT
    (
        SUBSCRIBER_KEY, MASTER_PATIENT_ID, PROSPECT_KEY, JOB_KEY, EVENT_TYPE_KEY, DATE_KEY,
        EVENT_TIMESTAMP, EVENT_TYPE, REASON, ACCOUNT_ID, JOB_ID, RECORD_TYPE,
        SOURCE_FILE_NAME, LOADED_AT, IS_SUPPRESSED, SUPPRESSION_REASON
    )
    SELECT
        u.SUBSCRIBER_KEY,
        dp.MASTER_PATIENT_ID,
        dp.PROSPECT_KEY,
        j.JOB_KEY,
        et.EVENT_TYPE_KEY,
        TO_NUMBER(TO_CHAR(TRY_TO_TIMESTAMP_NTZ(u.EVENT_DATE), 'YYYYMMDD')),
        TRY_TO_TIMESTAMP_NTZ(u.EVENT_DATE),
        'UNSUBSCRIBE',
        u.REASON,
        u.ACCOUNT_ID,
        u.JOB_ID,
        u.RECORD_TYPE,
        u._SOURCE_FILE_NAME,
        CURRENT_TIMESTAMP(),
        TRUE,
        COALESCE(sp.SUPPRESSION_REASON_VAL, 'UNSUBSCRIBE_EVENT')
    FROM QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_UNSUBSCRIBES u
    LEFT JOIN QA_FIPSAR_DW.GOLD.DIM_SFMC_JOB j
        ON u.JOB_ID = j.JOB_ID
    LEFT JOIN QA_FIPSAR_DW.GOLD.DIM_ENGAGEMENT_TYPE et
        ON et.EVENT_TYPE = 'UNSUBSCRIBE'
    LEFT JOIN QA_FIPSAR_DW.GOLD.DIM_PROSPECT dp
        ON dp.MASTER_PATIENT_ID = u.SUBSCRIBER_KEY
    LEFT JOIN (
        SELECT DISTINCT PROSPECT_ID, TRUE AS SUPPRESSION_FLAG_VAL, 'SUPPRESSION_FLAG=YES' AS SUPPRESSION_REASON_VAL
        FROM QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_PROSPECT_JOURNEY_DETAILS
        WHERE UPPER(TRIM(SUPPRESSION_FLAG)) IN ('YES','Y','TRUE','1')
          AND NULLIF(TRIM(PROSPECT_ID), '') IS NOT NULL
    ) sp
        ON sp.PROSPECT_ID = COALESCE(dp.MASTER_PATIENT_ID, u.SUBSCRIBER_KEY)
    WHERE NOT EXISTS (
        SELECT 1
        FROM QA_FIPSAR_DW.GOLD.FACT_SFMC_ENGAGEMENT f
        WHERE f.EVENT_TYPE = 'UNSUBSCRIBE'
          AND f.SOURCE_FILE_NAME = u._SOURCE_FILE_NAME
          AND COALESCE(f.SUBSCRIBER_KEY,'~') = COALESCE(u.SUBSCRIBER_KEY,'~')
          AND COALESCE(f.JOB_ID,-1) = COALESCE(u.JOB_ID,-1)
          AND COALESCE(f.EVENT_TIMESTAMP,TO_TIMESTAMP_NTZ('1900-01-01')) =
              COALESCE(TRY_TO_TIMESTAMP_NTZ(u.EVENT_DATE),TO_TIMESTAMP_NTZ('1900-01-01'))
    );

    v_rows_loaded := v_rows_loaded + SQLROWCOUNT;

    -- SPAM
    INSERT INTO QA_FIPSAR_DW.GOLD.FACT_SFMC_ENGAGEMENT
    (
        SUBSCRIBER_KEY, MASTER_PATIENT_ID, PROSPECT_KEY, JOB_KEY, EVENT_TYPE_KEY, DATE_KEY,
        EVENT_TIMESTAMP, EVENT_TYPE, REASON, ACCOUNT_ID, JOB_ID, RECORD_TYPE,
        SOURCE_FILE_NAME, LOADED_AT, IS_SUPPRESSED, SUPPRESSION_REASON
    )
    SELECT
        spm.SUBSCRIBER_KEY,
        dp.MASTER_PATIENT_ID,
        dp.PROSPECT_KEY,
        j.JOB_KEY,
        et.EVENT_TYPE_KEY,
        TO_NUMBER(TO_CHAR(TRY_TO_TIMESTAMP_NTZ(spm.EVENT_DATE), 'YYYYMMDD')),
        TRY_TO_TIMESTAMP_NTZ(spm.EVENT_DATE),
        'SPAM',
        spm.COMPLAINT_TYPE,
        spm.ACCOUNT_ID,
        spm.JOB_ID,
        spm.RECORD_TYPE,
        spm._SOURCE_FILE_NAME,
        CURRENT_TIMESTAMP(),
        TRUE,
        COALESCE(sp.SUPPRESSION_REASON_VAL, 'SPAM_EVENT')
    FROM QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_SPAM spm
    LEFT JOIN QA_FIPSAR_DW.GOLD.DIM_SFMC_JOB j
        ON spm.JOB_ID = j.JOB_ID
    LEFT JOIN QA_FIPSAR_DW.GOLD.DIM_ENGAGEMENT_TYPE et
        ON et.EVENT_TYPE = 'SPAM'
    LEFT JOIN QA_FIPSAR_DW.GOLD.DIM_PROSPECT dp
        ON dp.MASTER_PATIENT_ID = spm.SUBSCRIBER_KEY
    LEFT JOIN (
        SELECT DISTINCT PROSPECT_ID, TRUE AS SUPPRESSION_FLAG_VAL, 'SUPPRESSION_FLAG=YES' AS SUPPRESSION_REASON_VAL
        FROM QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_PROSPECT_JOURNEY_DETAILS
        WHERE UPPER(TRIM(SUPPRESSION_FLAG)) IN ('YES','Y','TRUE','1')
          AND NULLIF(TRIM(PROSPECT_ID), '') IS NOT NULL
    ) sp
        ON sp.PROSPECT_ID = COALESCE(dp.MASTER_PATIENT_ID, spm.SUBSCRIBER_KEY)
    WHERE NOT EXISTS (
        SELECT 1
        FROM QA_FIPSAR_DW.GOLD.FACT_SFMC_ENGAGEMENT f
        WHERE f.EVENT_TYPE = 'SPAM'
          AND f.SOURCE_FILE_NAME = spm._SOURCE_FILE_NAME
          AND COALESCE(f.SUBSCRIBER_KEY,'~') = COALESCE(spm.SUBSCRIBER_KEY,'~')
          AND COALESCE(f.JOB_ID,-1) = COALESCE(spm.JOB_ID,-1)
          AND COALESCE(f.EVENT_TIMESTAMP,TO_TIMESTAMP_NTZ('1900-01-01')) =
              COALESCE(TRY_TO_TIMESTAMP_NTZ(spm.EVENT_DATE),TO_TIMESTAMP_NTZ('1900-01-01'))
    );

    v_rows_loaded := v_rows_loaded + SQLROWCOUNT;

    UPDATE QA_FIPSAR_AUDIT.PIPELINE_AUDIT.PIPELINE_RUN_LOG
    SET STATUS = 'SUCCESS',
        ROWS_LOADED = :v_rows_loaded,
        ROWS_REJECTED = 0,
        ERROR_MESSAGE = NULL,
        COMPLETED_AT = CURRENT_TIMESTAMP()
    WHERE RUN_ID = :v_run_id;

    RETURN OBJECT_CONSTRUCT(
        'status','SUCCESS',
        'run_id',:v_run_id,
        'rows_loaded',:v_rows_loaded
    );

EXCEPTION
    WHEN OTHER THEN
        LET v_err VARCHAR DEFAULT sqlerrm;
        UPDATE QA_FIPSAR_AUDIT.PIPELINE_AUDIT.PIPELINE_RUN_LOG
        SET STATUS = 'FAILED',
            ERROR_MESSAGE = :v_err,
            COMPLETED_AT = CURRENT_TIMESTAMP()
        WHERE RUN_ID = :v_run_id;

        RETURN OBJECT_CONSTRUCT(
            'status','FAILED',
            'run_id',:v_run_id,
            'error',:v_err
        );
END;
$$;

-- =====================================================================================
-- PART 12: END-TO-END ORCHESTRATOR
-- =====================================================================================

CREATE OR REPLACE PROCEDURE QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.SP_RUN_SFMC_EVENTS_PIPELINE()
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    v_run_id          VARCHAR        DEFAULT UUID_STRING();
    v_start_time      TIMESTAMP_NTZ  DEFAULT CURRENT_TIMESTAMP();
    v_total_rows      NUMBER         DEFAULT 0;
    v_total_rejected  NUMBER         DEFAULT 0;
BEGIN
    INSERT INTO QA_FIPSAR_AUDIT.PIPELINE_AUDIT.PIPELINE_RUN_LOG
    (
        RUN_ID, PIPELINE_NAME, LAYER, TABLE_NAME, STATUS, STARTED_AT
    )
    VALUES
    (
        :v_run_id, 'SP_RUN_SFMC_EVENTS_PIPELINE', 'END_TO_END', 'SFMC_EVENTS_PIPELINE', 'STARTED', :v_start_time
    );

    CALL QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.SP_LOAD_RAW_SFMC_SENT();
    CALL QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.SP_LOAD_RAW_SFMC_OPENS();
    CALL QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.SP_LOAD_RAW_SFMC_CLICKS();
    CALL QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.SP_LOAD_RAW_SFMC_BOUNCES();
    CALL QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.SP_LOAD_RAW_SFMC_UNSUBSCRIBES();
    CALL QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.SP_LOAD_RAW_SFMC_SPAM();
    CALL QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.SP_LOAD_RAW_SFMC_JOB_METADATA();
    CALL QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.SP_LOAD_RAW_SFMC_JOB_DE_DETAIL();
    CALL QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.SP_LOAD_RAW_SFMC_UNSUBSCRIBE_DE_DETAIL();
    CALL QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.SP_LOAD_RAW_SFMC_PROSPECT_C();
    CALL QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.SP_LOAD_RAW_SFMC_PROSPECT_C_HISTORY();
    CALL QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.SP_LOAD_RAW_SFMC_PROSPECT_JOURNEY_DETAILS();
    CALL QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.SP_LOAD_RAW_SFMC_JOURNEY_ACTIVITY_GENERIC();

    CALL QA_FIPSAR_DW.GOLD.SP_LOAD_GOLD_DIM_ENGAGEMENT_TYPE();
    CALL QA_FIPSAR_DW.GOLD.SP_LOAD_GOLD_DIM_SFMC_JOB();
    CALL QA_FIPSAR_DW.GOLD.SP_LOG_SFMC_SUPPRESSIONS();

    -- FIX-04: Populate DIM_DATE for SFMC event dates before FACT load.
    -- SP_LOAD_GOLD_DIM_DATE only seeds dates from SLV_PROSPECT_MASTER;
    -- SFMC event dates would otherwise produce orphan DATE_KEYs in FACT_SFMC_ENGAGEMENT.
    INSERT INTO QA_FIPSAR_DW.GOLD.DIM_DATE
        (DATE_KEY, FULL_DATE, YEAR, QUARTER, MONTH, MONTH_NAME,
         WEEK_OF_YEAR, DAY_OF_MONTH, DAY_OF_WEEK, DAY_NAME, IS_WEEKEND)
    SELECT DISTINCT
        TO_NUMBER(TO_CHAR(evt_date, 'YYYYMMDD')) AS DATE_KEY,
        evt_date                                 AS FULL_DATE,
        YEAR(evt_date)                           AS YEAR,
        QUARTER(evt_date)                        AS QUARTER,
        MONTH(evt_date)                          AS MONTH,
        TO_CHAR(evt_date, 'MMMM')                AS MONTH_NAME,
        WEEKOFYEAR(evt_date)                     AS WEEK_OF_YEAR,
        DAY(evt_date)                            AS DAY_OF_MONTH,
        DAYOFWEEK(evt_date)                      AS DAY_OF_WEEK,
        TO_CHAR(evt_date, 'DY')                  AS DAY_NAME,
        CASE WHEN DAYOFWEEK(evt_date) IN (0, 6) THEN TRUE ELSE FALSE END AS IS_WEEKEND
    FROM (
        SELECT TRY_TO_DATE(EVENT_DATE) AS evt_date
        FROM QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_SENT
        WHERE EVENT_DATE IS NOT NULL
        UNION
        SELECT TRY_TO_DATE(EVENT_DATE)
        FROM QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_OPENS
        WHERE EVENT_DATE IS NOT NULL
        UNION
        SELECT TRY_TO_DATE(EVENT_DATE)
        FROM QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_CLICKS
        WHERE EVENT_DATE IS NOT NULL
        UNION
        SELECT TRY_TO_DATE(EVENT_DATE)
        FROM QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_BOUNCES
        WHERE EVENT_DATE IS NOT NULL
        UNION
        SELECT TRY_TO_DATE(EVENT_DATE)
        FROM QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_UNSUBSCRIBES
        WHERE EVENT_DATE IS NOT NULL
        UNION
        SELECT TRY_TO_DATE(EVENT_DATE)
        FROM QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_SPAM
        WHERE EVENT_DATE IS NOT NULL
    ) dates
    WHERE evt_date IS NOT NULL
      AND NOT EXISTS (
          SELECT 1
          FROM QA_FIPSAR_DW.GOLD.DIM_DATE d
          WHERE d.FULL_DATE = evt_date
      );

    CALL QA_FIPSAR_DW.GOLD.SP_LOAD_GOLD_FACT_SFMC_ENGAGEMENT();

    SELECT
        COALESCE(SUM(ROWS_LOADED), 0),
        COALESCE(SUM(ROWS_REJECTED), 0)
    INTO :v_total_rows, :v_total_rejected
    FROM QA_FIPSAR_AUDIT.PIPELINE_AUDIT.PIPELINE_RUN_LOG
    WHERE STARTED_AT >= :v_start_time
      AND RUN_ID     != :v_run_id
      AND STATUS      = 'SUCCESS';

    UPDATE QA_FIPSAR_AUDIT.PIPELINE_AUDIT.PIPELINE_RUN_LOG
    SET STATUS        = 'SUCCESS',
        ROWS_LOADED   = :v_total_rows,
        ROWS_REJECTED = :v_total_rejected,
        ERROR_MESSAGE = NULL,
        COMPLETED_AT  = CURRENT_TIMESTAMP()
    WHERE RUN_ID = :v_run_id;

    RETURN OBJECT_CONSTRUCT(
        'status',              'SUCCESS',
        'run_id',              :v_run_id,
        'pipeline',            'SFMC_EVENTS_PIPELINE',
        'total_rows_loaded',   :v_total_rows,
        'total_rows_rejected', :v_total_rejected
    );

EXCEPTION
    WHEN OTHER THEN
        LET v_err VARCHAR DEFAULT sqlerrm;
        UPDATE QA_FIPSAR_AUDIT.PIPELINE_AUDIT.PIPELINE_RUN_LOG
        SET STATUS        = 'FAILED',
            ERROR_MESSAGE = :v_err,
            COMPLETED_AT  = CURRENT_TIMESTAMP()
        WHERE RUN_ID = :v_run_id;

        RETURN OBJECT_CONSTRUCT(
            'status',  'FAILED',
            'run_id',  :v_run_id,
            'error',   :v_err
        );
END;
$$;

-- =====================================================================================
-- RECOMMENDED TEST ORDER
-- =====================================================================================

-- RAW one-by-one
-- CALL QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.SP_LOAD_RAW_SFMC_SENT();
-- CALL QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.SP_LOAD_RAW_SFMC_OPENS();
-- CALL QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.SP_LOAD_RAW_SFMC_CLICKS();
-- CALL QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.SP_LOAD_RAW_SFMC_BOUNCES();
-- CALL QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.SP_LOAD_RAW_SFMC_UNSUBSCRIBES();
-- CALL QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.SP_LOAD_RAW_SFMC_SPAM();
-- CALL QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.SP_LOAD_RAW_SFMC_JOB_METADATA();
-- CALL QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.SP_LOAD_RAW_SFMC_JOB_DE_DETAIL();
-- CALL QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.SP_LOAD_RAW_SFMC_PROSPECT_C();
-- CALL QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.SP_LOAD_RAW_SFMC_PROSPECT_C_HISTORY();
-- CALL QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.SP_LOAD_RAW_SFMC_PROSPECT_JOURNEY_DETAILS();

-- GOLD one-by-one
-- CALL QA_FIPSAR_DW.GOLD.SP_LOAD_GOLD_DIM_ENGAGEMENT_TYPE();
-- CALL QA_FIPSAR_DW.GOLD.SP_LOAD_GOLD_DIM_SFMC_JOB();
-- CALL QA_FIPSAR_DW.GOLD.SP_LOG_SFMC_SUPPRESSIONS();
-- CALL QA_FIPSAR_DW.GOLD.SP_LOAD_GOLD_FACT_SFMC_ENGAGEMENT();

-- END-TO-END
-- CALL QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.SP_RUN_SFMC_EVENTS_PIPELINE();

-- VALIDATION
-- SELECT * FROM QA_FIPSAR_AUDIT.PIPELINE_AUDIT.SFMC_FILE_LOAD_AUDIT ORDER BY STARTED_AT DESC;
-- SELECT * FROM QA_FIPSAR_AUDIT.PIPELINE_AUDIT.PIPELINE_RUN_LOG ORDER BY STARTED_AT DESC;
-- SELECT EVENT_TYPE, COUNT(*) FROM QA_FIPSAR_DW.GOLD.FACT_SFMC_ENGAGEMENT GROUP BY 1 ORDER BY 1;
-- SELECT COUNT(*) FROM QA_FIPSAR_AUDIT.PIPELINE_AUDIT.DQ_REJECTION_LOG WHERE REJECTION_REASON = 'SUPPRESSED_PROSPECT';
