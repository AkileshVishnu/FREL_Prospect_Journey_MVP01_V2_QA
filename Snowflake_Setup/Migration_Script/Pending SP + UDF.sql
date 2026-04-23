-- ─────────────────────────────────────────────────────────────────────
-- FIPSAR_PHI_HUB.PHI_CORE  (2 procedures)
-- ─────────────────────────────────────────────────────────────────────

CREATE OR REPLACE PROCEDURE "SP_LOAD_STAGING_FROM_S3"()
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE
    v_run_id        VARCHAR DEFAULT UUID_STRING();
    v_before_count  NUMBER DEFAULT 0;
    v_after_count   NUMBER DEFAULT 0;
    v_rows_loaded   NUMBER DEFAULT 0;
BEGIN
    INSERT INTO FIPSAR_AUDIT.PIPELINE_AUDIT.PIPELINE_RUN_LOG
    (
        RUN_ID,
        PIPELINE_NAME,
        LAYER,
        TABLE_NAME,
        STATUS
    )
    VALUES
    (
        :v_run_id,
        ''SP_LOAD_STAGING_FROM_S3'',
        ''PHI'',
        ''STG_PROSPECT_INTAKE'',
        ''STARTED''
    );

    SELECT COUNT(*)
    INTO :v_before_count
    FROM FIPSAR_PHI_HUB.STAGING.STG_PROSPECT_INTAKE;

    COPY INTO FIPSAR_PHI_HUB.STAGING.STG_PROSPECT_INTAKE
    (
        FIRST_NAME,
        LAST_NAME,
        EMAIL,
        PHONE_NUMBER,
        AGE,
        ADDRESS,
        APARTMENT_NO,
        CITY,
        STATE,
        ZIP_CODE,
        PATIENT_CONSENT,
        CHANNEL,
        SUBMISSION_TIMESTAMP,
        FILE_DATE,
        _SOURCE_FILE
    )
    FROM
    (
        SELECT
            $1::VARCHAR  AS FIRST_NAME,
            $2::VARCHAR  AS LAST_NAME,
            $3::VARCHAR  AS EMAIL,
            $4::VARCHAR  AS PHONE_NUMBER,
            $5::VARCHAR  AS AGE,
            $6::VARCHAR  AS ADDRESS,
            $7::VARCHAR  AS APARTMENT_NO,
            $8::VARCHAR  AS CITY,
            $9::VARCHAR  AS STATE,
            $10::VARCHAR AS ZIP_CODE,
            $11::VARCHAR AS PATIENT_CONSENT,
            $12::VARCHAR AS CHANNEL,
            $13::VARCHAR AS SUBMISSION_TIMESTAMP,
            $14::VARCHAR AS FILE_DATE,
            METADATA$FILENAME::VARCHAR AS _SOURCE_FILE
        FROM @FIPSAR_PHI_HUB.STAGING.STG_PROSPECT_INBOUND
    )
    FILE_FORMAT = (FORMAT_NAME = FIPSAR_PHI_HUB.STAGING.FF_PROSPECT_CSV)
    PATTERN     = ''.*prospect_campaign_.*[.]csv''
    ON_ERROR    = CONTINUE
    FORCE       = FALSE;

    SELECT COUNT(*)
    INTO :v_after_count
    FROM FIPSAR_PHI_HUB.STAGING.STG_PROSPECT_INTAKE;

    v_rows_loaded := v_after_count - v_before_count;

    UPDATE FIPSAR_AUDIT.PIPELINE_AUDIT.PIPELINE_RUN_LOG
    SET
        STATUS = ''SUCCESS'',
        ROWS_LOADED = :v_rows_loaded,
        ROWS_REJECTED = 0,
        ERROR_MESSAGE = NULL,
        COMPLETED_AT = CURRENT_TIMESTAMP()
    WHERE RUN_ID = :v_run_id;

    RETURN OBJECT_CONSTRUCT(
        ''status'', ''SUCCESS'',
        ''run_id'', :v_run_id,
        ''rows_loaded'', :v_rows_loaded
    );

EXCEPTION
    WHEN OTHER THEN
        UPDATE FIPSAR_AUDIT.PIPELINE_AUDIT.PIPELINE_RUN_LOG
        SET
            STATUS = ''FAILED'',
            ERROR_MESSAGE = ''SP_LOAD_STAGING_FROM_S3 failed'',
            COMPLETED_AT = CURRENT_TIMESTAMP()
        WHERE RUN_ID = :v_run_id;

        RETURN OBJECT_CONSTRUCT(
            ''status'', ''FAILED'',
            ''run_id'', :v_run_id,
            ''error'', ''SP_LOAD_STAGING_FROM_S3 failed''
        );
END;
';

CREATE OR REPLACE PROCEDURE "SP_LOAD_PHI_PROSPECT"()
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE
    v_run_id              VARCHAR DEFAULT UUID_STRING();
    v_rows_loaded         INTEGER DEFAULT 0;
    v_new_ids             INTEGER DEFAULT 0;
    v_rejected_rows       INTEGER DEFAULT 0;
    v_null_first_name     INTEGER DEFAULT 0;
    v_null_last_name      INTEGER DEFAULT 0;
    v_null_email          INTEGER DEFAULT 0;
    v_null_phone          INTEGER DEFAULT 0;
    v_test_email          INTEGER DEFAULT 0;
    v_invalid_file_date   INTEGER DEFAULT 0;
    v_last_run            TIMESTAMP_NTZ DEFAULT ''1900-01-01''::TIMESTAMP_NTZ;
BEGIN
    INSERT INTO FIPSAR_AUDIT.PIPELINE_AUDIT.PIPELINE_RUN_LOG
        (RUN_ID, PIPELINE_NAME, LAYER, TABLE_NAME, STATUS)
    VALUES
        (:v_run_id, ''SP_LOAD_PHI_PROSPECT'', ''PHI'', ''PHI_PROSPECT_MASTER'', ''STARTED'');

    -- Watermark: only process staging records loaded since the last successful run.
    -- Prevents duplicate DQ rejections for records that were already processed.
    -- Defaults to epoch on first-ever run so all existing staging rows are evaluated.
    SELECT COALESCE(MAX(COMPLETED_AT), ''1900-01-01''::TIMESTAMP_NTZ)
    INTO :v_last_run
    FROM FIPSAR_AUDIT.PIPELINE_AUDIT.PIPELINE_RUN_LOG
    WHERE PIPELINE_NAME = ''SP_LOAD_PHI_PROSPECT''
      AND STATUS = ''SUCCESS'';

    -- ------------------------------------------------------------------------
    -- STEP 1: Log rejected rows to DQ_REJECTION_LOG (new staging rows only)
    -- ------------------------------------------------------------------------

    -- NULL_FIRST_NAME
    INSERT INTO FIPSAR_AUDIT.PIPELINE_AUDIT.DQ_REJECTION_LOG
    (
        TABLE_NAME,
        REJECTION_REASON,
        REJECTED_RECORD,
        REJECTED_AT
    )
    SELECT
        ''PHI_PROSPECT_MASTER'' AS TABLE_NAME,
        ''NULL_FIRST_NAME'' AS REJECTION_REASON,
        OBJECT_CONSTRUCT(
            ''FIRST_NAME'', FIRST_NAME,
            ''LAST_NAME'', LAST_NAME,
            ''EMAIL'', EMAIL,
            ''PHONE_NUMBER'', PHONE_NUMBER,
            ''CHANNEL'', CHANNEL,
            ''FILE_DATE'', FILE_DATE,
            ''STAGE'', ''PHI_LOAD''
        ) AS REJECTED_RECORD,
        CURRENT_TIMESTAMP() AS REJECTED_AT
    FROM FIPSAR_PHI_HUB.STAGING.STG_PROSPECT_INTAKE
    WHERE _LOADED_AT > :v_last_run
      AND NULLIF(TRIM(FIRST_NAME), '''') IS NULL;

    v_null_first_name := SQLROWCOUNT;

    -- NULL_LAST_NAME
    INSERT INTO FIPSAR_AUDIT.PIPELINE_AUDIT.DQ_REJECTION_LOG
    (
        TABLE_NAME,
        REJECTION_REASON,
        REJECTED_RECORD,
        REJECTED_AT
    )
    SELECT
        ''PHI_PROSPECT_MASTER'',
        ''NULL_LAST_NAME'',
        OBJECT_CONSTRUCT(
            ''FIRST_NAME'', FIRST_NAME,
            ''LAST_NAME'', LAST_NAME,
            ''EMAIL'', EMAIL,
            ''PHONE_NUMBER'', PHONE_NUMBER,
            ''CHANNEL'', CHANNEL,
            ''FILE_DATE'', FILE_DATE,
            ''STAGE'', ''PHI_LOAD''
        ),
        CURRENT_TIMESTAMP()
    FROM FIPSAR_PHI_HUB.STAGING.STG_PROSPECT_INTAKE
    WHERE _LOADED_AT > :v_last_run
      AND NULLIF(TRIM(FIRST_NAME), '''') IS NOT NULL
      AND NULLIF(TRIM(LAST_NAME), '''') IS NULL;

    v_null_last_name := SQLROWCOUNT;

    -- NULL_EMAIL
    INSERT INTO FIPSAR_AUDIT.PIPELINE_AUDIT.DQ_REJECTION_LOG
    (
        TABLE_NAME,
        REJECTION_REASON,
        REJECTED_RECORD,
        REJECTED_AT
    )
    SELECT
        ''PHI_PROSPECT_MASTER'',
        ''NULL_EMAIL'',
        OBJECT_CONSTRUCT(
            ''FIRST_NAME'', FIRST_NAME,
            ''LAST_NAME'', LAST_NAME,
            ''EMAIL'', EMAIL,
            ''PHONE_NUMBER'', PHONE_NUMBER,
            ''CHANNEL'', CHANNEL,
            ''FILE_DATE'', FILE_DATE,
            ''STAGE'', ''PHI_LOAD''
        ),
        CURRENT_TIMESTAMP()
    FROM FIPSAR_PHI_HUB.STAGING.STG_PROSPECT_INTAKE
    WHERE _LOADED_AT > :v_last_run
      AND NULLIF(TRIM(FIRST_NAME), '''') IS NOT NULL
      AND NULLIF(TRIM(LAST_NAME), '''') IS NOT NULL
      AND NULLIF(TRIM(EMAIL), '''') IS NULL;

    v_null_email := SQLROWCOUNT;

    -- NULL_PHONE_NUMBER
    INSERT INTO FIPSAR_AUDIT.PIPELINE_AUDIT.DQ_REJECTION_LOG
    (
        TABLE_NAME,
        REJECTION_REASON,
        REJECTED_RECORD,
        REJECTED_AT
    )
    SELECT
        ''PHI_PROSPECT_MASTER'',
        ''NULL_PHONE_NUMBER'',
        OBJECT_CONSTRUCT(
            ''FIRST_NAME'', FIRST_NAME,
            ''LAST_NAME'', LAST_NAME,
            ''EMAIL'', EMAIL,
            ''PHONE_NUMBER'', PHONE_NUMBER,
            ''CHANNEL'', CHANNEL,
            ''FILE_DATE'', FILE_DATE,
            ''STAGE'', ''PHI_LOAD''
        ),
        CURRENT_TIMESTAMP()
    FROM FIPSAR_PHI_HUB.STAGING.STG_PROSPECT_INTAKE
    WHERE _LOADED_AT > :v_last_run
      AND NULLIF(TRIM(FIRST_NAME), '''') IS NOT NULL
      AND NULLIF(TRIM(LAST_NAME), '''') IS NOT NULL
      AND NULLIF(TRIM(EMAIL), '''') IS NOT NULL
      AND NULLIF(TRIM(PHONE_NUMBER), '''') IS NULL;

    v_null_phone := SQLROWCOUNT;

    -- TEST_EMAIL_DOMAIN
    INSERT INTO FIPSAR_AUDIT.PIPELINE_AUDIT.DQ_REJECTION_LOG
    (
        TABLE_NAME,
        REJECTION_REASON,
        REJECTED_RECORD,
        REJECTED_AT
    )
    SELECT
        ''PHI_PROSPECT_MASTER'',
        ''TEST_EMAIL_DOMAIN'',
        OBJECT_CONSTRUCT(
            ''FIRST_NAME'', FIRST_NAME,
            ''LAST_NAME'', LAST_NAME,
            ''EMAIL'', EMAIL,
            ''PHONE_NUMBER'', PHONE_NUMBER,
            ''CHANNEL'', CHANNEL,
            ''FILE_DATE'', FILE_DATE,
            ''STAGE'', ''PHI_LOAD''
        ),
        CURRENT_TIMESTAMP()
    FROM FIPSAR_PHI_HUB.STAGING.STG_PROSPECT_INTAKE
    WHERE _LOADED_AT > :v_last_run
      AND NULLIF(TRIM(FIRST_NAME), '''') IS NOT NULL
      AND NULLIF(TRIM(LAST_NAME), '''') IS NOT NULL
      AND NULLIF(TRIM(EMAIL), '''') IS NOT NULL
      AND NULLIF(TRIM(PHONE_NUMBER), '''') IS NOT NULL
      AND LOWER(TRIM(EMAIL)) LIKE ''%@test.com'';

    v_test_email := SQLROWCOUNT;

    -- INVALID_FILE_DATE
    INSERT INTO FIPSAR_AUDIT.PIPELINE_AUDIT.DQ_REJECTION_LOG
    (
        TABLE_NAME,
        REJECTION_REASON,
        REJECTED_RECORD,
        REJECTED_AT
    )
    SELECT
        ''PHI_PROSPECT_MASTER'',
        ''INVALID_FILE_DATE'',
        OBJECT_CONSTRUCT(
            ''FIRST_NAME'', FIRST_NAME,
            ''LAST_NAME'', LAST_NAME,
            ''EMAIL'', EMAIL,
            ''PHONE_NUMBER'', PHONE_NUMBER,
            ''CHANNEL'', CHANNEL,
            ''FILE_DATE'', FILE_DATE,
            ''STAGE'', ''PHI_LOAD''
        ),
        CURRENT_TIMESTAMP()
    FROM FIPSAR_PHI_HUB.STAGING.STG_PROSPECT_INTAKE
    WHERE _LOADED_AT > :v_last_run
      AND NULLIF(TRIM(FIRST_NAME), '''') IS NOT NULL
      AND NULLIF(TRIM(LAST_NAME), '''') IS NOT NULL
      AND NULLIF(TRIM(EMAIL), '''') IS NOT NULL
      AND NULLIF(TRIM(PHONE_NUMBER), '''') IS NOT NULL
      AND LOWER(TRIM(EMAIL)) NOT LIKE ''%@test.com''   -- exclude already-rejected test domains
      AND COALESCE(
            TRY_TO_DATE(FILE_DATE, ''YYYY-MM-DD''),
            TRY_TO_DATE(FILE_DATE, ''DD-MM-YYYY''),
            TRY_TO_DATE(FILE_DATE)
          ) IS NULL;

    v_invalid_file_date := SQLROWCOUNT;

    v_rejected_rows := v_null_first_name
                     + v_null_last_name
                     + v_null_email
                     + v_null_phone
                     + v_test_email
                     + v_invalid_file_date;

    -- ------------------------------------------------------------------------
    -- STEP 2: Merge only valid identities into PATIENT_IDENTITY_XREF
    -- ------------------------------------------------------------------------
    MERGE INTO FIPSAR_PHI_HUB.PHI_CORE.PATIENT_IDENTITY_XREF tgt
    USING (
        SELECT DISTINCT
            UPPER(TRIM(FIRST_NAME)) || ''|'' || UPPER(TRIM(LAST_NAME)) || ''|'' || LOWER(TRIM(EMAIL)) AS IDENTITY_KEY,
            TRIM(FIRST_NAME) AS FIRST_NAME,
            TRIM(LAST_NAME) AS LAST_NAME,
            LOWER(TRIM(EMAIL)) AS EMAIL
        FROM FIPSAR_PHI_HUB.STAGING.STG_PROSPECT_INTAKE
        WHERE _LOADED_AT > :v_last_run
          AND NULLIF(TRIM(FIRST_NAME), '''') IS NOT NULL
          AND NULLIF(TRIM(LAST_NAME), '''') IS NOT NULL
          AND NULLIF(TRIM(EMAIL), '''') IS NOT NULL
          AND NULLIF(TRIM(PHONE_NUMBER), '''') IS NOT NULL
          AND LOWER(TRIM(EMAIL)) NOT LIKE ''%@test.com''
          AND COALESCE(
                TRY_TO_DATE(FILE_DATE, ''YYYY-MM-DD''),
                TRY_TO_DATE(FILE_DATE, ''DD-MM-YYYY''),
                TRY_TO_DATE(FILE_DATE)
              ) IS NOT NULL
    ) src
    ON tgt.IDENTITY_KEY = src.IDENTITY_KEY
    WHEN NOT MATCHED THEN
        INSERT
        (
            MASTER_PATIENT_ID,
            IDENTITY_KEY,
            FIRST_NAME,
            LAST_NAME,
            EMAIL
        )
        VALUES
        (
            ''FIP'' || LPAD(FIPSAR_PHI_HUB.PHI_CORE.SEQ_MASTER_PATIENT_ID.NEXTVAL::VARCHAR, 10, ''0''),
            src.IDENTITY_KEY,
            src.FIRST_NAME,
            src.LAST_NAME,
            src.EMAIL
        );

    v_new_ids := SQLROWCOUNT;

    -- ------------------------------------------------------------------------
    -- STEP 3: Insert only valid rows into PHI_PROSPECT_MASTER
    -- Keep current minimal delta guard: (MASTER_PATIENT_ID, FILE_DATE)
    -- ------------------------------------------------------------------------
    INSERT INTO FIPSAR_PHI_HUB.PHI_CORE.PHI_PROSPECT_MASTER
    (
        MASTER_PATIENT_ID,
        FIRST_NAME,
        LAST_NAME,
        EMAIL,
        PHONE_NUMBER,
        AGE,
        ADDRESS,
        APARTMENT_NO,
        CITY,
        STATE,
        ZIP_CODE,
        PATIENT_CONSENT,
        CHANNEL,
        SUBMISSION_TIMESTAMP,
        FILE_DATE
    )
    SELECT
        xref.MASTER_PATIENT_ID,
        TRIM(stg.FIRST_NAME) AS FIRST_NAME,
        TRIM(stg.LAST_NAME) AS LAST_NAME,
        LOWER(TRIM(stg.EMAIL)) AS EMAIL,
        TRIM(stg.PHONE_NUMBER) AS PHONE_NUMBER,
        TRY_CAST(stg.AGE AS INTEGER) AS AGE,
        stg.ADDRESS,
        stg.APARTMENT_NO,
        stg.CITY,
        UPPER(stg.STATE) AS STATE,
        stg.ZIP_CODE,
        CASE
            WHEN UPPER(TRIM(stg.PATIENT_CONSENT)) = ''TRUE'' THEN TRUE
            ELSE FALSE
        END AS PATIENT_CONSENT,
        stg.CHANNEL,
        COALESCE(
            TRY_TO_TIMESTAMP_NTZ(stg.SUBMISSION_TIMESTAMP, ''YYYY-MM-DD HH24:MI:SS''),
            TRY_TO_TIMESTAMP_NTZ(stg.SUBMISSION_TIMESTAMP, ''DD-MM-YYYY HH24:MI''),
            TRY_TO_TIMESTAMP_NTZ(stg.SUBMISSION_TIMESTAMP)
        ) AS SUBMISSION_TIMESTAMP,
        COALESCE(
            TRY_TO_DATE(stg.FILE_DATE, ''YYYY-MM-DD''),
            TRY_TO_DATE(stg.FILE_DATE, ''DD-MM-YYYY''),
            TRY_TO_DATE(stg.FILE_DATE)
        ) AS FILE_DATE
    FROM FIPSAR_PHI_HUB.STAGING.STG_PROSPECT_INTAKE stg
    INNER JOIN FIPSAR_PHI_HUB.PHI_CORE.PATIENT_IDENTITY_XREF xref
        ON UPPER(TRIM(stg.FIRST_NAME)) || ''|'' || UPPER(TRIM(stg.LAST_NAME)) || ''|'' || LOWER(TRIM(stg.EMAIL))
         = xref.IDENTITY_KEY
    WHERE stg._LOADED_AT > :v_last_run
      AND NULLIF(TRIM(stg.FIRST_NAME), '''') IS NOT NULL
      AND NULLIF(TRIM(stg.LAST_NAME), '''') IS NOT NULL
      AND NULLIF(TRIM(stg.EMAIL), '''') IS NOT NULL
      AND NULLIF(TRIM(stg.PHONE_NUMBER), '''') IS NOT NULL
      AND LOWER(TRIM(stg.EMAIL)) NOT LIKE ''%@test.com''
      AND COALESCE(
            TRY_TO_DATE(stg.FILE_DATE, ''YYYY-MM-DD''),
            TRY_TO_DATE(stg.FILE_DATE, ''DD-MM-YYYY''),
            TRY_TO_DATE(stg.FILE_DATE)
          ) IS NOT NULL
      AND NOT EXISTS
      (
          SELECT 1
          FROM FIPSAR_PHI_HUB.PHI_CORE.PHI_PROSPECT_MASTER tgt
          WHERE tgt.MASTER_PATIENT_ID = xref.MASTER_PATIENT_ID
            AND tgt.FILE_DATE = COALESCE(
                                    TRY_TO_DATE(stg.FILE_DATE, ''YYYY-MM-DD''),
                                    TRY_TO_DATE(stg.FILE_DATE, ''DD-MM-YYYY''),
                                    TRY_TO_DATE(stg.FILE_DATE)
                                )
      );

    v_rows_loaded := SQLROWCOUNT;

    -- ------------------------------------------------------------------------
    -- STEP 4: Update run log
    -- ------------------------------------------------------------------------
    UPDATE FIPSAR_AUDIT.PIPELINE_AUDIT.PIPELINE_RUN_LOG
    SET
        STATUS = ''SUCCESS'',
        ROWS_LOADED = :v_rows_loaded,
        COMPLETED_AT = CURRENT_TIMESTAMP()
    WHERE RUN_ID = :v_run_id;

    RETURN OBJECT_CONSTRUCT(
        ''status'', ''SUCCESS'',
        ''run_id'', :v_run_id,
        ''rows_appended'', :v_rows_loaded,
        ''new_identities'', :v_new_ids,
        ''rejected_rows'', :v_rejected_rows,
        ''null_first_name'', :v_null_first_name,
        ''null_last_name'', :v_null_last_name,
        ''null_email'', :v_null_email,
        ''null_phone_number'', :v_null_phone,
        ''test_email_domain'', :v_test_email,
        ''invalid_file_date'', :v_invalid_file_date
    );

EXCEPTION
    WHEN OTHER THEN
        UPDATE FIPSAR_AUDIT.PIPELINE_AUDIT.PIPELINE_RUN_LOG
        SET
            STATUS = ''FAILED'',
            ERROR_MESSAGE = ''SP_LOAD_PHI_PROSPECT failed'',
            COMPLETED_AT = CURRENT_TIMESTAMP()
        WHERE RUN_ID = :v_run_id;

        RETURN OBJECT_CONSTRUCT(
            ''status'', ''FAILED'',
            ''run_id'', :v_run_id,
            ''error'', ''SP_LOAD_PHI_PROSPECT failed''
        );
END;
';

-- ─────────────────────────────────────────────────────────────────────
-- FIPSAR_DW.BRONZE  (3 procedures)
-- ─────────────────────────────────────────────────────────────────────

CREATE OR REPLACE PROCEDURE "SP_LOAD_BRONZE_PROSPECT"()
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE
    v_rows_loaded NUMBER DEFAULT 0;
    v_run_id      VARCHAR DEFAULT UUID_STRING();
BEGIN
    INSERT INTO FIPSAR_AUDIT.PIPELINE_AUDIT.PIPELINE_RUN_LOG
    (
        RUN_ID, PIPELINE_NAME, LAYER, TABLE_NAME, STATUS
    )
    VALUES
    (
        :v_run_id, ''SP_LOAD_BRONZE_PROSPECT'', ''BRONZE'', ''BRZ_PROSPECT_MASTER'', ''STARTED''
    );

    INSERT INTO FIPSAR_DW.BRONZE.BRZ_PROSPECT_MASTER
    (
        RECORD_ID, MASTER_PATIENT_ID, FIRST_NAME, LAST_NAME, EMAIL,
        PHONE_NUMBER, AGE, ADDRESS, APARTMENT_NO, CITY, STATE, ZIP_CODE,
        PATIENT_CONSENT, CHANNEL, SUBMISSION_TIMESTAMP, FILE_DATE,
        PHI_LOADED_AT, IS_ACTIVE, SOURCE_LAYER
    )
    SELECT
        p.RECORD_ID,
        p.MASTER_PATIENT_ID,
        p.FIRST_NAME,
        p.LAST_NAME,
        p.EMAIL,
        p.PHONE_NUMBER,
        p.AGE,
        p.ADDRESS,
        p.APARTMENT_NO,
        p.CITY,
        p.STATE,
        p.ZIP_CODE,
        p.PATIENT_CONSENT,
        p.CHANNEL,
        p.SUBMISSION_TIMESTAMP,
        p.FILE_DATE,
        p.PHI_LOADED_AT,
        p.IS_ACTIVE,
        ''PHI_HUB''
    FROM FIPSAR_PHI_HUB.PHI_CORE.PHI_PROSPECT_MASTER p
    WHERE NOT EXISTS
    (
        SELECT 1
        FROM FIPSAR_DW.BRONZE.BRZ_PROSPECT_MASTER b
        WHERE b.RECORD_ID = p.RECORD_ID
    );

    v_rows_loaded := SQLROWCOUNT;

    UPDATE FIPSAR_AUDIT.PIPELINE_AUDIT.PIPELINE_RUN_LOG
    SET
        STATUS = ''SUCCESS'',
        ROWS_LOADED = :v_rows_loaded,
        ROWS_REJECTED = 0,
        ERROR_MESSAGE = NULL,
        COMPLETED_AT = CURRENT_TIMESTAMP()
    WHERE RUN_ID = :v_run_id;

    RETURN OBJECT_CONSTRUCT(
        ''status'', ''SUCCESS'',
        ''run_id'', :v_run_id,
        ''rows_loaded'', :v_rows_loaded
    );

EXCEPTION
    WHEN OTHER THEN
        UPDATE FIPSAR_AUDIT.PIPELINE_AUDIT.PIPELINE_RUN_LOG
        SET
            STATUS = ''FAILED'',
            ERROR_MESSAGE = ''SP_LOAD_BRONZE_PROSPECT failed'',
            COMPLETED_AT = CURRENT_TIMESTAMP()
        WHERE RUN_ID = :v_run_id;

        RETURN OBJECT_CONSTRUCT(
            ''status'', ''FAILED'',
            ''run_id'', :v_run_id,
            ''error'', ''SP_LOAD_BRONZE_PROSPECT failed''
        );
END;
';

CREATE OR REPLACE PROCEDURE "SP_LOAD_PHI_PROSPECT"()
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE
    v_rows          INTEGER DEFAULT 0;
    v_new_ids       INTEGER DEFAULT 0;
    v_run_id        VARCHAR DEFAULT UUID_STRING();
BEGIN
    INSERT INTO FIPSAR_AUDIT.PIPELINE_AUDIT.PIPELINE_RUN_LOG
        (RUN_ID, PIPELINE_NAME, LAYER, TABLE_NAME, STATUS)
    VALUES (:v_run_id, ''SP_LOAD_PHI_PROSPECT'', ''PHI'', ''PHI_PROSPECT_MASTER'', ''STARTED'');

    -- Step 1: Resolve new identities → assign MASTER_PATIENT_ID via sequence
    MERGE INTO FIPSAR_PHI_HUB.PHI_CORE.PATIENT_IDENTITY_XREF tgt
    USING (
        SELECT DISTINCT
            UPPER(TRIM(FIRST_NAME)) || ''|'' || UPPER(TRIM(LAST_NAME)) || ''|'' || LOWER(TRIM(EMAIL)) AS IDENTITY_KEY,
            TRIM(FIRST_NAME) AS FIRST_NAME,
            TRIM(LAST_NAME)  AS LAST_NAME,
            LOWER(TRIM(EMAIL)) AS EMAIL
        FROM FIPSAR_PHI_HUB.STAGING.STG_PROSPECT_INTAKE
        WHERE NULLIF(TRIM(FIRST_NAME), '''') IS NOT NULL
          AND NULLIF(TRIM(LAST_NAME),  '''') IS NOT NULL
          AND NULLIF(TRIM(EMAIL),      '''') IS NOT NULL
    ) src
    ON tgt.IDENTITY_KEY = src.IDENTITY_KEY
    WHEN NOT MATCHED THEN
        INSERT (MASTER_PATIENT_ID, IDENTITY_KEY, FIRST_NAME, LAST_NAME, EMAIL)
        VALUES (
            ''FIP'' || LPAD(FIPSAR_PHI_HUB.PHI_CORE.SEQ_MASTER_PATIENT_ID.NEXTVAL::VARCHAR, 10, ''0''),
            src.IDENTITY_KEY, src.FIRST_NAME, src.LAST_NAME, src.EMAIL
        );

    v_new_ids := SQLROWCOUNT;

    -- Step 2: Append new FILE_DATE rows to PHI Master (idempotent — NOT EXISTS guard)
    INSERT INTO FIPSAR_PHI_HUB.PHI_CORE.PHI_PROSPECT_MASTER (
        MASTER_PATIENT_ID, FIRST_NAME, LAST_NAME, EMAIL, PHONE_NUMBER, AGE,
        ADDRESS, APARTMENT_NO, CITY, STATE, ZIP_CODE, PATIENT_CONSENT,
        CHANNEL, SUBMISSION_TIMESTAMP, FILE_DATE
    )
    SELECT
        xref.MASTER_PATIENT_ID,
        TRIM(stg.FIRST_NAME), TRIM(stg.LAST_NAME), LOWER(TRIM(stg.EMAIL)),
        stg.PHONE_NUMBER, TRY_CAST(stg.AGE AS INTEGER),
        stg.ADDRESS, stg.APARTMENT_NO, stg.CITY, UPPER(stg.STATE), stg.ZIP_CODE,
        CASE WHEN UPPER(TRIM(stg.PATIENT_CONSENT)) = ''TRUE'' THEN TRUE ELSE FALSE END,
        stg.CHANNEL,
        COALESCE(
            TRY_TO_TIMESTAMP_NTZ(stg.SUBMISSION_TIMESTAMP, ''YYYY-MM-DD HH24:MI:SS''),
            TRY_TO_TIMESTAMP_NTZ(stg.SUBMISSION_TIMESTAMP, ''DD-MM-YYYY HH24:MI''),
            TRY_TO_TIMESTAMP_NTZ(stg.SUBMISSION_TIMESTAMP)
        ),
        COALESCE(
            TRY_TO_DATE(stg.FILE_DATE, ''YYYY-MM-DD''),
            TRY_TO_DATE(stg.FILE_DATE, ''DD-MM-YYYY''),
            TRY_TO_DATE(stg.FILE_DATE)
        )
    FROM FIPSAR_PHI_HUB.STAGING.STG_PROSPECT_INTAKE stg
    INNER JOIN FIPSAR_PHI_HUB.PHI_CORE.PATIENT_IDENTITY_XREF xref
        ON UPPER(TRIM(stg.FIRST_NAME)) || ''|'' || UPPER(TRIM(stg.LAST_NAME)) || ''|'' || LOWER(TRIM(stg.EMAIL))
         = xref.IDENTITY_KEY
    WHERE NULLIF(TRIM(stg.FIRST_NAME), '''') IS NOT NULL
      AND NULLIF(TRIM(stg.LAST_NAME),  '''') IS NOT NULL
      AND NULLIF(TRIM(stg.EMAIL),      '''') IS NOT NULL
      AND NULLIF(TRIM(stg.FILE_DATE),  '''') IS NOT NULL
      AND COALESCE(TRY_TO_DATE(stg.FILE_DATE, ''YYYY-MM-DD''), TRY_TO_DATE(stg.FILE_DATE)) IS NOT NULL
    -- Idempotent guard: skip if (MASTER_PATIENT_ID, FILE_DATE) already in PHI
    AND NOT EXISTS (
        SELECT 1 FROM FIPSAR_PHI_HUB.PHI_CORE.PHI_PROSPECT_MASTER tgt
        WHERE tgt.MASTER_PATIENT_ID = xref.MASTER_PATIENT_ID
          AND tgt.FILE_DATE = COALESCE(TRY_TO_DATE(stg.FILE_DATE, ''YYYY-MM-DD''), TRY_TO_DATE(stg.FILE_DATE))
    );

    v_rows := SQLROWCOUNT;

    UPDATE FIPSAR_AUDIT.PIPELINE_AUDIT.PIPELINE_RUN_LOG
    SET STATUS = ''SUCCESS'', ROWS_LOADED = :v_rows, COMPLETED_AT = CURRENT_TIMESTAMP()
    WHERE RUN_ID = :v_run_id;

    RETURN OBJECT_CONSTRUCT(''status'',''SUCCESS'',''rows_appended'',:v_rows,''new_identities'',:v_new_ids);
EXCEPTION
    WHEN OTHER THEN
        UPDATE FIPSAR_AUDIT.PIPELINE_AUDIT.PIPELINE_RUN_LOG
        SET STATUS = ''FAILED'', ERROR_MESSAGE = SQLERRM, COMPLETED_AT = CURRENT_TIMESTAMP()
        WHERE RUN_ID = :v_run_id;
        RETURN OBJECT_CONSTRUCT(''status'', ''FAILED'', ''error'', SQLERRM);
END;
';

CREATE OR REPLACE PROCEDURE "SP_LOAD_STAGING_FROM_S3"()
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE
    v_rows   INTEGER DEFAULT 0;
    v_run_id VARCHAR DEFAULT UUID_STRING();
BEGIN
    INSERT INTO FIPSAR_AUDIT.PIPELINE_AUDIT.PIPELINE_RUN_LOG
        (RUN_ID, PIPELINE_NAME, LAYER, TABLE_NAME, STATUS)
    VALUES (:v_run_id, ''SP_LOAD_STAGING_FROM_S3'', ''PHI'', ''STG_PROSPECT_INTAKE'', ''STARTED'');

    TRUNCATE TABLE FIPSAR_PHI_HUB.STAGING.STG_PROSPECT_INTAKE;

    COPY INTO FIPSAR_PHI_HUB.STAGING.STG_PROSPECT_INTAKE (
        FIRST_NAME, LAST_NAME, EMAIL, PHONE_NUMBER, AGE,
        ADDRESS, APARTMENT_NO, CITY, STATE, ZIP_CODE,
        PATIENT_CONSENT, CHANNEL, SUBMISSION_TIMESTAMP, FILE_DATE
    )
    FROM @FIPSAR_PHI_HUB.STAGING.STG_PROSPECT_INBOUND
    FILE_FORMAT = (FORMAT_NAME = FIPSAR_PHI_HUB.STAGING.FF_PROSPECT_CSV)
    PATTERN     = ''.*prospect_campaign_.*[.]csv''
    ON_ERROR    = CONTINUE;

    SELECT COUNT(*) INTO :v_rows FROM FIPSAR_PHI_HUB.STAGING.STG_PROSPECT_INTAKE;

    UPDATE FIPSAR_AUDIT.PIPELINE_AUDIT.PIPELINE_RUN_LOG
    SET STATUS = ''SUCCESS'', ROWS_LOADED = :v_rows, COMPLETED_AT = CURRENT_TIMESTAMP()
    WHERE RUN_ID = :v_run_id;

    RETURN OBJECT_CONSTRUCT(''status'', ''SUCCESS'', ''staging_rows'', :v_rows);
EXCEPTION
    WHEN OTHER THEN
        UPDATE FIPSAR_AUDIT.PIPELINE_AUDIT.PIPELINE_RUN_LOG
        SET STATUS = ''FAILED'', ERROR_MESSAGE = SQLERRM, COMPLETED_AT = CURRENT_TIMESTAMP()
        WHERE RUN_ID = :v_run_id;
        RETURN OBJECT_CONSTRUCT(''status'', ''FAILED'', ''error'', SQLERRM);
END;
';

-- ─────────────────────────────────────────────────────────────────────
-- FIPSAR_DW.SILVER  (1 procedure)
-- ─────────────────────────────────────────────────────────────────────

CREATE OR REPLACE PROCEDURE "SP_LOAD_SILVER_PROSPECT"()
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE
    v_run_id           VARCHAR DEFAULT UUID_STRING();
    v_rows_loaded      NUMBER DEFAULT 0;
    v_rows_rejected    NUMBER DEFAULT 0;
    v_scd2_closed      NUMBER DEFAULT 0;
BEGIN
    INSERT INTO FIPSAR_AUDIT.PIPELINE_AUDIT.PIPELINE_RUN_LOG
    (
        RUN_ID, PIPELINE_NAME, LAYER, TABLE_NAME, STATUS
    )
    VALUES
    (
        :v_run_id, ''SP_LOAD_SILVER_PROSPECT'', ''SILVER'', ''SLV_PROSPECT_MASTER'', ''STARTED''
    );

    -- ----------------------------------------------------------------------
    -- 1. LOG DQ REJECTIONS
    -- Rules:
    --   a) required fields missing / blank / N/A email
    --   b) duplicate RECORD_ID already in Silver
    --   c) duplicate RECORD_ID within Bronze batch (keep one)
    -- Prevent duplicate DQ logs across reruns
    -- ----------------------------------------------------------------------

    INSERT INTO FIPSAR_AUDIT.PIPELINE_AUDIT.DQ_REJECTION_LOG
    (
        TABLE_NAME, REJECTION_REASON, REJECTED_RECORD, REJECTED_AT
    )
    WITH bronze_ranked AS (
        SELECT
            b.*,
            ROW_NUMBER() OVER (
                PARTITION BY b.RECORD_ID
                ORDER BY b.BRONZE_LOADED_AT, b.FILE_DATE, b.RECORD_ID
            ) AS RN_IN_BRONZE
        FROM FIPSAR_DW.BRONZE.BRZ_PROSPECT_MASTER b
    ),
    rejected_rows AS (
        SELECT
            ''SLV_PROSPECT_MASTER'' AS TABLE_NAME,
            CASE
                WHEN NULLIF(TRIM(MASTER_PATIENT_ID), '''') IS NULL THEN ''NULL_MASTER_PATIENT_ID''
                WHEN NULLIF(TRIM(FIRST_NAME), '''') IS NULL THEN ''NULL_FIRST_NAME''
                WHEN NULLIF(TRIM(LAST_NAME), '''') IS NULL THEN ''NULL_LAST_NAME''
                WHEN NULLIF(TRIM(EMAIL), '''') IS NULL OR UPPER(TRIM(EMAIL)) = ''N/A'' THEN ''NULL_EMAIL''
                WHEN RECORD_ID IN (SELECT RECORD_ID FROM FIPSAR_DW.SILVER.SLV_PROSPECT_MASTER) THEN ''DUPLICATE_RECORD_ID''
                WHEN RN_IN_BRONZE > 1 THEN ''DUPLICATE_RECORD_ID_IN_BRONZE''
            END AS REJECTION_REASON,
            OBJECT_CONSTRUCT(
                ''RECORD_ID'', RECORD_ID,
                ''MASTER_PATIENT_ID'', MASTER_PATIENT_ID,
                ''FIRST_NAME'', FIRST_NAME,
                ''LAST_NAME'', LAST_NAME,
                ''EMAIL'', EMAIL,
                ''PHONE_NUMBER'', PHONE_NUMBER,
                ''FILE_DATE'', FILE_DATE,
                ''CHANNEL'', CHANNEL,
                ''STAGE'', ''SILVER_LOAD''
            ) AS REJECTED_RECORD
        FROM bronze_ranked
        WHERE
            NULLIF(TRIM(MASTER_PATIENT_ID), '''') IS NULL
            OR NULLIF(TRIM(FIRST_NAME), '''') IS NULL
            OR NULLIF(TRIM(LAST_NAME), '''') IS NULL
            OR NULLIF(TRIM(EMAIL), '''') IS NULL
            OR UPPER(TRIM(EMAIL)) = ''N/A''
            OR RECORD_ID IN (SELECT RECORD_ID FROM FIPSAR_DW.SILVER.SLV_PROSPECT_MASTER)
            OR RN_IN_BRONZE > 1
    )
    SELECT
        r.TABLE_NAME,
        r.REJECTION_REASON,
        r.REJECTED_RECORD,
        CURRENT_TIMESTAMP()
    FROM rejected_rows r
    WHERE NOT EXISTS
    (
        SELECT 1
        FROM FIPSAR_AUDIT.PIPELINE_AUDIT.DQ_REJECTION_LOG d
        WHERE d.TABLE_NAME = r.TABLE_NAME
          AND d.REJECTION_REASON = r.REJECTION_REASON
          AND d.REJECTED_RECORD = r.REJECTED_RECORD
    );

    v_rows_rejected := SQLROWCOUNT;

    -- ----------------------------------------------------------------------
    -- 2. CLOSE CURRENT SCD2 ROWS ONLY WHEN CHANGED
    -- Use a valid bronze set:
    --   - required fields present
    --   - email not N/A
    --   - not already in silver by RECORD_ID
    --   - keep only one row per RECORD_ID from bronze batch
    -- ----------------------------------------------------------------------

    UPDATE FIPSAR_DW.SILVER.SLV_PROSPECT_MASTER slv
    SET
        EFF_END_DATE = valid_src.FILE_DATE,
        IS_CURRENT = FALSE
    FROM
    (
        WITH bronze_ranked AS (
            SELECT
                b.*,
                ROW_NUMBER() OVER (
                    PARTITION BY b.RECORD_ID
                    ORDER BY b.BRONZE_LOADED_AT, b.FILE_DATE, b.RECORD_ID
                ) AS RN_IN_BRONZE
            FROM FIPSAR_DW.BRONZE.BRZ_PROSPECT_MASTER b
        )
        SELECT br.*
        FROM bronze_ranked br
        WHERE NULLIF(TRIM(br.MASTER_PATIENT_ID), '''') IS NOT NULL
          AND NULLIF(TRIM(br.FIRST_NAME), '''') IS NOT NULL
          AND NULLIF(TRIM(br.LAST_NAME), '''') IS NOT NULL
          AND NULLIF(TRIM(br.EMAIL), '''') IS NOT NULL
          AND UPPER(TRIM(br.EMAIL)) <> ''N/A''
          AND br.RN_IN_BRONZE = 1
          AND NOT EXISTS
          (
              SELECT 1
              FROM FIPSAR_DW.SILVER.SLV_PROSPECT_MASTER s
              WHERE s.RECORD_ID = br.RECORD_ID
          )
    ) valid_src
    WHERE slv.MASTER_PATIENT_ID = valid_src.MASTER_PATIENT_ID
      AND slv.IS_CURRENT = TRUE
      AND (
            COALESCE(slv.FIRST_NAME, ''~'')      <> COALESCE(valid_src.FIRST_NAME, ''~'')
         OR COALESCE(slv.LAST_NAME, ''~'')       <> COALESCE(valid_src.LAST_NAME, ''~'')
         OR COALESCE(slv.EMAIL, ''~'')           <> COALESCE(valid_src.EMAIL, ''~'')
         OR COALESCE(slv.PHONE_NUMBER, ''~'')    <> COALESCE(valid_src.PHONE_NUMBER, ''~'')
         OR COALESCE(slv.AGE, -1)              <> COALESCE(valid_src.AGE, -1)
         OR COALESCE(slv.ADDRESS, ''~'')         <> COALESCE(valid_src.ADDRESS, ''~'')
         OR COALESCE(slv.APARTMENT_NO, ''~'')    <> COALESCE(valid_src.APARTMENT_NO, ''~'')
         OR COALESCE(slv.CITY, ''~'')            <> COALESCE(valid_src.CITY, ''~'')
         OR COALESCE(slv.STATE, ''~'')           <> COALESCE(valid_src.STATE, ''~'')
         OR COALESCE(slv.ZIP_CODE, ''~'')        <> COALESCE(valid_src.ZIP_CODE, ''~'')
         OR COALESCE(slv.PATIENT_CONSENT, FALSE) <> COALESCE(valid_src.PATIENT_CONSENT, FALSE)
         OR COALESCE(slv.CHANNEL, ''~'')         <> COALESCE(valid_src.CHANNEL, ''~'')
         OR COALESCE(slv.IS_ACTIVE, FALSE)     <> COALESCE(valid_src.IS_ACTIVE, FALSE)
      );

    v_scd2_closed := SQLROWCOUNT;

    -- ----------------------------------------------------------------------
    -- 3. INSERT NEW CURRENT ROWS
    -- Insert:
    --   - new patient never seen before
    --   - or changed patient after old current row was closed
    -- Do not insert unchanged duplicates
    -- ----------------------------------------------------------------------

    INSERT INTO FIPSAR_DW.SILVER.SLV_PROSPECT_MASTER
    (
        RECORD_ID,
        MASTER_PATIENT_ID,
        FIRST_NAME,
        LAST_NAME,
        EMAIL,
        PHONE_NUMBER,
        AGE,
        ADDRESS,
        APARTMENT_NO,
        CITY,
        STATE,
        ZIP_CODE,
        PATIENT_CONSENT,
        CHANNEL,
        SUBMISSION_TIMESTAMP,
        FILE_DATE,
        PHI_LOADED_AT,
        IS_ACTIVE,
        EFF_START_DATE,
        EFF_END_DATE,
        IS_CURRENT,
        VERSION_NUM,
        DQ_PASSED
    )
    WITH bronze_ranked AS (
        SELECT
            b.*,
            ROW_NUMBER() OVER (
                PARTITION BY b.RECORD_ID
                ORDER BY b.BRONZE_LOADED_AT, b.FILE_DATE, b.RECORD_ID
            ) AS RN_IN_BRONZE
        FROM FIPSAR_DW.BRONZE.BRZ_PROSPECT_MASTER b
    ),
    valid_bronze AS (
        SELECT br.*
        FROM bronze_ranked br
        WHERE NULLIF(TRIM(br.MASTER_PATIENT_ID), '''') IS NOT NULL
          AND NULLIF(TRIM(br.FIRST_NAME), '''') IS NOT NULL
          AND NULLIF(TRIM(br.LAST_NAME), '''') IS NOT NULL
          AND NULLIF(TRIM(br.EMAIL), '''') IS NOT NULL
          AND UPPER(TRIM(br.EMAIL)) <> ''N/A''
          AND br.RN_IN_BRONZE = 1
          AND NOT EXISTS
          (
              SELECT 1
              FROM FIPSAR_DW.SILVER.SLV_PROSPECT_MASTER s
              WHERE s.RECORD_ID = br.RECORD_ID
          )
    ),
    silver_curr AS (
        SELECT *
        FROM FIPSAR_DW.SILVER.SLV_PROSPECT_MASTER
        WHERE IS_CURRENT = TRUE
    ),
    max_ver AS (
        SELECT MASTER_PATIENT_ID, MAX(VERSION_NUM) AS MAX_VERSION_NUM
        FROM FIPSAR_DW.SILVER.SLV_PROSPECT_MASTER
        GROUP BY MASTER_PATIENT_ID
    )
    SELECT
        vb.RECORD_ID,
        vb.MASTER_PATIENT_ID,
        vb.FIRST_NAME,
        vb.LAST_NAME,
        LOWER(TRIM(vb.EMAIL)),
        vb.PHONE_NUMBER,
        vb.AGE,
        vb.ADDRESS,
        vb.APARTMENT_NO,
        vb.CITY,
        vb.STATE,
        vb.ZIP_CODE,
        vb.PATIENT_CONSENT,
        vb.CHANNEL,
        vb.SUBMISSION_TIMESTAMP,
        vb.FILE_DATE,
        vb.PHI_LOADED_AT,
        vb.IS_ACTIVE,
        vb.FILE_DATE AS EFF_START_DATE,
        TO_DATE(''9999-12-31'') AS EFF_END_DATE,
        TRUE AS IS_CURRENT,
        COALESCE(mv.MAX_VERSION_NUM, 0) + 1 AS VERSION_NUM,
        TRUE AS DQ_PASSED
    FROM valid_bronze vb
    LEFT JOIN silver_curr sc
        ON sc.MASTER_PATIENT_ID = vb.MASTER_PATIENT_ID
    LEFT JOIN max_ver mv
        ON mv.MASTER_PATIENT_ID = vb.MASTER_PATIENT_ID
    WHERE
        sc.MASTER_PATIENT_ID IS NULL
        OR (
            COALESCE(sc.FIRST_NAME, ''~'')      <> COALESCE(vb.FIRST_NAME, ''~'')
         OR COALESCE(sc.LAST_NAME, ''~'')       <> COALESCE(vb.LAST_NAME, ''~'')
         OR COALESCE(sc.EMAIL, ''~'')           <> COALESCE(vb.EMAIL, ''~'')
         OR COALESCE(sc.PHONE_NUMBER, ''~'')    <> COALESCE(vb.PHONE_NUMBER, ''~'')
         OR COALESCE(sc.AGE, -1)              <> COALESCE(vb.AGE, -1)
         OR COALESCE(sc.ADDRESS, ''~'')         <> COALESCE(vb.ADDRESS, ''~'')
         OR COALESCE(sc.APARTMENT_NO, ''~'')    <> COALESCE(vb.APARTMENT_NO, ''~'')
         OR COALESCE(sc.CITY, ''~'')            <> COALESCE(vb.CITY, ''~'')
         OR COALESCE(sc.STATE, ''~'')           <> COALESCE(vb.STATE, ''~'')
         OR COALESCE(sc.ZIP_CODE, ''~'')        <> COALESCE(vb.ZIP_CODE, ''~'')
         OR COALESCE(sc.PATIENT_CONSENT, FALSE) <> COALESCE(vb.PATIENT_CONSENT, FALSE)
         OR COALESCE(sc.CHANNEL, ''~'')         <> COALESCE(vb.CHANNEL, ''~'')
         OR COALESCE(sc.IS_ACTIVE, FALSE)     <> COALESCE(vb.IS_ACTIVE, FALSE)
        );

    v_rows_loaded := SQLROWCOUNT;

    UPDATE FIPSAR_AUDIT.PIPELINE_AUDIT.PIPELINE_RUN_LOG
    SET
        STATUS = ''SUCCESS'',
        ROWS_LOADED = :v_rows_loaded,
        ROWS_REJECTED = :v_rows_rejected,
        ERROR_MESSAGE = NULL,
        COMPLETED_AT = CURRENT_TIMESTAMP()
    WHERE RUN_ID = :v_run_id;

    RETURN OBJECT_CONSTRUCT(
        ''status'', ''SUCCESS'',
        ''run_id'', :v_run_id,
        ''rows_loaded'', :v_rows_loaded,
        ''rows_rejected'', :v_rows_rejected,
        ''scd2_closed'', :v_scd2_closed
    );

EXCEPTION
    WHEN OTHER THEN
        UPDATE FIPSAR_AUDIT.PIPELINE_AUDIT.PIPELINE_RUN_LOG
        SET
            STATUS = ''FAILED'',
            ERROR_MESSAGE = ''SP_LOAD_SILVER_PROSPECT failed'',
            COMPLETED_AT = CURRENT_TIMESTAMP()
        WHERE RUN_ID = :v_run_id;

        RETURN OBJECT_CONSTRUCT(
            ''status'', ''FAILED'',
            ''run_id'', :v_run_id,
            ''error'', ''SP_LOAD_SILVER_PROSPECT failed''
        );
END;
';

-- ─────────────────────────────────────────────────────────────────────
-- FIPSAR_DW.GOLD  (9 procedures)
-- ─────────────────────────────────────────────────────────────────────

CREATE OR REPLACE PROCEDURE "SP_LOAD_GOLD_DIM_PROSPECT"()
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE
    v_run_id      VARCHAR DEFAULT UUID_STRING();
    v_rows_loaded NUMBER DEFAULT 0;
BEGIN
    INSERT INTO FIPSAR_AUDIT.PIPELINE_AUDIT.PIPELINE_RUN_LOG
    (
        RUN_ID, PIPELINE_NAME, LAYER, TABLE_NAME, STATUS
    )
    VALUES
    (
        :v_run_id, ''SP_LOAD_GOLD_DIM_PROSPECT'', ''GOLD'', ''DIM_PROSPECT'', ''STARTED''
    );

    MERGE INTO FIPSAR_DW.GOLD.DIM_PROSPECT tgt
    USING (
        WITH current_rows AS (
            SELECT *
            FROM (
                SELECT
                    s.*,
                    ROW_NUMBER() OVER (
                        PARTITION BY s.MASTER_PATIENT_ID
                        ORDER BY s.EFF_START_DATE DESC,
                                 s.SILVER_LOADED_AT DESC,
                                 s.SLV_KEY DESC
                    ) AS RN
                FROM FIPSAR_DW.SILVER.SLV_PROSPECT_MASTER s
                WHERE s.DQ_PASSED = TRUE
                  AND s.IS_CURRENT = TRUE
            )
            WHERE RN = 1
        ),
        intake_stats AS (
            SELECT
                MASTER_PATIENT_ID,
                MIN(FILE_DATE) AS FIRST_INTAKE_DATE,
                MAX(FILE_DATE) AS LAST_INTAKE_DATE,
                COUNT(*) AS INTAKE_COUNT
            FROM FIPSAR_DW.SILVER.SLV_PROSPECT_MASTER
            WHERE DQ_PASSED = TRUE
            GROUP BY MASTER_PATIENT_ID
        ),
        primary_channel AS (
            SELECT
                MASTER_PATIENT_ID,
                CHANNEL AS PRIMARY_CHANNEL
            FROM (
                SELECT
                    MASTER_PATIENT_ID,
                    CHANNEL,
                    SUBMISSION_TIMESTAMP,
                    ROW_NUMBER() OVER (
                        PARTITION BY MASTER_PATIENT_ID
                        ORDER BY SUBMISSION_TIMESTAMP
                    ) AS RN
                FROM FIPSAR_DW.SILVER.SLV_PROSPECT_MASTER
                WHERE DQ_PASSED = TRUE
            )
            WHERE RN = 1
        ),
        -- Only process prospects that are new OR have a newer Silver FILE_DATE than Gold''s
        -- LAST_INTAKE_DATE — prevents reprocessing the full dimension on every run.
        incremental_candidates AS (
            SELECT DISTINCT s.MASTER_PATIENT_ID
            FROM FIPSAR_DW.SILVER.SLV_PROSPECT_MASTER s
            LEFT JOIN FIPSAR_DW.GOLD.DIM_PROSPECT g
                ON g.MASTER_PATIENT_ID = s.MASTER_PATIENT_ID
            WHERE s.DQ_PASSED = TRUE
              AND (
                  g.MASTER_PATIENT_ID IS NULL          -- brand-new: not yet in DIM_PROSPECT
                  OR s.FILE_DATE > g.LAST_INTAKE_DATE  -- existing: Silver has newer FILE_DATE than Gold
              )
        )
        SELECT
            c.MASTER_PATIENT_ID,
            c.FIRST_NAME,
            c.LAST_NAME,
            c.EMAIL,
            c.PHONE_NUMBER,
            c.AGE,
            CASE
                WHEN c.AGE < 18 THEN ''Under 18''
                WHEN c.AGE BETWEEN 18 AND 25 THEN ''18-25''
                WHEN c.AGE BETWEEN 26 AND 35 THEN ''26-35''
                WHEN c.AGE BETWEEN 36 AND 45 THEN ''36-45''
                WHEN c.AGE BETWEEN 46 AND 55 THEN ''46-55''
                WHEN c.AGE BETWEEN 56 AND 65 THEN ''56-65''
                WHEN c.AGE > 65 THEN ''65+''
                ELSE ''Unknown''
            END AS AGE_GROUP,
            c.CITY,
            c.STATE,
            c.ZIP_CODE,
            c.PATIENT_CONSENT,
            s.FIRST_INTAKE_DATE,
            s.LAST_INTAKE_DATE,
            s.INTAKE_COUNT,
            pc.PRIMARY_CHANNEL,
            c.IS_ACTIVE
        FROM current_rows c
        INNER JOIN intake_stats s
            ON c.MASTER_PATIENT_ID = s.MASTER_PATIENT_ID
        LEFT JOIN primary_channel pc
            ON c.MASTER_PATIENT_ID = pc.MASTER_PATIENT_ID
        INNER JOIN incremental_candidates ic
            ON c.MASTER_PATIENT_ID = ic.MASTER_PATIENT_ID
    ) src
    ON tgt.MASTER_PATIENT_ID = src.MASTER_PATIENT_ID
    WHEN MATCHED THEN UPDATE SET
        tgt.FIRST_NAME = src.FIRST_NAME,
        tgt.LAST_NAME = src.LAST_NAME,
        tgt.EMAIL = src.EMAIL,
        tgt.PHONE_NUMBER = src.PHONE_NUMBER,
        tgt.AGE = src.AGE,
        tgt.AGE_GROUP = src.AGE_GROUP,
        tgt.CITY = src.CITY,
        tgt.STATE = src.STATE,
        tgt.ZIP_CODE = src.ZIP_CODE,
        tgt.PATIENT_CONSENT = src.PATIENT_CONSENT,
        tgt.FIRST_INTAKE_DATE = src.FIRST_INTAKE_DATE,
        tgt.LAST_INTAKE_DATE = src.LAST_INTAKE_DATE,
        tgt.INTAKE_COUNT = src.INTAKE_COUNT,
        tgt.PRIMARY_CHANNEL = src.PRIMARY_CHANNEL,
        tgt.IS_ACTIVE = src.IS_ACTIVE,
        tgt.UPDATED_AT = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN
        INSERT
        (
            MASTER_PATIENT_ID, FIRST_NAME, LAST_NAME, EMAIL, PHONE_NUMBER,
            AGE, AGE_GROUP, CITY, STATE, ZIP_CODE, PATIENT_CONSENT,
            FIRST_INTAKE_DATE, LAST_INTAKE_DATE, INTAKE_COUNT,
            PRIMARY_CHANNEL, IS_ACTIVE
        )
        VALUES
        (
            src.MASTER_PATIENT_ID, src.FIRST_NAME, src.LAST_NAME, src.EMAIL, src.PHONE_NUMBER,
            src.AGE, src.AGE_GROUP, src.CITY, src.STATE, src.ZIP_CODE, src.PATIENT_CONSENT,
            src.FIRST_INTAKE_DATE, src.LAST_INTAKE_DATE, src.INTAKE_COUNT,
            src.PRIMARY_CHANNEL, src.IS_ACTIVE
        );

    v_rows_loaded := SQLROWCOUNT;

    UPDATE FIPSAR_AUDIT.PIPELINE_AUDIT.PIPELINE_RUN_LOG
    SET
        STATUS = ''SUCCESS'',
        ROWS_LOADED = :v_rows_loaded,
        ROWS_REJECTED = 0,
        ERROR_MESSAGE = NULL,
        COMPLETED_AT = CURRENT_TIMESTAMP()
    WHERE RUN_ID = :v_run_id;

    RETURN OBJECT_CONSTRUCT(
        ''status'', ''SUCCESS'',
        ''run_id'', :v_run_id,
        ''rows_loaded'', :v_rows_loaded
    );

EXCEPTION
    WHEN OTHER THEN
        UPDATE FIPSAR_AUDIT.PIPELINE_AUDIT.PIPELINE_RUN_LOG
        SET
            STATUS = ''FAILED'',
            ERROR_MESSAGE = ''SP_LOAD_GOLD_DIM_PROSPECT failed'',
            COMPLETED_AT = CURRENT_TIMESTAMP()
        WHERE RUN_ID = :v_run_id;

        RETURN OBJECT_CONSTRUCT(
            ''status'', ''FAILED'',
            ''run_id'', :v_run_id,
            ''error'', ''SP_LOAD_GOLD_DIM_PROSPECT failed''
        );
END;
';

CREATE OR REPLACE PROCEDURE "SP_LOAD_GOLD_FACT_INTAKE"()
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE
    v_run_id      VARCHAR DEFAULT UUID_STRING();
    v_rows_loaded NUMBER DEFAULT 0;
BEGIN
    INSERT INTO FIPSAR_AUDIT.PIPELINE_AUDIT.PIPELINE_RUN_LOG
    (
        RUN_ID, PIPELINE_NAME, LAYER, TABLE_NAME, STATUS
    )
    VALUES
    (
        :v_run_id, ''SP_LOAD_GOLD_FACT_INTAKE'', ''GOLD'', ''FACT_PROSPECT_INTAKE'', ''STARTED''
    );

    INSERT INTO FIPSAR_DW.GOLD.FACT_PROSPECT_INTAKE
    (
        RECORD_ID,
        MASTER_PATIENT_ID,
        PROSPECT_KEY,
        CHANNEL_KEY,
        DATE_KEY,
        GEO_KEY,
        SUBMISSION_TIMESTAMP,
        FILE_DATE,
        AGE,
        CONSENT_FLAG
    )
    SELECT
        slv.RECORD_ID,
        slv.MASTER_PATIENT_ID,
        dp.PROSPECT_KEY,
        dc.CHANNEL_KEY,
        dd.DATE_KEY,
        dg.GEO_KEY,
        slv.SUBMISSION_TIMESTAMP,
        slv.FILE_DATE,
        slv.AGE,
        CASE WHEN slv.PATIENT_CONSENT = TRUE THEN 1 ELSE 0 END AS CONSENT_FLAG
    FROM FIPSAR_DW.SILVER.SLV_PROSPECT_MASTER slv
    LEFT JOIN FIPSAR_DW.GOLD.DIM_PROSPECT dp
        ON slv.MASTER_PATIENT_ID = dp.MASTER_PATIENT_ID
    LEFT JOIN FIPSAR_DW.GOLD.DIM_CHANNEL dc
        ON slv.CHANNEL = dc.CHANNEL_NAME
    LEFT JOIN FIPSAR_DW.GOLD.DIM_DATE dd
        ON slv.FILE_DATE = dd.FULL_DATE
    LEFT JOIN FIPSAR_DW.GOLD.DIM_GEOGRAPHY dg
        ON slv.CITY = dg.CITY
       AND slv.STATE = dg.STATE
       AND slv.ZIP_CODE = dg.ZIP_CODE
    WHERE slv.DQ_PASSED = TRUE
      AND NOT EXISTS
      (
          SELECT 1
          FROM FIPSAR_DW.GOLD.FACT_PROSPECT_INTAKE f
          WHERE f.RECORD_ID = slv.RECORD_ID
      );

    v_rows_loaded := SQLROWCOUNT;

    UPDATE FIPSAR_AUDIT.PIPELINE_AUDIT.PIPELINE_RUN_LOG
    SET
        STATUS = ''SUCCESS'',
        ROWS_LOADED = :v_rows_loaded,
        ROWS_REJECTED = 0,
        ERROR_MESSAGE = NULL,
        COMPLETED_AT = CURRENT_TIMESTAMP()
    WHERE RUN_ID = :v_run_id;

    RETURN OBJECT_CONSTRUCT(
        ''status'', ''SUCCESS'',
        ''run_id'', :v_run_id,
        ''rows_loaded'', :v_rows_loaded
    );

EXCEPTION
    WHEN OTHER THEN
        UPDATE FIPSAR_AUDIT.PIPELINE_AUDIT.PIPELINE_RUN_LOG
        SET
            STATUS = ''FAILED'',
            ERROR_MESSAGE = ''SP_LOAD_GOLD_FACT_INTAKE failed'',
            COMPLETED_AT = CURRENT_TIMESTAMP()
        WHERE RUN_ID = :v_run_id;

        RETURN OBJECT_CONSTRUCT(
            ''status'', ''FAILED'',
            ''run_id'', :v_run_id,
            ''error'', ''SP_LOAD_GOLD_FACT_INTAKE failed''
        );
END;
';

CREATE OR REPLACE PROCEDURE "SP_EXPORT_SFMC_OUTBOUND"()
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE
    v_rows              INTEGER DEFAULT 0;
    v_run_id            VARCHAR DEFAULT UUID_STRING();
    v_timestamp         VARCHAR;
    v_target_file       VARCHAR;
    v_copy_sql          VARCHAR;
    -- Watermark: last successful export time — filters DIM_PROSPECT to new/changed rows only.
    -- Defaults to epoch on first ever run so everything is exported.
    v_last_export_str   VARCHAR;
BEGIN
    SELECT COALESCE(
               TO_VARCHAR(MAX(COMPLETED_AT), ''YYYY-MM-DD HH24:MI:SS.FF3''),
               ''1900-01-01 00:00:00.000''
           )
    INTO :v_last_export_str
    FROM FIPSAR_AUDIT.PIPELINE_AUDIT.PIPELINE_RUN_LOG
    WHERE PIPELINE_NAME = ''SP_EXPORT_SFMC_OUTBOUND''
      AND STATUS = ''SUCCESS'';

    INSERT INTO FIPSAR_AUDIT.PIPELINE_AUDIT.PIPELINE_RUN_LOG
    (
        RUN_ID,
        PIPELINE_NAME,
        LAYER,
        TABLE_NAME,
        STATUS
    )
    VALUES
    (
        :v_run_id,
        ''SP_EXPORT_SFMC_OUTBOUND'',
        ''GOLD'',
        ''VW_SFMC_PROSPECT_OUTBOUND'',
        ''STARTED''
    );

    v_timestamp := TO_VARCHAR(CURRENT_TIMESTAMP(), ''YYYYMMDD_HH24MI'');
    v_target_file := ''@FIPSAR_PHI_HUB.STAGING.STG_SFMC_OUTBOUND/Prospect_c_delta_'' || v_timestamp || ''.csv'';

    v_copy_sql := ''
        COPY INTO '' || v_target_file || ''
        FROM (
            SELECT
                dp.MASTER_PATIENT_ID   AS ProspectID,
                dp.FIRST_NAME          AS FirstName,
                dp.LAST_NAME           AS LastName,
                dp.EMAIL               AS EmailAddress,
                dp.PATIENT_CONSENT     AS MarketingConsent,
                TRUE                   AS HighEngagement,
                TO_VARCHAR(CURRENT_TIMESTAMP(), ''''MM/DD/YYYY HH12:MI:SS AM'''') AS RegistrationDate
            FROM FIPSAR_DW.GOLD.DIM_PROSPECT dp
            WHERE dp.IS_ACTIVE = TRUE
              AND dp.PATIENT_CONSENT = TRUE
              AND NULLIF(TRIM(dp.EMAIL), '''''''') IS NOT NULL
              AND UPPER(TRIM(dp.EMAIL)) <> ''''N/A''''
              AND dp.UPDATED_AT > TO_TIMESTAMP_NTZ('''''' || :v_last_export_str || '''''', ''''YYYY-MM-DD HH24:MI:SS.FF3'''')
        )
        FILE_FORMAT = (
            TYPE = CSV
            FIELD_OPTIONALLY_ENCLOSED_BY = ''''"''''
            COMPRESSION = NONE
        )
        HEADER = TRUE
        SINGLE = TRUE
        MAX_FILE_SIZE = 50000000
    '';

    EXECUTE IMMEDIATE v_copy_sql;

    -- EXECUTE IMMEDIATE does not propagate SQLROWCOUNT to the scripting caller,
    -- so we count the rows that would have been exported using the same filter.
    SELECT COUNT(*)
    INTO :v_rows
    FROM FIPSAR_DW.GOLD.DIM_PROSPECT dp
    WHERE dp.IS_ACTIVE = TRUE
      AND dp.PATIENT_CONSENT = TRUE
      AND NULLIF(TRIM(dp.EMAIL), '''') IS NOT NULL
      AND UPPER(TRIM(dp.EMAIL)) <> ''N/A''
      AND dp.UPDATED_AT > TO_TIMESTAMP_NTZ(:v_last_export_str, ''YYYY-MM-DD HH24:MI:SS.FF3'');

    UPDATE FIPSAR_AUDIT.PIPELINE_AUDIT.PIPELINE_RUN_LOG
    SET
        STATUS = ''SUCCESS'',
        ROWS_LOADED = :v_rows,
        ROWS_REJECTED = 0,
        ERROR_MESSAGE = NULL,
        COMPLETED_AT = CURRENT_TIMESTAMP()
    WHERE RUN_ID = :v_run_id;

    RETURN OBJECT_CONSTRUCT(
        ''status'', ''SUCCESS'',
        ''run_id'', :v_run_id,
        ''rows_exported'', :v_rows,
        ''file_name'', ''Prospect_c_delta_'' || :v_timestamp || ''.csv''
    );

EXCEPTION
    WHEN OTHER THEN
        UPDATE FIPSAR_AUDIT.PIPELINE_AUDIT.PIPELINE_RUN_LOG
        SET
            STATUS = ''FAILED'',
            ERROR_MESSAGE = ''SP_EXPORT_SFMC_OUTBOUND failed'',
            COMPLETED_AT = CURRENT_TIMESTAMP()
        WHERE RUN_ID = :v_run_id;

        RETURN OBJECT_CONSTRUCT(
            ''status'', ''FAILED'',
            ''run_id'', :v_run_id,
            ''error'', ''SP_EXPORT_SFMC_OUTBOUND failed''
        );
END;
';

CREATE OR REPLACE PROCEDURE "SP_LOAD_GOLD_DIM_CHANNEL"()
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE
    v_run_id      VARCHAR DEFAULT UUID_STRING();
    v_rows_loaded NUMBER DEFAULT 0;
BEGIN
    INSERT INTO FIPSAR_AUDIT.PIPELINE_AUDIT.PIPELINE_RUN_LOG
    (
        RUN_ID, PIPELINE_NAME, LAYER, TABLE_NAME, STATUS
    )
    VALUES
    (
        :v_run_id, ''SP_LOAD_GOLD_DIM_CHANNEL'', ''GOLD'', ''DIM_CHANNEL'', ''STARTED''
    );

    MERGE INTO FIPSAR_DW.GOLD.DIM_CHANNEL tgt
    USING (
        SELECT DISTINCT
            CHANNEL AS CHANNEL_NAME,
            CASE
                WHEN CHANNEL IN (''Instagram'', ''Facebook'', ''Website'', ''Campaign App'', ''Referral'', ''Survey'') THEN ''Digital''
                ELSE ''Other''
            END AS CHANNEL_CATEGORY
        FROM FIPSAR_DW.SILVER.SLV_PROSPECT_MASTER
        WHERE DQ_PASSED = TRUE
          AND NULLIF(TRIM(CHANNEL), '''') IS NOT NULL
    ) src
    ON tgt.CHANNEL_NAME = src.CHANNEL_NAME
    WHEN MATCHED THEN UPDATE SET
        tgt.CHANNEL_CATEGORY = src.CHANNEL_CATEGORY
    WHEN NOT MATCHED THEN
        INSERT (CHANNEL_NAME, CHANNEL_CATEGORY)
        VALUES (src.CHANNEL_NAME, src.CHANNEL_CATEGORY);

    v_rows_loaded := SQLROWCOUNT;

    UPDATE FIPSAR_AUDIT.PIPELINE_AUDIT.PIPELINE_RUN_LOG
    SET
        STATUS = ''SUCCESS'',
        ROWS_LOADED = :v_rows_loaded,
        ROWS_REJECTED = 0,
        ERROR_MESSAGE = NULL,
        COMPLETED_AT = CURRENT_TIMESTAMP()
    WHERE RUN_ID = :v_run_id;

    RETURN OBJECT_CONSTRUCT(
        ''status'', ''SUCCESS'',
        ''run_id'', :v_run_id,
        ''rows_loaded'', :v_rows_loaded
    );

EXCEPTION
    WHEN OTHER THEN
        UPDATE FIPSAR_AUDIT.PIPELINE_AUDIT.PIPELINE_RUN_LOG
        SET
            STATUS = ''FAILED'',
            ERROR_MESSAGE = ''SP_LOAD_GOLD_DIM_CHANNEL failed'',
            COMPLETED_AT = CURRENT_TIMESTAMP()
        WHERE RUN_ID = :v_run_id;

        RETURN OBJECT_CONSTRUCT(
            ''status'', ''FAILED'',
            ''run_id'', :v_run_id,
            ''error'', ''SP_LOAD_GOLD_DIM_CHANNEL failed''
        );
END;
';

CREATE OR REPLACE PROCEDURE "SP_LOAD_GOLD_DIM_DATE"()
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE
    v_run_id      VARCHAR DEFAULT UUID_STRING();
    v_rows_loaded NUMBER DEFAULT 0;
BEGIN
    INSERT INTO FIPSAR_AUDIT.PIPELINE_AUDIT.PIPELINE_RUN_LOG
    (
        RUN_ID, PIPELINE_NAME, LAYER, TABLE_NAME, STATUS
    )
    VALUES
    (
        :v_run_id, ''SP_LOAD_GOLD_DIM_DATE'', ''GOLD'', ''DIM_DATE'', ''STARTED''
    );

    INSERT INTO FIPSAR_DW.GOLD.DIM_DATE
    (
        DATE_KEY, FULL_DATE, YEAR, QUARTER, MONTH, MONTH_NAME,
        WEEK_OF_YEAR, DAY_OF_MONTH, DAY_OF_WEEK, DAY_NAME, IS_WEEKEND
    )
    SELECT
        TO_NUMBER(TO_CHAR(d.FILE_DATE, ''YYYYMMDD'')) AS DATE_KEY,
        d.FILE_DATE AS FULL_DATE,
        YEAR(d.FILE_DATE) AS YEAR,
        QUARTER(d.FILE_DATE) AS QUARTER,
        MONTH(d.FILE_DATE) AS MONTH,
        TO_CHAR(d.FILE_DATE, ''MMMM'') AS MONTH_NAME,
        WEEKOFYEAR(d.FILE_DATE) AS WEEK_OF_YEAR,
        DAY(d.FILE_DATE) AS DAY_OF_MONTH,
        DAYOFWEEK(d.FILE_DATE) AS DAY_OF_WEEK,
        TO_CHAR(d.FILE_DATE, ''DY'') AS DAY_NAME,
        CASE WHEN DAYOFWEEK(d.FILE_DATE) IN (0,6) THEN TRUE ELSE FALSE END AS IS_WEEKEND
    FROM (
        SELECT DISTINCT FILE_DATE
        FROM FIPSAR_DW.SILVER.SLV_PROSPECT_MASTER
        WHERE DQ_PASSED = TRUE
          AND FILE_DATE IS NOT NULL
    ) d
    WHERE NOT EXISTS
    (
        SELECT 1
        FROM FIPSAR_DW.GOLD.DIM_DATE tgt
        WHERE tgt.FULL_DATE = d.FILE_DATE
    );

    v_rows_loaded := SQLROWCOUNT;

    UPDATE FIPSAR_AUDIT.PIPELINE_AUDIT.PIPELINE_RUN_LOG
    SET
        STATUS = ''SUCCESS'',
        ROWS_LOADED = :v_rows_loaded,
        ROWS_REJECTED = 0,
        ERROR_MESSAGE = NULL,
        COMPLETED_AT = CURRENT_TIMESTAMP()
    WHERE RUN_ID = :v_run_id;

    RETURN OBJECT_CONSTRUCT(
        ''status'', ''SUCCESS'',
        ''run_id'', :v_run_id,
        ''rows_loaded'', :v_rows_loaded
    );

EXCEPTION
    WHEN OTHER THEN
        UPDATE FIPSAR_AUDIT.PIPELINE_AUDIT.PIPELINE_RUN_LOG
        SET
            STATUS = ''FAILED'',
            ERROR_MESSAGE = ''SP_LOAD_GOLD_DIM_DATE failed'',
            COMPLETED_AT = CURRENT_TIMESTAMP()
        WHERE RUN_ID = :v_run_id;

        RETURN OBJECT_CONSTRUCT(
            ''status'', ''FAILED'',
            ''run_id'', :v_run_id,
            ''error'', ''SP_LOAD_GOLD_DIM_DATE failed''
        );
END;
';

CREATE OR REPLACE PROCEDURE "SP_LOAD_GOLD_DIM_ENGAGEMENT_TYPE"()
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE
    v_run_id      VARCHAR DEFAULT UUID_STRING();
    v_rows_loaded NUMBER DEFAULT 0;
BEGIN
    INSERT INTO FIPSAR_AUDIT.PIPELINE_AUDIT.PIPELINE_RUN_LOG
    (
        RUN_ID, PIPELINE_NAME, LAYER, TABLE_NAME, STATUS
    )
    VALUES
    (
        :v_run_id, ''SP_LOAD_GOLD_DIM_ENGAGEMENT_TYPE'', ''GOLD'', ''DIM_ENGAGEMENT_TYPE'', ''STARTED''
    );

    MERGE INTO FIPSAR_DW.GOLD.DIM_ENGAGEMENT_TYPE tgt
    USING (
        SELECT ''SENT'' AS EVENT_TYPE UNION ALL
        SELECT ''OPEN'' UNION ALL
        SELECT ''CLICK'' UNION ALL
        SELECT ''BOUNCE'' UNION ALL
        SELECT ''UNSUBSCRIBE'' UNION ALL
        SELECT ''SPAM'' UNION ALL
        SELECT ''SUPPRESSED''
    ) src
    ON tgt.EVENT_TYPE = src.EVENT_TYPE
    WHEN NOT MATCHED THEN
        INSERT (EVENT_TYPE, IS_ACTIVE)
        VALUES (src.EVENT_TYPE, TRUE);

    v_rows_loaded := SQLROWCOUNT;

    UPDATE FIPSAR_AUDIT.PIPELINE_AUDIT.PIPELINE_RUN_LOG
    SET STATUS = ''SUCCESS'',
        ROWS_LOADED = :v_rows_loaded,
        ROWS_REJECTED = 0,
        ERROR_MESSAGE = NULL,
        COMPLETED_AT = CURRENT_TIMESTAMP()
    WHERE RUN_ID = :v_run_id;

    RETURN OBJECT_CONSTRUCT(
        ''status'',''SUCCESS'',
        ''run_id'',:v_run_id,
        ''rows_loaded'',:v_rows_loaded
    );

EXCEPTION
    WHEN OTHER THEN
        UPDATE FIPSAR_AUDIT.PIPELINE_AUDIT.PIPELINE_RUN_LOG
        SET STATUS = ''FAILED'',
            ERROR_MESSAGE = ''SP_LOAD_GOLD_DIM_ENGAGEMENT_TYPE failed'',
            COMPLETED_AT = CURRENT_TIMESTAMP()
        WHERE RUN_ID = :v_run_id;

        RETURN OBJECT_CONSTRUCT(
            ''status'',''FAILED'',
            ''run_id'',:v_run_id,
            ''error'',''SP_LOAD_GOLD_DIM_ENGAGEMENT_TYPE failed''
        );
END;
';

CREATE OR REPLACE PROCEDURE "SP_LOAD_GOLD_DIM_GEOGRAPHY"()
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE
    v_run_id      VARCHAR DEFAULT UUID_STRING();
    v_rows_loaded NUMBER DEFAULT 0;
BEGIN
    INSERT INTO FIPSAR_AUDIT.PIPELINE_AUDIT.PIPELINE_RUN_LOG
    (
        RUN_ID, PIPELINE_NAME, LAYER, TABLE_NAME, STATUS
    )
    VALUES
    (
        :v_run_id, ''SP_LOAD_GOLD_DIM_GEOGRAPHY'', ''GOLD'', ''DIM_GEOGRAPHY'', ''STARTED''
    );

    MERGE INTO FIPSAR_DW.GOLD.DIM_GEOGRAPHY tgt
    USING (
        SELECT DISTINCT
            CITY,
            STATE,
            ZIP_CODE,
            CASE
                WHEN STATE IN (''CT'',''ME'',''MA'',''NH'',''RI'',''VT'',''NJ'',''NY'',''PA'') THEN ''Northeast''
                WHEN STATE IN (''IL'',''IN'',''IA'',''KS'',''MI'',''MN'',''MO'',''NE'',''ND'',''OH'',''SD'',''WI'') THEN ''Midwest''
                WHEN STATE IN (''AL'',''AR'',''DE'',''FL'',''GA'',''KY'',''LA'',''MD'',''MS'',''NC'',''OK'',''SC'',''TN'',''TX'',''VA'',''WV'',''DC'') THEN ''South''
                WHEN STATE IN (''AZ'',''CO'',''ID'',''MT'',''NV'',''NM'',''UT'',''WY'',''AK'',''CA'',''HI'',''OR'',''WA'') THEN ''West''
                ELSE ''Other''
            END AS REGION
        FROM FIPSAR_DW.SILVER.SLV_PROSPECT_MASTER
        WHERE DQ_PASSED = TRUE
          AND NULLIF(TRIM(CITY), '''') IS NOT NULL
          AND NULLIF(TRIM(STATE), '''') IS NOT NULL
          AND NULLIF(TRIM(ZIP_CODE), '''') IS NOT NULL
    ) src
    ON tgt.CITY = src.CITY
   AND tgt.STATE = src.STATE
   AND tgt.ZIP_CODE = src.ZIP_CODE
    WHEN MATCHED THEN UPDATE SET
        tgt.REGION = src.REGION
    WHEN NOT MATCHED THEN
        INSERT (CITY, STATE, ZIP_CODE, REGION)
        VALUES (src.CITY, src.STATE, src.ZIP_CODE, src.REGION);

    v_rows_loaded := SQLROWCOUNT;

    UPDATE FIPSAR_AUDIT.PIPELINE_AUDIT.PIPELINE_RUN_LOG
    SET
        STATUS = ''SUCCESS'',
        ROWS_LOADED = :v_rows_loaded,
        ROWS_REJECTED = 0,
        ERROR_MESSAGE = NULL,
        COMPLETED_AT = CURRENT_TIMESTAMP()
    WHERE RUN_ID = :v_run_id;

    RETURN OBJECT_CONSTRUCT(
        ''status'', ''SUCCESS'',
        ''run_id'', :v_run_id,
        ''rows_loaded'', :v_rows_loaded
    );

EXCEPTION
    WHEN OTHER THEN
        UPDATE FIPSAR_AUDIT.PIPELINE_AUDIT.PIPELINE_RUN_LOG
        SET
            STATUS = ''FAILED'',
            ERROR_MESSAGE = ''SP_LOAD_GOLD_DIM_GEOGRAPHY failed'',
            COMPLETED_AT = CURRENT_TIMESTAMP()
        WHERE RUN_ID = :v_run_id;

        RETURN OBJECT_CONSTRUCT(
            ''status'', ''FAILED'',
            ''run_id'', :v_run_id,
            ''error'', ''SP_LOAD_GOLD_DIM_GEOGRAPHY failed''
        );
END;
';

CREATE OR REPLACE PROCEDURE "SP_LOAD_GOLD_DIM_SFMC_JOB"()
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE
    v_run_id      VARCHAR DEFAULT UUID_STRING();
    v_rows_loaded NUMBER DEFAULT 0;
BEGIN
    INSERT INTO FIPSAR_AUDIT.PIPELINE_AUDIT.PIPELINE_RUN_LOG
    (
        RUN_ID, PIPELINE_NAME, LAYER, TABLE_NAME, STATUS, STARTED_AT
    )
    VALUES
    (
        :v_run_id, ''SP_LOAD_GOLD_DIM_SFMC_JOB'', ''GOLD'', ''DIM_SFMC_JOB'', ''STARTED'', CURRENT_TIMESTAMP()
    );

    MERGE INTO FIPSAR_DW.GOLD.DIM_SFMC_JOB tgt
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
                FROM FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_JOB_METADATA
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
                FROM FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_JOB_DE_DETAIL
                WHERE TRY_TO_NUMBER(JOB_ID) IS NOT NULL
            )
            WHERE rn = 1
        )
        SELECT
            COALESCE(m.JOB_ID, d.JOB_ID) AS JOB_ID,
            m.JOURNEY_TYPE,
            m.MAPPED_STAGE,
            COALESCE(NULLIF(m.EMAIL_NAME, ''''), d.EMAIL_NAME) AS EMAIL_NAME,
            COALESCE(NULLIF(m.EMAIL_SUBJECT, ''''), d.EMAIL_SUBJECT) AS EMAIL_SUBJECT,
            COALESCE(m.RECORD_TYPE, ''JOB'') AS RECORD_TYPE
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

    UPDATE FIPSAR_AUDIT.PIPELINE_AUDIT.PIPELINE_RUN_LOG
    SET STATUS = ''SUCCESS'',
        ROWS_LOADED = :v_rows_loaded,
        ROWS_REJECTED = 0,
        ERROR_MESSAGE = NULL,
        COMPLETED_AT = CURRENT_TIMESTAMP()
    WHERE RUN_ID = :v_run_id;

    RETURN OBJECT_CONSTRUCT(
        ''status'',''SUCCESS'',
        ''run_id'',:v_run_id,
        ''rows_loaded'',:v_rows_loaded
    );

EXCEPTION
    WHEN OTHER THEN
        UPDATE FIPSAR_AUDIT.PIPELINE_AUDIT.PIPELINE_RUN_LOG
        SET STATUS = ''FAILED'',
            ERROR_MESSAGE = ''SP_LOAD_GOLD_DIM_SFMC_JOB failed'',
            COMPLETED_AT = CURRENT_TIMESTAMP()
        WHERE RUN_ID = :v_run_id;

        RETURN OBJECT_CONSTRUCT(
            ''status'',''FAILED'',
            ''run_id'',:v_run_id,
            ''error'',''SP_LOAD_GOLD_DIM_SFMC_JOB failed''
        );
END;
';

CREATE OR REPLACE PROCEDURE "SP_LOAD_GOLD_FACT_SFMC_ENGAGEMENT"()
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE
    v_run_id      VARCHAR DEFAULT UUID_STRING();
    v_rows_loaded NUMBER DEFAULT 0;
BEGIN
    INSERT INTO FIPSAR_AUDIT.PIPELINE_AUDIT.PIPELINE_RUN_LOG
    (
        RUN_ID, PIPELINE_NAME, LAYER, TABLE_NAME, STATUS, STARTED_AT
    )
    VALUES
    (
        :v_run_id, ''SP_LOAD_GOLD_FACT_SFMC_ENGAGEMENT'', ''GOLD'', ''FACT_SFMC_ENGAGEMENT'', ''STARTED'', CURRENT_TIMESTAMP()
    );

    -- SENT
    INSERT INTO FIPSAR_DW.GOLD.FACT_SFMC_ENGAGEMENT
    (
        SUBSCRIBER_KEY, MASTER_PATIENT_ID, PROSPECT_KEY, JOB_KEY, EVENT_TYPE_KEY, DATE_KEY,
        EVENT_TIMESTAMP, EVENT_TYPE, DOMAIN, ACCOUNT_ID, JOB_ID, RECORD_TYPE,
        SOURCE_FILE_NAME, LOADED_AT, IS_SUPPRESSED, SUPPRESSION_REASON
    )
    WITH suppressed AS (
        SELECT DISTINCT
            PROSPECT_ID,
            TRUE AS IS_SUPPRESSED,
            ''SUPPRESSION_FLAG=YES'' AS SUPPRESSION_REASON
        FROM FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_PROSPECT_JOURNEY_DETAILS
        WHERE UPPER(TRIM(SUPPRESSION_FLAG)) IN (''YES'',''Y'',''TRUE'',''1'')
          AND NULLIF(TRIM(PROSPECT_ID), '''') IS NOT NULL
    )
    SELECT
        s.SUBSCRIBER_KEY,
        dp.MASTER_PATIENT_ID,
        dp.PROSPECT_KEY,
        j.JOB_KEY,
        et.EVENT_TYPE_KEY,
        TO_NUMBER(TO_CHAR(TRY_TO_TIMESTAMP_NTZ(s.EVENT_DATE), ''YYYYMMDD'')),
        TRY_TO_TIMESTAMP_NTZ(s.EVENT_DATE),
        ''SENT'',
        s.DOMAIN,
        s.ACCOUNT_ID,
        s.JOB_ID,
        s.RECORD_TYPE,
        s._SOURCE_FILE_NAME,
        CURRENT_TIMESTAMP(),
        COALESCE(sp.IS_SUPPRESSED, FALSE),
        sp.SUPPRESSION_REASON
    FROM FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_SENT s
    LEFT JOIN FIPSAR_DW.GOLD.DIM_SFMC_JOB j
        ON s.JOB_ID = j.JOB_ID
    LEFT JOIN FIPSAR_DW.GOLD.DIM_ENGAGEMENT_TYPE et
        ON et.EVENT_TYPE = ''SENT''
    LEFT JOIN FIPSAR_DW.GOLD.DIM_PROSPECT dp
        ON dp.MASTER_PATIENT_ID = s.SUBSCRIBER_KEY
    LEFT JOIN suppressed sp
        ON sp.PROSPECT_ID = COALESCE(dp.MASTER_PATIENT_ID, s.SUBSCRIBER_KEY)
    WHERE NOT EXISTS (
        SELECT 1
        FROM FIPSAR_DW.GOLD.FACT_SFMC_ENGAGEMENT f
        WHERE f.EVENT_TYPE = ''SENT''
          AND f.SOURCE_FILE_NAME = s._SOURCE_FILE_NAME
          AND COALESCE(f.SUBSCRIBER_KEY,''~'') = COALESCE(s.SUBSCRIBER_KEY,''~'')
          AND COALESCE(f.JOB_ID,-1) = COALESCE(s.JOB_ID,-1)
          AND COALESCE(f.EVENT_TIMESTAMP,TO_TIMESTAMP_NTZ(''1900-01-01'')) =
              COALESCE(TRY_TO_TIMESTAMP_NTZ(s.EVENT_DATE),TO_TIMESTAMP_NTZ(''1900-01-01''))
    );

    v_rows_loaded := v_rows_loaded + SQLROWCOUNT;

    -- OPEN
    INSERT INTO FIPSAR_DW.GOLD.FACT_SFMC_ENGAGEMENT
    (
        SUBSCRIBER_KEY, MASTER_PATIENT_ID, PROSPECT_KEY, JOB_KEY, EVENT_TYPE_KEY, DATE_KEY,
        EVENT_TIMESTAMP, EVENT_TYPE, DOMAIN, IS_UNIQUE, ACCOUNT_ID, JOB_ID, RECORD_TYPE,
        SOURCE_FILE_NAME, LOADED_AT, IS_SUPPRESSED, SUPPRESSION_REASON
    )
    WITH suppressed AS (
        SELECT DISTINCT PROSPECT_ID, TRUE AS IS_SUPPRESSED, ''SUPPRESSION_FLAG=YES'' AS SUPPRESSION_REASON
        FROM FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_PROSPECT_JOURNEY_DETAILS
        WHERE UPPER(TRIM(SUPPRESSION_FLAG)) IN (''YES'',''Y'',''TRUE'',''1'')
          AND NULLIF(TRIM(PROSPECT_ID), '''') IS NOT NULL
    )
    SELECT
        o.SUBSCRIBER_KEY,
        dp.MASTER_PATIENT_ID,
        dp.PROSPECT_KEY,
        j.JOB_KEY,
        et.EVENT_TYPE_KEY,
        TO_NUMBER(TO_CHAR(TRY_TO_TIMESTAMP_NTZ(o.EVENT_DATE), ''YYYYMMDD'')),
        TRY_TO_TIMESTAMP_NTZ(o.EVENT_DATE),
        ''OPEN'',
        o.DOMAIN,
        CASE WHEN UPPER(o.IS_UNIQUE) IN (''TRUE'',''1'',''YES'',''Y'') THEN TRUE ELSE FALSE END,
        o.ACCOUNT_ID,
        o.JOB_ID,
        o.RECORD_TYPE,
        o._SOURCE_FILE_NAME,
        CURRENT_TIMESTAMP(),
        COALESCE(sp.IS_SUPPRESSED, FALSE),
        sp.SUPPRESSION_REASON
    FROM FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_OPENS o
    LEFT JOIN FIPSAR_DW.GOLD.DIM_SFMC_JOB j
        ON o.JOB_ID = j.JOB_ID
    LEFT JOIN FIPSAR_DW.GOLD.DIM_ENGAGEMENT_TYPE et
        ON et.EVENT_TYPE = ''OPEN''
    LEFT JOIN FIPSAR_DW.GOLD.DIM_PROSPECT dp
        ON dp.MASTER_PATIENT_ID = o.SUBSCRIBER_KEY
    LEFT JOIN suppressed sp
        ON sp.PROSPECT_ID = COALESCE(dp.MASTER_PATIENT_ID, o.SUBSCRIBER_KEY)
    WHERE NOT EXISTS (
        SELECT 1
        FROM FIPSAR_DW.GOLD.FACT_SFMC_ENGAGEMENT f
        WHERE f.EVENT_TYPE = ''OPEN''
          AND f.SOURCE_FILE_NAME = o._SOURCE_FILE_NAME
          AND COALESCE(f.SUBSCRIBER_KEY,''~'') = COALESCE(o.SUBSCRIBER_KEY,''~'')
          AND COALESCE(f.JOB_ID,-1) = COALESCE(o.JOB_ID,-1)
          AND COALESCE(f.EVENT_TIMESTAMP,TO_TIMESTAMP_NTZ(''1900-01-01'')) =
              COALESCE(TRY_TO_TIMESTAMP_NTZ(o.EVENT_DATE),TO_TIMESTAMP_NTZ(''1900-01-01''))
    );

    v_rows_loaded := v_rows_loaded + SQLROWCOUNT;

    -- CLICK
    INSERT INTO FIPSAR_DW.GOLD.FACT_SFMC_ENGAGEMENT
    (
        SUBSCRIBER_KEY, MASTER_PATIENT_ID, PROSPECT_KEY, JOB_KEY, EVENT_TYPE_KEY, DATE_KEY,
        EVENT_TIMESTAMP, EVENT_TYPE, DOMAIN, IS_UNIQUE, CLICK_URL, ACCOUNT_ID, JOB_ID, RECORD_TYPE,
        SOURCE_FILE_NAME, LOADED_AT, IS_SUPPRESSED, SUPPRESSION_REASON
    )
    WITH suppressed AS (
        SELECT DISTINCT PROSPECT_ID, TRUE AS IS_SUPPRESSED, ''SUPPRESSION_FLAG=YES'' AS SUPPRESSION_REASON
        FROM FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_PROSPECT_JOURNEY_DETAILS
        WHERE UPPER(TRIM(SUPPRESSION_FLAG)) IN (''YES'',''Y'',''TRUE'',''1'')
          AND NULLIF(TRIM(PROSPECT_ID), '''') IS NOT NULL
    )
    SELECT
        c.SUBSCRIBER_KEY,
        dp.MASTER_PATIENT_ID,
        dp.PROSPECT_KEY,
        j.JOB_KEY,
        et.EVENT_TYPE_KEY,
        TO_NUMBER(TO_CHAR(TRY_TO_TIMESTAMP_NTZ(c.EVENT_DATE), ''YYYYMMDD'')),
        TRY_TO_TIMESTAMP_NTZ(c.EVENT_DATE),
        ''CLICK'',
        c.DOMAIN,
        CASE WHEN UPPER(c.IS_UNIQUE) IN (''TRUE'',''1'',''YES'',''Y'') THEN TRUE ELSE FALSE END,
        c.URL,
        c.ACCOUNT_ID,
        c.JOB_ID,
        c.RECORD_TYPE,
        c._SOURCE_FILE_NAME,
        CURRENT_TIMESTAMP(),
        COALESCE(sp.IS_SUPPRESSED, FALSE),
        sp.SUPPRESSION_REASON
    FROM FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_CLICKS c
    LEFT JOIN FIPSAR_DW.GOLD.DIM_SFMC_JOB j
        ON c.JOB_ID = j.JOB_ID
    LEFT JOIN FIPSAR_DW.GOLD.DIM_ENGAGEMENT_TYPE et
        ON et.EVENT_TYPE = ''CLICK''
    LEFT JOIN FIPSAR_DW.GOLD.DIM_PROSPECT dp
        ON dp.MASTER_PATIENT_ID = c.SUBSCRIBER_KEY
    LEFT JOIN suppressed sp
        ON sp.PROSPECT_ID = COALESCE(dp.MASTER_PATIENT_ID, c.SUBSCRIBER_KEY)
    WHERE NOT EXISTS (
        SELECT 1
        FROM FIPSAR_DW.GOLD.FACT_SFMC_ENGAGEMENT f
        WHERE f.EVENT_TYPE = ''CLICK''
          AND f.SOURCE_FILE_NAME = c._SOURCE_FILE_NAME
          AND COALESCE(f.SUBSCRIBER_KEY,''~'') = COALESCE(c.SUBSCRIBER_KEY,''~'')
          AND COALESCE(f.JOB_ID,-1) = COALESCE(c.JOB_ID,-1)
          AND COALESCE(f.EVENT_TIMESTAMP,TO_TIMESTAMP_NTZ(''1900-01-01'')) =
              COALESCE(TRY_TO_TIMESTAMP_NTZ(c.EVENT_DATE),TO_TIMESTAMP_NTZ(''1900-01-01''))
          AND COALESCE(f.CLICK_URL,''~'') = COALESCE(c.URL,''~'')
    );

    v_rows_loaded := v_rows_loaded + SQLROWCOUNT;

    -- BOUNCE
    INSERT INTO FIPSAR_DW.GOLD.FACT_SFMC_ENGAGEMENT
    (
        SUBSCRIBER_KEY, MASTER_PATIENT_ID, PROSPECT_KEY, JOB_KEY, EVENT_TYPE_KEY, DATE_KEY,
        EVENT_TIMESTAMP, EVENT_TYPE, DOMAIN, BOUNCE_CATEGORY, BOUNCE_TYPE, ACCOUNT_ID, JOB_ID, RECORD_TYPE,
        SOURCE_FILE_NAME, LOADED_AT, IS_SUPPRESSED, SUPPRESSION_REASON
    )
    WITH suppressed AS (
        SELECT DISTINCT PROSPECT_ID, TRUE AS IS_SUPPRESSED, ''SUPPRESSION_FLAG=YES'' AS SUPPRESSION_REASON
        FROM FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_PROSPECT_JOURNEY_DETAILS
        WHERE UPPER(TRIM(SUPPRESSION_FLAG)) IN (''YES'',''Y'',''TRUE'',''1'')
          AND NULLIF(TRIM(PROSPECT_ID), '''') IS NOT NULL
    )
    SELECT
        b.SUBSCRIBER_KEY,
        dp.MASTER_PATIENT_ID,
        dp.PROSPECT_KEY,
        j.JOB_KEY,
        et.EVENT_TYPE_KEY,
        TO_NUMBER(TO_CHAR(TRY_TO_TIMESTAMP_NTZ(b.EVENT_DATE), ''YYYYMMDD'')),
        TRY_TO_TIMESTAMP_NTZ(b.EVENT_DATE),
        ''BOUNCE'',
        b.DOMAIN,
        b.BOUNCE_CATEGORY,
        b.BOUNCE_TYPE,
        b.ACCOUNT_ID,
        b.JOB_ID,
        b.RECORD_TYPE,
        b._SOURCE_FILE_NAME,
        CURRENT_TIMESTAMP(),
        COALESCE(sp.IS_SUPPRESSED, FALSE),
        sp.SUPPRESSION_REASON
    FROM FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_BOUNCES b
    LEFT JOIN FIPSAR_DW.GOLD.DIM_SFMC_JOB j
        ON b.JOB_ID = j.JOB_ID
    LEFT JOIN FIPSAR_DW.GOLD.DIM_ENGAGEMENT_TYPE et
        ON et.EVENT_TYPE = ''BOUNCE''
    LEFT JOIN FIPSAR_DW.GOLD.DIM_PROSPECT dp
        ON dp.MASTER_PATIENT_ID = b.SUBSCRIBER_KEY
    LEFT JOIN suppressed sp
        ON sp.PROSPECT_ID = COALESCE(dp.MASTER_PATIENT_ID, b.SUBSCRIBER_KEY)
    WHERE NOT EXISTS (
        SELECT 1
        FROM FIPSAR_DW.GOLD.FACT_SFMC_ENGAGEMENT f
        WHERE f.EVENT_TYPE = ''BOUNCE''
          AND f.SOURCE_FILE_NAME = b._SOURCE_FILE_NAME
          AND COALESCE(f.SUBSCRIBER_KEY,''~'') = COALESCE(b.SUBSCRIBER_KEY,''~'')
          AND COALESCE(f.JOB_ID,-1) = COALESCE(b.JOB_ID,-1)
          AND COALESCE(f.EVENT_TIMESTAMP,TO_TIMESTAMP_NTZ(''1900-01-01'')) =
              COALESCE(TRY_TO_TIMESTAMP_NTZ(b.EVENT_DATE),TO_TIMESTAMP_NTZ(''1900-01-01''))
          AND COALESCE(f.BOUNCE_CATEGORY,''~'') = COALESCE(b.BOUNCE_CATEGORY,''~'')
          AND COALESCE(f.BOUNCE_TYPE,''~'') = COALESCE(b.BOUNCE_TYPE,''~'')
    );

    v_rows_loaded := v_rows_loaded + SQLROWCOUNT;

    -- UNSUBSCRIBE
    INSERT INTO FIPSAR_DW.GOLD.FACT_SFMC_ENGAGEMENT
    (
        SUBSCRIBER_KEY, MASTER_PATIENT_ID, PROSPECT_KEY, JOB_KEY, EVENT_TYPE_KEY, DATE_KEY,
        EVENT_TIMESTAMP, EVENT_TYPE, REASON, ACCOUNT_ID, JOB_ID, RECORD_TYPE,
        SOURCE_FILE_NAME, LOADED_AT, IS_SUPPRESSED, SUPPRESSION_REASON
    )
    WITH suppressed AS (
        SELECT DISTINCT PROSPECT_ID, TRUE AS IS_SUPPRESSED, ''SUPPRESSION_FLAG=YES'' AS SUPPRESSION_REASON
        FROM FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_PROSPECT_JOURNEY_DETAILS
        WHERE UPPER(TRIM(SUPPRESSION_FLAG)) IN (''YES'',''Y'',''TRUE'',''1'')
          AND NULLIF(TRIM(PROSPECT_ID), '''') IS NOT NULL
    )
    SELECT
        u.SUBSCRIBER_KEY,
        dp.MASTER_PATIENT_ID,
        dp.PROSPECT_KEY,
        j.JOB_KEY,
        et.EVENT_TYPE_KEY,
        TO_NUMBER(TO_CHAR(TRY_TO_TIMESTAMP_NTZ(u.EVENT_DATE), ''YYYYMMDD'')),
        TRY_TO_TIMESTAMP_NTZ(u.EVENT_DATE),
        ''UNSUBSCRIBE'',
        u.REASON,
        u.ACCOUNT_ID,
        u.JOB_ID,
        u.RECORD_TYPE,
        u._SOURCE_FILE_NAME,
        CURRENT_TIMESTAMP(),
        TRUE,
        COALESCE(sp.SUPPRESSION_REASON, ''UNSUBSCRIBE_EVENT'')
    FROM FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_UNSUBSCRIBES u
    LEFT JOIN FIPSAR_DW.GOLD.DIM_SFMC_JOB j
        ON u.JOB_ID = j.JOB_ID
    LEFT JOIN FIPSAR_DW.GOLD.DIM_ENGAGEMENT_TYPE et
        ON et.EVENT_TYPE = ''UNSUBSCRIBE''
    LEFT JOIN FIPSAR_DW.GOLD.DIM_PROSPECT dp
        ON dp.MASTER_PATIENT_ID = u.SUBSCRIBER_KEY
    LEFT JOIN suppressed sp
        ON sp.PROSPECT_ID = COALESCE(dp.MASTER_PATIENT_ID, u.SUBSCRIBER_KEY)
    WHERE NOT EXISTS (
        SELECT 1
        FROM FIPSAR_DW.GOLD.FACT_SFMC_ENGAGEMENT f
        WHERE f.EVENT_TYPE = ''UNSUBSCRIBE''
          AND f.SOURCE_FILE_NAME = u._SOURCE_FILE_NAME
          AND COALESCE(f.SUBSCRIBER_KEY,''~'') = COALESCE(u.SUBSCRIBER_KEY,''~'')
          AND COALESCE(f.JOB_ID,-1) = COALESCE(u.JOB_ID,-1)
          AND COALESCE(f.EVENT_TIMESTAMP,TO_TIMESTAMP_NTZ(''1900-01-01'')) =
              COALESCE(TRY_TO_TIMESTAMP_NTZ(u.EVENT_DATE),TO_TIMESTAMP_NTZ(''1900-01-01''))
    );

    v_rows_loaded := v_rows_loaded + SQLROWCOUNT;

    -- SPAM
    INSERT INTO FIPSAR_DW.GOLD.FACT_SFMC_ENGAGEMENT
    (
        SUBSCRIBER_KEY, MASTER_PATIENT_ID, PROSPECT_KEY, JOB_KEY, EVENT_TYPE_KEY, DATE_KEY,
        EVENT_TIMESTAMP, EVENT_TYPE, REASON, ACCOUNT_ID, JOB_ID, RECORD_TYPE,
        SOURCE_FILE_NAME, LOADED_AT, IS_SUPPRESSED, SUPPRESSION_REASON
    )
    WITH suppressed AS (
        SELECT DISTINCT PROSPECT_ID, TRUE AS IS_SUPPRESSED, ''SUPPRESSION_FLAG=YES'' AS SUPPRESSION_REASON
        FROM FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_PROSPECT_JOURNEY_DETAILS
        WHERE UPPER(TRIM(SUPPRESSION_FLAG)) IN (''YES'',''Y'',''TRUE'',''1'')
          AND NULLIF(TRIM(PROSPECT_ID), '''') IS NOT NULL
    )
    SELECT
        spm.SUBSCRIBER_KEY,
        dp.MASTER_PATIENT_ID,
        dp.PROSPECT_KEY,
        j.JOB_KEY,
        et.EVENT_TYPE_KEY,
        TO_NUMBER(TO_CHAR(TRY_TO_TIMESTAMP_NTZ(spm.EVENT_DATE), ''YYYYMMDD'')),
        TRY_TO_TIMESTAMP_NTZ(spm.EVENT_DATE),
        ''SPAM'',
        spm.COMPLAINT_TYPE,
        spm.ACCOUNT_ID,
        spm.JOB_ID,
        spm.RECORD_TYPE,
        spm._SOURCE_FILE_NAME,
        CURRENT_TIMESTAMP(),
        TRUE,
        COALESCE(sp.SUPPRESSION_REASON, ''SPAM_EVENT'')
    FROM FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_SPAM spm
    LEFT JOIN FIPSAR_DW.GOLD.DIM_SFMC_JOB j
        ON spm.JOB_ID = j.JOB_ID
    LEFT JOIN FIPSAR_DW.GOLD.DIM_ENGAGEMENT_TYPE et
        ON et.EVENT_TYPE = ''SPAM''
    LEFT JOIN FIPSAR_DW.GOLD.DIM_PROSPECT dp
        ON dp.MASTER_PATIENT_ID = spm.SUBSCRIBER_KEY
    LEFT JOIN suppressed sp
        ON sp.PROSPECT_ID = COALESCE(dp.MASTER_PATIENT_ID, spm.SUBSCRIBER_KEY)
    WHERE NOT EXISTS (
        SELECT 1
        FROM FIPSAR_DW.GOLD.FACT_SFMC_ENGAGEMENT f
        WHERE f.EVENT_TYPE = ''SPAM''
          AND f.SOURCE_FILE_NAME = spm._SOURCE_FILE_NAME
          AND COALESCE(f.SUBSCRIBER_KEY,''~'') = COALESCE(spm.SUBSCRIBER_KEY,''~'')
          AND COALESCE(f.JOB_ID,-1) = COALESCE(spm.JOB_ID,-1)
          AND COALESCE(f.EVENT_TIMESTAMP,TO_TIMESTAMP_NTZ(''1900-01-01'')) =
              COALESCE(TRY_TO_TIMESTAMP_NTZ(spm.EVENT_DATE),TO_TIMESTAMP_NTZ(''1900-01-01''))
    );

    v_rows_loaded := v_rows_loaded + SQLROWCOUNT;

    -- SYNTHETIC SUPPRESSED EVENTS
    INSERT INTO FIPSAR_DW.GOLD.FACT_SFMC_ENGAGEMENT
    (
        SUBSCRIBER_KEY, MASTER_PATIENT_ID, PROSPECT_KEY, JOB_KEY, EVENT_TYPE_KEY, DATE_KEY,
        EVENT_TIMESTAMP, EVENT_TYPE, REASON, ACCOUNT_ID, JOB_ID, RECORD_TYPE,
        SOURCE_FILE_NAME, LOADED_AT, IS_SUPPRESSED, SUPPRESSION_REASON
    )
    SELECT
        r.PROSPECT_ID,
        dp.MASTER_PATIENT_ID,
        dp.PROSPECT_KEY,
        NULL,
        et.EVENT_TYPE_KEY,
        TO_NUMBER(TO_CHAR(COALESCE(r._LOADED_AT, CURRENT_TIMESTAMP()), ''YYYYMMDD'')),
        COALESCE(r._LOADED_AT, CURRENT_TIMESTAMP()),
        ''SUPPRESSED'',
        ''SUPPRESSION_FLAG=YES'',
        NULL,
        NULL,
        ''SUPPRESSION'',
        r._SOURCE_FILE_NAME,
        CURRENT_TIMESTAMP(),
        TRUE,
        ''SUPPRESSION_FLAG=YES''
    FROM FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_PROSPECT_JOURNEY_DETAILS r
    LEFT JOIN FIPSAR_DW.GOLD.DIM_PROSPECT dp
        ON dp.MASTER_PATIENT_ID = r.PROSPECT_ID
    LEFT JOIN FIPSAR_DW.GOLD.DIM_ENGAGEMENT_TYPE et
        ON et.EVENT_TYPE = ''SUPPRESSED''
    WHERE UPPER(TRIM(r.SUPPRESSION_FLAG)) IN (''YES'',''Y'',''TRUE'',''1'')
      AND NULLIF(TRIM(r.PROSPECT_ID), '''') IS NOT NULL
      AND NOT EXISTS (
          SELECT 1
          FROM FIPSAR_DW.GOLD.FACT_SFMC_ENGAGEMENT f
          WHERE f.EVENT_TYPE = ''SUPPRESSED''
            AND f.SOURCE_FILE_NAME = r._SOURCE_FILE_NAME
            AND COALESCE(f.SUBSCRIBER_KEY,''~'') = COALESCE(r.PROSPECT_ID,''~'')
            AND COALESCE(f.EVENT_TIMESTAMP,TO_TIMESTAMP_NTZ(''1900-01-01'')) =
                COALESCE(r._LOADED_AT,TO_TIMESTAMP_NTZ(''1900-01-01''))
      );

    v_rows_loaded := v_rows_loaded + SQLROWCOUNT;

    UPDATE FIPSAR_AUDIT.PIPELINE_AUDIT.PIPELINE_RUN_LOG
    SET STATUS = ''SUCCESS'',
        ROWS_LOADED = :v_rows_loaded,
        ROWS_REJECTED = 0,
        ERROR_MESSAGE = NULL,
        COMPLETED_AT = CURRENT_TIMESTAMP()
    WHERE RUN_ID = :v_run_id;

    RETURN OBJECT_CONSTRUCT(
        ''status'',''SUCCESS'',
        ''run_id'',:v_run_id,
        ''rows_loaded'',:v_rows_loaded
    );

EXCEPTION
    WHEN OTHER THEN
        UPDATE FIPSAR_AUDIT.PIPELINE_AUDIT.PIPELINE_RUN_LOG
        SET STATUS = ''FAILED'',
            ERROR_MESSAGE = ''SP_LOAD_GOLD_FACT_SFMC_ENGAGEMENT failed'',
            COMPLETED_AT = CURRENT_TIMESTAMP()
        WHERE RUN_ID = :v_run_id;

        RETURN OBJECT_CONSTRUCT(
            ''status'',''FAILED'',
            ''run_id'',:v_run_id,
            ''error'',''SP_LOAD_GOLD_FACT_SFMC_ENGAGEMENT failed''
        );
END;
';

CREATE OR REPLACE PROCEDURE "SP_LOG_SFMC_SUPPRESSIONS"()
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE
    v_run_id      VARCHAR DEFAULT UUID_STRING();
    v_rows_logged NUMBER DEFAULT 0;
BEGIN
    INSERT INTO FIPSAR_AUDIT.PIPELINE_AUDIT.PIPELINE_RUN_LOG
    (
        RUN_ID, PIPELINE_NAME, LAYER, TABLE_NAME, STATUS, STARTED_AT
    )
    VALUES
    (
        :v_run_id, ''SP_LOG_SFMC_SUPPRESSIONS'', ''GOLD'', ''DQ_REJECTION_LOG'', ''STARTED'', CURRENT_TIMESTAMP()
    );

    INSERT INTO FIPSAR_AUDIT.PIPELINE_AUDIT.DQ_REJECTION_LOG
    (
        TABLE_NAME,
        REJECTION_REASON,
        REJECTED_RECORD,
        REJECTED_AT
    )
    WITH suppressed AS (
        SELECT DISTINCT
            PROSPECT_ID,
            _SOURCE_FILE_NAME,
            _SOURCE_ROW_NUMBER
        FROM FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_PROSPECT_JOURNEY_DETAILS
        WHERE UPPER(TRIM(SUPPRESSION_FLAG)) IN (''YES'',''Y'',''TRUE'',''1'')
          AND NULLIF(TRIM(PROSPECT_ID), '''') IS NOT NULL
    )
    SELECT
        ''FACT_SFMC_ENGAGEMENT'' AS TABLE_NAME,
        ''SUPPRESSED_PROSPECT'' AS REJECTION_REASON,
        OBJECT_CONSTRUCT(
            ''PROSPECT_ID'', s.PROSPECT_ID,
            ''SUPPRESSION_FLAG'', ''YES'',
            ''STAGE'', ''SFMC_SUPPRESSION_DETECTION'',
            ''_SOURCE_FILE_NAME'', s._SOURCE_FILE_NAME,
            ''_SOURCE_ROW_NUMBER'', s._SOURCE_ROW_NUMBER
        ) AS REJECTED_RECORD,
        CURRENT_TIMESTAMP() AS REJECTED_AT
    FROM suppressed s
    WHERE NOT EXISTS (
        SELECT 1
        FROM FIPSAR_AUDIT.PIPELINE_AUDIT.DQ_REJECTION_LOG d
        WHERE d.TABLE_NAME = ''FACT_SFMC_ENGAGEMENT''
          AND d.REJECTION_REASON = ''SUPPRESSED_PROSPECT''
          AND d.REJECTED_RECORD = OBJECT_CONSTRUCT(
                ''PROSPECT_ID'', s.PROSPECT_ID,
                ''SUPPRESSION_FLAG'', ''YES'',
                ''STAGE'', ''SFMC_SUPPRESSION_DETECTION'',
                ''_SOURCE_FILE_NAME'', s._SOURCE_FILE_NAME,
                ''_SOURCE_ROW_NUMBER'', s._SOURCE_ROW_NUMBER
          )
    );

    v_rows_logged := SQLROWCOUNT;

    UPDATE FIPSAR_AUDIT.PIPELINE_AUDIT.PIPELINE_RUN_LOG
    SET STATUS = ''SUCCESS'',
        ROWS_LOADED = 0,
        ROWS_REJECTED = :v_rows_logged,
        ERROR_MESSAGE = NULL,
        COMPLETED_AT = CURRENT_TIMESTAMP()
    WHERE RUN_ID = :v_run_id;

    RETURN OBJECT_CONSTRUCT(
        ''status'',''SUCCESS'',
        ''run_id'',:v_run_id,
        ''rows_logged'',:v_rows_logged
    );

EXCEPTION
    WHEN OTHER THEN
        UPDATE FIPSAR_AUDIT.PIPELINE_AUDIT.PIPELINE_RUN_LOG
        SET STATUS = ''FAILED'',
            ERROR_MESSAGE = ''SP_LOG_SFMC_SUPPRESSIONS failed'',
            COMPLETED_AT = CURRENT_TIMESTAMP()
        WHERE RUN_ID = :v_run_id;

        RETURN OBJECT_CONSTRUCT(
            ''status'',''FAILED'',
            ''run_id'',:v_run_id,
            ''error'',''SP_LOG_SFMC_SUPPRESSIONS failed''
        );
END;
';

-- ─────────────────────────────────────────────────────────────────────
-- FIPSAR_DW.PUBLIC  (1 utility procedure)
-- ─────────────────────────────────────────────────────────────────────

CREATE OR REPLACE PROCEDURE "SP_EXTRACT_ALL_DDL"()
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
var ddl_parts = [];

// Header
ddl_parts.push("/*============================================================");
ddl_parts.push("  FULL ACCOUNT DDL EXPORT");
ddl_parts.push("  Source Account: ILB69694");
ddl_parts.push("  Exported: " + new Date().toISOString());
ddl_parts.push("  Execute this script in a new Snowflake account to replicate");
ddl_parts.push("  all objects. Run as ACCOUNTADMIN.");
ddl_parts.push("============================================================*/");
ddl_parts.push("");
ddl_parts.push("USE ROLE ACCOUNTADMIN;");
ddl_parts.push("");

// ── SECTION 1: WAREHOUSES ──
ddl_parts.push("-- ============================================================");
ddl_parts.push("-- SECTION 1: WAREHOUSES");
ddl_parts.push("-- ============================================================");
ddl_parts.push("");

var wh_names = [''COMPUTE_WH'', ''SYSTEM$STREAMLIT_NOTEBOOK_WH''];
for (var w = 0; w < wh_names.length; w++) {
    try {
        var rs = snowflake.execute({sqlText: "SELECT GET_DDL(''WAREHOUSE'', ''" + wh_names[w] + "'')"});
        rs.next();
        ddl_parts.push(rs.getColumnValue(1));
        ddl_parts.push("");
    } catch(e) {
        ddl_parts.push("-- ERROR getting DDL for warehouse " + wh_names[w] + ": " + e.message);
        ddl_parts.push("");
    }
}

// ── SECTION 2: DATABASES AND ALL OBJECTS ──
ddl_parts.push("-- ============================================================");
ddl_parts.push("-- SECTION 2: DATABASES AND ALL OBJECTS");
ddl_parts.push("-- ============================================================");
ddl_parts.push("");

var db_names = [
    ''FIPSAR_PHI_HUB'', ''FIPSAR_DW'', ''FIPSAR_SFMC_EVENTS'', ''FIPSAR_AI'', ''FIPSAR_AUDIT'',
    ''QA_FIPSAR_PHI_HUB'', ''QA_FIPSAR_DW'', ''QA_FIPSAR_SFMC_EVENTS'', ''QA_FIPSAR_AI'', ''QA_FIPSAR_AUDIT''
];

for (var d = 0; d < db_names.length; d++) {
    ddl_parts.push("-- ────────────────────────────────────────────────────────────");
    ddl_parts.push("-- DATABASE: " + db_names[d]);
    ddl_parts.push("-- ────────────────────────────────────────────────────────────");
    ddl_parts.push("");
    try {
        var rs = snowflake.execute({sqlText: "SELECT GET_DDL(''DATABASE'', ''" + db_names[d] + "'')"});
        rs.next();
        var db_ddl = rs.getColumnValue(1);
        ddl_parts.push(db_ddl);
        ddl_parts.push("");
    } catch(e) {
        ddl_parts.push("-- ERROR getting DDL for database " + db_names[d] + ": " + e.message);
        ddl_parts.push("");
    }
}

// ── SECTION 3: ROLE GRANTS ──
ddl_parts.push("-- ============================================================");
ddl_parts.push("-- SECTION 3: ROLE HIERARCHY GRANTS");
ddl_parts.push("-- ============================================================");
ddl_parts.push("");
ddl_parts.push("-- Standard role hierarchy (these are defaults but included for completeness)");
ddl_parts.push("GRANT ROLE SYSADMIN TO ROLE ACCOUNTADMIN;");
ddl_parts.push("GRANT ROLE SECURITYADMIN TO ROLE ACCOUNTADMIN;");
ddl_parts.push("GRANT ROLE USERADMIN TO ROLE SECURITYADMIN;");
ddl_parts.push("");

// ── SECTION 4: DATABASE GRANTS ──
ddl_parts.push("-- ============================================================");
ddl_parts.push("-- SECTION 4: DATABASE OWNERSHIP GRANTS");
ddl_parts.push("-- ============================================================");
ddl_parts.push("");
for (var d = 0; d < db_names.length; d++) {
    ddl_parts.push("GRANT OWNERSHIP ON DATABASE " + db_names[d] + " TO ROLE ACCOUNTADMIN;");
}
ddl_parts.push("");

// ── SECTION 5: WAREHOUSE GRANTS ──
ddl_parts.push("-- ============================================================");
ddl_parts.push("-- SECTION 5: WAREHOUSE OWNERSHIP GRANTS");
ddl_parts.push("-- ============================================================");
ddl_parts.push("");
for (var w = 0; w < wh_names.length; w++) {
    ddl_parts.push("GRANT OWNERSHIP ON WAREHOUSE " + wh_names[w] + " TO ROLE ACCOUNTADMIN;");
}
ddl_parts.push("");

var full_ddl = ddl_parts.join("\\n");
return full_ddl;
';

-- ─────────────────────────────────────────────────────────────────────
-- FIPSAR_AUDIT.PIPELINE_AUDIT  (1 procedure)
-- ─────────────────────────────────────────────────────────────────────

CREATE OR REPLACE PROCEDURE "SP_SEND_SILVER_DQ_STATUS_EMAIL"()
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE
    v_phi_status       VARCHAR DEFAULT ''NOT FOUND'';
    v_phi_rows         NUMBER  DEFAULT 0;
    v_phi_completed    TIMESTAMP_NTZ;

    v_bronze_status    VARCHAR DEFAULT ''NOT FOUND'';
    v_bronze_rows      NUMBER  DEFAULT 0;
    v_bronze_completed TIMESTAMP_NTZ;

    v_silver_status    VARCHAR DEFAULT ''NOT FOUND'';
    v_silver_rows      NUMBER  DEFAULT 0;
    v_silver_started   TIMESTAMP_NTZ;
    v_silver_completed TIMESTAMP_NTZ;

    v_dq_count         NUMBER  DEFAULT 0;
    v_dq_summary       VARCHAR DEFAULT ''No DQ rejections found.'';
    v_email_body       VARCHAR;
BEGIN
    SELECT
        COALESCE(STATUS, ''NOT FOUND''),
        COALESCE(ROWS_LOADED, 0),
        COMPLETED_AT
    INTO
        :v_phi_status,
        :v_phi_rows,
        :v_phi_completed
    FROM (
        SELECT STATUS, ROWS_LOADED, COMPLETED_AT
        FROM FIPSAR_AUDIT.PIPELINE_AUDIT.PIPELINE_RUN_LOG
        WHERE PIPELINE_NAME = ''SP_LOAD_PHI_PROSPECT''
        ORDER BY STARTED_AT DESC
        LIMIT 1
    );

    SELECT
        COALESCE(STATUS, ''NOT FOUND''),
        COALESCE(ROWS_LOADED, 0),
        COMPLETED_AT
    INTO
        :v_bronze_status,
        :v_bronze_rows,
        :v_bronze_completed
    FROM (
        SELECT STATUS, ROWS_LOADED, COMPLETED_AT
        FROM FIPSAR_AUDIT.PIPELINE_AUDIT.PIPELINE_RUN_LOG
        WHERE PIPELINE_NAME = ''SP_LOAD_BRONZE_PROSPECT''
        ORDER BY STARTED_AT DESC
        LIMIT 1
    );

    SELECT
        COALESCE(STATUS, ''NOT FOUND''),
        COALESCE(ROWS_LOADED, 0),
        STARTED_AT,
        COMPLETED_AT
    INTO
        :v_silver_status,
        :v_silver_rows,
        :v_silver_started,
        :v_silver_completed
    FROM (
        SELECT STATUS, ROWS_LOADED, STARTED_AT, COMPLETED_AT
        FROM FIPSAR_AUDIT.PIPELINE_AUDIT.PIPELINE_RUN_LOG
        WHERE PIPELINE_NAME = ''SP_LOAD_SILVER_PROSPECT''
        ORDER BY STARTED_AT DESC
        LIMIT 1
    );

    SELECT COUNT(*)
    INTO :v_dq_count
    FROM FIPSAR_AUDIT.PIPELINE_AUDIT.DQ_REJECTION_LOG
    WHERE REJECTED_AT >= COALESCE(:v_silver_started, DATEADD(HOUR, -2, CURRENT_TIMESTAMP()));

    SELECT COALESCE(
        LISTAGG(REJECTION_REASON || '' = '' || CNT, '' | ''),
        ''No DQ rejections found.''
    )
    INTO :v_dq_summary
    FROM (
        SELECT
            COALESCE(REJECTION_REASON, ''UNKNOWN'') AS REJECTION_REASON,
            COUNT(*) AS CNT
        FROM FIPSAR_AUDIT.PIPELINE_AUDIT.DQ_REJECTION_LOG
        WHERE REJECTED_AT >= COALESCE(:v_silver_started, DATEADD(HOUR, -2, CURRENT_TIMESTAMP()))
        GROUP BY COALESCE(REJECTION_REASON, ''UNKNOWN'')
    );

    v_email_body :=
          ''FIPSAR Pipeline Mid-Run Status Update'' || CHAR(10) || CHAR(10)
        || ''1) PHI Load: '' || COALESCE(:v_phi_status, ''NOT FOUND'')
        || '' | Rows: '' || COALESCE(TO_VARCHAR(:v_phi_rows), ''0'')
        || '' | Completed: '' || COALESCE(TO_VARCHAR(:v_phi_completed), ''N/A'') || CHAR(10)
        || ''2) Bronze Load: '' || COALESCE(:v_bronze_status, ''NOT FOUND'')
        || '' | Rows: '' || COALESCE(TO_VARCHAR(:v_bronze_rows), ''0'')
        || '' | Completed: '' || COALESCE(TO_VARCHAR(:v_bronze_completed), ''N/A'') || CHAR(10)
        || ''3) Silver Load: '' || COALESCE(:v_silver_status, ''NOT FOUND'')
        || '' | Rows: '' || COALESCE(TO_VARCHAR(:v_silver_rows), ''0'')
        || '' | Completed: '' || COALESCE(TO_VARCHAR(:v_silver_completed), ''N/A'') || CHAR(10) || CHAR(10)
        || ''4) DQ Rejections since Silver start: '' || COALESCE(TO_VARCHAR(:v_dq_count), ''0'') || CHAR(10)
        || ''   Breakdown: '' || COALESCE(:v_dq_summary, ''No DQ rejections found.'') || CHAR(10) || CHAR(10)
        || ''Tables: FIPSAR_AUDIT.PIPELINE_AUDIT.PIPELINE_RUN_LOG | DQ_REJECTION_LOG'';

    CALL SYSTEM$SEND_EMAIL(
        ''FIPSAR_EMAIL_NOTIFICATION'',
        ''rosnow1201@gmail.com'',
        ''FIPSAR Pipeline Status after Silver Load'',
        :v_email_body
    );

    RETURN OBJECT_CONSTRUCT(
        ''status'', ''SUCCESS'',
        ''dq_reject_count'', :v_dq_count
    );

EXCEPTION
    WHEN OTHER THEN
        RETURN OBJECT_CONSTRUCT(
            ''status'', ''FAILED'',
            ''error'', ''SP_SEND_SILVER_DQ_STATUS_EMAIL failed''
        );
END;
';

-- ─────────────────────────────────────────────────────────────────────
-- FIPSAR_SFMC_EVENTS.RAW_EVENTS  (18 procedures)
-- ─────────────────────────────────────────────────────────────────────

CREATE OR REPLACE PROCEDURE "SP_LOAD_RAW_SFMC_SENT"()
RETURNS VARIANT
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
const rs = snowflake.createStatement({
  sqlText: `CALL FIPSAR_SFMC_EVENTS.RAW_EVENTS.SP_COPY_NEW_FILES(?,?,?,?,?)`,
  binds: [
    ''RAW_SFMC_SENT'',
    ''FIPSAR_SFMC_EVENTS.RAW_EVENTS.STG_SFMC_SENT_DE'',
    ''FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_SENT'',
    ''FIPSAR_SFMC_EVENTS.RAW_EVENTS.FF_SFMC_CSV_UTF16'',
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
        ''SENT'',
        METADATA$FILENAME,
        METADATA$FILE_ROW_NUMBER,
        CURRENT_TIMESTAMP()`
  ]
}).execute();
rs.next();
return rs.getColumnValue(1);
';

CREATE OR REPLACE PROCEDURE "SP_LOAD_RAW_SFMC_OPENS"()
RETURNS VARIANT
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
const rs = snowflake.createStatement({
  sqlText: `CALL FIPSAR_SFMC_EVENTS.RAW_EVENTS.SP_COPY_NEW_FILES(?,?,?,?,?)`,
  binds: [
    ''RAW_SFMC_OPENS'',
    ''FIPSAR_SFMC_EVENTS.RAW_EVENTS.STG_SFMC_OPEN_DE'',
    ''FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_OPENS'',
    ''FIPSAR_SFMC_EVENTS.RAW_EVENTS.FF_SFMC_CSV_UTF16'',
    `SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, ''OPEN'',
            METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, CURRENT_TIMESTAMP()`
  ]
}).execute();
rs.next();
return rs.getColumnValue(1);
';

CREATE OR REPLACE PROCEDURE "SP_LOAD_RAW_SFMC_CLICKS"()
RETURNS VARIANT
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
const rs = snowflake.createStatement({
  sqlText: `CALL FIPSAR_SFMC_EVENTS.RAW_EVENTS.SP_COPY_NEW_FILES(?,?,?,?,?)`,
  binds: [
    ''RAW_SFMC_CLICKS'',
    ''FIPSAR_SFMC_EVENTS.RAW_EVENTS.STG_SFMC_CLICK_DE'',
    ''FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_CLICKS'',
    ''FIPSAR_SFMC_EVENTS.RAW_EVENTS.FF_SFMC_CSV_UTF16'',
    `SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, ''CLICK'',
            METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, CURRENT_TIMESTAMP()`
  ]
}).execute();
rs.next();
return rs.getColumnValue(1);
';

CREATE OR REPLACE PROCEDURE "SP_LOAD_RAW_SFMC_BOUNCES"()
RETURNS VARIANT
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
const rs = snowflake.createStatement({
  sqlText: `CALL FIPSAR_SFMC_EVENTS.RAW_EVENTS.SP_COPY_NEW_FILES(?,?,?,?,?)`,
  binds: [
    ''RAW_SFMC_BOUNCES'',
    ''FIPSAR_SFMC_EVENTS.RAW_EVENTS.STG_SFMC_BOUNCE_DE'',
    ''FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_BOUNCES'',
    ''FIPSAR_SFMC_EVENTS.RAW_EVENTS.FF_SFMC_CSV_UTF16'',
    `SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, ''BOUNCE'',
            METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, CURRENT_TIMESTAMP()`
  ]
}).execute();
rs.next();
return rs.getColumnValue(1);
';

CREATE OR REPLACE PROCEDURE "SP_LOAD_RAW_SFMC_UNSUBSCRIBES"()
RETURNS VARIANT
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
const rs = snowflake.createStatement({
  sqlText: `CALL FIPSAR_SFMC_EVENTS.RAW_EVENTS.SP_COPY_NEW_FILES(?,?,?,?,?)`,
  binds: [
    ''RAW_SFMC_UNSUBSCRIBES'',
    ''FIPSAR_SFMC_EVENTS.RAW_EVENTS.STG_SFMC_UNSUBSCRIBE_DE'',
    ''FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_UNSUBSCRIBES'',
    ''FIPSAR_SFMC_EVENTS.RAW_EVENTS.FF_SFMC_CSV_UTF16'',
    `SELECT $1, $7, $3, $8, NULL, ''UNSUBSCRIBE'',
            METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, CURRENT_TIMESTAMP()`
  ]
}).execute();
rs.next();
return rs.getColumnValue(1);
';

CREATE OR REPLACE PROCEDURE "SP_LOAD_RAW_SFMC_SPAM"()
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE
    v_run_id VARCHAR DEFAULT UUID_STRING();
BEGIN
    INSERT INTO FIPSAR_AUDIT.PIPELINE_AUDIT.PIPELINE_RUN_LOG
    (
        RUN_ID, PIPELINE_NAME, LAYER, TABLE_NAME, STATUS, STARTED_AT
    )
    VALUES
    (
        :v_run_id, ''SP_LOAD_RAW_SFMC_SPAM'', ''RAW'', ''RAW_SFMC_SPAM'', ''STARTED'', CURRENT_TIMESTAMP()
    );

    UPDATE FIPSAR_AUDIT.PIPELINE_AUDIT.PIPELINE_RUN_LOG
    SET STATUS = ''SUCCESS'',
        ROWS_LOADED = 0,
        ROWS_REJECTED = 0,
        ERROR_MESSAGE = NULL,
        COMPLETED_AT = CURRENT_TIMESTAMP()
    WHERE RUN_ID = :v_run_id;

    RETURN OBJECT_CONSTRUCT(
        ''status'',''SUCCESS'',
        ''run_id'',:v_run_id,
        ''rows_loaded'',0,
        ''message'',''No Spam source stage configured''
    );
END;
';

CREATE OR REPLACE PROCEDURE "SP_LOAD_RAW_SFMC_JOB_METADATA"()
RETURNS VARIANT
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
const rs = snowflake.createStatement({
  sqlText: `CALL FIPSAR_SFMC_EVENTS.RAW_EVENTS.SP_COPY_NEW_FILES(?,?,?,?,?)`,
  binds: [
    ''RAW_SFMC_JOB_METADATA'',
    ''FIPSAR_SFMC_EVENTS.RAW_EVENTS.STG_SFMC_JOB_DE'',
    ''FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_JOB_METADATA'',
    ''FIPSAR_SFMC_EVENTS.RAW_EVENTS.FF_SFMC_CSV_UTF16'',
    `SELECT $1, NULL, NULL, $16, RTRIM($17), ''JOB'',
            METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, CURRENT_TIMESTAMP()`
  ]
}).execute();
rs.next();
return rs.getColumnValue(1);
';

CREATE OR REPLACE PROCEDURE "SP_LOAD_RAW_SFMC_JOB_DE_DETAIL"()
RETURNS VARIANT
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
const rs = snowflake.createStatement({
  sqlText: `CALL FIPSAR_SFMC_EVENTS.RAW_EVENTS.SP_COPY_NEW_FILES(?,?,?,?,?)`,
  binds: [
    ''RAW_SFMC_JOB_DE_DETAIL'',
    ''FIPSAR_SFMC_EVENTS.RAW_EVENTS.STG_SFMC_JOB_DE'',
    ''FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_JOB_DE_DETAIL'',
    ''FIPSAR_SFMC_EVENTS.RAW_EVENTS.FF_SFMC_CSV_UTF16'',
    `SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, RTRIM($17),
            $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, $31, $32, $33, $34,
            $35, $36, $37, $38, $39, $40, $41, $42,
            METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, CURRENT_TIMESTAMP()`
  ]
}).execute();
rs.next();
return rs.getColumnValue(1);
';

CREATE OR REPLACE PROCEDURE "SP_LOAD_RAW_SFMC_UNSUBSCRIBE_DE_DETAIL"()
RETURNS VARIANT
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
const rs = snowflake.createStatement({
  sqlText: `CALL FIPSAR_SFMC_EVENTS.RAW_EVENTS.SP_COPY_NEW_FILES(?,?,?,?,?)`,
  binds: [
    ''RAW_SFMC_UNSUBSCRIBE_DE_DETAIL'',
    ''FIPSAR_SFMC_EVENTS.RAW_EVENTS.STG_SFMC_UNSUBSCRIBE_DE'',
    ''FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_UNSUBSCRIBE_DE_DETAIL'',
    ''FIPSAR_SFMC_EVENTS.RAW_EVENTS.FF_SFMC_CSV_UTF16'',
    `SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
            METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, CURRENT_TIMESTAMP()`
  ]
}).execute();
rs.next();
return rs.getColumnValue(1);
';

CREATE OR REPLACE PROCEDURE "SP_LOAD_RAW_SFMC_JOURNEY_ACTIVITY_GENERIC"()
RETURNS VARIANT
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
const rs = snowflake.createStatement({
  sqlText: `CALL FIPSAR_SFMC_EVENTS.RAW_EVENTS.SP_COPY_NEW_FILES(?,?,?,?,?)`,
  binds: [
    ''RAW_SFMC_JOURNEY_ACTIVITY_GENERIC'',
    ''FIPSAR_SFMC_EVENTS.RAW_EVENTS.STG_SFMC_JOURNEY_ACTIVITY_DE'',
    ''FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_JOURNEY_ACTIVITY_GENERIC'',
    ''FIPSAR_SFMC_EVENTS.RAW_EVENTS.FF_SFMC_CSV_UTF16'',
    `SELECT ARRAY_CONSTRUCT($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28,$29,$30,$31,$32,$33,$34,$35,$36,$37,$38,$39,$40,$41,$42,$43,$44,$45,$46,$47,$48,$49,$50),
            METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, CURRENT_TIMESTAMP()`
  ]
}).execute();
rs.next();
return rs.getColumnValue(1);
';

CREATE OR REPLACE PROCEDURE "SP_LOAD_RAW_SFMC_PROSPECT_C"()
RETURNS VARIANT
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
const rs = snowflake.createStatement({
  sqlText: `CALL FIPSAR_SFMC_EVENTS.RAW_EVENTS.SP_COPY_NEW_FILES(?,?,?,?,?)`,
  binds: [
    ''RAW_SFMC_PROSPECT_C'',
    ''FIPSAR_SFMC_EVENTS.RAW_EVENTS.STG_SFMC_PROSPECT_C'',
    ''FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_PROSPECT_C'',
    ''FIPSAR_SFMC_EVENTS.RAW_EVENTS.FF_SFMC_CSV_UTF16'',
    `SELECT $1, $2, $3, $4, $5, $6, $7, $8,
            METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, CURRENT_TIMESTAMP()`
  ]
}).execute();
rs.next();
return rs.getColumnValue(1);
';

CREATE OR REPLACE PROCEDURE "SP_LOAD_RAW_SFMC_PROSPECT_C_HISTORY"()
RETURNS VARIANT
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
const rs = snowflake.createStatement({
  sqlText: `CALL FIPSAR_SFMC_EVENTS.RAW_EVENTS.SP_COPY_NEW_FILES(?,?,?,?,?)`,
  binds: [
    ''RAW_SFMC_PROSPECT_C_HISTORY'',
    ''FIPSAR_SFMC_EVENTS.RAW_EVENTS.STG_SFMC_PROSPECT_C_HISTORY'',
    ''FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_PROSPECT_C_HISTORY'',
    ''FIPSAR_SFMC_EVENTS.RAW_EVENTS.FF_SFMC_CSV_UTF16'',
    `SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
            METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, CURRENT_TIMESTAMP()`
  ]
}).execute();
rs.next();
return rs.getColumnValue(1);
';

CREATE OR REPLACE PROCEDURE "SP_LOAD_RAW_SFMC_PROSPECT_JOURNEY_DETAILS"()
RETURNS VARIANT
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
const rs = snowflake.createStatement({
  sqlText: `CALL FIPSAR_SFMC_EVENTS.RAW_EVENTS.SP_COPY_NEW_FILES(?,?,?,?,?)`,
  binds: [
    ''RAW_SFMC_PROSPECT_JOURNEY_DETAILS'',
    ''FIPSAR_SFMC_EVENTS.RAW_EVENTS.STG_SFMC_PROSPECT_JOURNEY_DETAILS'',
    ''FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_PROSPECT_JOURNEY_DETAILS'',
    ''FIPSAR_SFMC_EVENTS.RAW_EVENTS.FF_SFMC_CSV_UTF16'',
    `SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22,
            METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, CURRENT_TIMESTAMP()`
  ]
}).execute();
rs.next();
return rs.getColumnValue(1);
';

CREATE OR REPLACE PROCEDURE "SP_LOAD_SFMC_JOB_METADATA"()
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS CALLER
AS '
BEGIN
    MERGE INTO FIPSAR_DW.GOLD.DIM_SFMC_JOB tgt
    USING (
        SELECT JOB_ID, JOURNEY_TYPE, MAPPED_STAGE, EMAIL_NAME, EMAIL_SUBJECT, RECORD_TYPE
        FROM (
            SELECT JOB_ID, JOURNEY_TYPE, MAPPED_STAGE, EMAIL_NAME, EMAIL_SUBJECT, RECORD_TYPE,
                   ROW_NUMBER() OVER (PARTITION BY JOB_ID ORDER BY _LOADED_AT DESC, _SOURCE_ROW_NUMBER DESC) AS rn
            FROM FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_JOB_METADATA
            WHERE JOB_ID IS NOT NULL
        )
        WHERE rn = 1
    ) src ON tgt.JOB_ID = src.JOB_ID
    WHEN MATCHED THEN UPDATE SET
        tgt.JOURNEY_TYPE = src.JOURNEY_TYPE, tgt.MAPPED_STAGE = src.MAPPED_STAGE,
        tgt.EMAIL_NAME   = src.EMAIL_NAME,   tgt.EMAIL_SUBJECT = src.EMAIL_SUBJECT,
        tgt.RECORD_TYPE  = src.RECORD_TYPE,  tgt.UPDATED_AT   = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN INSERT
        (JOB_ID, JOURNEY_TYPE, MAPPED_STAGE, EMAIL_NAME, EMAIL_SUBJECT, RECORD_TYPE)
    VALUES
        (src.JOB_ID, src.JOURNEY_TYPE, src.MAPPED_STAGE, src.EMAIL_NAME, src.EMAIL_SUBJECT, src.RECORD_TYPE);

    RETURN OBJECT_CONSTRUCT(''status'',''SUCCESS'',''table'',''DIM_SFMC_JOB'');
EXCEPTION WHEN OTHER THEN
    RETURN OBJECT_CONSTRUCT(''status'',''FAILED'',''error'',SQLERRM);
END;
';

CREATE OR REPLACE PROCEDURE "SP_LOAD_GOLD_FACT_ENGAGEMENT"()
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE v_rows INTEGER DEFAULT 0; v_run_id VARCHAR DEFAULT UUID_STRING();
BEGIN
    INSERT INTO FIPSAR_AUDIT.PIPELINE_AUDIT.PIPELINE_RUN_LOG
        (RUN_ID, PIPELINE_NAME, LAYER, TABLE_NAME, STATUS, STARTED_AT)
    VALUES (:v_run_id,''SP_LOAD_GOLD_FACT_ENGAGEMENT'',''GOLD'',''FACT_SFMC_ENGAGEMENT'',''STARTED'',CURRENT_TIMESTAMP());

    TRUNCATE TABLE FIPSAR_DW.GOLD.FACT_SFMC_ENGAGEMENT;

    -- SENT events
    INSERT INTO FIPSAR_DW.GOLD.FACT_SFMC_ENGAGEMENT
        (SUBSCRIBER_KEY,JOB_KEY,EVENT_TYPE_KEY,DATE_KEY,EVENT_TIMESTAMP,EVENT_TYPE,
         DOMAIN,ACCOUNT_ID,JOB_ID,RECORD_TYPE,SOURCE_FILE_NAME,LOADED_AT)
    SELECT s.SUBSCRIBER_KEY,j.JOB_KEY,et.EVENT_TYPE_KEY,
        TO_NUMBER(TO_CHAR(TRY_TO_TIMESTAMP_NTZ(s.EVENT_DATE),''YYYYMMDD'')),
        TRY_TO_TIMESTAMP_NTZ(s.EVENT_DATE),''SENT'',
        s.DOMAIN,s.ACCOUNT_ID,s.JOB_ID,s.RECORD_TYPE,s._SOURCE_FILE_NAME,CURRENT_TIMESTAMP()
    FROM FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_SENT s
    LEFT JOIN FIPSAR_DW.GOLD.DIM_SFMC_JOB j         ON s.JOB_ID=j.JOB_ID
    LEFT JOIN FIPSAR_DW.GOLD.DIM_ENGAGEMENT_TYPE et  ON et.EVENT_TYPE=''SENT'';

    -- OPEN events
    INSERT INTO FIPSAR_DW.GOLD.FACT_SFMC_ENGAGEMENT
        (SUBSCRIBER_KEY,JOB_KEY,EVENT_TYPE_KEY,DATE_KEY,EVENT_TIMESTAMP,EVENT_TYPE,
         DOMAIN,IS_UNIQUE,ACCOUNT_ID,JOB_ID,RECORD_TYPE,SOURCE_FILE_NAME,LOADED_AT)
    SELECT o.SUBSCRIBER_KEY,j.JOB_KEY,et.EVENT_TYPE_KEY,
        TO_NUMBER(TO_CHAR(TRY_TO_TIMESTAMP_NTZ(o.EVENT_DATE),''YYYYMMDD'')),
        TRY_TO_TIMESTAMP_NTZ(o.EVENT_DATE),''OPEN'',
        o.DOMAIN,CASE WHEN UPPER(o.IS_UNIQUE) IN (''TRUE'',''1'') THEN TRUE ELSE FALSE END,
        o.ACCOUNT_ID,o.JOB_ID,o.RECORD_TYPE,o._SOURCE_FILE_NAME,CURRENT_TIMESTAMP()
    FROM FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_OPENS o
    LEFT JOIN FIPSAR_DW.GOLD.DIM_SFMC_JOB j         ON o.JOB_ID=j.JOB_ID
    LEFT JOIN FIPSAR_DW.GOLD.DIM_ENGAGEMENT_TYPE et  ON et.EVENT_TYPE=''OPEN'';

    -- CLICK events
    INSERT INTO FIPSAR_DW.GOLD.FACT_SFMC_ENGAGEMENT
        (SUBSCRIBER_KEY,JOB_KEY,EVENT_TYPE_KEY,DATE_KEY,EVENT_TIMESTAMP,EVENT_TYPE,
         DOMAIN,IS_UNIQUE,CLICK_URL,ACCOUNT_ID,JOB_ID,RECORD_TYPE,SOURCE_FILE_NAME,LOADED_AT)
    SELECT c.SUBSCRIBER_KEY,j.JOB_KEY,et.EVENT_TYPE_KEY,
        TO_NUMBER(TO_CHAR(TRY_TO_TIMESTAMP_NTZ(c.EVENT_DATE),''YYYYMMDD'')),
        TRY_TO_TIMESTAMP_NTZ(c.EVENT_DATE),''CLICK'',
        c.DOMAIN,CASE WHEN UPPER(c.IS_UNIQUE) IN (''TRUE'',''1'') THEN TRUE ELSE FALSE END,
        c.URL,c.ACCOUNT_ID,c.JOB_ID,c.RECORD_TYPE,c._SOURCE_FILE_NAME,CURRENT_TIMESTAMP()
    FROM FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_CLICKS c
    LEFT JOIN FIPSAR_DW.GOLD.DIM_SFMC_JOB j         ON c.JOB_ID=j.JOB_ID
    LEFT JOIN FIPSAR_DW.GOLD.DIM_ENGAGEMENT_TYPE et  ON et.EVENT_TYPE=''CLICK'';

    -- BOUNCE events
    INSERT INTO FIPSAR_DW.GOLD.FACT_SFMC_ENGAGEMENT
        (SUBSCRIBER_KEY,JOB_KEY,EVENT_TYPE_KEY,DATE_KEY,EVENT_TIMESTAMP,EVENT_TYPE,
         DOMAIN,BOUNCE_CATEGORY,BOUNCE_TYPE,ACCOUNT_ID,JOB_ID,RECORD_TYPE,SOURCE_FILE_NAME,LOADED_AT)
    SELECT b.SUBSCRIBER_KEY,j.JOB_KEY,et.EVENT_TYPE_KEY,
        TO_NUMBER(TO_CHAR(TRY_TO_TIMESTAMP_NTZ(b.EVENT_DATE),''YYYYMMDD'')),
        TRY_TO_TIMESTAMP_NTZ(b.EVENT_DATE),''BOUNCE'',
        b.DOMAIN,b.BOUNCE_CATEGORY,b.BOUNCE_TYPE,
        b.ACCOUNT_ID,b.JOB_ID,b.RECORD_TYPE,b._SOURCE_FILE_NAME,CURRENT_TIMESTAMP()
    FROM FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_BOUNCES b
    LEFT JOIN FIPSAR_DW.GOLD.DIM_SFMC_JOB j         ON b.JOB_ID=j.JOB_ID
    LEFT JOIN FIPSAR_DW.GOLD.DIM_ENGAGEMENT_TYPE et  ON et.EVENT_TYPE=''BOUNCE'';

    -- UNSUBSCRIBE events
    INSERT INTO FIPSAR_DW.GOLD.FACT_SFMC_ENGAGEMENT
        (SUBSCRIBER_KEY,JOB_KEY,EVENT_TYPE_KEY,DATE_KEY,EVENT_TIMESTAMP,EVENT_TYPE,
         REASON,ACCOUNT_ID,JOB_ID,RECORD_TYPE,SOURCE_FILE_NAME,LOADED_AT)
    SELECT u.SUBSCRIBER_KEY,j.JOB_KEY,et.EVENT_TYPE_KEY,
        TO_NUMBER(TO_CHAR(TRY_TO_TIMESTAMP_NTZ(u.EVENT_DATE),''YYYYMMDD'')),
        TRY_TO_TIMESTAMP_NTZ(u.EVENT_DATE),''UNSUBSCRIBE'',
        u.REASON,u.ACCOUNT_ID,u.JOB_ID,u.RECORD_TYPE,u._SOURCE_FILE_NAME,CURRENT_TIMESTAMP()
    FROM FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_UNSUBSCRIBES u
    LEFT JOIN FIPSAR_DW.GOLD.DIM_SFMC_JOB j         ON u.JOB_ID=j.JOB_ID
    LEFT JOIN FIPSAR_DW.GOLD.DIM_ENGAGEMENT_TYPE et  ON et.EVENT_TYPE=''UNSUBSCRIBE'';

    -- SPAM events
    INSERT INTO FIPSAR_DW.GOLD.FACT_SFMC_ENGAGEMENT
        (SUBSCRIBER_KEY,JOB_KEY,EVENT_TYPE_KEY,DATE_KEY,EVENT_TIMESTAMP,EVENT_TYPE,
         REASON,ACCOUNT_ID,JOB_ID,RECORD_TYPE,SOURCE_FILE_NAME,LOADED_AT)
    SELECT sp.SUBSCRIBER_KEY,j.JOB_KEY,et.EVENT_TYPE_KEY,
        TO_NUMBER(TO_CHAR(TRY_TO_TIMESTAMP_NTZ(sp.EVENT_DATE),''YYYYMMDD'')),
        TRY_TO_TIMESTAMP_NTZ(sp.EVENT_DATE),''SPAM'',
        sp.COMPLAINT_TYPE,sp.ACCOUNT_ID,sp.JOB_ID,sp.RECORD_TYPE,sp._SOURCE_FILE_NAME,CURRENT_TIMESTAMP()
    FROM FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_SPAM sp
    LEFT JOIN FIPSAR_DW.GOLD.DIM_SFMC_JOB j         ON sp.JOB_ID=j.JOB_ID
    LEFT JOIN FIPSAR_DW.GOLD.DIM_ENGAGEMENT_TYPE et  ON et.EVENT_TYPE=''SPAM'';

    SELECT COUNT(*) INTO :v_rows FROM FIPSAR_DW.GOLD.FACT_SFMC_ENGAGEMENT;

    UPDATE FIPSAR_AUDIT.PIPELINE_AUDIT.PIPELINE_RUN_LOG
    SET STATUS=''SUCCESS'', ROWS_LOADED=:v_rows, COMPLETED_AT=CURRENT_TIMESTAMP()
    WHERE RUN_ID=:v_run_id;
    RETURN OBJECT_CONSTRUCT(''status'',''SUCCESS'',''total_rows'',:v_rows);
EXCEPTION WHEN OTHER THEN
    UPDATE FIPSAR_AUDIT.PIPELINE_AUDIT.PIPELINE_RUN_LOG
    SET STATUS=''FAILED'', ERROR_MESSAGE=SQLERRM, COMPLETED_AT=CURRENT_TIMESTAMP()
    WHERE RUN_ID=:v_run_id;
    RETURN OBJECT_CONSTRUCT(''status'',''FAILED'',''error'',SQLERRM);
END;
';

CREATE OR REPLACE PROCEDURE "SP_PROCESS_SFMC_SUPPRESSION"()
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE
    v_updated INTEGER DEFAULT 0;
    v_logged  INTEGER DEFAULT 0;
    v_run_id  VARCHAR DEFAULT UUID_STRING();
BEGIN
    INSERT INTO FIPSAR_AUDIT.PIPELINE_AUDIT.PIPELINE_RUN_LOG
        (RUN_ID, PIPELINE_NAME, LAYER, TABLE_NAME, STATUS)
    VALUES (:v_run_id, ''SP_PROCESS_SFMC_SUPPRESSION'', ''GOLD'', ''FACT_SFMC_ENGAGEMENT'', ''STARTED'');

    -- Mark suppressed prospects in FACT
    UPDATE FIPSAR_DW.GOLD.FACT_SFMC_ENGAGEMENT fe
    SET fe.IS_SUPPRESSED      = TRUE,
        fe.SUPPRESSION_REASON = ''Prospect unsubscribed — journey suppression flag active''
    FROM FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_PROSPECT_JOURNEY_DETAILS jd
    WHERE fe.SUBSCRIBER_KEY = jd.PROSPECT_ID
      AND jd.SUPPRESSION_FLAG = ''True''
      AND COALESCE(fe.IS_SUPPRESSED, FALSE) = FALSE;

    v_updated := SQLROWCOUNT;

    -- Log suppressed prospects to DQ_REJECTION_LOG
    INSERT INTO FIPSAR_AUDIT.PIPELINE_AUDIT.DQ_REJECTION_LOG
        (TABLE_NAME, REJECTION_REASON, REJECTED_RECORD)
    SELECT DISTINCT
        ''FACT_SFMC_ENGAGEMENT'',
        ''SUPPRESSED_PROSPECT'',
        OBJECT_CONSTRUCT(
            ''PROSPECT_ID'',              jd.PROSPECT_ID,
            ''SUPPRESSION_FLAG'',         jd.SUPPRESSION_FLAG,
            ''WELCOME_JOURNEY_COMPLETE'', jd.WELCOME_JOURNEY_COMPLETE,
            ''NURTURE_JOURNEY_COMPLETE'', jd.NURTURE_JOURNEY_COMPLETE,
            ''STAGE'',                    ''SFMC_SUPPRESSION_DETECTION''
        )
    FROM FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_PROSPECT_JOURNEY_DETAILS jd
    WHERE jd.SUPPRESSION_FLAG = ''True''
      AND NOT EXISTS (
          SELECT 1 FROM FIPSAR_AUDIT.PIPELINE_AUDIT.DQ_REJECTION_LOG dq
          WHERE dq.REJECTION_REASON = ''SUPPRESSED_PROSPECT''
            AND TRY_TO_VARCHAR(dq.REJECTED_RECORD:PROSPECT_ID) = jd.PROSPECT_ID
      );

    v_logged := SQLROWCOUNT;

    UPDATE FIPSAR_AUDIT.PIPELINE_AUDIT.PIPELINE_RUN_LOG
    SET STATUS=''SUCCESS'', ROWS_LOADED=:v_updated, ROWS_REJECTED=:v_logged,
        COMPLETED_AT=CURRENT_TIMESTAMP()
    WHERE RUN_ID=:v_run_id;

    RETURN OBJECT_CONSTRUCT(''status'',''SUCCESS'',''fact_rows_updated'',:v_updated,''dq_rows_logged'',:v_logged);
EXCEPTION WHEN OTHER THEN
    UPDATE FIPSAR_AUDIT.PIPELINE_AUDIT.PIPELINE_RUN_LOG
    SET STATUS=''FAILED'', ERROR_MESSAGE=SQLERRM, COMPLETED_AT=CURRENT_TIMESTAMP()
    WHERE RUN_ID=:v_run_id;
    RETURN OBJECT_CONSTRUCT(''status'',''FAILED'',''error'',SQLERRM);
END;
';

CREATE OR REPLACE PROCEDURE "SP_COPY_NEW_FILES"("P_ENTITY_NAME" VARCHAR, "P_STAGE_NAME" VARCHAR, "P_TARGET_TABLE" VARCHAR, "P_FILE_FORMAT" VARCHAR, "P_SELECT_SQL" VARCHAR)
RETURNS VARIANT
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
function getUuid() {
  const rs = snowflake.createStatement({sqlText: `SELECT UUID_STRING()`}).execute();
  rs.next();
  return rs.getColumnValue(1);
}

function parseTimestampFromFileName(fileName) {
  const baseName = String(fileName).split(''/'').pop();

  let m = baseName.match(/(\\d{8}_\\d{6})(?=\\.[^.]+$)/);
  if (m) {
    const s = m[1];
    return new Date(
      s.substring(0,4) + ''-'' + s.substring(4,6) + ''-'' + s.substring(6,8) + ''T'' +
      s.substring(9,11) + '':'' + s.substring(11,13) + '':'' + s.substring(13,15) + ''Z''
    );
  }

  m = baseName.match(/(\\d{8}_\\d{4})(?=\\.[^.]+$)/);
  if (m) {
    const s = m[1];
    return new Date(
      s.substring(0,4) + ''-'' + s.substring(4,6) + ''-'' + s.substring(6,8) + ''T'' +
      s.substring(9,11) + '':'' + s.substring(11,13) + '':00Z''
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
    INSERT INTO FIPSAR_AUDIT.PIPELINE_AUDIT.PIPELINE_RUN_LOG
    (RUN_ID, PIPELINE_NAME, LAYER, TABLE_NAME, STATUS, STARTED_AT)
    VALUES (?, ?, ''RAW'', ?, ''STARTED'', CURRENT_TIMESTAMP())
  `, [pipelineRunId, ''SP_COPY_NEW_FILES_'' + P_ENTITY_NAME, P_TARGET_TABLE]);

  const files = [];
  const listRs = exec(`LIST @${P_STAGE_NAME}`);

  while (listRs.next()) {
    const fullName = listRs.getColumnValue(1);
    const lastModified = listRs.getColumnValue(4);
    const baseName = String(fullName).split(''/'').pop();
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
      UPDATE FIPSAR_AUDIT.PIPELINE_AUDIT.PIPELINE_RUN_LOG
      SET STATUS = ''SUCCESS'',
          ROWS_LOADED = 0,
          ROWS_REJECTED = 0,
          ERROR_MESSAGE = NULL,
          COMPLETED_AT = CURRENT_TIMESTAMP()
      WHERE RUN_ID = ?
    `, [pipelineRunId]);

    return {
      status: ''SUCCESS'',
      run_id: pipelineRunId,
      entity: P_ENTITY_NAME,
      rows_loaded: 0,
      files_loaded: 0,
      files_skipped: 0,
      message: ''No files found in stage''
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
      ? currentFile.parsedTs.toISOString().replace(''T'', '' '').replace(''Z'', '''')
      : null;

    const chkRs = exec(`
      SELECT COUNT(*)
      FROM FIPSAR_AUDIT.PIPELINE_AUDIT.SFMC_FILE_LOAD_AUDIT
      WHERE ENTITY_NAME = ?
        AND STAGE_NAME = ?
        AND TARGET_TABLE_NAME = ?
        AND FILE_NAME = ?
        AND STATUS = ''SUCCESS''
    `, [P_ENTITY_NAME, P_STAGE_NAME, P_TARGET_TABLE, currentFile.baseName]);
    chkRs.next();
    const alreadyLoaded = Number(chkRs.getColumnValue(1));

    if (alreadyLoaded > 0) {
      filesSkipped++;

      exec(`
        INSERT INTO FIPSAR_AUDIT.PIPELINE_AUDIT.SFMC_FILE_LOAD_AUDIT
        (RUN_ID, ENTITY_NAME, STAGE_NAME, TARGET_TABLE_NAME, FILE_NAME, FILE_TIMESTAMP,
         STATUS, ROWS_LOADED, ERROR_MESSAGE, STARTED_AT, COMPLETED_AT)
        VALUES (?, ?, ?, ?, ?, TRY_TO_TIMESTAMP_NTZ(?),
                ''SKIPPED'', 0, ''File already loaded'', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP())
      `, [fileRunId, P_ENTITY_NAME, P_STAGE_NAME, P_TARGET_TABLE, currentFile.baseName, fileTimestampString]);

      continue;
    }

    exec(`
      INSERT INTO FIPSAR_AUDIT.PIPELINE_AUDIT.SFMC_FILE_LOAD_AUDIT
      (RUN_ID, ENTITY_NAME, STAGE_NAME, TARGET_TABLE_NAME, FILE_NAME, FILE_TIMESTAMP,
       STATUS, ROWS_LOADED, STARTED_AT)
      VALUES (?, ?, ?, ?, ?, TRY_TO_TIMESTAMP_NTZ(?),
              ''STARTED'', 0, CURRENT_TIMESTAMP())
    `, [fileRunId, P_ENTITY_NAME, P_STAGE_NAME, P_TARGET_TABLE, currentFile.baseName, fileTimestampString]);

    const escapedBaseName = currentFile.baseName.replace(/''/g, "''''");
    const copySql = `
      COPY INTO ${P_TARGET_TABLE}
      FROM (
        ${P_SELECT_SQL}
        FROM @${P_STAGE_NAME}
      )
      FILES = (''${escapedBaseName}'')
      FILE_FORMAT = (FORMAT_NAME = ${P_FILE_FORMAT})
      ON_ERROR = ''ABORT_STATEMENT''
    `;

    exec(copySql);

    const cntRs = exec(
      `SELECT COUNT(*) FROM ${P_TARGET_TABLE} WHERE _SOURCE_FILE_NAME ILIKE ?`,
      [''%'' + currentFile.baseName]
    );
    cntRs.next();
    const rowsForFile = Number(cntRs.getColumnValue(1));

    totalRowsLoaded += rowsForFile;
    filesLoaded++;

    exec(`
      UPDATE FIPSAR_AUDIT.PIPELINE_AUDIT.SFMC_FILE_LOAD_AUDIT
      SET STATUS = ''SUCCESS'',
          ROWS_LOADED = ?,
          ERROR_MESSAGE = NULL,
          COMPLETED_AT = CURRENT_TIMESTAMP()
      WHERE RUN_ID = ?
    `, [rowsForFile, fileRunId]);
  }

  exec(`
    UPDATE FIPSAR_AUDIT.PIPELINE_AUDIT.PIPELINE_RUN_LOG
    SET STATUS = ''SUCCESS'',
        ROWS_LOADED = ?,
        ROWS_REJECTED = 0,
        ERROR_MESSAGE = NULL,
        COMPLETED_AT = CURRENT_TIMESTAMP()
    WHERE RUN_ID = ?
  `, [totalRowsLoaded, pipelineRunId]);

  return {
    status: ''SUCCESS'',
    run_id: pipelineRunId,
    entity: P_ENTITY_NAME,
    files_seen: fileCount,
    files_loaded: filesLoaded,
    files_skipped: filesSkipped,
    rows_loaded: totalRowsLoaded
  };

} catch (err) {
  exec(`
    UPDATE FIPSAR_AUDIT.PIPELINE_AUDIT.PIPELINE_RUN_LOG
    SET STATUS = ''FAILED'',
        ERROR_MESSAGE = ?,
        COMPLETED_AT = CURRENT_TIMESTAMP()
    WHERE RUN_ID = ?
  `, [String(err), pipelineRunId]);

  if (currentFile !== null) {
    exec(`
      INSERT INTO FIPSAR_AUDIT.PIPELINE_AUDIT.SFMC_FILE_LOAD_AUDIT
      (RUN_ID, ENTITY_NAME, STAGE_NAME, TARGET_TABLE_NAME, FILE_NAME, FILE_TIMESTAMP,
       STATUS, ROWS_LOADED, ERROR_MESSAGE, STARTED_AT, COMPLETED_AT)
      VALUES (?, ?, ?, ?, ?, TRY_TO_TIMESTAMP_NTZ(?),
              ''FAILED'', 0, ?, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP())
    `, [
      getUuid(),
      P_ENTITY_NAME,
      P_STAGE_NAME,
      P_TARGET_TABLE,
      currentFile.baseName,
      currentFile.parsedTs ? currentFile.parsedTs.toISOString().replace(''T'','' '').replace(''Z'','''') : null,
      String(err)
    ]);
  }

  return {
    status: ''FAILED'',
    run_id: pipelineRunId,
    entity: P_ENTITY_NAME,
    error: String(err)
  };
}
';

CREATE OR REPLACE PROCEDURE "SP_RUN_SFMC_EVENTS_PIPELINE"()
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE
    v_run_id          VARCHAR        DEFAULT UUID_STRING();
    v_start_time      TIMESTAMP_NTZ  DEFAULT CURRENT_TIMESTAMP();
    v_total_rows      NUMBER         DEFAULT 0;
    v_total_rejected  NUMBER         DEFAULT 0;
BEGIN
    INSERT INTO FIPSAR_AUDIT.PIPELINE_AUDIT.PIPELINE_RUN_LOG
    (
        RUN_ID, PIPELINE_NAME, LAYER, TABLE_NAME, STATUS, STARTED_AT
    )
    VALUES
    (
        :v_run_id, ''SP_RUN_SFMC_EVENTS_PIPELINE'', ''END_TO_END'', ''SFMC_EVENTS_PIPELINE'', ''STARTED'', :v_start_time
    );

    -- RAW: load all new files from each S3 stage (idempotent via SFMC_FILE_LOAD_AUDIT)
    CALL FIPSAR_SFMC_EVENTS.RAW_EVENTS.SP_LOAD_RAW_SFMC_SENT();
    CALL FIPSAR_SFMC_EVENTS.RAW_EVENTS.SP_LOAD_RAW_SFMC_OPENS();
    CALL FIPSAR_SFMC_EVENTS.RAW_EVENTS.SP_LOAD_RAW_SFMC_CLICKS();
    CALL FIPSAR_SFMC_EVENTS.RAW_EVENTS.SP_LOAD_RAW_SFMC_BOUNCES();
    CALL FIPSAR_SFMC_EVENTS.RAW_EVENTS.SP_LOAD_RAW_SFMC_UNSUBSCRIBES();
    CALL FIPSAR_SFMC_EVENTS.RAW_EVENTS.SP_LOAD_RAW_SFMC_SPAM();
    CALL FIPSAR_SFMC_EVENTS.RAW_EVENTS.SP_LOAD_RAW_SFMC_JOB_METADATA();
    CALL FIPSAR_SFMC_EVENTS.RAW_EVENTS.SP_LOAD_RAW_SFMC_JOB_DE_DETAIL();
    CALL FIPSAR_SFMC_EVENTS.RAW_EVENTS.SP_LOAD_RAW_SFMC_UNSUBSCRIBE_DE_DETAIL();
    CALL FIPSAR_SFMC_EVENTS.RAW_EVENTS.SP_LOAD_RAW_SFMC_PROSPECT_C();
    CALL FIPSAR_SFMC_EVENTS.RAW_EVENTS.SP_LOAD_RAW_SFMC_PROSPECT_C_HISTORY();
    CALL FIPSAR_SFMC_EVENTS.RAW_EVENTS.SP_LOAD_RAW_SFMC_PROSPECT_JOURNEY_DETAILS();
    CALL FIPSAR_SFMC_EVENTS.RAW_EVENTS.SP_LOAD_RAW_SFMC_JOURNEY_ACTIVITY_GENERIC();

    -- GOLD: dims first, then fact (suppressions logged before engagement load)
    CALL FIPSAR_DW.GOLD.SP_LOAD_GOLD_DIM_ENGAGEMENT_TYPE();
    CALL FIPSAR_DW.GOLD.SP_LOAD_GOLD_DIM_SFMC_JOB();
    CALL FIPSAR_DW.GOLD.SP_LOG_SFMC_SUPPRESSIONS();
    CALL FIPSAR_DW.GOLD.SP_LOAD_GOLD_FACT_SFMC_ENGAGEMENT();

    -- Aggregate ROWS_LOADED and ROWS_REJECTED from all child SPs in this run window.
    -- Child SPs log their own entries to PIPELINE_RUN_LOG; sum them here for the
    -- orchestrator-level entry so monitoring queries see accurate totals at the top level.
    SELECT
        COALESCE(SUM(ROWS_LOADED), 0),
        COALESCE(SUM(ROWS_REJECTED), 0)
    INTO :v_total_rows, :v_total_rejected
    FROM FIPSAR_AUDIT.PIPELINE_AUDIT.PIPELINE_RUN_LOG
    WHERE STARTED_AT >= :v_start_time
      AND RUN_ID     != :v_run_id
      AND STATUS      = ''SUCCESS'';

    UPDATE FIPSAR_AUDIT.PIPELINE_AUDIT.PIPELINE_RUN_LOG
    SET STATUS        = ''SUCCESS'',
        ROWS_LOADED   = :v_total_rows,
        ROWS_REJECTED = :v_total_rejected,
        ERROR_MESSAGE = NULL,
        COMPLETED_AT  = CURRENT_TIMESTAMP()
    WHERE RUN_ID = :v_run_id;

    RETURN OBJECT_CONSTRUCT(
        ''status'',              ''SUCCESS'',
        ''run_id'',              :v_run_id,
        ''pipeline'',            ''SFMC_EVENTS_PIPELINE'',
        ''total_rows_loaded'',   :v_total_rows,
        ''total_rows_rejected'', :v_total_rejected
    );

EXCEPTION
    WHEN OTHER THEN
        UPDATE FIPSAR_AUDIT.PIPELINE_AUDIT.PIPELINE_RUN_LOG
        SET STATUS        = ''FAILED'',
            ERROR_MESSAGE = ''SP_RUN_SFMC_EVENTS_PIPELINE failed: '' || SQLERRM,
            COMPLETED_AT  = CURRENT_TIMESTAMP()
        WHERE RUN_ID = :v_run_id;

        RETURN OBJECT_CONSTRUCT(
            ''status'',  ''FAILED'',
            ''run_id'',  :v_run_id,
            ''error'',   ''SP_RUN_SFMC_EVENTS_PIPELINE failed: '' || SQLERRM
        );
END;
';

-- ─────────────────────────────────────────────────────────────────────
-- FIPSAR_AI.AI_PIPELINES  (7 procedures)
-- ─────────────────────────────────────────────────────────────────────

CREATE OR REPLACE PROCEDURE "RUN_SIGNAL_TRUST"("RUN_ID" VARCHAR)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('snowflake-snowpark-python','pandas','numpy')
HANDLER = 'main'
EXECUTE AS CALLER
AS '
import pandas as pd
import numpy as np
import json
from datetime import datetime

def main(session, run_id):
    model_version = ''v1.0.0-rules''

    df = session.sql("""
        SELECT
            e.FACT_ENGAGEMENT_KEY AS ENGAGEMENT_KEY,
            e.SUBSCRIBER_KEY,
            e.SUBSCRIBER_KEY AS MASTER_PATIENT_ID,
            e.JOB_ID,
            e.DATE_KEY,
            e.EVENT_TIMESTAMP,
            e.EVENT_TYPE,
            e.IS_UNIQUE,
            e.BOUNCE_CATEGORY,
            e.DOMAIN
        FROM FIPSAR_DW.GOLD.FACT_SFMC_ENGAGEMENT e
    """).to_pandas()

    df.columns = [c.upper() for c in df.columns]
    df[''EVENT_TIMESTAMP''] = pd.to_datetime(df[''EVENT_TIMESTAMP''])
    n_events = len(df)
    n_subs = df[''MASTER_PATIENT_ID''].nunique()

    df[''BOT_PROBABILITY''] = 0.0
    df[''IS_BOT_FLAG''] = False

    sent = df[df[''EVENT_TYPE''] == ''SENT''][[''SUBSCRIBER_KEY'', ''JOB_ID'', ''EVENT_TIMESTAMP'']].copy()
    sent.rename(columns={''EVENT_TIMESTAMP'': ''SEND_TIMESTAMP''}, inplace=True)
    opens_clicks = df[df[''EVENT_TYPE''].isin([''OPEN'', ''CLICK''])].copy()

    if len(sent) > 0 and len(opens_clicks) > 0:
        merged = opens_clicks.merge(sent, on=[''SUBSCRIBER_KEY'', ''JOB_ID''], how=''left'')
        merged[''SECONDS_AFTER_SEND''] = (merged[''EVENT_TIMESTAMP''] - merged[''SEND_TIMESTAMP'']).dt.total_seconds()
        fast_mask = merged[''SECONDS_AFTER_SEND''].notna() & (merged[''SECONDS_AFTER_SEND''] < 2)
        fast_keys = merged.loc[fast_mask, ''ENGAGEMENT_KEY''].values
        df.loc[df[''ENGAGEMENT_KEY''].isin(fast_keys), ''BOT_PROBABILITY''] += 0.6
        fast_per_job = merged.loc[fast_mask].groupby(''JOB_ID'').size()
        bot_jobs = fast_per_job[fast_per_job >= 5].index.tolist()
        job_mask = (df[''JOB_ID''].isin(bot_jobs)) & (df[''EVENT_TYPE''].isin([''OPEN'', ''CLICK'']))
        df.loc[job_mask, ''BOT_PROBABILITY''] += 0.2

    clicks_df = df[df[''EVENT_TYPE''] == ''CLICK''][[''ENGAGEMENT_KEY'', ''SUBSCRIBER_KEY'', ''JOB_ID'']]
    opens_df = df[df[''EVENT_TYPE''] == ''OPEN''][[''SUBSCRIBER_KEY'', ''JOB_ID'']].drop_duplicates()
    opens_df[''HAS_OPEN''] = True
    click_check = clicks_df.merge(opens_df, on=[''SUBSCRIBER_KEY'', ''JOB_ID''], how=''left'')
    no_open_keys = click_check[click_check[''HAS_OPEN''].isna()][''ENGAGEMENT_KEY''].values
    df.loc[df[''ENGAGEMENT_KEY''].isin(no_open_keys), ''BOT_PROBABILITY''] += 0.5

    non_unique = df[(df[''EVENT_TYPE''] == ''OPEN'') & (df[''IS_UNIQUE''] == False)][''ENGAGEMENT_KEY''].values
    df.loc[df[''ENGAGEMENT_KEY''].isin(non_unique), ''BOT_PROBABILITY''] += 0.3

    df[''BOT_PROBABILITY''] = df[''BOT_PROBABILITY''].clip(0, 1).round(4)
    df[''IS_BOT_FLAG''] = df[''BOT_PROBABILITY''] > 0.5
    non_eng = df[''EVENT_TYPE''].isin([''SENT'', ''BOUNCE'', ''UNSUBSCRIBE''])
    df.loc[non_eng, ''BOT_PROBABILITY''] = 0.0
    df.loc[non_eng, ''IS_BOT_FLAG''] = False

    df[''IS_ANOMALY''] = False
    df[''ANOMALY_SEVERITY''] = None
    df[''ANOMALY_TYPE''] = None

    daily = df.groupby([''JOB_ID'', ''DATE_KEY'', ''EVENT_TYPE'']).size().reset_index(name=''EVENT_COUNT'')
    stats = daily.groupby(''EVENT_TYPE'')[''EVENT_COUNT''].agg([''mean'', ''std'']).reset_index()
    stats.columns = [''EVENT_TYPE'', ''GMEAN'', ''GSTD'']
    daily = daily.merge(stats, on=''EVENT_TYPE'')
    daily[''Z_SCORE''] = np.where(daily[''GSTD''] > 0, (daily[''EVENT_COUNT''] - daily[''GMEAN'']) / daily[''GSTD''], 0)
    daily[''IS_ANOM''] = daily[''Z_SCORE''] > 3
    daily[''SEV''] = np.where(daily[''Z_SCORE''] > 5, ''HIGH'', np.where(daily[''Z_SCORE''] > 3, ''MEDIUM'', None))
    type_map = {''BOUNCE'': ''BOUNCE_SPIKE'', ''SENT'': ''SEND_VOLUME'', ''OPEN'': ''OPEN_ANOMALY'', ''CLICK'': ''CLICK_ANOMALY'', ''UNSUBSCRIBE'': ''UNSUB_SPIKE''}
    daily[''ATYPE''] = daily.apply(lambda r: type_map.get(r[''EVENT_TYPE'']) if r[''IS_ANOM''] else None, axis=1)

    anom = daily[daily[''IS_ANOM'']][[''JOB_ID'', ''DATE_KEY'', ''EVENT_TYPE'', ''SEV'', ''ATYPE'']]
    if len(anom) > 0:
        df = df.merge(anom, on=[''JOB_ID'', ''DATE_KEY'', ''EVENT_TYPE''], how=''left'')
        df[''IS_ANOMALY''] = df[''SEV''].notna()
        df[''ANOMALY_SEVERITY''] = df[''SEV'']
        df[''ANOMALY_TYPE''] = df[''ATYPE'']
        df.drop(columns=[''SEV'', ''ATYPE''], inplace=True, errors=''ignore'')

    trust = np.ones(len(df))
    trust -= df[''BOT_PROBABILITY''].values * 0.7
    anom_pen = np.where(df[''ANOMALY_SEVERITY''] == ''HIGH'', 0.3, np.where(df[''ANOMALY_SEVERITY''] == ''MEDIUM'', 0.15, 0))
    trust -= anom_pen
    df[''TRUST_SCORE''] = np.clip(trust, 0, 1).round(4)

    now = datetime.utcnow().strftime(''%Y-%m-%d %H:%M:%S'')
    out = df[[''ENGAGEMENT_KEY'', ''SUBSCRIBER_KEY'', ''MASTER_PATIENT_ID'', ''JOB_ID'', ''EVENT_TYPE'',
              ''BOT_PROBABILITY'', ''IS_BOT_FLAG'', ''IS_ANOMALY'', ''ANOMALY_SEVERITY'', ''ANOMALY_TYPE'', ''TRUST_SCORE'']].copy()
    out[''SCORED_AT''] = now
    out[''MODEL_VERSION''] = model_version
    out[''RUN_ID''] = run_id

    session.write_pandas(out, ''HIST_UCB_SIGNAL_TRUST_SCORES'', database=''FIPSAR_AI'', schema=''AI_SEMANTIC'', overwrite=False)

    bot_count = int(df[''IS_BOT_FLAG''].sum())
    anom_count = int(df[''IS_ANOMALY''].sum())
    avg_trust = round(float(df[''TRUST_SCORE''].mean()), 4)

    auc_json = json.dumps({"bot_flag_rate": round(bot_count/max(1,n_events), 4), "anomaly_rate": round(anom_count/max(1,n_events), 4), "avg_trust": avg_trust, "method": "rules-based"})
    session.sql(
        "INSERT INTO FIPSAR_AI.AI_SEMANTIC.HIST_AI_RUN_DETAILS (RUN_ID, MODEL_VERSION, SCORED_AT, TOTAL_PROSPECTS_SCORED, AUC_ROC) "
        "SELECT ''" + run_id + "'', ''" + model_version + "'', ''" + now + "'', " + str(n_events) + ", PARSE_JSON(''" + auc_json + "'')"
    ).collect()

    return f"Signal Trust complete: {n_events} events, {n_subs} subscribers, {bot_count} bots, {anom_count} anomalies, avg trust {avg_trust}"
';

CREATE OR REPLACE PROCEDURE "RUN_PROSPECT360"("RUN_ID" VARCHAR)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('snowflake-snowpark-python','pandas','numpy','scikit-learn')
HANDLER = 'main'
EXECUTE AS CALLER
AS '
import pandas as pd
import numpy as np
import json
from datetime import datetime
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.cluster import KMeans
from sklearn.preprocessing import LabelEncoder, StandardScaler
from sklearn.metrics import roc_auc_score

def main(session, run_id):
    model_version = ''v1.1.0''

    FEATURES = [
        ''AGE'', ''AGE_GROUP'', ''STATE'', ''REGION'', ''PRIMARY_CHANNEL'',
        ''INTAKE_COUNT'', ''DAYS_SINCE_FIRST_INTAKE'',
        ''TOTAL_SENDS'', ''TOTAL_OPENS'', ''TOTAL_CLICKS'', ''TOTAL_BOUNCES'', ''TOTAL_UNSUBS'',
        ''OPEN_RATE'', ''CLICK_TO_OPEN_RATE'',
        ''UNIQUE_STAGES_REACHED'', ''MAX_STAGE_ORDINAL'', ''LAST_ENGAGEMENT_DAYS_AGO'',
        ''HAS_CLICKED'', ''DAYS_IN_JOURNEY'', ''DAYS_SINCE_LAST_OPEN'', ''ENGAGEMENT_DECLINE_FLAG'',
    ]
    CAT_COLS = [''AGE_GROUP'', ''STATE'', ''REGION'', ''PRIMARY_CHANNEL'']
    TARGETS = {''conversion'': ''CONVERTED_FLAG'', ''dropoff'': ''JOURNEY_DROPPED_FLAG'', ''fatigue'': ''EMAIL_FATIGUED_FLAG''}

    df_train = session.sql("SELECT * FROM FIPSAR_AI.AI_FEATURES.FEAT_UCA_PROSPECT_360").to_pandas()
    df_train.columns = [c.upper() for c in df_train.columns]

    df_test = session.sql("SELECT * FROM FIPSAR_AI.AI_FEATURES.TEST_UCA_PROSPECT_360").to_pandas()
    df_test.columns = [c.upper() for c in df_test.columns]

    def prep(df, encoders=None):
        X = df[FEATURES].copy()
        new_enc = {}
        for col in CAT_COLS:
            X[col] = X[col].fillna(''UNKNOWN'').astype(str)
            if encoders is None:
                le = LabelEncoder().fit(X[col])
                X[col], new_enc[col] = le.transform(X[col]), le
            else:
                le = encoders[col]
                X[col] = X[col].map(lambda x, _le=le: _le.transform([x])[0] if x in _le.classes_ else -1)
        for col in [''HAS_CLICKED'', ''ENGAGEMENT_DECLINE_FLAG'']:
            X[col] = X[col].astype(int)
        return X.fillna(0).values, (new_enc if encoders is None else encoders)

    X_train, encoders = prep(df_train)
    X_test, _ = prep(df_test, encoders)

    models, auc_scores = {}, {}
    for key, target in TARGETS.items():
        clf = GradientBoostingClassifier(n_estimators=150, random_state=42)
        clf.fit(X_train, df_train[target].astype(int))
        models[key] = clf
        auc = roc_auc_score(df_train[target].astype(int), clf.predict_proba(X_train)[:, 1])
        auc_scores[key] = round(auc, 4)

    scaler = StandardScaler()
    X_sc = scaler.fit_transform(X_train)
    kmeans = KMeans(n_clusters=4, random_state=42, n_init=10).fit(X_sc)

    probs = {k: m.predict_proba(X_test)[:, 1] for k, m in models.items()}
    clusters = kmeans.predict(scaler.transform(X_test))

    cluster_avg = {}
    for c in range(4):
        mask = clusters == c
        cluster_avg[c] = probs[''conversion''][mask].mean() if mask.sum() > 0 else 0
    sorted_c = sorted(cluster_avg, key=cluster_avg.get, reverse=True)
    labels = [''High Engagement'', ''Moderate Engagement'', ''Low Engagement'', ''At Risk'']
    label_map = {sorted_c[i]: labels[i] for i in range(len(sorted_c))}
    id_map = {sorted_c[i]: i + 1 for i in range(len(sorted_c))}

    scored_at = datetime.utcnow().strftime(''%Y-%m-%d %H:%M:%S'')

    scores = pd.DataFrame({
        ''MASTER_PATIENT_ID'': df_test[''MASTER_PATIENT_ID''].values,
        ''CONVERSION_PROBABILITY'': np.round(probs[''conversion''], 4),
        ''CONVERSION_RISK_TIER'': np.where(probs[''conversion''] > 0.7, ''HIGH'', np.where(probs[''conversion''] > 0.4, ''MEDIUM'', ''LOW'')),
        ''DROPOFF_PROBABILITY'': np.round(probs[''dropoff''], 4),
        ''DROPOFF_RISK_TIER'': np.where(probs[''dropoff''] > 0.7, ''HIGH'', np.where(probs[''dropoff''] > 0.4, ''MEDIUM'', ''LOW'')),
        ''FATIGUE_SCORE'': np.round(probs[''fatigue''], 4),
        ''IS_FATIGUED'': probs[''fatigue''] > 0.5,
        ''CLUSTER_SEGMENT_ID'': [id_map[c] for c in clusters],
        ''CLUSTER_LABEL'': [label_map[c] for c in clusters],
        ''COMPOSITE_HEALTH_SCORE'': np.round(
            probs[''conversion''] * 0.35 + (1 - probs[''dropoff'']) * 0.3 +
            (1 - probs[''fatigue'']) * 0.2 + (clusters < 2).astype(float) * 0.15, 4),
        ''SCORED_AT'': scored_at,
        ''MODEL_VERSION'': model_version,
        ''RUN_ID'': run_id,
    })

    session.write_pandas(scores, ''HIST_UCA_PROSPECT_360_SCORES'', database=''FIPSAR_AI'', schema=''AI_SEMANTIC'', overwrite=False)

    session.sql(f"""
        INSERT INTO FIPSAR_AI.AI_SEMANTIC.HIST_AI_RUN_DETAILS (RUN_ID, MODEL_VERSION, SCORED_AT, TOTAL_PROSPECTS_SCORED, AUC_ROC)
        SELECT ''{run_id}'', ''{model_version}'', ''{scored_at}'', {len(scores)}, PARSE_JSON(''{json.dumps(auc_scores)}'')
    """).collect()

    return f"Prospect 360 complete: {len(scores)} prospects scored, AUC: {auc_scores}"
';

CREATE OR REPLACE PROCEDURE "RUN_SEND_TIME"("RUN_ID" VARCHAR)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('snowflake-snowpark-python','pandas','numpy','scikit-learn')
HANDLER = 'main'
EXECUTE AS CALLER
AS '
import pandas as pd
import numpy as np
import json
from datetime import datetime
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.preprocessing import LabelEncoder
from sklearn.metrics import roc_auc_score

def main(session, run_id):
    model_version = ''v1.0.0-mvp''

    FEATURES = [
        ''SEND_HOUR'', ''DAY_OF_WEEK'', ''IS_WEEKEND'',
        ''SENDS_AT_HOUR'', ''OPENS_AT_HOUR'', ''CLICKS_AT_HOUR'',
        ''OPEN_RATE_AT_HOUR'', ''CLICK_RATE_AT_HOUR'',
        ''AGE_GROUP'', ''REGION'', ''PRIMARY_CHANNEL'',
        ''TOTAL_SENDS_ALLHOURS'', ''TOTAL_OPENS_ALLHOURS'',
        ''OVERALL_OPEN_RATE'', ''HOUR_VS_AVG_LIFT'',
    ]
    CAT_COLS = [''DAY_OF_WEEK'', ''AGE_GROUP'', ''REGION'', ''PRIMARY_CHANNEL'']

    df_train = session.sql("SELECT * FROM FIPSAR_AI.AI_FEATURES.FEAT_UC03_SEND_TIME").to_pandas()
    df_train.columns = [c.upper() for c in df_train.columns]

    df_test = session.sql("SELECT * FROM FIPSAR_AI.AI_FEATURES.TEST_UC03_SEND_TIME").to_pandas()
    df_test.columns = [c.upper() for c in df_test.columns]

    def prep(df, encoders=None, fit=False):
        X = df[FEATURES].copy()
        enc = {} if encoders is None else encoders
        for col in CAT_COLS:
            X[col] = X[col].fillna(''UNKNOWN'')
            if fit:
                le = LabelEncoder()
                X[col] = le.fit_transform(X[col])
                enc[col] = le
            else:
                le = enc[col]
                X[col] = X[col].map(lambda x, _le=le: _le.transform([x])[0] if x in _le.classes_ else -1)
        X[''IS_WEEKEND''] = X[''IS_WEEKEND''].astype(int)
        return X.fillna(0).values, enc

    X_train, encoders = prep(df_train, fit=True)
    X_test, _ = prep(df_test, encoders=encoders)

    y = df_train[''BEST_HOUR_FLAG''].astype(int).values
    clf = GradientBoostingClassifier(n_estimators=200, max_depth=4, learning_rate=0.1, subsample=0.8, random_state=42)
    clf.fit(X_train, y)
    train_auc = roc_auc_score(y, clf.predict_proba(X_train)[:, 1])

    probs = clf.predict_proba(X_test)[:, 1]
    df_pred = df_test[[''MASTER_PATIENT_ID'', ''SEND_HOUR'', ''DAY_OF_WEEK'', ''OPEN_RATE_AT_HOUR'', ''OVERALL_OPEN_RATE'', ''TOTAL_SENDS_ALLHOURS'']].copy()
    df_pred[''PROB''] = probs

    results = []
    for mpid, group in df_pred.groupby(''MASTER_PATIENT_ID''):
        best = group.loc[group[''PROB''].idxmax()]
        bh = int(best[''SEND_HOUR''])
        bd = best[''DAY_OF_WEEK'']
        h12 = bh % 12 or 12
        ampm = ''AM'' if bh < 12 else ''PM''
        nh12 = (bh + 1) % 12 or 12
        window = f"{bd} {h12}-{nh12} {ampm}"
        pred_or = float(best[''OPEN_RATE_AT_HOUR''])
        base_or = float(best[''OVERALL_OPEN_RATE''])

        results.append({
            ''MASTER_PATIENT_ID'': mpid,
            ''BEST_SEND_HOUR'': bh,
            ''BEST_SEND_DAY'': bd,
            ''BEST_SEND_WINDOW'': window,
            ''PREDICTED_OPEN_RATE'': round(pred_or, 4),
            ''BASELINE_OPEN_RATE'': round(base_or, 4),
            ''ENGAGEMENT_LIFT'': round(pred_or - base_or, 4),
            ''CONFIDENCE_FLAG'': ''HIGH'' if int(best[''TOTAL_SENDS_ALLHOURS'']) >= 10 else ''LOW'',
        })

    scores = pd.DataFrame(results)
    scored_at = datetime.utcnow().strftime(''%Y-%m-%d %H:%M:%S'')
    scores[''SCORED_AT''] = scored_at
    scores[''MODEL_VERSION''] = model_version
    scores[''RUN_ID''] = run_id

    session.write_pandas(scores, ''HIST_UC03_SEND_TIME_SCORES'', database=''FIPSAR_AI'', schema=''AI_SEMANTIC'', overwrite=False)

    auc_json = json.dumps({"best_hour_classification": round(train_auc, 4)})
    session.sql(f"""
        INSERT INTO FIPSAR_AI.AI_SEMANTIC.HIST_AI_RUN_DETAILS (RUN_ID, MODEL_VERSION, SCORED_AT, TOTAL_PROSPECTS_SCORED, AUC_ROC)
        SELECT ''{run_id}'', ''{model_version}'', ''{scored_at}'', {len(scores)}, PARSE_JSON(''{auc_json}'')
    """).collect()

    return f"Send Time complete: {len(scores)} subscribers scored, Train AUC: {train_auc:.3f}"
';

CREATE OR REPLACE PROCEDURE "REFRESH_CURRENT_TABLES"("RUN_ID" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS '
BEGIN
    TRUNCATE TABLE FIPSAR_AI.AI_SEMANTIC.SEM_UCB_SIGNAL_TRUST_SCORES;
    TRUNCATE TABLE FIPSAR_AI.AI_SEMANTIC.SEM_UCA_PROSPECT_360_SCORES;
    TRUNCATE TABLE FIPSAR_AI.AI_SEMANTIC.SEM_UC03_SEND_TIME_SCORES;
    TRUNCATE TABLE FIPSAR_AI.AI_SEMANTIC.AI_RUN_DETAILS;

    INSERT INTO FIPSAR_AI.AI_SEMANTIC.SEM_UCB_SIGNAL_TRUST_SCORES
    SELECT * FROM FIPSAR_AI.AI_SEMANTIC.HIST_UCB_SIGNAL_TRUST_SCORES WHERE RUN_ID = :RUN_ID;

    INSERT INTO FIPSAR_AI.AI_SEMANTIC.SEM_UCA_PROSPECT_360_SCORES
    SELECT * FROM FIPSAR_AI.AI_SEMANTIC.HIST_UCA_PROSPECT_360_SCORES WHERE RUN_ID = :RUN_ID;

    INSERT INTO FIPSAR_AI.AI_SEMANTIC.SEM_UC03_SEND_TIME_SCORES
    SELECT * FROM FIPSAR_AI.AI_SEMANTIC.HIST_UC03_SEND_TIME_SCORES WHERE RUN_ID = :RUN_ID;

    INSERT INTO FIPSAR_AI.AI_SEMANTIC.AI_RUN_DETAILS
    SELECT * FROM FIPSAR_AI.AI_SEMANTIC.HIST_AI_RUN_DETAILS WHERE RUN_ID = :RUN_ID;

    RETURN ''Current tables refreshed for RUN_ID: '' || :RUN_ID;
END;
';

CREATE OR REPLACE PROCEDURE "BUILD_UCA_TEST_FEATURES"("RUN_ID" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE
    build_ts TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP();
    row_count INTEGER;
BEGIN
    -- Clear previous features before rebuilding
    TRUNCATE TABLE FIPSAR_AI.AI_FEATURES.TEST_UCA_PROSPECT_360;

    INSERT INTO FIPSAR_AI.AI_FEATURES.TEST_UCA_PROSPECT_360
    WITH
    trusted_events AS (
        SELECT
            e.FACT_ENGAGEMENT_KEY AS ENGAGEMENT_KEY,
            e.SUBSCRIBER_KEY AS MASTER_PATIENT_ID,
            e.SUBSCRIBER_KEY,
            e.EVENT_TYPE, e.EVENT_TIMESTAMP, e.JOB_ID, e.DATE_KEY, e.IS_UNIQUE, e.BOUNCE_CATEGORY
        FROM FIPSAR_DW.GOLD.FACT_SFMC_ENGAGEMENT e
        LEFT JOIN FIPSAR_AI.AI_SEMANTIC.SEM_UCB_SIGNAL_TRUST_SCORES t
            ON e.FACT_ENGAGEMENT_KEY = t.ENGAGEMENT_KEY
        WHERE COALESCE(t.IS_BOT_FLAG, FALSE) = FALSE
    ),
    engagement_agg AS (
        SELECT MASTER_PATIENT_ID,
            COUNT(CASE WHEN EVENT_TYPE = ''SENT'' THEN 1 END) AS TOTAL_SENDS,
            COUNT(CASE WHEN EVENT_TYPE = ''OPEN'' THEN 1 END) AS TOTAL_OPENS,
            COUNT(CASE WHEN EVENT_TYPE = ''CLICK'' THEN 1 END) AS TOTAL_CLICKS,
            COUNT(CASE WHEN EVENT_TYPE = ''BOUNCE'' THEN 1 END) AS TOTAL_BOUNCES,
            COUNT(CASE WHEN EVENT_TYPE = ''UNSUBSCRIBE'' THEN 1 END) AS TOTAL_UNSUBS,
            MAX(EVENT_TIMESTAMP) AS LAST_EVENT_TS,
            MIN(EVENT_TIMESTAMP) AS FIRST_EVENT_TS,
            MAX(CASE WHEN EVENT_TYPE = ''OPEN'' THEN EVENT_TIMESTAMP END) AS LAST_OPEN_TS,
            COUNT(DISTINCT DATE_KEY) AS DAYS_ACTIVE
        FROM trusted_events WHERE MASTER_PATIENT_ID IS NOT NULL
        GROUP BY MASTER_PATIENT_ID
    ),
    stage_info AS (
        SELECT e.MASTER_PATIENT_ID,
            COUNT(DISTINCT j.MAPPED_STAGE) AS UNIQUE_STAGES_REACHED,
            MAX(CASE j.MAPPED_STAGE
                WHEN ''Welcome'' THEN 1 WHEN ''Follow Up'' THEN 2
                WHEN ''First Dispense'' THEN 3 WHEN ''Completion'' THEN 4 ELSE 0
            END) AS MAX_STAGE_ORDINAL
        FROM trusted_events e
        JOIN FIPSAR_DW.GOLD.DIM_SFMC_JOB j ON e.JOB_ID = j.JOB_ID
        WHERE e.MASTER_PATIENT_ID IS NOT NULL
        GROUP BY e.MASTER_PATIENT_ID
    ),
    decline_check AS (
        SELECT MASTER_PATIENT_ID,
            COUNT(CASE WHEN EVENT_TYPE = ''OPEN'' AND EVENT_TIMESTAMP >= DATEADD(DAY, -7, CURRENT_TIMESTAMP()) THEN 1 END) AS OPENS_LAST_7D,
            COUNT(CASE WHEN EVENT_TYPE = ''OPEN'' AND EVENT_TIMESTAMP >= DATEADD(DAY, -14, CURRENT_TIMESTAMP()) AND EVENT_TIMESTAMP < DATEADD(DAY, -7, CURRENT_TIMESTAMP()) THEN 1 END) AS OPENS_PRIOR_7D
        FROM trusted_events WHERE MASTER_PATIENT_ID IS NOT NULL
        GROUP BY MASTER_PATIENT_ID
    )
    SELECT
        p.MASTER_PATIENT_ID, p.AGE, p.AGE_GROUP, p.STATE,
        CASE
            WHEN p.STATE IN (''NJ'',''NY'',''PA'',''MA'',''CT'',''RI'',''VT'',''NH'',''ME'') THEN ''Northeast''
            WHEN p.STATE IN (''TX'',''FL'',''GA'',''NC'',''VA'',''SC'',''AL'',''MS'',''LA'',''TN'',''KY'',''AR'',''OK'') THEN ''South''
            WHEN p.STATE IN (''CA'',''WA'',''AZ'',''OR'',''NV'',''CO'',''UT'',''NM'',''HI'',''AK'') THEN ''West''
            WHEN p.STATE IN (''IL'',''OH'',''MI'',''IN'',''WI'',''MN'',''MO'',''IA'',''KS'',''NE'',''ND'',''SD'') THEN ''Midwest''
            ELSE ''Other''
        END AS REGION,
        p.PRIMARY_CHANNEL, p.INTAKE_COUNT,
        DATEDIFF(DAY, p.FIRST_INTAKE_DATE, CURRENT_DATE()),
        COALESCE(ea.TOTAL_SENDS, 0), COALESCE(ea.TOTAL_OPENS, 0),
        COALESCE(ea.TOTAL_CLICKS, 0), COALESCE(ea.TOTAL_BOUNCES, 0), COALESCE(ea.TOTAL_UNSUBS, 0),
        CASE WHEN COALESCE(ea.TOTAL_SENDS, 0) > 0 THEN ROUND(ea.TOTAL_OPENS::FLOAT / ea.TOTAL_SENDS, 4) ELSE 0 END,
        CASE WHEN COALESCE(ea.TOTAL_OPENS, 0) > 0 THEN ROUND(ea.TOTAL_CLICKS::FLOAT / ea.TOTAL_OPENS, 4) ELSE 0 END,
        COALESCE(si.UNIQUE_STAGES_REACHED, 1), COALESCE(si.MAX_STAGE_ORDINAL, 1),
        COALESCE(DATEDIFF(DAY, ea.LAST_EVENT_TS, CURRENT_TIMESTAMP()), 999),
        CASE WHEN COALESCE(ea.TOTAL_CLICKS, 0) > 0 THEN TRUE ELSE FALSE END,
        COALESCE(DATEDIFF(DAY, ea.FIRST_EVENT_TS, ea.LAST_EVENT_TS), 0),
        COALESCE(DATEDIFF(DAY, ea.LAST_OPEN_TS, CURRENT_TIMESTAMP()), DATEDIFF(DAY, p.FIRST_INTAKE_DATE, CURRENT_DATE())),
        CASE WHEN dc.OPENS_LAST_7D < dc.OPENS_PRIOR_7D THEN TRUE ELSE FALSE END,
        NULL, NULL, NULL,
        :build_ts
    FROM FIPSAR_DW.GOLD.DIM_PROSPECT p
    LEFT JOIN engagement_agg ea ON p.MASTER_PATIENT_ID = ea.MASTER_PATIENT_ID
    LEFT JOIN stage_info si ON p.MASTER_PATIENT_ID = si.MASTER_PATIENT_ID
    LEFT JOIN decline_check dc ON p.MASTER_PATIENT_ID = dc.MASTER_PATIENT_ID
    WHERE p.IS_ACTIVE = TRUE AND p.MASTER_PATIENT_ID IS NOT NULL;

    SELECT COUNT(*) INTO :row_count FROM FIPSAR_AI.AI_FEATURES.TEST_UCA_PROSPECT_360 WHERE FEATURE_BUILT_AT = :build_ts;
    RETURN ''UC-A features built: '' || :row_count || '' rows at '' || :build_ts::VARCHAR;
END;
';

CREATE OR REPLACE PROCEDURE "BUILD_UC03_TEST_FEATURES"("RUN_ID" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE
    build_ts TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP();
    row_count INTEGER;
BEGIN
    -- Clear previous features before rebuilding
    TRUNCATE TABLE FIPSAR_AI.AI_FEATURES.TEST_UC03_SEND_TIME;

    INSERT INTO FIPSAR_AI.AI_FEATURES.TEST_UC03_SEND_TIME
    WITH
    trusted AS (
        SELECT e.SUBSCRIBER_KEY AS MASTER_PATIENT_ID, e.SUBSCRIBER_KEY,
            e.EVENT_TYPE, e.EVENT_TIMESTAMP, e.JOB_ID, e.DATE_KEY
        FROM FIPSAR_DW.GOLD.FACT_SFMC_ENGAGEMENT e
        LEFT JOIN FIPSAR_AI.AI_SEMANTIC.SEM_UCB_SIGNAL_TRUST_SCORES t
            ON e.FACT_ENGAGEMENT_KEY = t.ENGAGEMENT_KEY
        WHERE COALESCE(t.IS_BOT_FLAG, FALSE) = FALSE AND e.SUBSCRIBER_KEY IS NOT NULL
    ),
    sent_with_hour AS (
        SELECT MASTER_PATIENT_ID, EVENT_TIMESTAMP, HOUR(EVENT_TIMESTAMP) AS SEND_HOUR, JOB_ID
        FROM trusted WHERE EVENT_TYPE = ''SENT''
    ),
    sends_per_hour AS (
        SELECT MASTER_PATIENT_ID, SEND_HOUR, COUNT(*) AS SENDS_AT_HOUR
        FROM sent_with_hour GROUP BY MASTER_PATIENT_ID, SEND_HOUR
    ),
    opens_per_hour AS (
        SELECT s.MASTER_PATIENT_ID, s.SEND_HOUR, COUNT(o.EVENT_TIMESTAMP) AS OPENS_AT_HOUR
        FROM sent_with_hour s
        LEFT JOIN trusted o ON s.MASTER_PATIENT_ID = o.MASTER_PATIENT_ID AND o.EVENT_TYPE = ''OPEN''
            AND o.EVENT_TIMESTAMP BETWEEN s.EVENT_TIMESTAMP AND DATEADD(HOUR, 2, s.EVENT_TIMESTAMP)
        GROUP BY s.MASTER_PATIENT_ID, s.SEND_HOUR
    ),
    clicks_per_hour AS (
        SELECT s.MASTER_PATIENT_ID, s.SEND_HOUR, COUNT(c.EVENT_TIMESTAMP) AS CLICKS_AT_HOUR
        FROM sent_with_hour s
        LEFT JOIN trusted c ON s.MASTER_PATIENT_ID = c.MASTER_PATIENT_ID AND c.EVENT_TYPE = ''CLICK''
            AND c.EVENT_TIMESTAMP BETWEEN s.EVENT_TIMESTAMP AND DATEADD(HOUR, 2, s.EVENT_TIMESTAMP)
        GROUP BY s.MASTER_PATIENT_ID, s.SEND_HOUR
    ),
    subscriber_totals AS (
        SELECT MASTER_PATIENT_ID,
            COUNT(CASE WHEN EVENT_TYPE = ''SENT'' THEN 1 END) AS TOTAL_SENDS_ALLHOURS,
            COUNT(CASE WHEN EVENT_TYPE = ''OPEN'' THEN 1 END) AS TOTAL_OPENS_ALLHOURS
        FROM trusted GROUP BY MASTER_PATIENT_ID
    ),
    hour_dominant_day AS (
        SELECT MASTER_PATIENT_ID, SEND_HOUR, DAYNAME(EVENT_TIMESTAMP) AS DAY_NAME,
            ROW_NUMBER() OVER (PARTITION BY MASTER_PATIENT_ID, SEND_HOUR ORDER BY COUNT(*) DESC) AS RN
        FROM sent_with_hour GROUP BY MASTER_PATIENT_ID, SEND_HOUR, DAYNAME(EVENT_TIMESTAMP)
    ),
    hour_features AS (
        SELECT sph.MASTER_PATIENT_ID, sph.SEND_HOUR,
            LEFT(hdd.DAY_NAME, 3) AS DAY_OF_WEEK,
            CASE WHEN hdd.DAY_NAME IN (''Saturday'',''Sunday'') THEN TRUE ELSE FALSE END AS IS_WEEKEND,
            sph.SENDS_AT_HOUR,
            COALESCE(oph.OPENS_AT_HOUR, 0) AS OPENS_AT_HOUR,
            COALESCE(cph.CLICKS_AT_HOUR, 0) AS CLICKS_AT_HOUR,
            CASE WHEN sph.SENDS_AT_HOUR > 0 THEN ROUND(COALESCE(oph.OPENS_AT_HOUR, 0)::FLOAT / sph.SENDS_AT_HOUR, 4) ELSE 0 END AS OPEN_RATE_AT_HOUR,
            CASE WHEN sph.SENDS_AT_HOUR > 0 THEN ROUND(COALESCE(cph.CLICKS_AT_HOUR, 0)::FLOAT / sph.SENDS_AT_HOUR, 4) ELSE 0 END AS CLICK_RATE_AT_HOUR,
            st.TOTAL_SENDS_ALLHOURS, st.TOTAL_OPENS_ALLHOURS,
            CASE WHEN st.TOTAL_SENDS_ALLHOURS > 0 THEN ROUND(st.TOTAL_OPENS_ALLHOURS::FLOAT / st.TOTAL_SENDS_ALLHOURS, 4) ELSE 0 END AS OVERALL_OPEN_RATE
        FROM sends_per_hour sph
        LEFT JOIN opens_per_hour oph ON sph.MASTER_PATIENT_ID = oph.MASTER_PATIENT_ID AND sph.SEND_HOUR = oph.SEND_HOUR
        LEFT JOIN clicks_per_hour cph ON sph.MASTER_PATIENT_ID = cph.MASTER_PATIENT_ID AND sph.SEND_HOUR = cph.SEND_HOUR
        LEFT JOIN subscriber_totals st ON sph.MASTER_PATIENT_ID = st.MASTER_PATIENT_ID
        LEFT JOIN hour_dominant_day hdd ON sph.MASTER_PATIENT_ID = hdd.MASTER_PATIENT_ID AND sph.SEND_HOUR = hdd.SEND_HOUR AND hdd.RN = 1
    ),
    ranked_hours AS (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY MASTER_PATIENT_ID ORDER BY OPEN_RATE_AT_HOUR DESC, SENDS_AT_HOUR DESC) AS HOUR_RANK
        FROM hour_features WHERE OPEN_RATE_AT_HOUR > 0
    )
    SELECT
        hf.MASTER_PATIENT_ID, hf.SEND_HOUR, hf.DAY_OF_WEEK, hf.IS_WEEKEND,
        hf.SENDS_AT_HOUR, hf.OPENS_AT_HOUR, hf.CLICKS_AT_HOUR,
        hf.OPEN_RATE_AT_HOUR, hf.CLICK_RATE_AT_HOUR,
        p.AGE_GROUP,
        CASE
            WHEN p.STATE IN (''NJ'',''NY'',''PA'',''MA'',''CT'',''RI'',''VT'',''NH'',''ME'') THEN ''Northeast''
            WHEN p.STATE IN (''TX'',''FL'',''GA'',''NC'',''VA'',''SC'',''AL'',''MS'',''LA'',''TN'',''KY'',''AR'',''OK'') THEN ''South''
            WHEN p.STATE IN (''CA'',''WA'',''AZ'',''OR'',''NV'',''CO'',''UT'',''NM'',''HI'',''AK'') THEN ''West''
            WHEN p.STATE IN (''IL'',''OH'',''MI'',''IN'',''WI'',''MN'',''MO'',''IA'',''KS'',''NE'',''ND'',''SD'') THEN ''Midwest''
            ELSE ''Other''
        END AS REGION,
        p.PRIMARY_CHANNEL,
        hf.TOTAL_SENDS_ALLHOURS, hf.TOTAL_OPENS_ALLHOURS, hf.OVERALL_OPEN_RATE,
        ROUND(hf.OPEN_RATE_AT_HOUR - hf.OVERALL_OPEN_RATE, 4) AS HOUR_VS_AVG_LIFT,
        CASE WHEN rh.HOUR_RANK = 1 THEN TRUE ELSE FALSE END AS BEST_HOUR_FLAG,
        :build_ts AS FEATURE_BUILT_AT
    FROM hour_features hf
    JOIN FIPSAR_DW.GOLD.DIM_PROSPECT p ON hf.MASTER_PATIENT_ID = p.MASTER_PATIENT_ID
    LEFT JOIN ranked_hours rh ON hf.MASTER_PATIENT_ID = rh.MASTER_PATIENT_ID AND hf.SEND_HOUR = rh.SEND_HOUR AND rh.HOUR_RANK = 1
    WHERE p.IS_ACTIVE = TRUE;

    SELECT COUNT(*) INTO :row_count FROM FIPSAR_AI.AI_FEATURES.TEST_UC03_SEND_TIME WHERE FEATURE_BUILT_AT = :build_ts;
    RETURN ''UC03 features built: '' || :row_count || '' rows at '' || :build_ts::VARCHAR;
END;
';

CREATE OR REPLACE PROCEDURE "RUN_FULL_PIPELINE"()
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python','pandas','scikit-learn')
HANDLER = 'main'
EXECUTE AS CALLER
AS '
import datetime, hashlib

def main(session):
    ts = datetime.datetime.now().strftime(''%Y%m%d_%H%M%S'')
    h = hashlib.md5(ts.encode()).hexdigest()[:6]
    run_id = f"fipsar_{ts}_{h}"

    results = []

    # Step 1: Signal Trust (BLOCKING)
    try:
        r = session.call(''FIPSAR_AI.AI_PIPELINES.RUN_SIGNAL_TRUST'', run_id)
        results.append(f"[OK] Signal Trust: {r}")
    except Exception as e:
        return f"[ABORT] Signal Trust failed: {str(e)}. Pipeline halted."

    # Step 2: Feature Engineering
    try:
        r = session.call(''FIPSAR_AI.AI_PIPELINES.BUILD_UCA_TEST_FEATURES'', run_id)
        results.append(f"[OK] UC-A Features: {r}")
    except Exception as e:
        results.append(f"[WARN] UC-A Features: {str(e)}")

    try:
        r = session.call(''FIPSAR_AI.AI_PIPELINES.BUILD_UC03_TEST_FEATURES'', run_id)
        results.append(f"[OK] UC03 Features: {r}")
    except Exception as e:
        results.append(f"[WARN] UC03 Features: {str(e)}")

    # Step 3: ML Scoring
    try:
        r = session.call(''FIPSAR_AI.AI_PIPELINES.RUN_PROSPECT360'', run_id)
        results.append(f"[OK] Prospect 360: {r}")
    except Exception as e:
        results.append(f"[FAIL] Prospect 360: {str(e)}")

    try:
        r = session.call(''FIPSAR_AI.AI_PIPELINES.RUN_SEND_TIME'', run_id)
        results.append(f"[OK] Send Time: {r}")
    except Exception as e:
        results.append(f"[FAIL] Send Time: {str(e)}")

    # Step 4: Refresh current tables from history
    try:
        r = session.call(''FIPSAR_AI.AI_PIPELINES.REFRESH_CURRENT_TABLES'', run_id)
        results.append(f"[OK] Current tables: {r}")
    except Exception as e:
        results.append(f"[FAIL] Current tables refresh: {str(e)}")

    summary = f"Run ID: {run_id}\\n" + "\\n".join(results)
    return summary
';

