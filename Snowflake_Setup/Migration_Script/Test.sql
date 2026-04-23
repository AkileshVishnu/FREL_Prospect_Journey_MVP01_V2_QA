
--QA=================================================================================================

select * from QA_fipsar_audit.pipeline_audit.pipeline_run_log order by started_at desc;

select * from QA_FIPSAR_PHI_HUB.STAGING.STG_PROSPECT_INTAKE order by _loaded_at desc;



select * from  QA_fipsar_dw.gold.fact_sfmc_engagement;

--output rows = 802


select * from QA_fipsar_phi_hub.phi_core.phi_prospect_master order by file_date;

select * from QA_fipsar_audit.pipeline_audit.dq_rejection_log where rejection_reason in ('NULL_EMAIL','NULL_PHONE_NUMBER'); and rejected_at like '2026-04%' and rejected_record like '%karthik%';



select * from QA_fipsar_audit.pipeline_audit.pipeline_run_log order by started_at desc;
select * from QA_fipsar_dw.bronze.brz_prospect_master;
--Date Column: File_Date


select * from QA_fipsar_dw.silver.slv_prospect_master;
--Date COlumn: File_Date


select * from QA_fipsar_dw.gold.dim_prospect;
--Date column: First_Intake_Date

select * from QA_fipsar_dw.gold.fact_prospect_intake;
--Date Column: File_Date

select * from QA_FIPSAR_DW.GOLD.VW_SFMC_PROSPECT_OUTBOUND;
select * from QA_FIPSAR_DW.GOLD.FACT_SFMC_ENGAGEMENT; 
--Date Column: Event_Timestamp

--where is_suppressed = TRUE;
select top 5 * from fipsar_sfmc_events.raw_events.raw_sfmc_prospect_journey_details; where suppression_flag = true;
select * from QA_fipsar_sfmc_events.raw_events.raw_sfmc_prospect_c_history;
select * from QA_fipsar_sfmc_events.raw_events.raw_sfmc_prospect_c;
select * from QA_fipsar_sfmc_events.raw_events.raw_sfmc_opens; 

select * from fipsar_sfmc_events.raw_events.raw_sfmc_sent order by event_date;
--output rows = 4300
select top 10 event_date from fipsar_sfmc_events.raw_events.raw_sfmc_opens order by event_date;
--output rows = 3261

select top 10 * from fipsar_sfmc_events.raw_events.raw_sfmc_clicks order by event_date;
--output rows = 2900

select * from fipsar_sfmc_events.raw_events.raw_sfmc_unsubscribes order by event_date;
--output rows = 118

select * from FIPSAR_DW.GOLD.vw_mart_journey_intelligence;



SHOW TASKS IN SCHEMA QA_FIPSAR_PHI_HUB.PHI_CORE;

SHOW TASKS IN SCHEMA QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS;


desc integration FIPSAR_S3_INTEGRATION;


--PROD====================================================================================================





select * from fipsar_audit.pipeline_audit.pipeline_run_log order by started_at desc;

select * from FIPSAR_PHI_HUB.STAGING.STG_PROSPECT_INTAKE order by _loaded_at desc;

List @FIPSAR_PHI_HUB.STAGING.STG_PROSPECT_INBOUND;

select * from  fipsar_dw.gold.fact_sfmc_engagement;

--output rows = 802


select * from fipsar_phi_hub.phi_core.phi_prospect_master order by file_date;
delete from fipsar_phi_hub.phi_core.phi_prospect_master where email in ('raju@fipsar.com','bal@example.com');
--output rows = 766

select * from fipsar_audit.pipeline_audit.dq_rejection_log where rejection_reason in ('NULL_EMAIL','NULL_PHONE_NUMBER'); and rejected_at like '2026-04%' and rejected_record like '%karthik%';

delete from fipsar_audit.pipeline_audit.dq_rejection_log where rejection_id = '8c22ba9f-f290-4287-a304-a7d1f4139cba';
--output rows = 71

INSERT INTO fipsar_audit.pipeline_audit.dq_rejection_log
SELECT *
FROM fipsar_audit.pipeline_audit.dq_rejection_log
AT (TIMESTAMP => DATEADD(MINUTE, -10, CURRENT_TIMESTAMP()));


select * from fipsar_audit.pipeline_audit.pipeline_run_log order by started_at desc;
select * from fipsar_dw.bronze.brz_prospect_master;
--Date Column: File_Date


select * from fipsar_dw.silver.slv_prospect_master;
--Date COlumn: File_Date


select * from fipsar_dw.gold.dim_prospect;
--Date column: First_Intake_Date

select * from fipsar_dw.gold.fact_prospect_intake;
--Date Column: File_Date

select * from FIPSAR_DW.GOLD.VW_SFMC_PROSPECT_OUTBOUND;
select * from FIPSAR_DW.GOLD.FACT_SFMC_ENGAGEMENT; 
--Date Column: Event_Timestamp

--where is_suppressed = TRUE;
select top 5 * from fipsar_sfmc_events.raw_events.raw_sfmc_prospect_journey_details; where suppression_flag = true;
select * from fipsar_sfmc_events.raw_events.raw_sfmc_prospect_c_history;
select * from fipsar_sfmc_events.raw_events.raw_sfmc_prospect_c;
select * from fipsar_sfmc_events.raw_events.raw_sfmc_opens; 

select * from fipsar_sfmc_events.raw_events.raw_sfmc_sent order by event_date;
--output rows = 4300
select top 10 event_date from fipsar_sfmc_events.raw_events.raw_sfmc_opens order by event_date;
--output rows = 3261

select top 10 * from fipsar_sfmc_events.raw_events.raw_sfmc_clicks order by event_date;
--output rows = 2900

select * from fipsar_sfmc_events.raw_events.raw_sfmc_unsubscribes order by event_date;
--output rows = 118

select * from FIPSAR_DW.GOLD.vw_mart_journey_intelligence;



delete from fipsar_audit.pipeline_audit.dq_rejection_log where rejected_at = '2026-03-31 22:02:00.000';

select * from fipsar_phi_hub.staging.stg_prospect_intake;

SELECT *
FROM FIPSAR_AUDIT.PIPELINE_AUDIT.PIPELINE_RUN_LOG
WHERE RUN_ID = 'f85d4b8d-118d-4bf0-ad09-58bffc4ffd34';

CALL FIPSAR_PHI_HUB.PHI_CORE.SP_LOAD_STAGING_FROM_S3();

CALL FIPSAR_PHI_HUB.PHI_CORE.SP_LOAD_PHI_PROSPECT();

CALL FIPSAR_DW.BRONZE.SP_LOAD_BRONZE_PROSPECT();

CALL FIPSAR_DW.SILVER.SP_LOAD_SILVER_PROSPECT();

CALL FIPSAR_DW.GOLD.SP_LOAD_GOLD_DIM_CHANNEL();
CALL FIPSAR_DW.GOLD.SP_LOAD_GOLD_DIM_DATE();
CALL FIPSAR_DW.GOLD.SP_LOAD_GOLD_DIM_GEOGRAPHY();
CALL FIPSAR_DW.GOLD.SP_LOAD_GOLD_DIM_PROSPECT();
CALL FIPSAR_DW.GOLD.SP_LOAD_GOLD_FACT_INTAKE();


call  FIPSAR_DW.GOLD.SP_EXPORT_SFMC_OUTBOUND();



-- RAW one-by-one
CALL FIPSAR_SFMC_EVENTS.RAW_EVENTS.SP_LOAD_RAW_SFMC_SENT();
CALL FIPSAR_SFMC_EVENTS.RAW_EVENTS.SP_LOAD_RAW_SFMC_OPENS();
CALL FIPSAR_SFMC_EVENTS.RAW_EVENTS.SP_LOAD_RAW_SFMC_CLICKS();
CALL FIPSAR_SFMC_EVENTS.RAW_EVENTS.SP_LOAD_RAW_SFMC_BOUNCES();
CALL FIPSAR_SFMC_EVENTS.RAW_EVENTS.SP_LOAD_RAW_SFMC_UNSUBSCRIBES();
CALL FIPSAR_SFMC_EVENTS.RAW_EVENTS.SP_LOAD_RAW_SFMC_JOB_METADATA();
CALL FIPSAR_SFMC_EVENTS.RAW_EVENTS.SP_LOAD_RAW_SFMC_JOB_DE_DETAIL();
CALL FIPSAR_SFMC_EVENTS.RAW_EVENTS.SP_LOAD_RAW_SFMC_PROSPECT_C();
CALL FIPSAR_SFMC_EVENTS.RAW_EVENTS.SP_LOAD_RAW_SFMC_PROSPECT_C_HISTORY();
CALL FIPSAR_SFMC_EVENTS.RAW_EVENTS.SP_LOAD_RAW_SFMC_PROSPECT_JOURNEY_DETAILS();

--GOLD one-by-one
CALL FIPSAR_DW.GOLD.SP_LOAD_GOLD_DIM_ENGAGEMENT_TYPE();
CALL FIPSAR_DW.GOLD.SP_LOAD_GOLD_DIM_SFMC_JOB();
CALL FIPSAR_DW.GOLD.SP_LOG_SFMC_SUPPRESSIONS();
CALL FIPSAR_DW.GOLD.SP_LOAD_GOLD_FACT_SFMC_ENGAGEMENT();

--END-TO-END
CALL FIPSAR_SFMC_EVENTS.RAW_EVENTS.SP_RUN_SFMC_EVENTS_PIPELINE();

VALIDATION
SELECT * FROM FIPSAR_AUDIT.PIPELINE_AUDIT.SFMC_FILE_LOAD_AUDIT ORDER BY STARTED_AT DESC;
SELECT * FROM FIPSAR_AUDIT.PIPELINE_AUDIT.PIPELINE_RUN_LOG ORDER BY STARTED_AT DESC;
SELECT EVENT_TYPE, COUNT(*) FROM FIPSAR_DW.GOLD.FACT_SFMC_ENGAGEMENT GROUP BY 1 ORDER BY 1;
SELECT COUNT(*) FROM FIPSAR_AUDIT.PIPELINE_AUDIT.DQ_REJECTION_LOG WHERE REJECTION_REASON = 'SUPPRESSED_PROSPECT';



-- ============================================================================
-- STEP: RESUME ALL TASKS
-- IMPORTANT: Resume child tasks BEFORE parent. Root task last.
-- ============================================================================

ALTER TASK TASK_SFMC_OUTBOUND_EXPORT         RESUME;
ALTER TASK TASK_GOLD_LOAD                    RESUME;
ALTER TASK TASK_SILVER_DQ_NOTIFICATION       RESUME;
ALTER TASK TASK_SILVER_LOAD                  RESUME;
ALTER TASK TASK_BRONZE_LOAD                  RESUME;
ALTER TASK TASK_PHI_IDENTITY_LOAD            RESUME;
ALTER TASK TASK_PHI_ORCHESTRATOR             RESUME;   -- Root last
ALTER TASK FIPSAR_SFMC_EVENTS.RAW_EVENTS.TASK_SFMC_EVENTS_LOAD RESUME;

-- ============================================================================
-- VERIFY TASKS
-- ============================================================================
SHOW TASKS IN SCHEMA FIPSAR_PHI_HUB.PHI_CORE;
SHOW TASKS IN SCHEMA FIPSAR_SFMC_EVENTS.RAW_EVENTS;


-- ============================================================================
-- STEP: Suspend ALL TASKS
-- IMPORTANT: Resume child tasks BEFORE parent. Root task last.
-- ============================================================================


use database FIPSAR_PHI_HUB;
use schema PHI_CORE;


ALTER TASK TASK_PHI_ORCHESTRATOR             SUSPEND;
ALTER TASK TASK_PHI_IDENTITY_LOAD            SUSPEND;
ALTER TASK TASK_BRONZE_LOAD                  SUSPEND;
ALTER TASK TASK_SILVER_LOAD                  SUSPEND;
ALTER TASK TASK_GOLD_LOAD                    SUSPEND;
ALTER TASK TASK_SFMC_OUTBOUND_EXPORT         SUSPEND;
Alter task TASK_SILVER_DQ_NOTIFICATION SUSPEND;
alter task TASK_PIPELINE_NOTIFICATION SUSPEND;
ALTER TASK FIPSAR_SFMC_EVENTS.RAW_EVENTS.TASK_SFMC_EVENTS_LOAD SUSPEND;
alter task FIPSAR_AI.AI_PIPELINES.TASK_DAILY_PIPELINE suspend;
Alter task FIPSAR_AI.AI_PIPELINES.TASK_RUN_FULL_PIPELINE suspend;