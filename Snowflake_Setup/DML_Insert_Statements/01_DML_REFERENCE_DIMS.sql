/*================================================================================
FIPSAR Q1 2026 | 01_DML_REFERENCE_DIMS.sql — Run SECOND
DIM_CHANNEL(6), DIM_GEOGRAPHY(20), DIM_ENGAGEMENT_TYPE(7),
DIM_SFMC_JOB(12), RAW_SFMC_JOB_METADATA(12)
================================================================================*/
USE ROLE ACCOUNTADMIN; USE WAREHOUSE COMPUTE_WH;

INSERT INTO QA_FIPSAR_DW.GOLD.DIM_CHANNEL (CHANNEL_KEY,CHANNEL_NAME,CHANNEL_CATEGORY) VALUES
  (1,'Instagram','Social Media'),(2,'Campaign App','Web Campaign'),
  (3,'Facebook','Social Media'),(4,'Website','Web Campaign'),
  (5,'Referral','Referral'),(6,'Survey','Survey');

INSERT INTO QA_FIPSAR_DW.GOLD.DIM_GEOGRAPHY (GEO_KEY,CITY,STATE,ZIP_CODE,REGION) VALUES
  (1,'UNKNOWN','PA','00000','Northeast'),
  (2,'UNKNOWN','NY','00000','Northeast'),
  (3,'UNKNOWN','TX','00000','South'),
  (4,'UNKNOWN','AZ','00000','West'),
  (5,'UNKNOWN','OH','00000','Midwest'),
  (6,'UNKNOWN','CO','00000','West'),
  (7,'UNKNOWN','FL','00000','South'),
  (8,'UNKNOWN','MI','00000','Midwest'),
  (9,'UNKNOWN','CA','00000','West'),
  (10,'UNKNOWN','VA','00000','South'),
  (11,'UNKNOWN','NC','00000','South'),
  (12,'UNKNOWN','GA','00000','South'),
  (13,'UNKNOWN','WA','00000','West'),
  (14,'UNKNOWN','MN','00000','Midwest'),
  (15,'UNKNOWN','OR','00000','West'),
  (16,'UNKNOWN','WI','00000','Midwest'),
  (17,'UNKNOWN','NJ','00000','Northeast'),
  (18,'UNKNOWN','IL','00000','Midwest'),
  (19,'UNKNOWN','MA','00000','Northeast'),
  (20,'UNKNOWN','CT','00000','Northeast');

INSERT INTO QA_FIPSAR_DW.GOLD.DIM_ENGAGEMENT_TYPE (EVENT_TYPE_KEY,EVENT_TYPE,EVENT_CATEGORY,IS_POSITIVE) VALUES
  (1,'SENT','Delivery',TRUE),(2,'OPEN','Engagement',TRUE),(3,'CLICK','Engagement',TRUE),
  (4,'BOUNCE','Negative',FALSE),(5,'UNSUBSCRIBE','Negative',FALSE),
  (6,'SPAM','Negative',FALSE),(7,'UNSENT','Suppression',FALSE);

INSERT INTO QA_FIPSAR_DW.GOLD.DIM_SFMC_JOB
  (JOB_KEY,JOB_ID,JOURNEY_TYPE,MAPPED_STAGE,EMAIL_NAME,EMAIL_SUBJECT,RECORD_TYPE)
VALUES
  (1,1001,'Prospect','Welcome Email','Prospect_Welcome_01_Welcome_Email','Welcome! Your Journey Starts Here','PROSPECT'),
  (2,1002,'Prospect','Welcome Email','Prospect_Welcome_01_Welcome_Email_Resend','We''d love to connect — your journey starts here','PROSPECT'),
  (3,1003,'Prospect','Education Email','Prospect_Welcome_02_Education_Email','Understanding Type 2 Diabetes: What You Need to Know','PROSPECT'),
  (4,1004,'Prospect','Education Email 1','Prospect_Nurture_01_Education_Email_1','Living with Type 2 Diabetes: Key Facts and Insights','PROSPECT'),
  (5,1005,'Prospect','Education Email 1','Prospect_Nurture_01_Education_Email_1_Resend','Don''t miss this — key facts about Type 2 Diabetes','PROSPECT'),
  (6,1006,'Prospect','Education Email 2','Prospect_Nurture_02_Education_Email_2','Treatment Options for Type 2 Diabetes: What You Should Know','PROSPECT'),
  (7,1007,'Prospect','Prospect Story Email','Prospect_Nurture_03_Prospect_Story_Email','A Real Story about Support','PROSPECT'),
  (8,1008,'Prospect','Prospect Story Email','Prospect_Nurture_03_Prospect_Story_Email_Resend','A story we think you''ll find meaningful','PROSPECT'),
  (9,1009,'Prospect','Conversion Email','Prospect_Conversion_01_Conversion_Email','Take the Next Step: We Are Here to Help','PROSPECT'),
  (10,1010,'Prospect','Reminder Email','Prospect_Conversion_02_Reminder_Email','A Gentle Reminder: Support is Just a Click Away','PROSPECT'),
  (11,1011,'Prospect','Re-engagement Email','Prospect_ReEngagement_01_ReEngagement_Email','We Miss You! Here Is Something You Might Like','PROSPECT'),
  (12,1012,'Prospect','Final Reminder Email','Prospect_ReEngagement_02_Final_Reminder_Email','Last Chance: Stay Connected With Us','PROSPECT');

INSERT INTO QA_FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_JOB_METADATA
  (JOB_ID,JOURNEY_TYPE,MAPPED_STAGE,EMAIL_NAME,EMAIL_SUBJECT,RECORD_TYPE,_SOURCE_FILE_NAME,_SOURCE_ROW_NUMBER)
VALUES
  (1001,'Prospect','Welcome Email','Prospect_Welcome_01_Welcome_Email','Welcome! Your Journey Starts Here','PROSPECT','BACKFILL_Q1_2026',1),
  (1002,'Prospect','Welcome Email','Prospect_Welcome_01_Welcome_Email_Resend','We''d love to connect — your journey starts here','PROSPECT','BACKFILL_Q1_2026',2),
  (1003,'Prospect','Education Email','Prospect_Welcome_02_Education_Email','Understanding Type 2 Diabetes: What You Need to Know','PROSPECT','BACKFILL_Q1_2026',3),
  (1004,'Prospect','Education Email 1','Prospect_Nurture_01_Education_Email_1','Living with Type 2 Diabetes: Key Facts and Insights','PROSPECT','BACKFILL_Q1_2026',4),
  (1005,'Prospect','Education Email 1','Prospect_Nurture_01_Education_Email_1_Resend','Don''t miss this — key facts about Type 2 Diabetes','PROSPECT','BACKFILL_Q1_2026',5),
  (1006,'Prospect','Education Email 2','Prospect_Nurture_02_Education_Email_2','Treatment Options for Type 2 Diabetes: What You Should Know','PROSPECT','BACKFILL_Q1_2026',6),
  (1007,'Prospect','Prospect Story Email','Prospect_Nurture_03_Prospect_Story_Email','A Real Story about Support','PROSPECT','BACKFILL_Q1_2026',7),
  (1008,'Prospect','Prospect Story Email','Prospect_Nurture_03_Prospect_Story_Email_Resend','A story we think you''ll find meaningful','PROSPECT','BACKFILL_Q1_2026',8),
  (1009,'Prospect','Conversion Email','Prospect_Conversion_01_Conversion_Email','Take the Next Step: We Are Here to Help','PROSPECT','BACKFILL_Q1_2026',9),
  (1010,'Prospect','Reminder Email','Prospect_Conversion_02_Reminder_Email','A Gentle Reminder: Support is Just a Click Away','PROSPECT','BACKFILL_Q1_2026',10),
  (1011,'Prospect','Re-engagement Email','Prospect_ReEngagement_01_ReEngagement_Email','We Miss You! Here Is Something You Might Like','PROSPECT','BACKFILL_Q1_2026',11),
  (1012,'Prospect','Final Reminder Email','Prospect_ReEngagement_02_Final_Reminder_Email','Last Chance: Stay Connected With Us','PROSPECT','BACKFILL_Q1_2026',12);

