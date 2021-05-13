{{ config(
	post_hook="grant all privileges on {{ this }} to {{ var('snowflake_user_grant_privileges') }}")
}}

with F_SURVEY_TARHET_3YRS as (
SELECT
   st.id as survey_target_id
  ,st.ACCOUNT_VOD__C as account_id
  ,st.STATUS_VOD__C as survey_target_status
  ,CASE WHEN LEN(st.JJ_EVENT__C)>0 THEN st.JJ_EVENT__C ELSE NULL END as event_id
  ,TO_CHAR(TO_DATE(st.LASTMODIFIEDDATE, 'YYYYMMDD HH24:MI:SS'), 'YYYYMMDD') as date  
  ,st.name as survey_target_name
FROM {{ source('raw', 'survey_target') }} st
LEFT OUTER JOIN {{ source('raw', 'account') }} ac on ac.ID = st.ACCOUNT_VOD__C
LEFT OUTER JOIN {{ source('raw', 'country_settings') }} cs ON cs.JJ_COUNTRY_ISO_CODE__C = ac.country_iso_code
LEFT OUTER JOIN {{ source('raw', 'question_response') }} qr ON st.ID = qr.SURVEY_TARGET_VOD__C
WHERE LOWER(st.STATUS_VOD__C) IN ('saved_vod', 'pending_vod', 'late_submission_vod', 'submitted_vod') 
AND TO_CHAR(TO_DATE(st.LASTMODIFIEDDATE,'YYYYMMDD HH24:MI:SS'),'yyyymmdd') BETWEEN 
	to_char(add_months(CURRENT_DATE,-36),'YYYYMM')+'01' AND TO_CHAR(current_date(), 'yyyymmdd')
)

,AUX_F_SURVEY_TARGET_EVENT as (
select 
	CASE WHEN Event_Id IS NULL THEN ROW_NUMBER() OVER (ORDER BY survey_target_id)::varchar(50) else Event_Id end as Event_key
	,*
from F_SURVEY_TARHET_3YRS
where event_id != ''
and LOWER(Survey_Target_Status) IN ('submitted_vod','late_submission_vod')
)

,MAP_EVENTIDSURVEYTARGETID_EVENTSURVEYDATE as (
select
	 Event_Key || '-' || Survey_Target_Id as Map_Event_Id_Survey_Target_Id 
	,Max(Date) as Map_Date_Event_Survey
from AUX_F_SURVEY_TARGET_EVENT
group by Event_Key, Survey_Target_Id
)

select 
	 AUX_F_SURVEY_TARGET_EVENT.event_key as event_id
	,AUX_F_SURVEY_TARGET_EVENT.Survey_Target_Id || ' - FK_ACCOUNT' as Customer_Feedback_Id
	,AUX_F_SURVEY_TARGET_EVENT.Survey_Target_Id as Survey_Target_Id
	,AUX_F_SURVEY_TARGET_EVENT.account_id as account_id
from AUX_F_SURVEY_TARGET_EVENT
left outer join MAP_EVENTIDSURVEYTARGETID_EVENTSURVEYDATE map on map.Map_Event_Id_Survey_Target_Id =
	AUX_F_SURVEY_TARGET_EVENT.event_key || '-' || AUX_F_SURVEY_TARGET_EVENT.Survey_Target_Id
where AUX_F_SURVEY_TARGET_EVENT.Account_id IN ('', ' ', NULL) 
and not LOWER(AUX_F_SURVEY_TARGET_EVENT.Survey_Target_Name) like '%speaker%'
and AUX_F_SURVEY_TARGET_EVENT.date = map.Map_Date_Event_Survey
