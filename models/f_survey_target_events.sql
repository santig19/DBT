{{ config(
	post_hook="grant all privileges on {{ this }} to {{ var('snowflake_user_grant_privileges') }}")
}}

with aggr1 as (
	SELECT event_id, account_id, MAX(survey_last_modified_date_time) AS survey_last_modified_date_time
	FROM {{ ref('f_survey_target') }}
	WHERE LEN(event_id) > 0
	AND LOWER(survey_target_status) IN ('submitted_vod', 'late_submission_vod') 
	GROUP BY event_id, account_id
) 
, aggr2_anonym_surv as (
	SELECT event_id, survey_target_id, MAX(survey_last_modified_date_time) AS survey_last_modified_date_time
	FROM {{ ref('f_survey_target') }}
	WHERE LEN(event_id) > 0
	AND LOWER(survey_target_status) IN ('submitted_vod', 'late_submission_vod') 
	AND account_id IN ('', ' ', NULL)
	AND CHARINDEX('Speaker', survey_target_account_name) = 0
	GROUP BY event_id, survey_target_id
)
, aggr3_event_null as (
	SELECT account_id, MAX(survey_last_modified_date_time) AS survey_last_modified_date_time
	FROM {{ ref('f_survey_target') }}
	WHERE event_id IS NULL
	AND LOWER(survey_target_status) IN ('submitted_vod', 'late_submission_vod') 
	GROUP BY account_id
)

SELECT
	 (st.event_id || '-' || st.account_id)::varchar(255) AS Customer_Feedback_Id
	,('Event Survey')::varchar(255) AS Customer_Feedback_Origin 
	,max(st.survey_target_nps_score)::numeric(10, 0) AS survey_target_nps_score 
	,st.event_id
	,st.account_id
	,st.survey_target_id AS survey_target_id
	,CASE 
  		WHEN LEN(acc.Country_JJ__c)>0
     	THEN acc.Country_JJ__c 
        ELSE acc.JJ_country__c
  	END::varchar(255) AS Survey_Target_Events_Country_Code
	,cs.jj_region__c AS Survey_Target_Events_Region
FROM aggr1
LEFT OUTER JOIN {{ ref('f_survey_target') }} AS st ON st.event_id=aggr1.event_id and st.account_id=aggr1.account_id 
	and st.survey_last_modified_date_time=aggr1.survey_last_modified_date_time
LEFT OUTER JOIN {{ var('schema') }}.country_settings_raw cs ON cs.jj_country_iso_code__c = st.country_code
LEFT OUTER JOIN {{ var('schema') }}.account_raw acc ON acc.Id = st.account_id
GROUP BY st.event_id, st.account_id, st.survey_target_id, acc.Country_JJ__c, acc.JJ_country__c, cs.jj_region__c

UNION ALL

SELECT
	 (st.event_id || '-' || st.account_id)::varchar(255) AS Customer_Feedback_Id
	,('Event Survey')::varchar(255) AS Customer_Feedback_Origin 
	,max(st.survey_target_nps_score)::numeric(10, 0) AS survey_target_nps_score 
	,st.event_id
	,st.account_id
	,st.survey_target_id::varchar(255) AS survey_target_id
	,CASE 
  		WHEN LEN(acc.Country_JJ__c)>0
     	THEN acc.Country_JJ__c 
        ELSE acc.JJ_country__c
  	END::varchar(255) AS Survey_Target_Events_Country_Code
	,cs.jj_region__c::varchar(255) AS Survey_Target_Events_Region
FROM aggr2_anonym_surv aggr2
LEFT OUTER JOIN {{ ref('f_survey_target') }} AS st ON st.event_id=aggr2.event_id and st.survey_target_id=aggr2.survey_target_id 
	and st.survey_last_modified_date_time=aggr2.survey_last_modified_date_time
LEFT OUTER JOIN {{ var('schema') }}.country_settings_raw AS cs ON cs.jj_country_iso_code__c = st.country_code
LEFT OUTER JOIN {{ var('schema') }}.account_raw AS acc ON acc.Id = st.account_id
GROUP BY st.event_id, st.account_id, st.survey_target_id, acc.Country_JJ__c, acc.JJ_country__c, cs.jj_region__c

UNION ALL

SELECT
	 NULL::varchar(255) AS Customer_Feedback_Id
	,('Event Survey')::varchar(255) AS Customer_Feedback_Origin 
	,max(st.survey_target_nps_score)::numeric(10, 0) AS survey_target_nps_score 
	,st.event_id
	,st.account_id
	,st.survey_target_id::varchar(255) AS survey_target_id
	,CASE 
  		WHEN LEN(acc.Country_JJ__c)>0
     	THEN acc.Country_JJ__c 
        ELSE acc.JJ_country__c
  	END::varchar(255) AS Survey_Target_Events_Country_Code
	,cs.jj_region__c::varchar(255) AS Survey_Target_Events_Region
FROM aggr3_event_null aggr3
LEFT OUTER JOIN {{ ref('f_survey_target') }} AS st ON st.account_id=aggr3.account_id 
	and st.survey_last_modified_date_time=aggr3.survey_last_modified_date_time
LEFT OUTER JOIN {{ var('schema') }}.country_settings_raw AS cs ON cs.jj_country_iso_code__c = st.country_code
LEFT OUTER JOIN {{ var('schema') }}.account_raw AS acc ON acc.Id = st.account_id
GROUP BY st.event_id, st.account_id, st.survey_target_id, acc.Country_JJ__c, acc.JJ_country__c, cs.jj_region__c
