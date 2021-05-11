{{ config(
	post_hook="grant all privileges on {{ this }} to {{ var('snowflake_user_grant_privileges') }}")
}}

SELECT 

CASE 
	WHEN LOWER(CP.active_vod__c) IN ('true', '1')
    		THEN 'Active'
    		ELSE 'Inactive'   
END::varchar(255) AS "Cycle_Status",
CP.Comments_vod__c::varchar(255) AS "Cycle_Comments",
CP.COUNTRY_ISO_CODE::varchar(255) AS "Cycle_Country_Code",
cs.Name::varchar(255) AS "Cycle_Country",
cs.jj_region__c ::varchar(255) AS "Cycle_Region",
TO_CHAR(TO_DATE(CP.start_date_vod__c, 'YYYYMMDD HH24:MI:SS'), 'YYYYMMDD')::numeric(8,0) AS "Cycle_Start_Date",
TO_CHAR(TO_DATE(CP.end_date_vod__c, 'YYYYMMDD HH24:MI:SS'), 'YYYYMMDD')::numeric(8,0) AS "Cycle_End_Date",
CP.Name::varchar(255) AS "Cycle_Plan_Name",
CP.Id::varchar(255) AS "Cycle_Plan_id",
CASE
	WHEN CP.Status_vod__c = ''
 		THEN 'NoValue'
    		ELSE  CP.Status_vod__c
END::varchar(255) AS "Cycle_Plan_Status",
TO_CHAR(TO_TIMESTAMP(CP.LastModifiedDate, 'YYYYMMDD HH24:MI:SS'), 'DD/MM/YYYY hh24:mi:ss')::varchar(255) AS "Cycle_Plan_Last_Modified_Timestamp",
CASE
	WHEN CP.id = USR.JJ_Active_cycle_Plan__c
    		THEN 'Yes'
    		ELSE 'NM'
END::varchar(255) AS "Cycle_Plan_Current_Previous_Flag"

FROM {{ var('schema') }}.cycle_plan_raw AS CP 
LEFT OUTER JOIN {{ var('schema') }}.country_settings_raw AS cs ON CP.country_iso_code = cs.country_iso_code
LEFT OUTER JOIN {{ var('schema') }}.user_raw USR ON CP.id = USR.JJ_Active_Cycle_plan__c
