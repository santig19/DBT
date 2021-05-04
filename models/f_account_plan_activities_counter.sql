{{ config(
	post_hook="grant all privileges on {{ this }} to {{ var('snowflake_user_grant_privileges') }}")
}}

SELECT DISTINCT

task.subject::varchar(255) as account_plan_activity_id,
CASE WHEN task.accountid = acc.id
	THEN CASE WHEN LEN(acc.country_jj__c)>0 THEN acc.country_jj__c ELSE acc.jj_country__c END
	ELSE ''
END::varchar(2) as account_plan_activity_country_code,
CASE WHEN jjc.jj_country_iso_code__c = account_plan_activity_country_code
	THEN jjc.name
	ELSE ''
END::varchar(255) as account_plan_activity_country,
CASE 
	WHEN jjc.jj_country_iso_code__c = account_plan_activity_country_code
	THEN jjc.jj_region__c
	ELSE ''
END::varchar(255) as account_plan_activity_region,
1::integer AS account_plan_activity_counter

FROM {{ var('schema') }}.account_raw as acc
LEFT OUTER JOIN {{ var('schema') }}.task_raw as task ON task.accountid = acc.id
LEFT OUTER JOIN {{ var('schema') }}.country_settings_raw as JJC ON jjc.country_iso_code = 
	CASE WHEN task.accountid = acc.id
		THEN CASE WHEN LEN(acc.country_jj__c)>0 THEN acc.country_jj__c ELSE acc.jj_country__c END
		ELSE null
	END
