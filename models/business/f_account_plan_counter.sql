{{ config(
	post_hook="grant all privileges on {{ this }} to {{ var('snowflake_user_grant_privileges') }}")
}}

SELECT DISTINCT

accplan.id::varchar(20) as account_plan_id,
CASE
	WHEN accplan.account_vod__c = acc.id
	THEN CASE WHEN LEN(acc.country_jj__c) > 0 THEN acc.country_jj__c ELSE acc.jj_country__c END
	ELSE '' 
END::varchar(2) as account_plan_counter_country_code,
CASE
	WHEN JJC.jj_country_iso_code__c = account_plan_counter_country_code
	THEN JJC.name
	ELSE ''
END::varchar(255) as account_plan_counter_country,
CASE
	WHEN JJC.jj_country_iso_code__c = account_plan_counter_country_code
	THEN JJC.jj_region__c
	ELSE ''
END::varchar(255) as account_plan_counter_region,
1::integer as account_plan_counter
	
FROM {{ source('raw', 'account_plan') }}  as accplan
LEFT OUTER JOIN {{ source('raw', 'account') }} as acc ON acc.id = accplan.account_vod__c
LEFT OUTER JOIN {{ source('raw', 'country_settings') }} as JJC ON JJC.jj_country_iso_code__c = 
	CASE WHEN LEN(acc.country_jj__c) > 0 THEN acc.country_jj__c ELSE acc.jj_country__c END
