{{ config(
	post_hook="grant all privileges on {{ this }} to {{ var('snowflake_user_grant_privileges') }}")
}}

SELECT DISTINCT

tactic.id::varchar(20) as account_objective_id,
CASE WHEN tactic.account_vod__c = acc.id 
	THEN CASE WHEN LEN(acc.country_jj__c)>0 THEN acc.country_jj__c ELSE acc.jj_country__c END
	ELSE ''
END::varchar(2) as account_objective_country_code,
CASE WHEN JJC.jj_country_iso_code__c = account_objective_country_code
	THEN JJC.name
	ELSE ''
END::varchar(255) as account_objective_country,
CASE WHEN JJC.jj_country_iso_code__c = account_objective_country_code
	THEN JJC.jj_region__c
	ELSE ''
END::varchar(255) as account_objective_Region,
1::integer as account_objective_counter
					
FROM {{ source('raw', 'account_tactic') }} AS tactic
LEFT OUTER JOIN {{ source('raw', 'account') }} AS acc ON tactic.account_vod__c = acc.id
LEFT OUTER JOIN {{ source('raw', 'country_settings') }} as JJC ON JJC.jj_country_iso_code__c = 
	CASE WHEN tactic.account_vod__c = acc.id AND LEN(acc.country_jj__c)>0 THEN acc.country_jj__c ELSE acc.jj_country__c END
