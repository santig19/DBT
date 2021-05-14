{{ config(
	post_hook="grant all privileges on {{ this }} to {{ var('snowflake_user_grant_privileges') }}")
}}

WITH extended_profiletable AS (
SELECT DISTINCT 
	profileTable.id, 
	profileTable.Name, 
	profile_id, 
	user_id 
FROM (
	SELECT
	 	u1.profileid as profile_id, 
		u1.userid as user_id
	FROM (
		SELECT 
			ROW_NUMBER() OVER (PARTITION BY profileid ORDER BY lastmodifieddate DESC) as rowid, 
			USR.id as userid, 
			USR.profileid as profileid
		FROM {{ source('raw', 'user') }} as USR 
	 ) AS u1	
	LEFT OUTER JOIN {{ source('raw', 'user') }} as u2 ON u1.userid = u2.id
	WHERE u1.rowid=1) 

LEFT OUTER JOIN {{ source('raw', 'profile') }} as profileTable ON profileTable.id = profile_id
)							
							
SELECT DISTINCT

JJTeamMember.jj_account_plan__c::varchar(20) as account_plan_profile_id,
CASE
	WHEN JJTeamMember.jj_team_member__c = extended_profiletable.user_id     
		THEN extended_profiletable.Name
		ELSE NULL
END::varchar(255) as profile_account_plan_id,
CASE
	WHEN accplan.ACCOUNT_VOD__C = acc.id  
		THEN CASE 
				WHEN (CASE WHEN LEN(acc.country_jj__c) > 0
					THEN lower(acc.country_jj__c) 
					ELSE lower(acc.jj_country__c)
				END) = lower(jjc.jj_country_iso_code__c)
					THEN jjc.JJ_REGION__C
					ELSE NULL
			END		
		ELSE 'NULL'
END::varchar(255) AS account_plan_profile_region,
1::integer as account_plan_profile_counter
		
FROM {{ source('raw', 'account_plan_team_member') }} AS JJTeamMember 
LEFT OUTER JOIN extended_profiletable ON JJTeammember.jj_team_member__c = extended_profiletable.user_id
LEFT OUTER JOIN {{ source('raw', 'account') }} AS acc ON user_id = JJTeamMember.jj_team_member__c
LEFT OUTER JOIN {{ source('raw', 'account_plan') }} AS accplan on acc.id = accplan.account_vod__c
LEFT OUTER JOIN {{ source('raw', 'country_settings') }} AS jjc on jjc.JJ_COUNTRY_ISO_CODE__C =
	CASE WHEN len(acc.country_jj__c) > 0 THEN acc.country_jj__c ELSE acc.JJ_country__c END
WHERE JJTeamMember.jj_team_member__c = extended_profiletable.user_id AND accplan.ACCOUNT_VOD__C = acc.id
