{{ config(
	post_hook="grant all privileges on {{ this }} to {{ var('snowflake_user_grant_privileges') }}")
}}

SELECT account.id AS account_id,
	   CASE WHEN account.brick_number_jj__c <> '' THEN account.country_iso_code::TEXT || '_' || account.brick_number_jj__c ::TEXT ELSE account.country_iso_code::TEXT || '_No_Value' ::TEXT END AS brick_code,
	   account.id AS assignment_id,
	   CASE WHEN account.country_jj__c <> '' THEN account.country_jj__c ELSE account.jj_country__c END AS country_code,
	   country_settings.name AS country,
	   country_settings.jj_Region__c AS region,
	   country_settings.jj_Region__c AS null_country_values_region,
	   CASE WHEN account.country_jj__c <> '' THEN 'Non Visited'::TEXT || '_' || account.country_jj__c::TEXT ELSE 'Non Visited'::TEXT || '_' || account.jj_country__c::TEXT END AS account_visited_non_visited_technical,
	   'Null Country Values' AS origin,
	   MAX(ObjectTerritory2Association.territory2id) AS territory_id,
	   ObjectTerritory2Association.territory2id AS security_key,
	   MAX(UserTerritory.UserId) AS employee_id
  FROM      {{ source('raw', 'account') }}                     AS account
  LEFT JOIN {{ source('raw', 'country_settings') }}      AS country_settings
  	ON country_settings.JJ_COUNTRY_ISO_CODE__C                         = account.country_iso_code
  LEFT JOIN {{ source('raw', 'object_territory_association') }} AS ObjectTerritory2Association
  	ON ObjectTerritory2Association.ObjectId                            = account.id
  LEFT JOIN {{ source('raw', 'user_territory_association') }}   AS UserTerritory
  	ON UserTerritory.Territory2Id                                      = ObjectTerritory2Association.territory2id
 WHERE LEN(ObjectTerritory2Association.territory2id) > 0
 GROUP BY account_id,
 	      brick_code, 
 	      assignment_id,
 	      country_code,
 	      country,
 	      region,
 	      null_country_values_region,
 	      account_visited_non_visited_technical,
 	      origin,
 	      security_key