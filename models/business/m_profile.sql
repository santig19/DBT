{{ config(
	post_hook="grant all privileges on {{ this }} to {{ var('snowflake_user_grant_privileges') }}")
}}

with AUXX_TABLE as (

SELECT 
AUX_TABLE.*,
JJC.jj_region__c::varchar(255) AS "Profile_Region", 
JJC.name::varchar(255) as "Country",
PR.Name::varchar(255) as "Profile"

FROM (
	SELECT
 	u1.profileid::varchar(255) as "profile_id", 
	u2.country_iso_code::varchar(255) as "profile_country_code"
	FROM (
		SELECT 
			ROW_NUMBER() OVER (PARTITION BY profileid ORDER BY lastmodifieddate DESC) as rowid, 
			USR.id as "userid", 
			USR.profileid as "profileid"
		FROM {{ source('raw', 'user') }} as USR 
		) AS u1	
	LEFT OUTER JOIN {{ source('raw', 'user') }} as u2 ON u1.userid = u2.id
	WHERE u1.rowid=1 
	) as AUX_TABLE

LEFT OUTER JOIN {{ source('raw', 'country_settings') }} as JJC on AUX_TABLE.profile_country_code = jjc.country_iso_code
LEFT OUTER JOIN {{ source('raw', 'profile') }} AS PR on AUX_TABLE.profile_country_code = PR.country_iso_code AND PR.id = profile_id

ORDER BY profile_id )


SELECT DISTINCT 
	id::varchar(255) as "profile_id", 
	name::varchar(255) as "profile", 
	CASE WHEN profile_country_code = '' THEN 'NM' ELSE COALESCE(profile_country_code, 'NM') END::varchar(255) as "profile_country_code", 
	COALESCE(Country, 'NM')::varchar(255) as "profile_country",
	COALESCE(profile_region, 'NM')::varchar(255) as "Profile_region" 
FROM {{ source('raw', 'medical_insight') }}.zz_fwk_icon_profile_business as TMP
LEFT OUTER JOIN AUXX_TABLE ON AUXX_TABLE.profile_id = tmp.id
;