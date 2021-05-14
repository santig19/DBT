{{ config(
	post_hook="grant all privileges on {{ this }} to {{ var('snowflake_user_grant_privileges') }}")
}}

SELECT 

	u.territory2ID::varchar(20) as user_territory_id,
	t.name::varchar(256) as user_territory_name,
	u.userID::varchar(20) as employee_territory_id,
	u.country_iso_Code::varchar(2) as user_territory_country_code,
	c.name::varchar(256) as user_territory_country,
	c.jj_region__c::varchar(256) as user_territory_region

FROM {{ source('raw', 'user_territory_association') }} u

LEFT JOIN {{ source('raw', 'territory') }} t 
	ON u.territory2ID = t.id
LEFT JOIN {{ source('raw', 'country_settings') }} c 
	ON u.Country_ISO_Code = c.jj_country_iso_code__c

WHERE (user_territory_name LIKE 'L1%' OR user_territory_name LIKE 'L2%' OR user_territory_name LIKE 'L3%' OR user_territory_name LIKE 'L4%' OR user_territory_name LIKE 'L5%' OR user_territory_name LIKE 'L6%' OR user_territory_name LIKE 'L7%' OR user_territory_name LIKE 'L8%')
AND t.id not in (
	select id from {{ source('raw', 'territory') }} where name like '%L4%Consumer%'
union
	select id from {{ source('raw', 'territory') }} where parentterritory2id in
		(select id from {{ source('raw', 'territory') }} where name like '%L4%Consumer%')
union
	select id from {{ source('raw', 'territory') }} where parentterritory2id in
		(select id from {{ source('raw', 'territory') }} where parentterritory2id in
			(select id from {{ source('raw', 'territory') }} where name like '%L4%Consumer%'))
union
	select id from {{ source('raw', 'territory') }} where parentterritory2id in
		(select id from {{ source('raw', 'territory') }} where parentterritory2id in
			(select id from {{ source('raw', 'territory') }} where parentterritory2id in
				(select id from {{ source('raw', 'territory') }} where name like '%L4%Consumer%')))
union
	select id from {{ source('raw', 'territory') }} where parentterritory2id in
		(select id from {{ source('raw', 'territory') }} where parentterritory2id in
			(select id from {{ source('raw', 'territory') }} where parentterritory2id in
				(select id from {{ source('raw', 'territory') }} where parentterritory2id in
					(select id from {{ source('raw', 'territory') }} where name like '%L4%Consumer%'))))
union
	select id from {{ source('raw', 'territory') }} where parentterritory2id in
		(select id from {{ source('raw', 'territory') }} where parentterritory2id in
			(select id from {{ source('raw', 'territory') }} where parentterritory2id in
				(select id from {{ source('raw', 'territory') }} where parentterritory2id in
					(select id from {{ source('raw', 'territory') }} where parentterritory2id in
						(select id from {{ source('raw', 'territory') }} where name like '%L4%Consumer%'))))))
