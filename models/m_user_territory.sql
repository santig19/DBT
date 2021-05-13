{{ config(
	post_hook="grant all privileges on {{ this }} to {{ var('snowflake_user_grant_privileges') }}")
}}

SELECT 

	convert (varchar(20), u.territory2ID) as user_territory_id,
	convert (varchar(256), t.name) as user_territory_name,
	convert (varchar(20), u.userID) as employee_territory_id,
	convert (varchar(2), u.country_iso_Code) as user_territory_country_code,
	convert (varchar(256), c.name) as user_territory_country,
	convert (varchar(256), c.jj_region__c) as user_territory_region

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
