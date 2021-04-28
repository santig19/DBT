{{ config(
	post_hook="grant all privileges on {{ this }} to {{ var('snowflake_user_grant_privileges') }}")
}}

SELECT ut_uniq.id, ut_uniq.userid, ut.territory2id as territoryid
FROM 
(
	SELECT max(id) as id, userid
	FROM 
	(
		SELECT ut.Id, ut.UserId
		FROM {{ var('schema') }}.user_territory_association_raw ut
		JOIN {{ var('schema') }}.user_raw u on u.id = ut.userid AND u.isactive != 0
	)
	GROUP BY userid
) ut_uniq 
JOIN {{ var('schema') }}.user_territory_association_raw ut on ut.id = ut_uniq.id
