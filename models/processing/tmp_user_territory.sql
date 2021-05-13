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
		FROM {{ source('raw', 'user_territory_association') }} ut
		JOIN {{ source('raw', 'user') }} u on u.id = ut.userid AND u.isactive != 0
	)
	GROUP BY userid
) ut_uniq 
JOIN {{ source('raw', 'user_territory_association') }} ut on ut.id = ut_uniq.id
