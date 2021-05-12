{{ config(
	post_hook="grant all privileges on {{ this }} to {{ var('snowflake_user_grant_privileges') }}")
}}

SELECT DISTINCT
	    ac.ObjectId AS Assignment_Id
	   ,ac.ObjectId AS Account_Id
	   ,gr.ID AS User_Group_Id
	   ,ac.country_iso_code AS Assignment_Country_Code
	   ,1::numeric(10,2) AS Assignment_Percentage
	   ,ac.territory2id AS Territory_Assignment_Id
  FROM      {{ source('raw', 'object_territory_association') }} ac
  LEFT JOIN {{ source('raw', 'group') }} gr
    ON gr.relatedid = ac.territory2id 
   AND UPPER(gr.type) = 'TERRITORY'

UNION

SELECT DISTINCT
	    ut.USERID AS Assignment_Id
	   ,NULL AS Account_Id
	   ,NULL AS User_Group_Id
	   ,ut.country_iso_code AS Assignment_Country_Code
	   ,1::numeric(10,2) AS Assignment_Percentage
	   ,ut.TERRITORY2ID AS Territory_Assignment_Id
FROM {{ source('raw', 'user_territory_association') }} ut
JOIN 
	(	
	SELECT id, isactive, ROW_NUMBER() OVER (PARTITION BY Id ORDER BY lastmodifieddate DESC) as rowid
	FROM {{ source('raw', 'user') }}
	) u on u.id = ut.userid AND u.rowid=1 AND u.isactive != 0 