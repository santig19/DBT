{{ config(
	post_hook="grant all privileges on {{ this }} to {{ var('snowflake_user_grant_privileges') }}")
}}

SELECT DISTINCT
c.id		 as Call_Id, 
c.name	 as Call_Name_Hierarchy, 
c1.name	 as Parent_Call,
c.name	 as Child_Call
FROM {{ var('schema') }}.calls_business c
JOIN {{ var('schema') }}.calls_business c1 on c1.id=c.parent_call_vod__c

UNION

SELECT DISTINCT 
c.id		 as Call_Id, 
c.name	 as Call_Name_Hierarchy, 
c1.name	 as Parent_Call,
null		 as Child_Call
FROM {{ var('schema') }}.calls_business c
JOIN {{ var('schema') }}.calls_business c1 on c1.id=c.id
WHERE c.parent_call_vod__c=''