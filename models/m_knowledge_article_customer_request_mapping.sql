{{ config(
	post_hook="grant all privileges on {{ this }} to {{ var('snowflake_user_grant_privileges') }}")
}}

SELECT 
	 CASEID
	,KNOWLEDGEARTICLEID
FROM {{ source('raw', 'case_article') }}
