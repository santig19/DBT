{{ config(
	post_hook="grant all privileges on {{ this }} to {{ var('snowflake_user_grant_privileges') }}")
}}

SELECT 
	 CASEID
	,KNOWLEDGEARTICLEID
FROM {{ var('schema') }}.case_article_raw
