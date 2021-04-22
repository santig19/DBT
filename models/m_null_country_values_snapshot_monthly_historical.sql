{{ config(
	post_hook="grant all privileges on {{ this }} to {{ var('snowflake_user_grant_privileges') }}")
}}

SELECT *
FROM {{ var('schema') }}.buw_alignment_m_null_country_values_snapshot_monthly_historical