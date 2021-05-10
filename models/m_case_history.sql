{{ config(
	post_hook="grant all privileges on {{ this }} to {{ var('snowflake_user_grant_privileges') }}")
}}

SELECT
	 case_history.ID            														AS customer_request_history_id
	,case_history.CASEID        														AS customer_request_id
	,case_history.CREATEDBYID   														AS created_by_id
	,"user".NAME																		AS created_by_name
	,TO_CHAR(TO_DATE(case_history.CREATEDDATE, 'YYYYMMDD HH24:MI:SS'), 'YYYYMMDD')  	AS created_date
	,TO_CHAR(TO_TIMESTAMP(case_history.CREATEDDATE,'YYYYMMDD HH24:MI:SS'), 'YYYYMMDD HH24:MI:SS')::varchar(255)   AS created_time
	,case_history.FIELD         AS changed_field
	,case_history.OLDVALUE      AS old_value
	,case_history.NEWVALUE      AS new_value
FROM {{ var('schema') }}.case_history_raw AS case_history
LEFT JOIN {{ var('schema') }}.user_raw AS "user" 
	ON "user".ID = case_history.CREATEDBYID
WHERE 
	case_history.FIELD IN ('Owner')