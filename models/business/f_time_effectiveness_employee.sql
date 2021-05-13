{{ config(
	post_hook="grant all privileges on {{ this }} to {{ var('snowflake_user_grant_privileges') }}")
}}

SELECT 

(time_eff.employee_id || '-' || time_eff.date)::varchar(255) AS time_effectiveness_employee_id,
time_eff.time_effectiveness_date::varchar(255) AS time_effectiveness_employee_date,
time_eff.time_effectiveness_type::varchar(255) AS time_effectiveness_employee_type,
time_eff.time_effectiveness_status::varchar(255) AS time_effectiveness_employee_status,
AUX.time_effectiveness_hours::varchar(255) AS time_effectiveness_employee_hours,
AUX.time_effectiveness_percentage::varchar(255) AS time_effectiveness_employee_percentage,
time_eff.time_effectiveness_record_type::varchar(255) AS time_effectiveness_employee_record_type,
time_eff.time_effectiveness::varchar(255) AS time_effectiveness_employee,
time_eff.Region::varchar(255) AS time_effectiveness_employee_region,
time_eff.country::varchar(255) AS time_effectiveness_employee_country,
time_eff.country_code::varchar(255) AS time_effectiveness_employee_country_code,
time_eff.time_effectiveness_reason::varchar(255) AS time_effectiveness_employee_reason

FROM {{ ref('f_time_effectiveness') }} AS time_eff
LEFT OUTER JOIN {{ ref('tmp_f_time_effectiveness') }} AS AUX ON time_eff.Time_Effectiveness_Id = AUX.Time_Effectiveness_Id
