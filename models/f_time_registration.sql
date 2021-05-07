{{ config(
	post_hook="grant all privileges on {{ this }} to {{ var('snowflake_user_grant_privileges') }}")
}}

SELECT 

tr.id as Time_Registration_Id, 
tr.JJ_COMMENT__C as Time_Registration_Comments, 
CASE WHEN tr.JJ_DATE__C = '' THEN NULL ELSE TO_CHAR(TO_DATE(tr.JJ_DATE__C, 'YYYYMMDD HH24:MI:SS'), 'DD-MM-YYYY') END as Time_Registration_Date,
CASE WHEN tr.JJ_DATE__C = '' THEN NULL ELSE TO_CHAR(TO_DATE(tr.JJ_DATE__C, 'YYYYMMDD HH24:MI:SS'), 'Mon-YYYY') END as Time_Registration_MonthYear,
LEFT(tr.JJ_DATE__C, 6) as Order_Time_Registration_YearMonth,
CASE WHEN tr.JJ_DATE__C = '' THEN NULL ELSE TO_CHAR(TO_DATE(tr.JJ_DATE__C, 'YYYYMMDD HH24:MI:SS'), 'Mon') END as Time_Registration_Month,
LEFT(tr.JJ_DATE__C, 4) as Time_Registration_Year,
CASE WHEN tr.JJ_DATE__C = '' THEN NULL ELSE TO_CHAR(TO_DATE(tr.JJ_DATE__C, 'YYYYMMDD HH24:MI:SS'), 'HH24') END as Time_Registration_Created_Hour,
tr.JJ_DATE__C as Time_Registration_Created_Timestamp,
tr.JJ_SYSTEM_PERCENTAGE__C as Time_Registration_Percentage,
tr.JJ_PRODUCT__C as Product_Id,
tr.NAME as Time_Registration,
tr.OWNERID as Employee_Id,
ut.TERRITORYID as Territory_Nominal_Id,
tr.JJ_TIME_OFF__C as Time_Registration_Time_Off,
tr.COUNTRY_ISO_CODE as Country_Code,
cs.JJ_REGION__C as Region 

FROM {{ var('schema') }}.time_registration_raw tr 
LEFT OUTER JOIN {{ var('schema') }}.country_settings_raw cs ON tr.COUNTRY_ISO_CODE = cs.COUNTRY_ISO_CODE 
LEFT OUTER JOIN {{ ref('tmp_user_territory') }} ut ON ut.USERID = tr.OWNERID
