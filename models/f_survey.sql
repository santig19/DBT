{{ config(
	post_hook="grant all privileges on {{ this }} to {{ var('snowflake_user_grant_privileges') }}")
}}

SELECT 
	 su.ID::varchar(20) as Survey_Id
	,su.JJ_EMEA_SURVEY__C::varchar(1) as Survey_EMEA
	,su.JJ_HCBI_APPROVED__C::varchar(1) as Survey_HCBI_Approved
	,su.JJ_MEDICAL_APPROVED__C::varchar(1) as Survey_Medical_Approved
	,su.JJ_MINDSET_PRODUCT__C::varchar(20) as Survey_Product_Id
	,su.JJ_SURVEY_CHANNEL__C::varchar(255) as Survey_Channel
	,su.JJ_SURVEY_TYPE__C::varchar(255) as Survey_Type
	,su.NAME::varchar(255) as Survey_Name
	,su.OWNERID::varchar(20) as Survey_Owner_Id
	,su.RECORDTYPEID::varchar(20) as Survey_Record_Type
	,CASE WHEN COALESCE(TRIM(su.START_DATE_VOD__C),'')=''
		THEN NULL
		ELSE TO_CHAR(TO_DATE(su.START_DATE_VOD__C, 'YYYYMMDD HH24:MI:SS'), 'YYYYMMDD')
	 END::varchar(8) as Survey_Start_Date
	,su.STATUS_VOD__C::varchar(255) as Survey_Status
	,su.TERRITORY_VOD__C::varchar(50) as Territory_Id
	,CASE WHEN COALESCE(TRIM(su.END_DATE_VOD__C),'')=''
		THEN NULL
		ELSE TO_CHAR(TO_DATE(su.END_DATE_VOD__C, 'YYYYMMDD HH24:MI:SS'), 'YYYYMMDD')
	 END::varchar(8) as Survey_End_Date
	,su.JJ_PRE_APPROVED_SURVEY__C::varchar(1) as Survey_Pre_Approved	
	,su.JJ_CLOSE_THE_LOOP_COORDINATOR__C::varchar(20) as Survey_Close_the_Loop_Cordinator_Id
	,su.JJ_PV_APPROVED__C::varchar(1) as Survey_PV_Approved
	,su.JJ_CONFIRM_TRAINING__C::varchar(1) as Survey_Confirm_Training
	,su.JJ_UNITAS_NUMBER__C::varchar(20) as Survey_RRA_Number
	,1::integer as Survey_Counter
	,su.JJ_EXT_SURVEY_ID__C::varchar(60) as Ext_Survey_Id
FROM {{ source('raw', 'survey') }} su
