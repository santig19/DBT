{{ config(
	post_hook="grant all privileges on {{ this }} to {{ var('snowflake_user_grant_privileges') }}")
}}

SELECT 
    cp.country_iso_code::VARCHAR(2) 											AS Channel_Country_Code
   ,cs.NAME::VARCHAR(255) 														AS Channel_Country
   ,REPLACE(REPLACE(cs.JJ_REGION__C, ' ', ''), '/', '-')::VARCHAR(255) 			AS Channel_Region
   ,CASE 
		WHEN cp.JJ_CHANNEL_LABEL__C = 'eMail' THEN 'Email' 
		ELSE cp.JJ_CHANNEL_LABEL__C 
	END::VARCHAR(255) 															AS Channel
   ,'All Uses'::VARCHAR(255) 													AS Channel_Indication
   ,CASE 
		WHEN UPPER(cp.OPT_TYPE_VOD__C) = 'OPT_IN_VOD' THEN 'Opt-in'
		WHEN UPPER(cp.OPT_TYPE_VOD__C) = 'OPT_OUT_VOD' THEN 'Opt-out'
		ELSE cp.OPT_TYPE_VOD__C 
	END::VARCHAR(255) 															AS Channel_Permission
   ,TO_CHAR(TO_DATE(cp.CREATEDDATE, 'YYYYMMDD HH24:MI:SS'), 'DD-MM-YYYY') 		AS Channel_Permission_Created_Date
   ,TO_CHAR(TO_DATE(cp.LASTMODIFIEDDATE, 'YYYYMMDD HH24:MI:SS'), 'DD-MM-YYYY') 	AS Channel_Permission_Last_Modified_Date
   ,cp.ID::VARCHAR(20) 															AS Channel_Permission_Id
   ,cp.ACCOUNT_VOD__C::VARCHAR(20) 												AS Channel_Permission_Account_Id
   ,TO_TIMESTAMP(NULLIF(cp.Capture_datetime_vod__c, ''),'YYYYMMDD HH24:MI:SS')::DATETIME  							AS Capture_datetime   
   ,CASE 
		WHEN (Position(CHR(92) || CHR(92), cp.Channel_value_vod__c) > 0) OR Position(CHR(124) || CHR(34), cp.Channel_value_vod__c) > 0 OR Position(CHR(92) || CHR(124), cp.Channel_value_vod__c) > 0 OR Position(CHR(92) || CHR(34), cp.Channel_value_vod__c) > 0
		THEN REPLACE(REPLACE(REPLACE(REPLACE(cp.Channel_value_vod__c, CHR(92) || CHR(92), CHR(92)), CHR(124) || CHR(34), CHR(34)), CHR(92) || CHR(124), CHR(124)), CHR(92) || CHR(34), CHR(34))
		ELSE cp.Channel_value_vod__c 
	 END 								 										AS Channel_Value
   ,cp.JJ_Primary_Additional__c													AS Primary_Additional
FROM 
	{{ source('raw', 'multichannel_consent') }} AS cp
LEFT JOIN {{ source('raw', 'country_settings') }} AS cs
    ON cs.jj_Country_ISO_Code__c = cp.COUNTRY_ISO_CODE
LEFT JOIN {{ source('raw', 'account') }} AS ac
    ON cp.ACCOUNT_VOD__C = ac.ID
WHERE 
	cp.OPT_EXPIRATION_DATE_VOD__C = ''