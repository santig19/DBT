{{ config(
	post_hook="grant all privileges on {{ this }} to {{ var('snowflake_user_grant_privileges') }}")
}}

SELECT 	
CASE WHEN km.ACTIVE_VOD__C = 1 THEN 'Active' ELSE 'Inactive' END::varchar(8) as Key_Message_Status, 	
km.CLM_ID_VOD__C::varchar(255) as Key_Message_CLM_Id,	
km.CATEGORY_VOD__C::varchar(255) as Key_Message_Category, 	
km.CREATEDBYID::varchar(20) as Key_Message_Created_by_Id,	
CASE 	
    WHEN TRIM(km.CREATEDDATE)=''	
    THEN NULL	
    ELSE TO_CHAR(TO_DATE(km.CREATEDDATE, 'YYYYMMDD HH24:MI:SS'), 'DD-MM-YYYY')	
END::varchar(10) as Key_Message_Date, 	
km.CURRENCYISOCODE::varchar(3) as Key_Message_Currency_Code,	
km.CUSTOM_REACTION_VOD__C::varchar(255) as Key_Message_Custom_Reaction, 	
km.DESCRIPTION_VOD__C::varchar(255) as Key_Message_Description,	
km.DISPLAY_ORDER_VOD__C::varchar(10) as Key_Message_Display_Order, 	
km.ID::varchar(20) as Key_Message_Id, 	
km.JJ_EMEA_KEY_MESSAGE__C::varchar(20) as Key_Message_EMEA_Key_Message,	
km.JJ_EXTERNAL_ID__C::varchar(255) as Key_Message_External_Id,	
km.JJ_MESSAGE_UPLOADED_HAS_BEEN_VALIDATED__C::varchar(1) as Key_Message_Validated,	
km.LANGUAGE_VOD__C::varchar(255) as Key_Message_Language, 	
km.LASTMODIFIEDBYID::varchar(20) as Key_Message_Last_Modified_by_Id, 	
km.LASTMODIFIEDDATE::varchar(17) as Key_Message_Last_Modified_Date,	
CASE WHEN km.MANDATORY_DELIVERY_AT_CALL__C = 1 THEN 'Yes' ELSE 'No' END::varchar(3) as Key_Message_Mandatory_Item, 	
km.MEDIA_FILE_CRC_VOD__C::varchar(255) as Key_Message_Media_File_CRC,	
km.MEDIA_FILE_NAME_VOD__C::varchar(255) as Key_Message_File_Name, 	
CASE 	
    WHEN regexp_like(TRIM(km.MEDIA_FILE_SIZE_VOD__C),'^[0-9]+$')	
    THEN km.MEDIA_FILE_SIZE_VOD__C::integer	
    ELSE NULL	
END::integer as Key_Message_File_Size, 	
km.NAME::varchar(255) as Key_Message,	
km.OWNERID::varchar(20) as Employee_Id, 	
km.PRODUCT_STRATEGY_VOD__C::varchar(20) as Key_Message_Product_Strategy, 	
km.PRODUCT_VOD__C::varchar(20) as Product_Key_Message_Id,	
rt.NAME::varchar(255) as Key_Message_Record_Type, 	
km.SEGMENT_VOD__C::varchar(255) as Key_Message_Segment, 	
km.SLIDE_VERSION_VOD__C::varchar(255) as Key_Message_Slide_Version,	
km.VEHICLE_VOD__C::varchar(255) as Key_Message_Vehicle, 	
CASE 	
    WHEN M_PRODUCT.product_id = product_vod__c  	
    THEN CASE WHEN COALESCE(M_PRODUCT.PRODUCT_COUNTRY_CODE,'')='' THEN 'XX' ELSE M_PRODUCT.PRODUCT_COUNTRY_CODE END	
    ELSE 'NM'	
END::varchar(255) as Country_ISO_Code, 	
cs.JJ_REGION__C::varchar(255) as Key_Message_Region,	
cs.NAME::varchar(255) as Key_Message_Country, 	
1::integer as Key_Message_Counter,	
km.COUNTRY_ISO_CODE as key_message_country_code,  
km.JJ_PATIENT_GROUP__C as Patient_Group,   
CASE WHEN (Position(CHR(92) || CHR(92), km.JJ_Key_Message_Sub_Group__c) > 0) OR Position(CHR(124) || CHR(34), km.JJ_Key_Message_Sub_Group__c) > 0 OR Position(CHR(92) || CHR(124), km.JJ_Key_Message_Sub_Group__c) > 0 OR Position(CHR(92) || CHR(34), km.JJ_Key_Message_Sub_Group__c) > 0
    THEN REPLACE(REPLACE(REPLACE(REPLACE(km.JJ_Key_Message_Sub_Group__c, CHR(92) || CHR(92), CHR(92)), CHR(124) || CHR(34), CHR(34)), CHR(92) || CHR(124), CHR(124)), CHR(92) || CHR(34), CHR(34))
ELSE km.JJ_Key_Message_Sub_Group__c END AS key_message_sub_group,
km.JJ_KEY_MESSAGE_GROUP__C as Key_Message_Group	
FROM {{ var('schema') }}.key_message_raw km 	
LEFT OUTER JOIN {{ var('schema') }}.record_type_raw rt ON rt.ID = km.RECORDTYPEID 	
LEFT OUTER JOIN {{ var('schema') }}.country_settings_raw cs ON cs.JJ_COUNTRY_ISO_CODE__C = km.COUNTRY_ISO_CODE 	
LEFT OUTER JOIN {{ ref('m_product') }} AS M_PRODUCT ON M_PRODUCT.product_id = product_vod__c