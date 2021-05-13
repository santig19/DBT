{{ config(
	post_hook="grant all privileges on {{ this }} to {{ var('snowflake_user_grant_privileges') }}")
}}

with veeva1 AS (
	SELECT
		CASE WHEN LEFT(REPLACE(key, 'PicklistValue.', ''), Position('.', REPLACE(key, 'PicklistValue.', '')) - 1) = 'Product_Metrics_vod__c'
																					-- this seems fault tolerant and reliable, but is not elegant. these expressions
																					-- can have 2 or 3 times '.' in them												
			THEN LEFT(key, Position( '.', REPLACE(key, 'PicklistValue.', '')) - 1)   	-- recycling the expression from WHEN
			|| UPPER(REPLACE(REPLACE(key, 'PicklistValue.', '')
			 ,LEFT(REPLACE(key, 'PicklistValue.', ''), Position('.', REPLACE(key, 'PicklistValue.', '')) - 1), ''))
																					-- again not elegant, but for the UPPER part:
																					-- since expression can have 2 or 3 '.', and we need in uppercase everything
																					-- after the second occurence of '.', we need to take the original string,
																					-- remove the already calculated substring, and put the remainder of the string in uppercase.																			
			ELSE CASE WHEN LEN(key) - LEN(REPLACE(key, '.', '')) = 3       			--when # points in the string is three
					THEN LEFT(key, LEN(key) - CHARINDEX('.',REVERSE(key))) 			--display everything until the third occurence of '.'
					ELSE NULL 
				END
		END::varchar(255) as key
		,label as emea_value
		,translation as translated_value
		,country_code as translation_language
	FROM {{ source('raw', 'veeva') }} as veeva1
	WHERE veeva1.key LIKE 'PicklistValue.%'
)

, veeva2 AS (
	SELECT 
		 LEFT(key, Position('.', REPLACE(key, 'PicklistValue.', '')) - 1) || '.Recordtype' AS key																																
		,label as emea_value
		,translation as translated_value
		,country_code as translation_language
	FROM {{ source('raw', 'veeva') }} as veeva2
	WHERE veeva2.key LIKE 'RecordType.%'
)

, veeva3 AS (
	SELECT
		'PicklistValue.Account.Individual_Sub_Type_JJ' as key
		,label as emea_value
		,translation as translated_value
		,country_code as translation_language
	FROM {{ source('raw', 'veeva') }} as veeva3
	WHERE veeva3.key|| '.RecordType' = 'Recordtype.Account.RecordType'
)

, veeva4 AS (
	SELECT
		'PicklistValue.Account.Individual_Type' as key
		,label as emea_value
		,translation as translated_value
		,country_code as translation_language
	FROM {{ source('raw', 'veeva') }} as veeva4
	WHERE veeva4.key like 'PicklistValue.Account.Account_Type%'
)

, veeva5 AS (
	SELECT
		'RecordType.Account.RecordType' as key
		,label as emea_value
		,translation as translated_value
		,country_code as translation_language
	FROM {{ source('raw', 'veeva') }} as veeva5
	WHERE veeva5.key LIKE '%Recordtype.Account%'
)

, veeva6 AS (
	SELECT
		'PicklistValue.Account.Individual_Type' as key
		,label as emea_value
		,translation as translated_value
		,country_code as translation_language
	FROM {{ source('raw', 'veeva') }} as veeva6
	WHERE veeva6.key LIKE '%Recordtype.PersonAccount__c%'
)

, AUX_TRANSLATION_TABLE AS (
	SELECT * FROM veeva1
	UNION
	SELECT * FROM veeva2
	UNION 
	SELECT * FROM veeva3
	UNION
	SELECT * FROM veeva4
	UNION
	SELECT * FROM veeva5
	UNION 
	SELECT * FROM veeva6
)

, AUX_TRANSLATION_TABLE_CALCULATED_FIELDS AS (
	SELECT 
		CF.key || '.' || CF.country_code || '.'|| CF.label as sys_map_emea_picklist,
		CF.label as emea_value,
		CF.translation as aux_translated_value,
		CF.country_code as translation_language
	FROM {{ source('raw', 'calculatedfields') }} as CF
)

, AUX_MAP_EMEAPL_TRPL AS (
	SELECT 
		 AUX_TRANSLATION_TABLE.key || '.' || translation_language || '.' || emea_value as sys_map_emea_picklist
		,translated_value as sys_map_translated_picklist
	FROM AUX_TRANSLATION_TABLE
	UNION
	SELECT
		 sys_map_emea_picklist
		,aux_translated_value as sys_map_translated_picklist
	FROM AUX_TRANSLATION_TABLE_CALCULATED_FIELDS
)
, F_CALL_KEY_MESSAGE as (
	SELECT
		kmv.presentation_id_vod__c AS edetail_presentation_id,
		kmv.key_message_vod__c AS key_message_id,
		CASE
			WHEN LEN(TRIM(kmv.presentation_id_vod__c)) > 0
			THEN 'Yes'
			ELSE 'No'
		END AS key_message_clm,
		kmv.account_vod__c || '_' || kmv.product_vod__c || '_' || kmv.call2_vod__c AS key_call_message_link_id,
		kmv.key_message_vod__c || '_' || kmv.product_vod__c || '_' || kmv.call2_vod__c AS call_clickstream_id,
		kmv.product_vod__c|| '_' || kmv.key_message_vod__c AS aux_key_message_id,
		kmv.duration_vod__c AS call_message_duration,
		kmv.id as call_key_message_id,
		CASE WHEN kmv.account_vod__c = acc.id
			THEN CASE WHEN LEN(acc.country_JJ__c) > 0 THEN acc.country_jj__c ELSE acc.jj_country__c END
			ELSE ''
		END AS call_message_country_code,
		rgb.colour_reference_id as call_message_reaction_colour,
		CASE WHEN kmv.reaction_vod__c = '' OR kmv.reaction_vod__c IS NULL
			THEN 'No Value'
			ELSE CASE WHEN 'PicklistValue.Call2_Key_Message_vod__C.Reaction_vod.EN' || kmv.Reaction_vod__c IN (SELECT sys_map_emea_picklist from AUX_MAP_EMEAPL_TRPL) 
					THEN AUX_MAP_EMEAPL_TRPL.sys_map_translated_picklist
					ELSE kmv.reaction_vod__c
				END						
		END as call_message_reaction
	
	FROM {{ source('raw', 'call_key_message') }} AS kmv
	LEFT OUTER JOIN {{ source('raw', 'account') }} AS acc ON acc.id = kmv.account_vod__c
	LEFT OUTER JOIN {{ source('raw', 'rgb') }} as rgb ON kmv.reaction_vod__c = rgb.field_value
	LEFT OUTER JOIN AUX_MAP_EMEAPL_TRPL ON
	'PicklistValue.Call2_Key_Message_vod__C.Reaction_vod.EN' || kmv.Reaction_vod__c = AUX_MAP_EMEAPL_TRPL.sys_map_emea_picklist
)
					
SELECT DISTINCT
	F_CALL_KEY_MESSAGE.edetail_presentation_id::varchar(20),
	F_CALL_KEY_MESSAGE.key_message_id::varchar(20), 
	F_CALL_KEY_MESSAGE.key_message_clm::varchar(3),
	F_CALL_KEY_MESSAGE.key_call_message_link_id::varchar(255),
	F_CALL_KEY_MESSAGE.call_clickstream_id::varchar(255), 
	F_CALL_KEY_MESSAGE.call_message_duration, 
	F_CALL_KEY_MESSAGE.call_key_message_id::varchar(20),
	F_CALL_KEY_MESSAGE.call_message_country_code::varchar(2),
	CASE
		WHEN F_CALL_KEY_MESSAGE.Call_Message_Country_Code = jjc.jj_country_iso_code__c
		THEN jjc.jj_region__c
		ELSE ''
	END::varchar(255) AS call_message_region,
	CASE
		WHEN F_CALL_KEY_MESSAGE.Call_Message_Country_Code = jjc.jj_country_iso_code__c
		THEN jjc.name
		ELSE ''
	END::varchar(255) AS call_message_country,
	F_CALL_KEY_MESSAGE.call_message_reaction_colour,
	F_CALL_KEY_MESSAGE.call_message_reaction::varchar(255),
	M_KEY_MESSAGE.Key_Message_record_type::varchar(255),
	M_KEY_MESSAGE.Key_message_description::varchar(255),
	M_KEY_MESSAGE.key_message_mandatory_item::varchar(3),
	M_KEY_MESSAGE.key_message_status::varchar(8),
	M_KEY_MESSAGE.key_message,
	M_KEY_MESSAGE.key_message_emea_key_message,
	M_KEY_MESSAGE.EMEA_key_message_status::varchar(8)

FROM F_CALL_KEY_MESSAGE
LEFT OUTER JOIN {{ source('raw', 'country_settings') }} AS jjc ON F_CALL_KEY_MESSAGE.Call_Message_Country_Code = jjc.jj_country_iso_code__c
LEFT OUTER JOIN (
	SELECT
		CASE WHEN km.recordtypeId=rt.id THEN rt.Name ELSE 'NM' END as key_message_record_type,
		km.DESCRIPTION_VOD__C as Key_Message_Description,
		CASE WHEN km.MANDATORY_DELIVERY_AT_CALL__C = 1 THEN 'Yes' ELSE 'No' END as key_message_mandatory_item, 
		km.JJ_EMEA_Key_Message__c as Aux_Key_Message_EMEA_Key_Message,
		CASE WHEN km.ACTIVE_VOD__C = 1 THEN 'Active' ELSE 'Inactive' END as key_message_status,  
		km.NAME as key_message,
		km.JJ_EMEA_KEY_MESSAGE__C as key_message_emea_key_message,
		km.product_vod__c || '_' || km.id as aux_key_message_id,		
		CASE WHEN km.active_vod__c IN ('True', '1') AND km.JJ_EMEA_Key_Message__c = km.id
			THEN 'Active'
			ELSE 'Inactive'
		END as EMEA_key_message_status
		FROM {{ source('raw', 'key_message') }} km
		LEFT OUTER JOIN {{ source('raw', 'record_type') }} rt ON rt.ID = km.RECORDTYPEID 
		LEFT OUTER JOIN {{ source('raw', 'country_settings') }} cs ON cs.JJ_COUNTRY_ISO_CODE__C = km.COUNTRY_ISO_CODE 
		LEFT OUTER JOIN {{ ref('m_product') }} AS M_PRODUCT ON M_PRODUCT.product_id = product_vod__c
) AS M_KEY_MESSAGE ON F_CALL_KEY_MESSAGE.aux_key_message_id = M_KEY_MESSAGE.aux_key_message_id
