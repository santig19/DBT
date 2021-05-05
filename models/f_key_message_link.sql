{{ config(
	post_hook="grant all privileges on {{ this }} to {{ var('snowflake_user_grant_privileges') }}")
}}

SELECT
FC.Key_Call_Message_id 	as key_call_message_id,
FC.Country_Code 		as Key_Call_Message_Link_Country_Code,
FC.Country 			as Key_Call_Message_Link_Country,
FC.Region 			as Key_Call_Message_Link_Region,
FC.Key_Call_Message_id 	as key_call_message_link_id

FROM {{ ref('f_call') }} AS FC
JOIN {{ var('schema') }}.call_key_message_raw CKM 
	on FC.Key_Call_Message_id = CKM.Account_vod__c || '_' || CKM.Product_vod__c || '_' || CKM.Call2_vod__c

UNION

SELECT
FC.Key_Call_Message_id 	as key_call_message_id,
FC.Country_Code 		as Key_Call_Message_Link_Country_Code,
FC.Country 			as Key_Call_Message_Link_Country,
FC.Region 			as Key_Call_Message_Link_Region,
FC.Key_Call_Message_General_Comment as key_call_message_link_id

FROM {{ ref('f_call') }} AS FC
JOIN {{ var('schema') }}.call_key_message_raw CKM 
	on FC.Key_Call_Message_General_Comment = CKM.Account_vod__c || '_' || CKM.Product_vod__c || '_' || CKM.Call2_vod__c
