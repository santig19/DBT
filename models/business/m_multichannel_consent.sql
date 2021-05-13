{{ config(
	post_hook="grant all privileges on {{ this }} to {{ var('snowflake_user_grant_privileges') }}")
}}

SELECT 
     NVL(mc.id,'')                          			AS id
    ,NVL(mc.account_vod__c,'')              			AS account
    ,NVL(mc.opt_type_vod__c,'')             			AS opt_type
    ,NVL(mc.opt_expiration_date_vod__c,'')  			AS opt_expiration_date
    ,NVL(mc.jj_channel_label__c,'')         			AS channel_label
    ,NVL(mc.createddate ,'')                			AS created_date
    ,NULLIF(Capture_datetime_vod__c, '')::DATETIME 		AS Capture_datetime
    ,CASE WHEN (Position(CHR(92) || CHR(92), Channel_value_vod__c) > 0) OR Position(CHR(124) || CHR(34), Channel_value_vod__c) > 0 OR Position(CHR(92) || CHR(124), Channel_value_vod__c) > 0 OR Position(CHR(92) || CHR(34), Channel_value_vod__c) > 0
		THEN REPLACE(REPLACE(REPLACE(REPLACE(Channel_value_vod__c, CHR(92) || CHR(92), CHR(92)), CHR(124) || CHR(34), CHR(34)), CHR(92) || CHR(124), CHR(124)), CHR(92) || CHR(34), CHR(34))
		ELSE Channel_value_vod__c 
	 END 												AS Channel
    ,jj_RecordType_DevName__c							AS RecordType
	,NULLIF(LastModifiedDate, '')::DATETIME 			AS LastModifiedDate
    ,JJ_Primary_Additional__c							AS Primary_Additional
FROM 
    {{ source('raw', 'multichannel_consent') }} AS mc
WHERE 
    NVL(mc.isdeleted,'') = '0'
GROUP BY 
    NVL(mc.id,'')
   ,NVL(mc.account_vod__c,'')
   ,NVL(mc.opt_type_vod__c,'')
   ,NVL(mc.opt_expiration_date_vod__c,'')
   ,NVL(mc.jj_channel_label__c,'')
   ,NVL(mc.createddate ,'') 
   ,Capture_datetime_vod__c
   ,Channel_value_vod__c
   ,jj_RecordType_DevName__c
   ,LastModifiedDate     
   ,JJ_Primary_Additional__c