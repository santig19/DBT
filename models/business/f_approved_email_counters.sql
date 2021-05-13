{{ config(
	post_hook="grant all privileges on {{ this }} to {{ var('snowflake_user_grant_privileges') }}")
}}

SELECT
	SENT_EMAIL.id::varchar(20) AS Approved_Email_Counter_Id,

	CASE 
		WHEN lower(SENT_EMAIL.status_vod__c) IN ('delivered_vod','marked_spam_vod') 
		THEN 1 
		ELSE NULL 
	END::integer AS Approved_Email_Sent_Counter,
	CASE 
		WHEN lower(SENT_EMAIL.status_vod__c) IN ('delivered_vod','marked_spam_vod')
		THEN REGEXP_SUBSTR(trim(SENT_EMAIL.open_count_vod__c),'^[0-9]+') 
		ELSE NULL 
	END::integer AS Approved_Email_Total_Opened_Counter,
	CASE 
		WHEN lower(SENT_EMAIL.status_vod__c) IN ('delivered_vod','marked_spam_vod') AND SENT_EMAIL.opened_vod__c='1' 
		THEN 1 
		ELSE NULL 
	END::integer AS Approved_Email_Opened_Counter,
	CASE 
		WHEN lower(SENT_EMAIL.status_vod__c) IN ('delivered_vod','marked_spam_vod') 
		THEN REGEXP_SUBSTR(trim(SENT_EMAIL.click_count_vod__c),'^[0-9]+') 
		ELSE NULL
 	END::integer AS Approved_Email_Total_ClickThrough_Counter,
	CASE 
		WHEN lower(SENT_EMAIL.status_vod__c) IN ('delivered_vod','marked_spam_vod') AND SENT_EMAIL.clicked_vod__c='1' 
		THEN 1 
		ELSE NULL 
	END::integer AS Approved_Email_ClickThrough_Counter,

	CS.name AS Approved_Email_Counters_Country,
	CS.jj_region__c AS Approved_Email_Counters_Region
	
FROM {{ source('raw', 'sent_email') }} AS SENT_EMAIL
LEFT OUTER JOIN {{ source('raw', 'country_settings') }} AS CS ON CS.country_iso_code = SENT_EMAIL.country_iso_code
WHERE SENT_EMAIL.originating_from_1_to_1_email__c IN ('0', 'false')
