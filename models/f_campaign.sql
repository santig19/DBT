{{ config(
	post_hook="grant all privileges on {{ this }} to {{ var('snowflake_user_grant_privileges') }}")
}}

SELECT 
	TMP.Date as Campaign_Date, 
	TMP.Order_Campaign_MonthYear,
	TMP.Campaign_Last_Modified_Date,
	TMP.Campaign_Description,
	TMP.Campaign_Id,
	TMP.Campaign_Id_15_Digits,
	TMP.Campaign_Approved_UNITAS,
	TMP.Campaign_Country_Location,
	TMP.Campaign_Emails_Sent,
	TMP.Campaign_End_Date,
	TMP.Campaign_Start_Date,
	TMP.Campaign_Status,
	TMP.Campaign_UNITAS_Number,
	TMP.Campaign_Name,
	TMP.Campaign_Country_Code,
	TMP.Campaign_Region,
	TMP.Campaign_Country,
	TMP.Campaign_Eloqua_Id,
	TMP.Campaign_Record_Type,
	TMP.Campaign_Counter,
	TMP.Campaign_Steps_Number,
	TMP.Content_Topic::varchar(510) as Content_Topic,
	TMP.Content_Sub_Topic::varchar(510) as Content_Sub_Topic,
	TMP.Key_Message_Category::varchar(16777216) as Key_Message_Category,
	TMP.Key_Message_Sub_Group::varchar(510) as Key_Message_Sub_Group,
	TMP.Purpose::varchar(3000)as Purpose,
	sha2('campaign' || TMP.campaign_name, 256) as eevent_key
FROM {{ ref('tmp_f_campaign') }} AS TMP
