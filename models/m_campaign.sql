{{ config(
	post_hook="grant all privileges on {{ this }} to {{ var('snowflake_user_grant_privileges') }}")
}}

SELECT Campaign_Id, Employee_Id, Product_Id, Therapeutic_Area_Name, Therapeutic_Area_Brand_Name, Territory_Nominal_Id,
	Date, Campaign_Target_Id, Account_Id, Campaign_Response_Id, Country_Code, Brick_Id, Security_Key, Origin, Activity, Activity_Type,	Assignment_Id, 
	Account_SCD_Id, Product_Metrics_SCD_Id, Multichannel_Type, Multichannel_Activity, Multichannel_Activity_Type, Country, Region, Product_Analytic_Group_Id, Product_Target_Class_Key, Campaign_Email_Status, Activity_Date, 
	Campaign_ClickThrough_Id, Campaign_Open_Id, Campaign_Email_Send_Counter, Campaign_Email_Bounced_Counter, Campaign_Pageview_Url, Campaign_WebSite_Url, Campaign_Form_Name, 
	Campaign_Pageview_Counter, Campaign_Form_Counter, Campaign_Pageview_Duration, Campaign_Email_Link

FROM (SELECT
	TMP.campaign_id as Campaign_Id,
	TMP.Employee_Id as Employee_Id,
	TMP.Product_Id as Product_Id,
	TMP.Therapeutic_Area_Name as Therapeutic_Area_Name,
	TMP.Therapeutic_Area_Brand_Name as Therapeutic_Area_Brand_Name,
	TMP.Territory_Nominal_Id as Territory_Nominal_Id,
	TMP.campaign_date as Date,
	F_CAMPAIGN_TARGET.Campaign_Target_Id AS CAMPAIGN_TARGET_ID,
	F_CAMPAIGN_TARGET.Account_Id as Account_Id,
	F_CAMPAIGN_RESPONSE.Campaign_Response_Id as Campaign_Response_Id,
	(CASE
		WHEN F_CAMPAIGN_TARGET.Account_Id = ACC.id THEN ACC.country_iso_code 
		WHEN TMP.Employee_Id = USR.id THEN USR.country_iso_code
		ELSE 'NM'
	END)::varchar(255) as Country_Code,
	('')::varchar(255) as Brick_Id,
	(CASE
		WHEN LEN(F_CAMPAIGN_TARGET.Account_Id) > 0 THEN F_CAMPAIGN_TARGET.Account_Id||'-'||TMP.Employee_Id ELSE TMP.Employee_Id||'-'||TMP.Employee_Id
	END)::varchar(255) as Security_Key,
	('Campaign')::varchar(255) as Origin,
	(CASE
		WHEN LOWER(Campaign.jj_status__c) IN ('active', 'inactive') THEN 'Campaign' ELSE NULL
	END)::varchar(255) as Activity,
	(CASE
		WHEN LOWER(Campaign.jj_status__c) IN ('active', 'inactive') THEN TMP.campaign_record_type ELSE NULL
	END)::varchar(255) as Activity_Type,
	(CASE WHEN LEN(F_CAMPAIGN_TARGET.Account_Id)>0 THEN F_CAMPAIGN_TARGET.Account_Id ELSE TMP.Employee_Id END)::varchar(255) as Assignment_Id,
	(COALESCE(F_CAMPAIGN_TARGET.Account_Id,'')||'-'||
		DATEDIFF(day, to_date('1899-12-30','YYYY-MM-DD'),CASE WHEN Date = '' THEN NULL ELSE TO_DATE(Date,'DD-MM-YYYY') END)::varchar(255))::varchar(255) as Account_SCD_Id,
	(COALESCE(F_CAMPAIGN_TARGET.Account_Id,'')||'-'||TMP.Product_Id||'-'||
		DATEDIFF(day, to_date('1899-12-30','YYYY-MM-DD'),CASE WHEN Date = '' THEN NULL ELSE TO_DATE(Date,'DD-MM-YYYY') END)::varchar(255))::varchar(255) as Product_Metrics_SCD_Id,
	(CASE
		WHEN (TMP.campaign_record_type='Phone' AND F_CAMPAIGN_TARGET.Call_Stat='Completed') OR
			 (TMP.campaign_record_type='Traditional Mail' AND (Campaign.jj_status__c='Inactive' OR Campaign.jj_status__c='Active') AND
			 LEN(F_CAMPAIGN_TARGET.Campaign_Target_Id)>0) OR
			 (TMP.campaign_record_type='Fax' AND (Campaign.jj_status__c='Inactive' OR Campaign.jj_status__c='Active') AND
			 LEN(F_CAMPAIGN_TARGET.Campaign_Target_Id)>0) THEN 'Non Face to Face Contacts' ELSE NULL
	END)::varchar(255) as Multichannel_Type,
	(CASE
		WHEN (TMP.campaign_record_type='Phone' AND F_CAMPAIGN_TARGET.Call_Stat='Completed') THEN 'Phone Campaigns'
		WHEN (TMP.campaign_record_type='Traditional Mail' AND (Campaign.jj_status__c='Inactive' OR Campaign.jj_status__c='Active') AND
			 LEN(F_CAMPAIGN_TARGET.Campaign_Target_Id)>0) THEN 'Traditional Mail Campaigns'
		WHEN (TMP.campaign_record_type='Fax' AND (Campaign.jj_status__c='Inactive' OR Campaign.jj_status__c='Active') AND
			 LEN(F_CAMPAIGN_TARGET.Campaign_Target_Id)>0) THEN 'Fax Campaigns'
		ELSE NULL
	END)::varchar(255) as Multichannel_Activity,
	NULL::varchar(255) as Multichannel_Activity_Type,
	(CASE
		WHEN (CASE WHEN F_CAMPAIGN_TARGET.Account_Id = ACC.id THEN ACC.country_iso_code WHEN TMP.Employee_Id = USR.id THEN USR.country_iso_code ELSE 'NM' END) = COUNTRY.jj_Country_ISO_Code__C
		THEN COUNTRY.Name ELSE 'NM'
	END)::varchar(255) as Country,
	(CASE
		WHEN (CASE WHEN F_CAMPAIGN_TARGET.Account_Id = ACC.id THEN ACC.country_iso_code WHEN TMP.Employee_Id = USR.id THEN USR.country_iso_code ELSE 'NM' END) = COUNTRY.jj_Country_ISO_Code__C
		THEN COUNTRY.jj_Region__c ELSE 'NM'
	END)::varchar(255) as Region,
	PRODUCT_GROUP_MAP.Analytics_Product_Group_vod__c::varchar(255) as Product_Analytic_Group_Id,
	(CASE
		WHEN (PRODUCT_GROUP_MAP.Product_vod__c = TMP.Product_Id) 
		THEN (COALESCE(F_CAMPAIGN_TARGET.Account_Id,'')||'-'||COALESCE(PRODUCT_GROUP_MAP.Analytics_Product_Group_vod__c,'')||'-'||(CASE WHEN M_PRODUCT.Product_Id = Campaign.jj_product__c THEN M_PRODUCT.product_country_code ELSE 'NM' END)::varchar(255))
		ELSE (COALESCE(F_CAMPAIGN_TARGET.Account_Id,'')||'-'||(CASE WHEN M_PRODUCT.Product_Id = TMP.Product_Id THEN M_PRODUCT.product_detail_id ELSE 'NM' END)::varchar(255))
	END)::varchar(255) as Product_Target_Class_Key,
	
	NULL as Campaign_Email_Status,
	NULL as Activity_Date,
	NULL as Campaign_ClickThrough_Id,
	NULL as Campaign_Open_Id,
	NULL as Campaign_Email_Send_Counter,
	NULL as Campaign_Email_Bounced_Counter,
	NULL as Campaign_Pageview_Url,
	NULL as Campaign_WebSite_Url,
	NULL as Campaign_Form_Name,
	NULL as Campaign_Pageview_Counter,
	NULL as Campaign_Form_Counter,
	NULL as Campaign_Pageview_Duration,
	NULL as Campaign_Email_Link

FROM {{ ref('tmp_f_campaign') }} AS TMP
LEFT OUTER JOIN {{ var('schema') }}.campaign_raw AS Campaign ON TMP.campaign_id = Campaign.id
LEFT OUTER JOIN {{ var('schema') }}.product_raw AS Product ON Campaign.jj_product__c = Product.id 
LEFT OUTER JOIN {{ var('schema') }}.product_group_map_raw AS PRODUCT_GROUP_MAP ON Campaign.jj_product__c = PRODUCT_GROUP_MAP.Product_vod__c	
LEFT OUTER JOIN {{ var('schema') }}.user_raw USR ON Campaign.ownerid = USR.id
LEFT OUTER JOIN {{ ref('m_product') }} AS M_PRODUCT ON M_PRODUCT.Product_Id = Campaign.jj_product__c

LEFT OUTER JOIN (
	SELECT 
		Target.campaign_vod__c as Campaign_Target_Id,
		Target.target_account_vod__c as Account_Id, 
		Target.JJ_Call_Status__c as Call_Stat		
	FROM {{ var('schema') }}.campaign_target_raw AS Target
	LEFT OUTER JOIN {{ var('schema') }}.account_raw AS Acc ON Acc.Id = Target.target_account_vod__c
	LEFT OUTER JOIN {{ var('schema') }}.country_settings_raw AS CS ON CS.jj_country_ISO_Code__c = (CASE WHEN Acc.Id = Target.target_account_vod__c THEN ACC.country_iso_code ELSE 'NM' END)
     LEFT OUTER JOIN {{ var('schema') }}.campaign_raw AS Campaign ON Campaign.Id = Target.campaign_vod__c
	LEFT OUTER JOIN {{ var('schema') }}.record_type_raw AS RT ON Campaign.recordtypeid = RT.id
	WHERE LOWER(RT.name) NOT IN ('customer journey', 'email', 'sms')
	) AS F_CAMPAIGN_TARGET ON TMP.Campaign_Id = F_CAMPAIGN_TARGET.Campaign_Target_Id
		
LEFT OUTER JOIN {{ var('schema') }}.account_raw AS ACC ON ACC.id = F_CAMPAIGN_TARGET.Account_Id

LEFT OUTER JOIN (
	SELECT Ass.id as Campaign_Response_Id, Ass.jj_Campaign_Target__c as CAMPAIGN_TARGET_ID
	FROM {{ var('schema') }}.assessment_raw AS Ass
	LEFT OUTER JOIN {{ var('schema') }}.account_raw AS Acc ON Ass.jj_Campaign_Target__c = Acc.Id
	LEFT OUTER JOIN {{ var('schema') }}.country_settings_raw AS CS ON CS.jj_country_ISO_Code__c = (CASE WHEN Ass.jj_campaign_target__c = Acc.Id THEN Acc.country_iso_code ELSE 'NM' END)
	) AS F_CAMPAIGN_RESPONSE ON F_CAMPAIGN_TARGET.Campaign_Target_Id = F_CAMPAIGN_RESPONSE.Campaign_Target_Id

LEFT OUTER JOIN {{ var('schema') }}.country_settings_raw AS COUNTRY ON 
	CASE
		WHEN F_CAMPAIGN_TARGET.Account_Id = ACC.id THEN ACC.country_iso_code
		WHEN Campaign.ownerid = USR.id THEN USR.country_iso_code
		ELSE 'NM'
	END = COUNTRY.jj_Country_ISO_Code__C

UNION

SELECT
	F_ACTIVITY_EMAIL_ELOQUA.Campaign_Id::varchar(255) as Campaign_Id,
	(CASE WHEN F_ACTIVITY_EMAIL_ELOQUA.Campaign_Id = TMP.Campaign_Id THEN TMP.Employee_Id ELSE 'NM' END)::varchar(255) as Employee_Id,
	(CASE WHEN F_ACTIVITY_EMAIL_ELOQUA.Campaign_Id = TMP.Campaign_Id THEN TMP.Product_Id ELSE 'NM' END)::varchar(255) as Product_Id,
	(CASE 
		WHEN (CASE WHEN F_ACTIVITY_EMAIL_ELOQUA.Campaign_Id = TMP.Campaign_Id THEN TMP.Product_Id ELSE 'NM' END) = P.ID 
			THEN CASE 
					WHEN LEN(P.Therapeutic_Area_vod__c)>0
						THEN P.Therapeutic_Area_vod__c 
						ELSE 'No Value'
					END
			ELSE 'NM'
	END)::varchar(255) as Therapeutic_Area_Name,
	(CASE 
		WHEN LOWER(P.product_type_vod__c) = 'brand' 
			THEN Therapeutic_Area_Name
			ELSE 'NM'
	END)::varchar(255) as Therapeutic_Area_Brand_Name,
	(CASE WHEN F_ACTIVITY_EMAIL_ELOQUA.Campaign_Id = TMP.Campaign_Id 
		THEN TMP.Territory_Nominal_Id
	END)::varchar(255) as Territory_Nominal_Id,
	F_ACTIVITY_EMAIL_ELOQUA.Date::varchar(255) as Date,
	NULL AS CAMPAIGN_TARGET_ID,
	F_ACTIVITY_EMAIL_ELOQUA.Account_Id::varchar(255) as Account_Id,
	NULL as Campaign_Response_Id,
	(CASE WHEN (F_ACTIVITY_EMAIL_ELOQUA.Campaign_Email_Status = 'Bounceback') 
		THEN (CASE WHEN F_ACTIVITY_EMAIL_ELOQUA.Campaign_Id = TMP.Campaign_id THEN TMP.Campaign_Country_Code ELSE 'NM' END) 
		ELSE (CASE WHEN (F_ACTIVITY_EMAIL_ELOQUA.Account_Id = ACCOUNT.Id AND LEN(ACCOUNT.country_iso_code)>0) THEN ACCOUNT.country_iso_code
				   WHEN F_ACTIVITY_EMAIL_ELOQUA.Account_Id = ACCOUNT.Id THEN ACCOUNT.country_iso_code ELSE 'NM' END) 
	END)::varchar(255) as Country_Code,
	NULL as Brick_Id,
	F_ACTIVITY_EMAIL_ELOQUA.Account_Id::varchar(255) as Security_Key,
	F_ACTIVITY_EMAIL_ELOQUA.Origin::varchar(255) as Origin,
	(CASE WHEN TMP.campaign_record_type IN ('SMS','Email','Customer Journey') THEN 'Campaign'
		WHEN TMP.Campaign_Status IN ('Active','Inactive') THEN 'Campaign' ELSE NULL 
	END)::varchar(255) as Activity,
	(CASE WHEN TMP.campaign_record_type IN ('SMS','Email','Customer Journey') 
			THEN (CASE WHEN F_ACTIVITY_EMAIL_ELOQUA.Campaign_Id = TMP.Campaign_Id THEN TMP.campaign_record_type ELSE 'NM' END)
		WHEN TMP.Campaign_Status IN ('Active', 'Inactive') THEN (CASE WHEN F_ACTIVITY_EMAIL_ELOQUA.Campaign_Id = TMP.Campaign_Id THEN TMP.campaign_record_type ELSE 'NM' END) 
		ELSE NULL 
	END)::varchar(255) as Activity_Type,
	F_ACTIVITY_EMAIL_ELOQUA.Account_Id::varchar(255) as Assignment_Id,
	NULL as Account_SCD_Id,
	NULL as Product_Metrics_Id,
	(CASE 
		WHEN (TMP.campaign_record_type = 'SMS' AND LEN(F_ACTIVITY_EMAIL_ELOQUA.Campaign_Email_Send_Counter)>0) 
			OR (TMP.campaign_record_type IN ('Email','Customer Journey') AND F_ACTIVITY_EMAIL_ELOQUA.Campaign_Email_Open_Counter = 1) 
		THEN 'Non Face to Face Contacts' ELSE NULL 
	END)::varchar(255) as Multichannel_Type,
	(CASE 
		WHEN (TMP.campaign_record_type = 'SMS' AND LEN(F_ACTIVITY_EMAIL_ELOQUA.Campaign_Email_Send_Counter)>0) THEN 'SMS Campaigns'
		WHEN (TMP.campaign_record_type IN ('Email','Customer Journey') AND F_ACTIVITY_EMAIL_ELOQUA.Campaign_Email_Open_Counter = 1) THEN 'Mass Email Campaigns' 
		ELSE NULL 
	END)::varchar(255) as Multichannel_Activity,
	(CASE WHEN TMP.campaign_record_type IN ('Email','Customer Journey') AND F_ACTIVITY_EMAIL_ELOQUA.Campaign_Email_Open_Counter = 1 
		THEN 'Email Campaign Sent by Janssen' ELSE NULL 
	END)::varchar(255) as Multichannel_Activity_Type,
	(CASE WHEN F_ACTIVITY_EMAIL_ELOQUA.Campaign_Email_Status = 'Bounceback' 
		THEN (CASE WHEN F_ACTIVITY_EMAIL_ELOQUA.Campaign_Id = TMP.Campaign_Id THEN TMP.Campaign_Country ELSE 'NM' END) 
		ELSE (CASE WHEN F_ACTIVITY_EMAIL_ELOQUA.Account_Id = ACCOUNT.Id THEN CS.Name ELSE 'NM' END) 
	END)::varchar(255) as Country,
	(CASE WHEN F_ACTIVITY_EMAIL_ELOQUA.Campaign_Email_Status = 'Bounceback' 
		THEN (CASE WHEN F_ACTIVITY_EMAIL_ELOQUA.Campaign_Id = TMP.Campaign_Id THEN TMP.Campaign_Region ELSE 'NM' END) 
		ELSE (CASE WHEN F_ACTIVITY_EMAIL_ELOQUA.Account_Id = ACCOUNT.Id THEN CS.jj_Region__c ELSE 'NM' END) 
	END)::varchar(255) as Region,
	P_GROUP_MAP.Analytics_Product_Group_vod__c::varchar(255) as Product_Analytic_Group_Id,
	(CASE WHEN P_GROUP_MAP.Product_vod__c = (CASE WHEN F_ACTIVITY_EMAIL_ELOQUA.Campaign_Id = TMP.Campaign_Id THEN TMP.Product_Id ELSE 'NM' END)
		THEN COALESCE(F_ACTIVITY_EMAIL_ELOQUA.Account_Id,'') ||'-'|| COALESCE(P_GROUP_MAP.Analytics_Product_Group_vod__c,'') ||'-'|| (
			CASE WHEN M_PRODUCT.Product_Id = (CASE WHEN F_ACTIVITY_EMAIL_ELOQUA.Campaign_Id = TMP.Campaign_Id THEN TMP.Product_Id ELSE 'NM' END) 
				THEN M_PRODUCT.product_country_code ELSE 'NM' END)::varchar(255) 
		ELSE COALESCE(F_ACTIVITY_EMAIL_ELOQUA.Account_Id,'') ||'-'|| (CASE WHEN M_PRODUCT.Product_Id = (CASE WHEN F_ACTIVITY_EMAIL_ELOQUA.Campaign_Id = TMP.Campaign_Id THEN TMP.Product_Id ELSE 'NM' END) 
				THEN M_PRODUCT.product_detail_id ELSE 'NM' END)::varchar(255) 
	END)::varchar(255) as Product_Target_Class_Key,
				
	F_ACTIVITY_EMAIL_ELOQUA.Campaign_Email_Status::varchar(255) as Campaign_Email_Status,
	F_ACTIVITY_EMAIL_ELOQUA.Activity_Date::varchar(255) as Activity_Date,
	F_ACTIVITY_EMAIL_ELOQUA.Campaign_ClickThrough_Id::varchar(255) as Campaign_ClickThrough_Id,
	F_ACTIVITY_EMAIL_ELOQUA.Campaign_Open_Id::varchar(255) as Campaign_Open_Id,
	F_ACTIVITY_EMAIL_ELOQUA.Campaign_Email_Send_Counter::numeric(10, 0) as Campaign_Email_Send_Counter,
	F_ACTIVITY_EMAIL_ELOQUA.Campaign_Email_Bounced_Counter::numeric(10, 0) as Campaign_Email_Bounced_Counter,
	F_ACTIVITY_EMAIL_ELOQUA.Campaign_Pageview_Url::varchar(1000) as Campaign_Pageview_Url,
	F_ACTIVITY_EMAIL_ELOQUA.Campaign_WebSite_Url::varchar(1000) as Campaign_WebSite_Url,
	F_ACTIVITY_EMAIL_ELOQUA.Campaign_Form_Name::varchar(255) as Campaign_Form_Name,
	F_ACTIVITY_EMAIL_ELOQUA.Campaign_pageview_counter::numeric(10, 0) as Campaign_Pageview_Counter,
	F_ACTIVITY_EMAIL_ELOQUA.Campaign_form_counter::numeric(10, 0) as Campaign_Form_Counter,
	F_ACTIVITY_EMAIL_ELOQUA.Campaign_Pageview_Duration::numeric(10, 2) as Campaign_Pageview_Duration,
	F_ACTIVITY_EMAIL_ELOQUA.Campaign_Email_Link::varchar(1000) as Campaign_Email_Link
	
FROM (
	SELECT
		AUX_ELOQUA.campaign_email_status as campaign_email_status,
		CASE
			WHEN AUX_ELOQUA.campaign_id = campaign.id
			THEN campaign.JJ_Start_date__c
			ELSE NULL
		END::varchar(255) as Date,
		AUX_ELOQUA.Date as Activity_Date,
		AUX_ELOQUA.campaign_id as Campaign_id,
		COALESCE(AUX_ELOQUA.campaign_id,'') || '-' || COALESCE(AUX_ELOQUA.account_id,'') || '-' || COALESCE(AUX_ELOQUA.assetId,'') as Campaign_Clickthrough_id,
		COALESCE(AUX_ELOQUA.campaign_id,'') || '-' || COALESCE(AUX_ELOQUA.account_id,'') || '-' || COALESCE(AUX_ELOQUA.campaign_email_name,'') as Campaign_Open_id,
		AUX_ELOQUA.account_id as Account_Id,
		AUX_ELOQUA.Origin as origin,
		AUX_ELOQUA.Campaign_Pageview_url as Campaign_Pageview_url,
		AUX_ELOQUA.campaign_website_url as Campaign_website_url,
		AUX_ELOQUA.campaign_form_name as Campaign_form_name,
		AUX_ELOQUA.campaign_email_name as Campaign_email_name,
		AUX_ELOQUA.EmailClickedThroughLink as Campaign_email_link,
		SUM(COALESCE(AUX_ELOQUA.Campaign_email_send_counter::numeric(10,2),0))::numeric(10,2) as Campaign_Email_send_counter,
		MAX(COALESCE(AUX_ELOQUA.campaign_email_open_counter::numeric(10,2),0))::numeric(10,2) as Campaign_email_open_counter,
		MAX(COALESCE(AUX_ELOQUA.campaign_email_bounced_counter::numeric(10,2),0))::numeric(10,2) as Campaign_email_Bounced_counter,
		SUM(COALESCE(AUX_ELOQUA.campaign_pageview_counter::numeric(10,2),0))::numeric(10,2) as Campaign_pageview_counter,
		SUM(COALESCE(AUX_ELOQUA.Campaign_form_counter::numeric(10,2),0))::numeric(10,2) as Campaign_form_counter,
		AVG(COALESCE(AUX_ELOQUA.Campaign_pageview_duration::numeric(10,2),0))::numeric(10,2) as Campaign_pageview_duration

	FROM {{ ref('tmp_f_activity_email_eloqua') }} AS AUX_ELOQUA
	LEFT OUTER JOIN {{ var('schema') }}.campaign_raw AS campaign ON campaign.Id = aux_eloqua.campaign_id
	WHERE aux_eloqua.account_id IS NOT NULL
	GROUP BY aux_eloqua.campaign_email_status,aux_eloqua.campaign_id,campaign.id,campaign.jj_start_date__c,aux_eloqua.date,
		aux_eloqua.account_id,aux_eloqua.assetid,aux_eloqua.campaign_email_name,aux_eloqua.origin,aux_eloqua.campaign_pageview_url,
		aux_eloqua.campaign_website_url,aux_eloqua.campaign_form_name,aux_eloqua.emailclickedthroughlink
	) AS F_ACTIVITY_EMAIL_ELOQUA

LEFT OUTER JOIN {{ ref('tmp_f_campaign') }} AS TMP ON F_ACTIVITY_EMAIL_ELOQUA.Campaign_Id = TMP.Campaign_Id
LEFT OUTER JOIN {{ var('schema') }}.account_raw AS ACCOUNT ON F_ACTIVITY_EMAIL_ELOQUA.Account_Id = Account.Id
LEFT OUTER JOIN {{ var('schema') }}.country_settings_raw AS CS ON TMP.Campaign_Country_Code = CS.jj_Country_ISO_Code__C
LEFT OUTER JOIN {{ var('schema') }}.product_raw AS P ON (CASE WHEN F_ACTIVITY_EMAIL_ELOQUA.Campaign_Id = TMP.Campaign_Id THEN TMP.Product_Id ELSE 'NM' END) = P.Id
LEFT OUTER JOIN {{ var('schema') }}.product_group_map_raw AS P_GROUP_MAP ON (CASE WHEN F_ACTIVITY_EMAIL_ELOQUA.Campaign_Id = TMP.Campaign_Id THEN TMP.Product_Id ELSE 'NM' END) = P_GROUP_MAP.Product_vod__c
LEFT OUTER JOIN {{ ref('m_product') }} AS M_PRODUCT ON (CASE WHEN F_ACTIVITY_EMAIL_ELOQUA.Campaign_Id = TMP.Campaign_Id THEN TMP.Product_Id ELSE 'NM' END) = M_PRODUCT.Product_id)