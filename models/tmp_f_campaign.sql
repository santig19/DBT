{{ config(
	post_hook="grant all privileges on {{ this }} to {{ var('snowflake_user_grant_privileges') }}")
}}

SELECT 
	--TO_CHAR(TO_DATE(c.JJ_Start_Date__c, 'YYYYMMDD HH24:MI:SS'), 'DD-MM-YYYY') as Date,
	CASE WHEN TRIM(c.JJ_Start_Date__c) = '' 
		THEN ''
		ELSE TO_CHAR(TO_DATE(c.JJ_Start_Date__c, 'YYYYMMDD HH24:MI:SS'), 'DD-MM-YYYY')
	END as Date,
	c.JJ_Start_Date__c as Campaign_Date,
	--TO_CHAR(TO_DATE(c.JJ_Start_Date__c, 'YYYYMMDD HH24:MI:SS'), 'YYYYMM') as Order_Campaign_MonthYear,
	REPLACE(SUBSTRING(Date, 4), '-', '') as Order_Campaign_MonthYear,
	TO_CHAR(TO_DATE(c.LastModifiedDate, 'YYYYMMDD HH24:MI:SS'), 'DD-MM-YYYY') as Campaign_Last_Modified_Date, 
	c.Description_vod__c as Campaign_Description, 
	c.Id as Campaign_Id, 
	LEFT(c.Id,15) as Campaign_Id_15_Digits,
	CASE 
		WHEN c.JJ_Campaign_approved_in_UNITAS__c IN ('True','true','1') THEN 'Yes'
		WHEN c.JJ_Campaign_approved_in_UNITAS__c IN ('False','false','0') THEN 'No'
		ELSE 'No Value'
	END as Campaign_Approved_UNITAS, 
	c.JJ_Channel__c AS Campaign_Channel,
	c.JJ_Country__c as Campaign_Country_Location,
	c.Emails_Sent__c as Campaign_Emails_Sent,	
    CASE WHEN c.JJ_End_Date__c = '' THEN NULL ELSE TO_CHAR(TO_DATE(c.JJ_End_Date__c, 'YYYYMMDD HH24:MI:SS'), 'DD/MM/YYYY') END as Campaign_End_Date,
	c.jj_Product__c AS Product_Id,
	CASE WHEN LEN(mp.product_therapeutic_area_1)>0 THEN mp.product_therapeutic_area_1 ELSE 'NM' END as Therapeutic_Area_Name,
	CASE WHEN LEN(mp.product_brand_therapeutic_area_1_null)>0 THEN mp.product_brand_therapeutic_area_1_null ELSE 'NM' END as Therapeutic_Area_Brand_Name,
    CASE WHEN c.JJ_Start_Date__c = '' THEN NULL ELSE TO_CHAR(TO_DATE(c.JJ_Start_Date__c, 'YYYYMMDD HH24:MI:SS'), 'DD/MM/YYYY') END as Campaign_Start_Date,
	c.JJ_Status__c as Campaign_Status,
	c.JJ_UNITAS_Number__c as Campaign_UNITAS_Number,
	c.Name as Campaign_Name,
	c.ownerid AS Employee_Id,
	CASE WHEN c.ownerid = UT.USERID THEN UT.TERRITORYID ELSE 'NM' END AS Territory_Nominal_Id,
	c.country_iso_code as Campaign_Country_Code,
	CASE WHEN c.country_iso_code = CS.jj_country_ISO_CODE__c THEN CS.jj_Region__c ELSE 'NM' END as Campaign_Region,
	CASE WHEN c.country_iso_code = CS.jj_Country_ISO_Code__c THEN CS.name ELSE 'NM' END as Campaign_Country,
	c.jj_eloqua_Id__c as Campaign_Eloqua_Id,
	CASE WHEN c.recordtypeid = RT.Id THEN RT.Name ELSE 'NM' END as Campaign_Record_Type,
	1 as Campaign_Counter,
	CASE WHEN c.Id = Campaign_Step.JJ_campaign_name__c THEN Count(Campaign_Step.Id)::varchar(10) ELSE 'NM' END::varchar(10) AS Campaign_Steps_Number,
	c.JJ_CAMPAIGN_TYPE__C as campaign_type,
	
	c.JJ_Content_Topic__c 			AS	Content_Topic,
	c.JJ_Content_Sub_Topic__c 		AS	Content_Sub_Topic,
	c.JJ_Key_Message_Category__c 	AS 	Key_Message_Category,
	c.JJ_Key_Message_Sub_Group__c 	AS 	Key_Message_Sub_Group,
	c.JJ_Purpose__c 				AS	Purpose
	
FROM {{ var('schema') }}.campaign_raw AS c 
--LEFT OUTER JOIN emea_mto.product_vod__c_business AS Product ON c.jj_product__c = p.id
LEFT OUTER JOIN {{ var('schema') }}.user_raw U ON c.ownerid = U.id
LEFT OUTER JOIN {{ var('schema') }}.country_settings_raw AS CS ON c.country_iso_code = CS.jj_country_iso_code__c
LEFT OUTER JOIN {{ var('schema') }}.record_type_raw AS RT ON c.recordtypeid = RT.id
LEFT OUTER JOIN {{ var('schema') }}.campaign_step_raw AS Campaign_Step ON c.id = Campaign_Step.jj_campaign_name__c
LEFT OUTER JOIN {{ ref('tmp_user_territory') }} ut ON ut.USERID = c.ownerid 
LEFT OUTER JOIN (
	SELECT *, CASE WHEN product_brand_therapeutic_area_1 = 'No Value' THEN NULL ELSE product_brand_therapeutic_area_1 END as product_brand_therapeutic_area_1_null 
	FROM {{ ref('m_product') }} 
) mp ON mp.product_id = c.jj_product__c

GROUP BY c.jj_start_date__c, c.lastmodifieddate, c.description_vod__c, c.id, c.jj_campaign_approved_in_unitas__c, c.jj_country__c, 
	c.emails_sent__c, c.jj_end_date__c, c.jj_status__c, c.jj_unitas_number__c, c.name, c.country_iso_code, cs.jj_country_iso_code__c,
	cs.jj_region__c, cs.name, c.jj_eloqua_id__c, c.recordtypeid, rt.id, rt.name, campaign_step.jj_campaign_name__c, c.jj_channel__c, c.jj_product__c,
	c.ownerid, ut.userid, ut.territoryid, mp.product_therapeutic_area_1, mp.product_brand_therapeutic_area_1_null, c.JJ_CAMPAIGN_TYPE__C,c.JJ_Content_Topic__c ,c.JJ_Content_Sub_Topic__c, c.JJ_Key_Message_Category__c, c.JJ_Key_Message_Sub_Group__c, c.JJ_Purpose__c 