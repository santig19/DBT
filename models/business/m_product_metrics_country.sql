{{ config(
	post_hook="grant all privileges on {{ this }} to {{ var('snowflake_user_grant_privileges') }}")
}}

with AUX_TABLE as (
SELECT
	 pm.account_vod__c as Account_Id
	,pm.products_vod__c as Product_Id
	,pm.country_iso_code as Country_Code
	,(CASE 
		WHEN COALESCE(mpag.product_analytic_group_id, '') != ''
			THEN pm.account_vod__c || '-' || mpag.product_analytic_group_id || '-' ||
				CASE WHEN mp2.product_id = pm.products_vod__c THEN mp2.product_country_code ELSE 'NM' END
		WHEN COALESCE(mp2.product_detail_id, '') != ''
			THEN pm.account_vod__c || '-' || mp2.product_detail_id
		WHEN COALESCE(mp2.product_detail_id, '') = ''
			THEN pm.account_vod__c || '-NM'
		WHEN COALESCE(pm.products_vod__c, '') = ''
			THEN pm.account_vod__c || '-NM'
	END) as Product_Target_Class_Key
	,mpag.Indication_Flag as Indication_Flag
	,(CASE
    		WHEN pm.JJ_INC_Therapy__c = 'Aftercare'  THEN 'Only few own patients, but influences other HCPs'
    		WHEN pm.JJ_INC_Therapy__c = 'Non-Validated' THEN 'Decides for own patients only'
    		WHEN pm.JJ_INC_Therapy__c = 'None' THEN 'Unknown'
    		WHEN pm.JJ_INC_Therapy__c = 'Start of Therapy'  THEN  'Decides for own patients and influences other HCPs'
   		WHEN pm.JJ_INC_Therapy__c = 'Transfer to a Specialist'  THEN 'No decision, seeks advice for all cases'
    		WHEN pm.JJ_INC_Therapy__c = 'Unknown'  THEN 'Decides for familiar patient cases and seeks advice for more complex patient cases'
    		ELSE pm.JJ_INC_Therapy__c
  	END) as therapy_decision
FROM {{ source('raw', 'product_metrics') }} pm 
LEFT OUTER JOIN {{ source('raw', 'product') }} pr ON pr.id = pm.products_vod__c 
LEFT OUTER JOIN {{ ref('m_product') }} mp2 ON mp2.product_id = pm.products_vod__c 
LEFT OUTER JOIN 
	(SELECT *
	FROM (
		SELECT
			an.id as product_analytic_group_id 
			,pg.product_vod__c as product_id
			,1 as Indication_Flag
			,ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY product_analytic_group_id ASC) as rowid
		FROM {{ source('raw', 'analytics_product_group') }} an	
		JOIN {{ source('raw', 'product_group_map') }} pg ON an.id = pg.analytics_product_group_vod__c
		WHERE COALESCE(pg.product_vod__c, '') != ''
		) 
	WHERE rowid=1
	) mpag ON mpag.product_id = pm.products_vod__c
WHERE pm.country_iso_code = 'DE'
GROUP BY
 pm.account_vod__c
,pm.products_vod__c
,pm.country_iso_code
,Product_Target_Class_Key
,mpag.Indication_Flag
,pm.JJ_INC_Therapy__c


UNION


SELECT
	 pm.account_vod__c as Account_Id
	,pm.products_vod__c as Product_Id
	,pm.country_iso_code as Country_Code
	,(CASE 
		WHEN COALESCE(mp2.product_detail_id, '') != ''
			THEN pm.account_vod__c || '-' || mp2.product_detail_id
		WHEN COALESCE(mp2.product_detail_id, '') = ''
			THEN pm.account_vod__c || '-NM'
		WHEN COALESCE(mpag.product_analytic_group_id, '') != ''
			THEN pm.account_vod__c || '-' || mpag.product_analytic_group_id || '-' ||
				CASE WHEN mp2.product_id = pm.products_vod__c THEN mp2.product_country_code ELSE 'NM' END
		WHEN COALESCE(pm.products_vod__c, '') = ''
			THEN pm.account_vod__c || '-NM'
	END) as Product_Target_Class_Key
	,NULL as Indication_Flag
	,(CASE
   		WHEN pm.JJ_INC_Therapy__c = 'Aftercare'  THEN 'Only few own patients, but influences other HCPs'
    		WHEN pm.JJ_INC_Therapy__c = 'Non-Validated' THEN 'Decides for own patients only'
    		WHEN pm.JJ_INC_Therapy__c = 'None' THEN 'Unknown'
    		WHEN pm.JJ_INC_Therapy__c = 'Start of Therapy'  THEN  'Decides for own patients and influences other HCPs'
    		WHEN pm.JJ_INC_Therapy__c = 'Transfer to a Specialist'  THEN 'No decision, seeks advice for all cases'
    		WHEN pm.JJ_INC_Therapy__c = 'Unknown'  THEN 'Decides for familiar patient cases and seeks advice for more complex patient cases'
    		ELSE pm.JJ_INC_Therapy__c
  	END) as therapy_decision
FROM {{ source('raw', 'product_metrics') }} pm 
LEFT OUTER JOIN {{ ref('m_product') }} mp2 ON mp2.product_id = pm.products_vod__c 
LEFT OUTER JOIN 
	(SELECT *
	FROM (
		SELECT
			 an.id as product_analytic_group_id 
			,pg.product_vod__c as product_id
			,1 as Indication_Flag
			,ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY product_analytic_group_id ASC) as rowid
		FROM {{ source('raw', 'analytics_product_group') }} an	
		JOIN {{ source('raw', 'product_group_map') }} pg ON an.id = pg.analytics_product_group_vod__c
		WHERE COALESCE(pg.product_vod__c, '') != ''
		) 
	WHERE rowid=1
	) mpag ON mpag.product_id = pm.products_vod__c
WHERE pm.country_iso_code = 'DE'
GROUP BY
 pm.account_vod__c
,pm.products_vod__c
,pm.country_iso_code
,Product_Target_Class_Key
,pm.JJ_INC_Therapy__c
)	

--RESULTANT TABLE M_PRODUCT_METRICS_COUNTRY
SELECT
	 AUX_TABLE.Account_Id::varchar(255)
	,AUX_TABLE.Product_Id::varchar(255)
	,a.territory_assignment_id::varchar(255) as Territory_Nominal_Id
	,AUX_TABLE.Country_Code::varchar(255)
	,(CASE WHEN LEN(mp2.product_therapeutic_area_1)>0 THEN mp2.product_therapeutic_area_1 ELSE 'NM' END)::varchar(255) as Therapeutic_Area_Name
	,(CASE WHEN LEN(mp2.product_brand_therapeutic_area_1_null)>0 THEN mp2.product_brand_therapeutic_area_1_null ELSE 'NM' END)::varchar(255) as Therapeutic_Area_Brand_Name 
	,cs.name::varchar(255) as Country
	,cs.jj_region__c::varchar(255) as Region
	,AUX_TABLE.Product_Target_Class_Key::varchar(255)
	,AUX_TABLE.Indication_Flag::varchar(1) as Indication_Flag
	,MAX(AUX_TABLE.therapy_decision) AS therapy_decision
FROM AUX_TABLE as AUX_TABLE
LEFT OUTER JOIN {{ source('raw', 'country_settings') }} cs ON cs.jj_country_iso_code__c = AUX_TABLE.Country_Code
LEFT OUTER JOIN {{ ref('m_assignment') }} a ON a.assignment_id = AUX_TABLE.Account_Id
LEFT OUTER JOIN 
	(SELECT *, CASE WHEN product_brand_therapeutic_area_1 = 'No Value' THEN NULL ELSE product_brand_therapeutic_area_1 END as product_brand_therapeutic_area_1_null 
	FROM {{ ref('m_product') }} 
	) mp2 ON mp2.product_id = AUX_TABLE.Product_Id
GROUP BY
	 AUX_TABLE.Account_Id::varchar(255)
	,AUX_TABLE.Product_Id::varchar(255)
	,a.territory_assignment_id::varchar(255)
	,AUX_TABLE.Country_Code::varchar(255)
	,(CASE WHEN LEN(mp2.product_therapeutic_area_1)>0 THEN mp2.product_therapeutic_area_1 ELSE 'NM' END)::varchar(255)
	,(CASE WHEN LEN(mp2.product_brand_therapeutic_area_1_null)>0 THEN mp2.product_brand_therapeutic_area_1_null ELSE 'NM' END)::varchar(255)
	,cs.name::varchar(255)
	,cs.jj_region__c::varchar(255)
	,AUX_TABLE.Product_Target_Class_Key::varchar(255)
	,AUX_TABLE.Indication_Flag::varchar(1)