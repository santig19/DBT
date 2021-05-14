{{ config(
	post_hook="grant all privileges on {{ this }} to {{ var('snowflake_user_grant_privileges') }}")
}}

with COUNTRIES_FILTER as
(
select country_iso_code, name, upper(name)
  from {{ source('raw', 'country_settings') }}
 where upper(name) in ('ALGERIA','ARMENIA','AUSTRIA','AZERBAIJAN','BAHRAIN','BELARUS','BELGIUM','BULGARIA','CROATIA','CZECH REPUBLIC','DEMOLAND','DEMOTRA',
                       'EGYPT','EMEA','ESTONIA','FRANCE','GERMANY','GHANA','GREECE','HUNGARY','IRAN','IRAQ','IRELAND','ISRAEL','ITALY','JORDAN','KAZAKHSTAN',
                       'KENYA','KUWAIT','LATVIA','LEBANON','LIBYA','LITHUANIA','LUXEMBOURG','MAURITIUS','MOLDOVA','MOROCCO','NETHERLANDS','NIGERIA','OMAN',
                       'POLAND','PORTUGAL','QATAR','ROMANIA','RUSSIA','SAUDI ARABIA','SERBIA','SLOVAKIA','SLOVENIA','SOUTH AFRICA','SPAIN','SUDAN','SWITZERLAND',
                       'SYRIA','TUNISIA','TURKEY','UAE','UKRAINE','UNITED KINGDOM','YEMEN')
 group by country_iso_code, name, upper(name)                       
)                       
,AUX_TABLE as (
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
    ,max(pm.JJ_Site_Readiness__c) as JJ_Site_Readiness__c
    ,max(pm.JJ_Spravato_3LMDD__c) as JJ_Spravato_3LMDD__c
    ,max(pm.JJ_Spravato_Account_Status__c) as JJ_Spravato_Account_Status__c
    ,max(pm.JJ_Spravato_Bed_Chair__c) as JJ_Spravato_Bed_Chair__c
    ,max(pm.JJ_Spravato_Capacity__c) as JJ_Spravato_Capacity__c
    ,max(pm.JJ_Spravato_Monitoring__c) as JJ_Spravato_Monitoring__c
    ,max(pm.JJ_Spravato_Nurse__c) as JJ_Spravato_Nurse__c
    ,max(pm.JJ_Spravato_Obsroom__c) as JJ_Spravato_Obsroom__c
    ,max(pm.JJ_Spravato_Pharmacy__c) as JJ_Spravato_Pharmacy__c
    ,max(pm.JJ_Spravato_Planned_Capacity__c) as JJ_Spravato_Planned_Capacity__c
    ,max(pm.JJ_Spravato_Referrals__c) as JJ_Spravato_Referrals__c
    ,max(pm.JJ_Spravato_Reimbursement__c) as JJ_Spravato_Reimbursement__c
    ,max(pm.JJ_Spravato_TRD_Target__c) as JJ_Spravato_TRD_Target__c
    ,max(pm.JJ_Spravato_Attitude__c) as JJ_Spravato_Attitude__c
    ,max(pm.JJ_Spravato_Sales_Stage__c) as JJ_Spravato_Sales_Stage__c
    ,max(pm.JJ_Mindset_Spravato__c) as JJ_Mindset_Spravato__c	
FROM {{ source('raw', 'product_metrics') }} pm 
LEFT OUTER JOIN {{ ref('m_product') }} mp2 ON mp2.product_id = pm.products_vod__c 
LEFT OUTER JOIN (
				SELECT *
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
WHERE pm.country_iso_code in (select country_iso_code from COUNTRIES_FILTER group by country_iso_code)
GROUP BY
 pm.account_vod__c
,pm.products_vod__c
,pm.country_iso_code
,Product_Target_Class_Key
,mpag.Indication_Flag

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
    ,max(pm.JJ_Site_Readiness__c) as JJ_Site_Readiness__c
    ,max(pm.JJ_Spravato_3LMDD__c) as JJ_Spravato_3LMDD__c
    ,max(pm.JJ_Spravato_Account_Status__c) as JJ_Spravato_Account_Status__c
    ,max(pm.JJ_Spravato_Bed_Chair__c) as JJ_Spravato_Bed_Chair__c
    ,max(pm.JJ_Spravato_Capacity__c) as JJ_Spravato_Capacity__c
    ,max(pm.JJ_Spravato_Monitoring__c) as JJ_Spravato_Monitoring__c
    ,max(pm.JJ_Spravato_Nurse__c) as JJ_Spravato_Nurse__c
    ,max(pm.JJ_Spravato_Obsroom__c) as JJ_Spravato_Obsroom__c
    ,max(pm.JJ_Spravato_Pharmacy__c) as JJ_Spravato_Pharmacy__c
    ,max(pm.JJ_Spravato_Planned_Capacity__c) as JJ_Spravato_Planned_Capacity__c
    ,max(pm.JJ_Spravato_Referrals__c) as JJ_Spravato_Referrals__c
    ,max(pm.JJ_Spravato_Reimbursement__c) as JJ_Spravato_Reimbursement__c
    ,max(pm.JJ_Spravato_TRD_Target__c) as JJ_Spravato_TRD_Target__c
    ,max(pm.JJ_Spravato_Attitude__c) as JJ_Spravato_Attitude__c
    ,max(pm.JJ_Spravato_Sales_Stage__c) as JJ_Spravato_Sales_Stage__c
    ,max(pm.JJ_Mindset_Spravato__c) as JJ_Mindset_Spravato__c	

FROM {{ source('raw', 'product_metrics') }} pm 
LEFT OUTER JOIN {{ ref('m_product') }} mp2 ON mp2.product_id = pm.products_vod__c 
LEFT OUTER JOIN (
				SELECT *
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
WHERE pm.country_iso_code in (select country_iso_code from COUNTRIES_FILTER group by country_iso_code)
GROUP BY
 pm.account_vod__c
,pm.products_vod__c
,pm.country_iso_code
,Product_Target_Class_Key
)	
, AUX_TABLE_2 as
(
SELECT 
	 AUX_TABLE.Account_Id
	,AUX_TABLE.Product_Id
	,a.territory_assignment_id as Territory_Nominal_Id
	,AUX_TABLE.Country_Code
	,AUX_TABLE.Product_Target_Class_Key
	,AUX_TABLE.Indication_Flag as Indication_Flag
    ,AUX_TABLE.JJ_Site_Readiness__c
    ,AUX_TABLE.JJ_Spravato_3LMDD__c
    ,AUX_TABLE.JJ_Spravato_Account_Status__c
    ,AUX_TABLE.JJ_Spravato_Bed_Chair__c
    ,AUX_TABLE.JJ_Spravato_Capacity__c
    ,AUX_TABLE.JJ_Spravato_Monitoring__c
    ,AUX_TABLE.JJ_Spravato_Nurse__c
    ,AUX_TABLE.JJ_Spravato_Obsroom__c
    ,AUX_TABLE.JJ_Spravato_Pharmacy__c
    ,AUX_TABLE.JJ_Spravato_Planned_Capacity__c
    ,AUX_TABLE.JJ_Spravato_Referrals__c
    ,AUX_TABLE.JJ_Spravato_Reimbursement__c
    ,AUX_TABLE.JJ_Spravato_TRD_Target__c
    ,AUX_TABLE.JJ_Spravato_Attitude__c
    ,AUX_TABLE.JJ_Spravato_Sales_Stage__c
    ,AUX_TABLE.JJ_Mindset_Spravato__c
FROM AUX_TABLE as AUX_TABLE
LEFT OUTER JOIN {{ source('raw', 'country_settings') }} cs ON cs.jj_country_iso_code__c = AUX_TABLE.Country_Code
LEFT OUTER JOIN {{ ref('m_assignment') }} a ON a.assignment_id = AUX_TABLE.Account_Id
GROUP BY
	 AUX_TABLE.Account_Id
	,AUX_TABLE.Product_Id
	,a.territory_assignment_id
	,AUX_TABLE.Country_Code
	,AUX_TABLE.Product_Target_Class_Key
	,AUX_TABLE.Indication_Flag
    ,AUX_TABLE.JJ_Site_Readiness__c
    ,AUX_TABLE.JJ_Spravato_3LMDD__c
    ,AUX_TABLE.JJ_Spravato_Account_Status__c
    ,AUX_TABLE.JJ_Spravato_Bed_Chair__c
    ,AUX_TABLE.JJ_Spravato_Capacity__c
    ,AUX_TABLE.JJ_Spravato_Monitoring__c
    ,AUX_TABLE.JJ_Spravato_Nurse__c
    ,AUX_TABLE.JJ_Spravato_Obsroom__c
    ,AUX_TABLE.JJ_Spravato_Pharmacy__c
    ,AUX_TABLE.JJ_Spravato_Planned_Capacity__c
    ,AUX_TABLE.JJ_Spravato_Referrals__c
    ,AUX_TABLE.JJ_Spravato_Reimbursement__c
    ,AUX_TABLE.JJ_Spravato_TRD_Target__c
    ,AUX_TABLE.JJ_Spravato_Attitude__c
    ,AUX_TABLE.JJ_Spravato_Sales_Stage__c
    ,AUX_TABLE.JJ_Mindset_Spravato__c
)
select account_id::varchar(255) as account_id,
       product_id::varchar(255) as product_id,
       territory_nominal_Id::varchar(255) as territory_nominal_Id,
       country_code::varchar(255) as country_code,
       product_target_class_key::varchar(255) as product_target_class_key,
       indication_flag::varchar(1) as indication_flag,
       product_metric_indicator_original_field::varchar(255) as product_metric_indicator_original_field,
       product_metric_indicator_field::varchar(255) as product_metric_indicator_field,
       product_metric_indicator_value::varchar(10000) as product_metric_indicator_value
  from 
(
select aux.Account_Id
	    ,aux.Product_Id
	    ,aux.Territory_Nominal_Id
	    ,aux.Country_Code
	    ,aux.Product_Target_Class_Key
	    ,aux.Indication_Flag
		,'JJ_Site_Readiness__c' as product_metric_indicator_original_field
		,'Site_Readiness' as product_metric_indicator_field
		,aux.JJ_Site_Readiness__c as product_metric_indicator_value  
  from AUX_TABLE_2 as aux
 union all 
select aux.Account_Id
	    ,aux.Product_Id
	    ,aux.Territory_Nominal_Id
	    ,aux.Country_Code
	    ,aux.Product_Target_Class_Key
	    ,aux.Indication_Flag
		,'JJ_Spravato_3LMDD__c' as product_metric_indicator_original_field
		,'3LMDD' as product_metric_indicator_field
		,aux.JJ_Spravato_3LMDD__c as product_metric_indicator_value     
  from AUX_TABLE_2 as aux
 union all 
select aux.Account_Id
	    ,aux.Product_Id
	    ,aux.Territory_Nominal_Id
	    ,aux.Country_Code
	    ,aux.Product_Target_Class_Key
	    ,aux.Indication_Flag
		,'JJ_Spravato_Account_Status__c' as product_metric_indicator_original_field
		,'Account_Status' as product_metric_indicator_field
		,aux.JJ_Spravato_Account_Status__c as product_metric_indicator_value         
  from AUX_TABLE_2 as aux
 union all 
select aux.Account_Id
	    ,aux.Product_Id
	    ,aux.Territory_Nominal_Id
	    ,aux.Country_Code
	    ,aux.Product_Target_Class_Key
	    ,aux.Indication_Flag
		,'JJ_Spravato_Bed_Chair__c' as product_metric_indicator_original_field
		,'Bed_Chair' as product_metric_indicator_field
		,aux.JJ_Spravato_Bed_Chair__c as product_metric_indicator_value       
  from AUX_TABLE_2 as aux
 union all 
select aux.Account_Id
	    ,aux.Product_Id
	    ,aux.Territory_Nominal_Id
	    ,aux.Country_Code
	    ,aux.Product_Target_Class_Key
	    ,aux.Indication_Flag
		,'JJ_Spravato_Capacity__c' as product_metric_indicator_original_field
		,'Capacity' as product_metric_indicator_field
		,aux.JJ_Spravato_Capacity__c as product_metric_indicator_value       
  from AUX_TABLE_2 as aux
 union all 
select aux.Account_Id
	    ,aux.Product_Id
	    ,aux.Territory_Nominal_Id
	    ,aux.Country_Code
	    ,aux.Product_Target_Class_Key
	    ,aux.Indication_Flag
		,'JJ_Spravato_Monitoring__c' as product_metric_indicator_original_field
		,'Monitoring' as product_metric_indicator_field
		,aux.JJ_Spravato_Monitoring__c as product_metric_indicator_value         
  from AUX_TABLE_2 as aux
 union all 
select aux.Account_Id
	    ,aux.Product_Id
	    ,aux.Territory_Nominal_Id
	    ,aux.Country_Code
	    ,aux.Product_Target_Class_Key
	    ,aux.Indication_Flag
		,'JJ_Spravato_Nurse__c' as product_metric_indicator_original_field
		,'Nurse' as product_metric_indicator_field
		,aux.JJ_Spravato_Nurse__c as product_metric_indicator_value        
  from AUX_TABLE_2 as aux
 union all
select aux.Account_Id
	    ,aux.Product_Id
	    ,aux.Territory_Nominal_Id
	    ,aux.Country_Code
	    ,aux.Product_Target_Class_Key
	    ,aux.Indication_Flag
		,'JJ_Spravato_Obsroom__c' as product_metric_indicator_original_field
		,'Obsroom' as product_metric_indicator_field
		,aux.JJ_Spravato_Obsroom__c as product_metric_indicator_value        
  from AUX_TABLE_2 as aux
 union all 
select aux.Account_Id
	    ,aux.Product_Id
	    ,aux.Territory_Nominal_Id
	    ,aux.Country_Code
	    ,aux.Product_Target_Class_Key
	    ,aux.Indication_Flag
		,'JJ_Spravato_Pharmacy__c' as product_metric_indicator_original_field
		,'Pharmacy' as product_metric_indicator_field
		,aux.JJ_Spravato_Pharmacy__c as product_metric_indicator_value       
  from AUX_TABLE_2 as aux
 union all 
select aux.Account_Id
	    ,aux.Product_Id
	    ,aux.Territory_Nominal_Id
	    ,aux.Country_Code
	    ,aux.Product_Target_Class_Key
	    ,aux.Indication_Flag
		,'JJ_Spravato_Planned_Capacity__c' as product_metric_indicator_original_field
		,'Planned_Capacity' as product_metric_indicator_field
		,aux.JJ_Spravato_Planned_Capacity__c as product_metric_indicator_value     
  from AUX_TABLE_2 as aux
 union all 
select aux.Account_Id
	    ,aux.Product_Id
	    ,aux.Territory_Nominal_Id
	    ,aux.Country_Code
	    ,aux.Product_Target_Class_Key
	    ,aux.Indication_Flag
		,'JJ_Spravato_Referrals__c' as product_metric_indicator_original_field
		,'Referrals' as product_metric_indicator_field
		,aux.JJ_Spravato_Referrals__c as product_metric_indicator_value       
  from AUX_TABLE_2 as aux
 union all 
select aux.Account_Id
	    ,aux.Product_Id
	    ,aux.Territory_Nominal_Id
	    ,aux.Country_Code
	    ,aux.Product_Target_Class_Key
	    ,aux.Indication_Flag
		,'JJ_Spravato_Reimbursement__c' as product_metric_indicator_original_field
		,'Reimbursement' as product_metric_indicator_field
		,aux.JJ_Spravato_Reimbursement__c as product_metric_indicator_value     
  from AUX_TABLE_2 as aux
 union all 
select aux.Account_Id
	    ,aux.Product_Id
	    ,aux.Territory_Nominal_Id
	    ,aux.Country_Code
	    ,aux.Product_Target_Class_Key
	    ,aux.Indication_Flag
		,'JJ_Spravato_TRD_Target__c' as product_metric_indicator_original_field
		,'TRD_Target' as product_metric_indicator_field
		,aux.JJ_Spravato_TRD_Target__c as product_metric_indicator_value       
  from AUX_TABLE_2 as aux
 union all 
select aux.Account_Id
	    ,aux.Product_Id
	    ,aux.Territory_Nominal_Id
	    ,aux.Country_Code
	    ,aux.Product_Target_Class_Key
	    ,aux.Indication_Flag
		,'JJ_Spravato_Attitude__c' as product_metric_indicator_original_field
		,'Attitude' as product_metric_indicator_field
		,aux.JJ_Spravato_Attitude__c as product_metric_indicator_value      
  from AUX_TABLE_2 as aux
 union all 
select aux.Account_Id
	    ,aux.Product_Id
	    ,aux.Territory_Nominal_Id
	    ,aux.Country_Code
	    ,aux.Product_Target_Class_Key
	    ,aux.Indication_Flag
		,'JJ_Spravato_Sales_Stage__c' as product_metric_indicator_original_field
		,'Sales_Stage' as product_metric_indicator_field
		,aux.JJ_Spravato_Sales_Stage__c as product_metric_indicator_value         
  from AUX_TABLE_2 as aux
 union all
select aux.Account_Id
	    ,aux.Product_Id
	    ,aux.Territory_Nominal_Id
	    ,aux.Country_Code
	    ,aux.Product_Target_Class_Key
	    ,aux.Indication_Flag
		,'JJ_Mindset_Spravato__c' as product_metric_indicator_original_field
		,'Mindset' as product_metric_indicator_field
		,aux.JJ_Mindset_Spravato__c as product_metric_indicator_value        
  from AUX_TABLE_2 as aux
)