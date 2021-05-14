{{ config(
	post_hook="grant all privileges on {{ this }} to {{ var('snowflake_user_grant_privileges') }}")
}}

with AUX_TABLE as (
SELECT
	 pm.account_vod__c as Account_Id
	,pm.products_vod__c as Product_Id
	,pm.country_iso_code as Country_Code
	,MAX(pm.jj_penetration__c) as Penetration
	,MAX(pm.jj_potential__c) as Potential
	,MAX(pm.JJ_Penetration_Number__c) as Penetration_number
	,MAX(pm.JJ_Potential_Number__c) as Potential_number
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
	,MAX(pm.jj_tc__c) as Product_Target_Class
	,MAX(CASE 
		WHEN LEN(tc.non_prescribing) > 0 
			THEN tc.target_class
			ELSE 
				CASE WHEN TRIM(mp1.product) = 
				(SELECT TRIM(product_detail_group)
					FROM (
						SELECT product_detail_group, row_number() over (order by product_detail_group) as seqnum
						FROM {{ source('raw', 'emea_matrix') }}
						ORDER BY seqnum LIMIT 1
					))
				THEN ma.proposed_tc 
				END
	END) as Targeting_System_Proposal
	,MAX(pmnp.therapy_mindset) as Therapy_mindset
	,MAX(pmnp.sales_stage) as Sales_stage
	,mpag.Indication_Flag as Indication_Flag
FROM {{ source('raw', 'product_metrics') }} pm 
LEFT OUTER JOIN {{ source('raw', 'product') }} pr ON pr.id = pm.products_vod__c 
LEFT OUTER JOIN {{ source('raw', 'country_settings') }} cs ON cs.jj_country_iso_code__c = pm.country_iso_code
LEFT OUTER JOIN {{ source('raw', 'emea_matrix_map_target_class') }} tc ON tc.non_prescribing = pm.jj_non_prescribing__c 
LEFT OUTER JOIN {{ source('raw', 'emea_matrix') }} ma ON ma.target = CASE WHEN pm.jj_target__c = 0 THEN 'false' ELSE 'true' END
	AND ma.potential = pm.jj_potential__c AND ma.penetration = pm.jj_penetration__c
LEFT OUTER JOIN {{ source('raw', 'product_metrics_normalized_pivot') }} pmnp ON pmnp.account = pm.account_vod__c AND pmnp.product = pm.products_vod__c 
LEFT OUTER JOIN {{ ref('m_product') }} mp1 ON mp1.product_id = pm.detail_group_vod__c 
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
	,MAX(pm.jj_penetration__c) as Penetration
	,MAX(pm.jj_potential__c) as Potential
	,MAX(pm.JJ_Penetration_Number__c) as Penetration_number
	,MAX(pm.JJ_Potential_Number__c) as Potential_number
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
	,MAX(pm.jj_tc__c) as Product_Target_Class
	,MAX(CASE 
		WHEN LEN(tc.non_prescribing) > 0 
			THEN tc.target_class
			ELSE 
				CASE WHEN TRIM(mp1.product) = 
				(SELECT TRIM(product_detail_group)
					FROM (
						SELECT product_detail_group, row_number() over (order by product_detail_group) as seqnum
						FROM {{ source('raw', 'emea_matrix') }}
						ORDER BY seqnum LIMIT 1
					))
				THEN ma.proposed_tc 
				END
	END) as Targeting_System_Proposal
	,MAX(pmnp.therapy_mindset) as Therapy_mindset
	,MAX(pmnp.sales_stage) as Sales_stage
	,NULL as Indication_Flag
FROM {{ source('raw', 'product_metrics') }} pm 
LEFT OUTER JOIN {{ source('raw', 'product') }} pr ON pr.id = pm.products_vod__c 
LEFT OUTER JOIN {{ source('raw', 'country_settings') }} cs ON cs.jj_country_iso_code__c = pm.country_iso_code
LEFT OUTER JOIN {{ source('raw', 'emea_matrix_map_target_class') }} tc ON tc.non_prescribing = pm.jj_non_prescribing__c 
LEFT OUTER JOIN {{ source('raw', 'emea_matrix') }} ma ON ma.target = CASE WHEN pm.jj_target__c = 0 THEN 'false' ELSE 'true' END
	AND ma.potential = pm.jj_potential__c AND ma.penetration = pm.jj_penetration__c
LEFT OUTER JOIN {{ source('raw', 'product_metrics_normalized_pivot') }} pmnp ON pmnp.account = pm.account_vod__c AND pmnp.product = pm.products_vod__c 
LEFT OUTER JOIN {{ ref('m_product') }} mp1 ON mp1.product_id = pm.detail_group_vod__c 
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
GROUP BY
 pm.account_vod__c
,pm.products_vod__c
,pm.country_iso_code
,Product_Target_Class_Key
)	

--RESULTANT TABLE M_PRODUCT_METRICS_EMEA
SELECT
	 AUX_TABLE.Account_Id::varchar(255) as Account_Id
	,AUX_TABLE.Product_Id::varchar(255) as Product_Id
	,a.territory_assignment_id::varchar(255) as Territory_Nominal_Id
	,AUX_TABLE.Country_Code::varchar(255) as Country_Code
	,(CASE WHEN LEN(mp2.product_therapeutic_area_1)>0 THEN mp2.product_therapeutic_area_1 ELSE 'NM' END)::varchar(255) as Therapeutic_Area_Name
	,(CASE WHEN LEN(mp2.product_brand_therapeutic_area_1_null)>0 THEN mp2.product_brand_therapeutic_area_1_null ELSE 'NM' END)::varchar(255) as Therapeutic_Area_Brand_Name 
	,cs.name::varchar(255) as Country
	,cs.jj_region__c::varchar(255) as Region
	,AUX_TABLE.Penetration::varchar(255) as Penetration
	,AUX_TABLE.Potential::varchar(255) as Potential
	,AUX_TABLE.Penetration_number::varchar(255) as Penetration_number
	,AUX_TABLE.Potential_number::varchar(255) as Potential_number
	,AUX_TABLE.Product_Target_Class_Key::varchar(255) as Product_Target_Class_Key
	,AUX_TABLE.Product_Target_Class::varchar(255) as Product_Target_Class
	,AUX_TABLE.Targeting_System_Proposal::varchar(255) as Targeting_System_Proposal
	,AUX_TABLE.Therapy_mindset::varchar(255) as Therapy_mindset
	,AUX_TABLE.Sales_stage::varchar(255) as Sales_stage
	,CASE
		WHEN LEN(AUX_TABLE.Therapy_mindset) > 0 OR LEN(AUX_TABLE.Sales_stage) > 0 
			THEN 'Yes'
			ELSE 'No'
	END::varchar(3) as EMEA_Indicator_Filled
	,CASE
		WHEN LEN(AUX_TABLE.Therapy_mindset) > 0
			THEN 'Yes'
			ELSE 'No'
	END::varchar(3) as Therapy_mindset_Filled
	,CASE
		WHEN LEN(AUX_TABLE.Sales_stage) > 0
			THEN 'Yes'
			ELSE 'No'
	END::varchar(3) as Sales_stage_Filled
	,CASE
		WHEN LEN(AUX_TABLE.Penetration) > 0
			THEN 'Yes'
			ELSE 'No'
	END::varchar(3) as Penetration_Filled
	,CASE
		WHEN LEN(AUX_TABLE.Potential) > 0
			THEN 'Yes'
			ELSE 'No'
	END::varchar(3) as Potential_Filled
	,CASE
		WHEN LEN(AUX_TABLE.Product_Target_Class) > 0
			THEN 'Yes'
			ELSE 'No'
	END::varchar(3) as Account_Targeted
	,AUX_TABLE.Indication_Flag::varchar(1) as Indication_Flag
	,'Product Metrics EMEA' as Origin
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
	,AUX_TABLE.Penetration::varchar(255)
	,AUX_TABLE.Potential::varchar(255)
	,AUX_TABLE.Penetration_number::varchar(255)
	,AUX_TABLE.Potential_number::varchar(255)
	,AUX_TABLE.Product_Target_Class_Key::varchar(255)
	,AUX_TABLE.Product_Target_Class::varchar(255)
	,AUX_TABLE.Targeting_System_Proposal::varchar(255)
	,AUX_TABLE.Therapy_mindset::varchar(255)
	,AUX_TABLE.Sales_stage::varchar(255)
	,CASE
		WHEN LEN(AUX_TABLE.Therapy_mindset) > 0 OR LEN(AUX_TABLE.Sales_stage) > 0 
			THEN 'Yes'
			ELSE 'No'
	END::varchar(3)
	,CASE
		WHEN LEN(AUX_TABLE.Therapy_mindset) > 0
			THEN 'Yes'
			ELSE 'No'
	END::varchar(3)
	,CASE
		WHEN LEN(AUX_TABLE.Sales_stage) > 0
			THEN 'Yes'
			ELSE 'No'
	END::varchar(3)
	,CASE
		WHEN LEN(AUX_TABLE.Penetration) > 0
			THEN 'Yes'
			ELSE 'No'
	END::varchar(3)
	,CASE
		WHEN LEN(AUX_TABLE.Potential) > 0
			THEN 'Yes'
			ELSE 'No'
	END::varchar(3)
	,CASE
		WHEN LEN(AUX_TABLE.Product_Target_Class) > 0
			THEN 'Yes'
			ELSE 'No'
	END::varchar(3)
	,AUX_TABLE.Indication_Flag::varchar(1)