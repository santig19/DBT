{{ config(
	post_hook="grant all privileges on {{ this }} to {{ var('snowflake_user_grant_privileges') }}")
}}

with m_product as (

select distinct
	 root.country_iso_code::varchar(2) as product_country_code
	,COALESCE(cs.name, '')::varchar(255) as product_country
	,COALESCE(cs.jj_region__c, '')::varchar(255) as product_region
	,root.id::varchar(255) as product_id --need to be able to load fake_product_id's
	,root.parent_product_vod__c::varchar(20) as parent_product_id
	,root.name::varchar(255) as product
	,COALESCE(L4_TOPIC.product_type, L3_CALL.product_type, L3_METRIC.product_type, L3_GROUP.product_type, L3_CASE.product_type,
		L2_DETAIL.product_type, L1_BRAND.product_type, root.product_type_vod__c)::varchar(255) as product_type
	,CASE WHEN root.Company_Product_vod__c IN ('True', 'true', '1') THEN 'Yes' ELSE 'No' END::varchar(3) as Product_Janssen
	,CASE WHEN root.JJ_Active__c IN ('True', 'true', '1') THEN 'Yes' ELSE 'No' END::varchar(3) as Product_Is_Active
	,CASE 
		WHEN LEN(root.JJ_Janssen_Life_Cycle__c)>0 
			THEN root.JJ_Janssen_Life_Cycle__c 
		WHEN LEN(root.JJ_Janssen_Life_Cycle_Parent__c)>0
			THEN root.JJ_Janssen_Life_Cycle_Parent__c 
		ELSE 'No Value'
	END::varchar(255) as Product_Life_Cycle_Group
	,root.JJ_Indication__c::varchar(1000) as Product_Indication_1
	,root.JJ_Indication_2__c::varchar(1000) as Product_Indication_2
	,CASE WHEN LEN(root.Therapeutic_Area_vod__c)>0 THEN root.Therapeutic_Area_vod__c ELSE 'No Value' END::varchar(255) as product_Therapeutic_Area_1
	,CASE WHEN LEN(root.JJ_Therapeutic_Area_2__c)>0 THEN root.JJ_Therapeutic_Area_2__c ELSE 'No Value' END::varchar(255) as product_Therapeutic_Area_2
	,BRAND_TA.product_brand_therapeutic_area_1::varchar(255) as product_brand_therapeutic_area_1
	,L1_BRAND.product_brand::varchar(255) as product_brand
	,L2_DETAIL.product_detail::varchar(255) as product_detail
	,L2_DETAIL.product_id::varchar(20) as product_detail_id
	,L3_CALL.product_call::varchar(255) as product_call
	,L3_METRIC.product_metric::varchar(255) as product_metric
	,L3_GROUP.product_group::varchar(255) as product_group
	,L3_CASE.product_case::varchar(255) as product_case
	,L4_TOPIC.product_topic::varchar(255) as product_topic
from {{ var('schema') }}.product_raw root
left join {{ var('schema') }}.country_settings_raw cs on cs.country_iso_code = root.country_iso_code
left join (
	select id as product_id, parent_product_vod__c as parent_product_id, name as product_topic, 'Topic' as product_type
	from {{ var('schema') }}.product_raw
	where lower(product_type_vod__c) = 'detail topic'
) L4_TOPIC on L4_TOPIC.product_id = root.id
left join (
	select id as product_id, parent_product_vod__c as parent_product_id, name as product_call, 'Call' as product_type
	from {{ var('schema') }}.product_raw
	where lower(product_type_vod__c) = 'detail' and lower(JJ_Detail_Sub_Type__c) = 'call'
) L3_CALL on L3_CALL.product_id = COALESCE(L4_TOPIC.parent_product_id, root.id)
left join (
	select id as product_id, parent_product_vod__c as parent_product_id, name as product_metric, 'Metric' as product_type
	from {{ var('schema') }}.product_raw
	where lower(product_type_vod__c) = 'detail' and lower(JJ_Detail_Sub_Type__c) = 'metric'
) L3_METRIC on L3_METRIC.product_id = COALESCE(L4_TOPIC.parent_product_id, root.id)
left join (
	select id as product_id, parent_product_vod__c as parent_product_id, name as product_group, 'Group' as product_type
	from {{ var('schema') }}.product_raw
	where lower(product_type_vod__c) = 'detail group'
) L3_GROUP on L3_GROUP.product_id = COALESCE(L4_TOPIC.parent_product_id, root.id)
left join (
	select id as product_id, parent_product_vod__c as parent_product_id, name as product_case, 'Case' as product_type
	from {{ var('schema') }}.product_raw
	where lower(product_type_vod__c) = 'put-up'
) L3_CASE on L3_CASE.product_id = COALESCE(L4_TOPIC.parent_product_id, root.id)
left join (
	select id as product_id, parent_product_vod__c as parent_product_id, name as product_detail, 'Detail' as product_type
	from {{ var('schema') }}.product_raw
	where lower(product_type_vod__c) = 'detail' and lower(JJ_Detail_Sub_Type__c) = 'country'
) L2_DETAIL on L2_DETAIL.product_id = COALESCE(L3_CALL.parent_product_id, L3_METRIC.parent_product_id, L3_GROUP.parent_product_id, L3_CASE.parent_product_id, root.id)
--brand is higest level -> no parents
left join (
	select id as product_id, name as product_brand, 'Brand' as product_type
	from {{ var('schema') }}.product_raw
	where lower(product_type_vod__c) = 'brand'	
) L1_BRAND on L1_BRAND.product_id = COALESCE(L2_DETAIL.parent_product_id, root.id)
--join on brand level
left join (
	select name, Therapeutic_Area_vod__c as product_brand_therapeutic_area_1
	from {{ var('schema') }}.product_raw
	where lower(product_type_vod__c) = 'brand'	
) BRAND_TA on BRAND_TA.name = L1_BRAND.product_brand
where lower(root.product_type_vod__c) in ('brand', 'detail', 'detail group', 'put-up', 'detail topic')

)
--product_brand can only be empty when there is no parent_product_id
select * from m_product where product_brand is not null or (parent_product_id = '' and product_brand is null)

UNION

select distinct 
 PRODUCT_COUNTRY_CODE::varchar(2)
,PRODUCT_COUNTRY::varchar(255)
,PRODUCT_REGION::varchar(255)
,('(FK_THERAREA)_' || PRODUCT_THERAPEUTIC_AREA_1 ||'_'|| PRODUCT_COUNTRY_CODE)::varchar(255) as PRODUCT_ID
,NULL::varchar(20) as PARENT_PRODUCT_ID
,NULL::varchar(255) as PRODUCT
,NULL::varchar(255) as PRODUCT_TYPE
,NULL::varchar(3) as PRODUCT_JANSSEN
,NULL::varchar(3) as PRODUCT_IS_ACTIVE
,PRODUCT_LIFE_CYCLE_GROUP
,NULL::varchar(1000) as PRODUCT_INDICATION_1
,NULL::varchar(1000) as PRODUCT_INDICATION_2
,PRODUCT_THERAPEUTIC_AREA_1
,NULL::varchar(255) as PRODUCT_THERAPEUTIC_AREA_2
,NULL::varchar(255) as PRODUCT_BRAND_THERAPEUTIC_AREA_1
,NULL::varchar(255) as PRODUCT_BRAND
,NULL::varchar(255) as PRODUCT_DETAIL
,NULL::varchar(20) as PRODUCT_DETAIL_ID
,NULL::varchar(255) as PRODUCT_CALL
,NULL::varchar(255) as PRODUCT_METRIC
,NULL::varchar(255) as PRODUCT_GROUP
,NULL::varchar(255) as PRODUCT_CASE
,NULL::varchar(255) as PRODUCT_TOPIC
from m_product