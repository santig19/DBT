{{ config(
	post_hook="grant all privileges on {{ this }} to {{ var('snowflake_user_grant_privileges') }}")
}}

SELECT
h.Call_Name_Hierarchy,
h.Parent_Call,
h.Child_Call,
h.Account_Plan_Id,
h.Call_Therapeutical_Area,
h.Call_Counter_And_Expenses_Id,
h.Account_Id,
h.Brick_Code,
h.Assignment_Id,
h.Call_Territory,
h.Country_Code,
case when len(COALESCE(h.Call_Attendee_Type,''))>0 and right(h.Call_Attendee_Type,4)='_vod' then replace(substring(h.Call_Attendee_Type,1,len(h.Call_Attendee_Type)-4),'_',' ') else replace(h.Call_Attendee_Type,'_',' ') end::varchar(255) as Call_Attendee_Type,
h.Call_Attendees,
h.Call_Comments,
h.Date,
h.Call_Date,
h.Call_Timestamp,
h.Call_Objective,
h.Call_Channel,
h.Call_Type,
h.Call_CLM,
h.Call_Currency_Code,
h.Call_Group_Currency,
h.Call_Duration,
h.Call_Id,
h.is_parent_call,
h.Call_Attendee_Status,
h.Call_Status,
h.Call_Customer_Request_Number,
h.Call_Customer_Request_Veeva_Link,
h.Call_IspersonAccount,
h.Call_Expense_Type,
case when len(COALESCE(h.Call_Device,''))>0 and right(h.Call_Device,4)='_vod' then replace(substring(h.Call_Device,1,len(h.Call_Device)-4),'_',' ') else replace(h.Call_Device,'_',' ') end::varchar(255) as Call_Device,
h.Call,
h.Next_Call_Notes,
h.Pre_Call_notes,
h.Call_Proactive,
h.Call_Record_Type,
h.Activity,
h.Activity_Type,
h.Call_Subtype,
h.Account_Visited_NonVisited_Technical,
h.Origin,
h.Territory_Nominal_Id,
h.Employee_Id,
h.Call_Employee_Type,
h.Region,
h.Compliance_Region,
h.Compliance_Country,
h.Country,
h.Call_Product_Priority,
case when len(COALESCE(h.Call_Detail_Type,''))>0 and right(h.Call_Detail_Type,4)='_vod' then replace(substring(h.Call_Detail_Type,1,len(h.Call_Detail_Type)-4),'_',' ') else replace(h.Call_Detail_Type,'_',' ') end::varchar(255) as Call_Detail_Type,
h.Call_Interactions_Counter,
h.Account_SCD_Id,
h.Product_Metrics_SCD_Id,
h.Security_Key,
h.key_call_message_id,
h.key_call_message_general_comment,
h.Call_Sample_Id,
h.Call_Survey_Feedback_Id,
h.Parent_Call_Id,
h.Compliance_Visibility,
h.Sys_Compliance_Visibility,
h.Product_Id,
h.Therapeutic_Area_Name,
h.Therapeutic_Area_Brand_Name,
h.Coverage_Ratio_Key,
h.Multichannel_Type,
h.Multichannel_Activity,
h.Multichannel_Activity_Type,
h.Call_Product_Topic,
case
when(h.Call_Record_Type = 'Medical Call')
then Null
else case
when(m.Product_Analytic_Group_Id is not null)
then concat(COALESCE(h.Account_Id,''),'-',COALESCE(h.Product_Analytic_Group_Id,''),'-',case when trim(d.product_country_code)='' or d.product_country_code is null then 'NM' 
else d.product_country_code end)
else concat(COALESCE(h.Account_Id,''),'-',case when trim(d.Product_Detail_Id)='' or d.Product_Detail_Id is null then 'NM' else d.Product_Detail_Id end)
end
end::varchar(255) as Product_Target_Class_Key,
h.veeva_remote_meeting_id,
B.Medical_Event,
h.Call_Proactive_Reactive_Flag,
CASE WHEN mncv.Account_Id IS NOT NULL AND mncv.Territory_Id IS NOT NULL
THEN 1
ELSE 0
END AS flag_territory_belong,
CASE WHEN mncvs.Account_Id IS NOT NULL AND mncvs.Territory_Id IS NOT NULL AND yearmonth IS NOT NULL
THEN 1
ELSE 0
END AS historical_flag_territory_belong,
h.average_call_expenses_lc,
h.total_call_expenses_lc,
h.call_attachments,
NULL as channel_new
 
FROM {{ ref('tmp_call') }} h
 
LEFT JOIN (
SELECT max(product_id) as product_id
FROM {{ ref('m_product') }} group by product_id
) p on h.Aux_Product_Id = p.Product_Id
 
LEFT JOIN (
SELECT max(product_id) as product_id,Product_Detail_Id,product_country_code
FROM {{ ref('m_product') }}
GROUP BY Product_Detail_Id,product_id,product_country_code
) d on d.product_id = h.Product_Id
 
LEFT JOIN (
SELECT
min(an.id) as product_analytic_group_id
,pg.product_vod__c as product_id
FROM {{ source('raw', 'analytics_product_group') }} an
JOIN {{ source('raw', 'product_group_map') }} pg ON an.id = pg.analytics_product_group_vod__c
WHERE pg.product_vod__c IS NOT NULL AND pg.product_vod__c <> ''
GROUP BY pg.product_vod__c
) M on M.Product_Id = h.Product_Id
 
LEFT JOIN (
 
SELECT DISTINCT(ID), Medical_Event_vod__c AS Medical_Event from
{{ source('raw', 'call') }}) b
on H.Call_Id = b.id
 
LEFT JOIN (SELECT Account_Id, Territory_Id FROM {{ ref('m_null_country_values') }}
GROUP BY Account_Id, Territory_Id) mncv
ON h.Account_Id = mncv.Account_Id AND h.Territory_Nominal_Id = mncv.Territory_Id
 
LEFT JOIN (SELECT Account_Id, Territory_Id, yearmonth FROM {{ source('raw', 'm_null_country_values_snapshot_monthly_historical') }}
GROUP BY Account_Id, Territory_Id, yearmonth) mncvs
ON h.Account_Id = mncvs.Account_Id AND h.Territory_Nominal_Id = mncvs.Territory_Id AND (split_part(h.Date,'-',3) || split_part(h.Date,'-',2)) = mncvs.yearmonth
 
group by
h.Call_Name_Hierarchy,
h.Parent_Call,
h.Child_Call,
h.Account_Plan_Id,
h.Call_Therapeutical_Area,
h.Call_Counter_And_Expenses_Id,
h.Account_Id,
h.Brick_Code,
h.Assignment_Id,
h.Call_Territory,
h.Country_Code,
case when len(COALESCE(h.Call_Attendee_Type,''))>0 and right(h.Call_Attendee_Type,4)='_vod' then replace(substring(h.Call_Attendee_Type,1,len(h.Call_Attendee_Type)-4),'_',' ') else replace(h.Call_Attendee_Type,'_',' ') end,
h.Call_Attendees,
h.Call_Comments,
h.Date,
h.Call_Date,
h.Call_Timestamp,
h.Call_Objective,
h.Call_Channel,
h.Call_Type,
h.Call_CLM,
h.Call_Currency_Code,
h.Call_Group_Currency,
h.Call_Duration,
h.Call_Id,
h.is_parent_call,
h.Call_Attendee_Status,
h.Call_Status,
h.Call_Customer_Request_Number,
h.Call_Customer_Request_Veeva_Link,
h.Call_IspersonAccount,
h.Call_Expense_Type,
case when len(COALESCE(h.Call_Device,''))>0 and right(h.Call_Device,4)='_vod' then replace(substring(h.Call_Device,1,len(h.Call_Device)-4),'_',' ') else replace(h.Call_Device,'_',' ') end,
h.Call,
h.Next_Call_Notes,
h.Pre_Call_notes,
h.Call_Proactive,
h.Call_Record_Type,
h.Activity,
h.Activity_Type,
h.Call_Subtype,
h.Account_Visited_NonVisited_Technical,
h.Origin,
h.Territory_Nominal_Id,
h.Employee_Id,
h.Call_Employee_Type,
h.Region,
h.Compliance_Region,
h.Compliance_Country,
h.Country,
h.Call_Product_Priority,
case when len(COALESCE(h.Call_Detail_Type,''))>0 and right(h.Call_Detail_Type,4)='_vod' then replace(substring(h.Call_Detail_Type,1,len(h.Call_Detail_Type)-4),'_',' ') else replace(h.Call_Detail_Type,'_',' ') end,
h.Call_Interactions_Counter,
h.Account_SCD_Id,
h.Product_Metrics_SCD_Id,
h.Security_Key,
h.key_call_message_id,
h.key_call_message_general_comment,
h.Call_Sample_Id,
h.Call_Survey_Feedback_Id,
h.Parent_Call_Id,
h.Compliance_Visibility,
h.Sys_Compliance_Visibility,
h.Product_Id,
h.Therapeutic_Area_Name,
h.Therapeutic_Area_Brand_Name,
h.Coverage_Ratio_Key,
h.Multichannel_Type,
h.Multichannel_Activity,
h.Multichannel_Activity_Type,
h.Call_Product_Topic,
case
when(h.Call_Record_Type = 'Medical Call')
then Null
else case
when(m.Product_Analytic_Group_Id is not null)
then concat(COALESCE(h.Account_Id,''),'-',COALESCE(h.Product_Analytic_Group_Id,''),'-',case when trim(d.product_country_code)='' or d.product_country_code is null then 'NM' else d.product_country_code end)
else concat(COALESCE(h.Account_Id,''),'-',case when trim(d.Product_Detail_Id)='' or d.Product_Detail_Id is null then 'NM' else d.Product_Detail_Id end)
end
end,
h.veeva_remote_meeting_id,
B.Medical_Event,
h.Call_Proactive_Reactive_Flag,
mncv.Account_Id,
mncv.Territory_Id,
mncvs.Account_Id,
mncvs.Territory_Id,
mncvs.yearmonth,
h.average_call_expenses_lc,
h.total_call_expenses_lc,
h.call_attachments,
channel_new