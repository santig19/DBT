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
h.Call_Attendee_Type,
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
h.Call_Device,
h.Call,
h.Single_Call_to_Hospital,
h.Next_Call_Notes,
h.pre_call_notes,
h.Call_Proactive,
h.Call_Record_Type,
h.Activity,
h.Activity_Type,
h.Call_Subtype,
h.Account_Visited_NonVisited_Technical,
h.Origin,
h.Aux_Owner_Id,
h.Aux_User_Id,
h.Aux_Parent_Call_Id,
h.Territory_Nominal_Id,
h.Employee_Id,
h.Call_Employee_Type,
h.Region,
h.Compliance_Region,
h.Compliance_Country,
h.Country,
h.Call_Product_Priority,
h.Aux_Product_Id,
h.Call_Detail_Type,
h.Call_Interactions_Counter,
concat(COALESCE(h.Account_Id,''), '-',
	DATEDIFF(day,TO_DATE('30-12-1899','DD-MM-YYYY'),date_trunc('month', DATEADD(day,DATEDIFF(day,TO_DATE('30-12-1899','DD-MM-YYYY'),TO_DATE(h.date, 'dd-mm-YYYY')),TO_DATE('30-12-1899','DD-MM-YYYY')))))::varchar(255) as Account_SCD_Id,
concat(COALESCE(h.Account_Id,''),'-',COALESCE(h.Aux_Product_Id,''),'-', 
	DATEDIFF(day,TO_DATE('30-12-1899','DD-MM-YYYY'),date_trunc('month', DATEADD(day,DATEDIFF(day,TO_DATE('30-12-1899','DD-MM-YYYY'),TO_DATE(h.date, 'dd-mm-YYYY')),TO_DATE('30-12-1899','DD-MM-YYYY')))))::varchar(255) as Product_Metrics_SCD_Id,
concat(COALESCE(h.Assignment_Id,''),'-',COALESCE(h.Employee_Id,''))::varchar(255) as Security_Key,
concat(COALESCE(h.Account_Id,''),'_',COALESCE(h.Aux_Product_Id,''),'_',COALESCE(h.Call_Id,''))::varchar(255) as key_call_message_id,
concat(COALESCE(h.Account_Id,''),'__',COALESCE(h.Call_Id,''))::varchar(255) as key_call_message_general_comment,
concat(COALESCE(h.Account_Id,''),'_',COALESCE(h.Call_Id,''))::varchar(255) as Call_Sample_Id,
concat(COALESCE(h.Call_Id,''),'-',COALESCE(h.Account_Id,''))::varchar(255) as Call_Survey_Feedback_Id,
COALESCE(h.Parent_Call,'') as Parent_Call_Id,
'My Region' as Compliance_Visibility,
'1' as Sys_Compliance_Visibility,
(case when (h.Call_Record_Type = 'Medical Call') 
	then concat('(FK_THERAREA)_',COALESCE(h.Call_Therapeutical_Area,''),'_',COALESCE(h.Country_Code,''))
	else 
		case when Len(replace(h.Aux_Product_Id,' ',''))>0 
			then h.Aux_Product_Id
			else concat('(FK_THERAREA)_',h.Call_Therapeutical_Area,'_',h.Country_Code )
		end 
end)::varchar(255) as Product_Id,
case when (h.Call_Record_Type = 'Medical Call' or h.Call_Record_Type = 'Other Call') 
	then h.Call_Therapeutical_Area else p.Therapeutic_Area_vod__c 
end::varchar(255) as Therapeutic_Area_Name,
case when (h.Call_Record_Type = 'Medical Call' or h.Call_Record_Type = 'Other Call') 
	then h.Call_Therapeutical_Area
	else COALESCE(b.Product_Brand_Therapeutic_Area_1,'NM')
end::varchar(255) as Therapeutic_Area_Brand_Name,
concat(COALESCE(M.product_brand,'No Value'),'#',COALESCE(h.Country,''), '_',COALESCE(h.Country_Code,''))::varchar(255) as Coverage_Ratio_Key,
case when (h.Call_Status = 'Submitted' and h.Call_Channel = 'Face to face') 
	then 'Face to Face Contacts' 
	when (h.Call_Status = 'Submitted' and h.Call_Channel <> 'Face to face') 
		then 'Non Face to Face Contacts' 
else Null end::varchar(26) as Multichannel_Type,
case when (h.Call_Status = 'Submitted' and h.Call_Channel = 'Face to face') 
		then 'F2F Interactions' 
	when (h.Call_Status = 'Submitted' and h.Call_Channel = 'Email') 
		then 'Email Call' 
	when (h.Call_Status = 'Submitted' and h.Call_Channel = 'Phone') 
		then 'Phone Call' 
	when (h.Call_Status = 'Submitted' and h.Call_Channel = 'Webcall') 
		then 'Webcall' 
	when (h.Call_Status = 'Submitted' and h.Call_Channel = 'Traditional Mail') 
		then 'Traditional Mail'
else Null end::varchar(16) as Multichannel_Activity,
case when (h.Call_Status = 'Submitted' and h.Call_Channel = 'Face to face' and h.Call_Record_Type = 'Promotional Call') then 'F2F Promotional Interactions'
	when (h.Call_Status = 'Submitted' and h.Call_Channel = 'Face to face' and h.Call_Record_Type = 'Medical Call') then 'F2F Medical Interactions'
	when (h.Call_Status = 'Submitted' and h.Call_Channel = 'Face to face' and h.Call_Record_Type = 'Other Call') then 'F2F Other Interactions'
	when (h.Call_Status = 'Submitted' and h.Call_Channel = 'Email' and h.Call_Record_Type = 'Promotional Call') then 'Promotional Email Call'
	when (h.Call_Status = 'Submitted' and h.Call_Channel = 'Email' and h.Call_Record_Type = 'Medical Call') then 'Medical Email Call'
	when (h.Call_Status = 'Submitted' and h.Call_Channel = 'Email' and h.Call_Record_Type = 'Other Call') then 'Other Email Call'
	when (h.Call_Status = 'Submitted' and h.Call_Channel = 'Phone' and h.Call_Record_Type = 'Promotional Call') then 'Promotional Phone Call'
	when (h.Call_Status = 'Submitted' and h.Call_Channel = 'Phone' and h.Call_Record_Type = 'Medical Call') then 'Medical Phone Call'
	when (h.Call_Status = 'Submitted' and h.Call_Channel = 'Phone' and h.Call_Record_Type = 'Other Call') then 'Other Phone Call'
	when (h.Call_Status = 'Submitted' and h.Call_Channel = 'Webcall' and h.Call_Record_Type = 'Promotional Call') then 'Promotional Webcall'
	when (h.Call_Status = 'Submitted' and h.Call_Channel = 'Webcall' and h.Call_Record_Type = 'Medical Call') then  'Medical Webcall'
	when (h.Call_Status = 'Submitted' and h.Call_Channel = 'Webcall' and h.Call_Record_Type = 'Other Call') then 'Other Webcall'
	when (h.Call_Status = 'Submitted' and h.Call_Channel = 'Traditional Mail' and h.Call_Record_Type = 'Promotional Call') then 'Promotional Traditional Mail'
	when (h.Call_Status = 'Submitted' and h.Call_Channel = 'Traditional Mail' and h.Call_Record_Type = 'Medical Call') then 'Medical Traditional Mail'
	when (h.Call_Status = 'Submitted' and h.Call_Channel = 'Traditional Mail' and h.Call_Record_Type = 'Other Call') then 'Other Traditional Mail'
else Null end::varchar(28) as Multichannel_Activity_Type,
pt.Call_Product_Topic as Call_Product_Topic,
g.Analytics_Product_Group_vod__c as Product_Analytic_Group_Id,
h.veeva_remote_meeting_id,
h.Call_Proactive_Reactive_Flag,
h.average_call_expenses_lc,
h.total_call_expenses_lc,
h.call_attachments
FROM {{ ref('tmp_call_hierarchy_2') }} h
LEFT OUTER JOIN {{ var('schema') }}.products_business p on p.Id=
	case when Len(replace(h.Aux_Product_Id,' ',''))>0 
		then h.Aux_Product_Id else concat('(FK_THERAREA)_',COALESCE(h.Call_Therapeutical_Area,''),'_',COALESCE(h.Country_Code,'')) 
	end
LEFT OUTER JOIN {{ var('schema') }}.product_group_map_business g on g.Product_vod__c=
	case when (h.Call_Record_Type = 'Medical Call') 
		then concat('(FK_THERAREA)_',COALESCE(h.Call_Therapeutical_Area,''),'_',COALESCE(h.Country_Code,''))
		else case when Len(replace(h.Aux_Product_Id,' ',''))>0 then h.Aux_Product_Id else concat('(FK_THERAREA)_',COALESCE(h.Call_Therapeutical_Area,''),'_',COALESCE(h.Country_Code,'')) end 
	end
LEFT OUTER JOIN (
	SELECT max(product_id) as product_id,product_brand 
	FROM {{ ref('m_product') }} 
	GROUP BY product_brand,product_id
) M on M.product_id=case when Len(replace(h.Aux_Product_Id,' ',''))>0 then h.Aux_Product_Id else concat('(FK_THERAREA)_',COALESCE(h.Call_Therapeutical_Area,''),'_',COALESCE(h.Country_Code,'')) end
LEFT OUTER JOIN (
	SELECT a.product_id, max(a.Product_Brand_Therapeutic_Area_1) as Product_Brand_Therapeutic_Area_1
	FROM (
		SELECT a.product_id, a.Product_Therapeutic_Area_1 as Product_Brand_Therapeutic_Area_1
		FROM {{ ref('m_product') }} a
		LEFT OUTER JOIN (SELECT * FROM {{ ref('m_product') }} WHERE product_type='Brand') b on a.product_brand=b.product_brand
	) a
	GROUP BY a.product_id
) b on b.product_id=case when Len(replace(h.Aux_Product_Id,' ',''))>0 then h.Aux_Product_Id else concat('(FK_THERAREA)_',COALESCE(h.Call_Therapeutical_Area,''),'_',COALESCE(h.Country_Code,'')) end
LEFT OUTER JOIN (
	SELECT M.Product_Topic as Call_Product_Topic, M.Product_Id
	FROM {{ var('schema') }}.products_business t
	JOIN {{ ref('m_product') }} M on M.Product_Id=t.Id
) pt on pt.product_id= 
	case when (h.Call_Record_Type = 'Medical Call') 
		then concat('(FK_THERAREA)_',COALESCE(h.Call_Therapeutical_Area,''),'_',COALESCE(h.Country_Code,''))
		else case when Len(replace(h.Aux_Product_Id,' ',''))>0 then h.Aux_Product_Id else concat('(FK_THERAREA)_',COALESCE(h.Call_Therapeutical_Area,''),'_',COALESCE(h.Country_Code,'')) end 
	end