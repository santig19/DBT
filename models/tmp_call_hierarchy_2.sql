{{ config(
	post_hook="grant all privileges on {{ this }} to {{ var('snowflake_user_grant_privileges') }}")
}}

SELECT
ch.Call_Name_Hierarchy,
ch.Parent_Call,
ch.Child_Call,
ch.Account_Plan_Id,
ch.Call_Therapeutical_Area,
ch.Call_Counter_And_Expenses_Id,
ch.Account_Id,
ch.Brick_Code,
ch.Assignment_Id,
ch.Call_Territory,
ch.Country_Code, 
ch.Call_Attendee_Type,
ch.Call_Attendees,
ch.Call_Comments,
ch.Date,
ch.Call_Date,
ch.Call_Timestamp,
ch.Call_Objective,
ch.Call_Channel,
ch.Call_Type,
ch.Call_CLM,
ch.Call_Currency_Code,
ch.Call_Group_Currency,
ch.Call_Duration,
ch.Call_Id,
ch.is_parent_call,
ch.Call_Attendee_Status,
ch.Call_Status,
ch.Call_Customer_Request_Number,
ch.Call_Customer_Request_Veeva_Link,
ch.Call_IspersonAccount,
ch.Call_Expense_Type,
ch.Call_Device,
ch.Call,
ch.Single_Call_to_Hospital,
ch.Next_Call_Notes,
ch.pre_call_notes,
ch.Call_Proactive,
ch.Call_Record_Type,
ch.Activity,
ch.Activity_Type,
ch.Call_Subtype,
ch.Account_Visited_NonVisited_Technical,
'Call' as Origin,
ch.Aux_Owner_Id,
ch.Aux_User_Id,
ch.Aux_Parent_Call_Id,
ch.Territory_Nominal_Id,
ch.Employee_Id,
ch.Call_Employee_Type,
ch.Region,
ch.Compliance_Region,
ch.Compliance_Country,
ch.Country,
a.Call_Product_Priority,
a.Aux_Product_Id,
a.Call_Detail_Type,
a.Call_Interactions_Counter,
ch.veeva_remote_meeting_id,
ch.Call_Proactive_Reactive_Flag,
ch.average_call_expenses_lc,
ch.total_call_expenses_lc,
ch.call_attachments
FROM {{ ref('tmp_call_hierarchy_1') }} ch
LEFT OUTER JOIN (
	SELECT 
		cd.Detail_Priority_vod__c as Call_Product_Priority,
		cd.Product_vod__c as Aux_Product_Id, 
		cd.Type_vod__c as Call_Detail_Type,
		cd.Call2_vod__c as call_id,
		case when (c.Call_IspersonAccount = 'Yes' or c.Single_Call_to_Hospital = 'Yes') then '1' else '0' end::numeric(10,0) as Call_Interactions_Counter
		FROM {{ var('schema') }}.call_detail_raw cd 
		LEFT OUTER JOIN (
			SELECT DISTINCT Call_IspersonAccount,Single_Call_to_Hospital,Call_Id 
			FROM {{ ref('tmp_f_call') }}
		) c on c.Call_Id=cd.Call2_vod__c 
) a on ch.Call_Id = a.Call_id