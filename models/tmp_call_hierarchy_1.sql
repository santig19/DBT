{{ config(
	post_hook="grant all privileges on {{ this }} to {{ var('snowflake_user_grant_privileges') }}")
}}

SELECT
ch.Call_Name_Hierarchy,
ch.Parent_Call,
ch.Child_Call,
rj.Account_Plan_Id,
rj.Call_Therapeutical_Area,
rj.Call_Counter_And_Expenses_Id,
rj.Account_Id,
rj.Brick_Code,
rj.Assignment_Id,
rj.Call_Territory,
rj.Country_Code, 
rj.Call_Attendee_Type,
rj.Call_Attendees,
rj.Call_Comments,
rj.Date,
rj.Call_Date,
rj.Call_Timestamp,
rj.Call_Objective,
rj.Call_Channel,
rj.Call_Type,
rj.Call_CLM,
rj.Call_Currency_Code,
rj.Call_Group_Currency,
rj.Call_Duration,
rj.Call_Id,
rj.is_parent_call,
rj.Call_Attendee_Status,
rj.Call_Status,
rj.Call_Customer_Request_Number,
rj.Call_Customer_Request_Veeva_Link,
rj.Call_IspersonAccount,
rj.Call_Expense_Type,
rj.Call_Device,
rj.Call,
rj.Single_Call_to_Hospital,
rj.Next_Call_Notes,
rj.Pre_Call_Notes,
rj.Call_Proactive,
rj.Call_Record_Type,
rj.Activity,
rj.Activity_Type,
rj.Call_Subtype,
rj.Account_Visited_NonVisited_Technical,
rj.Origin,
rj.Aux_Owner_Id,
rj.Aux_User_Id,
rj.Aux_Parent_Call_Id,
rj.Territory_Nominal_Id,
rj.Employee_Id,
rj.Call_Employee_Type,
rj.Region,
rj.Compliance_Region,
rj.Compliance_Country,
rj.Country,
rj.veeva_remote_meeting_id,
rj.Call_Proactive_Reactive_Flag,
rj.average_call_expenses_lc,
rj.total_call_expenses_lc,
rj.call_attachments
FROM {{ ref('tmp_call_hierarchy') }} ch
RIGHT JOIN
(
SELECT
a.Account_Plan_Id,
a.Call_Therapeutical_Area,
a.Call_Counter_And_Expenses_Id,
a.Account_Id,
a.Brick_Code,
a.Assignment_Id,
a.Call_Territory,
CASE 
	WHEN a.account_id = ACC.id AND LEN(ACC.CODS_external_id__c)>0 THEN (CASE WHEN acc.Country_jj__C IS NULL THEN acc.jj_country__C ELSE acc.Country_jj__C END)
	WHEN USR.id = a.aux_owner_id THEN cs2.country_iso_code
	ELSE 'NM' 
END::varchar(2) AS Country_Code,  
a.Call_Attendee_Type,
a.Call_Attendees,
a.Call_Comments,
a.Date,
a.Call_Date,
a.Call_Timestamp,
a.Call_Objective,
a.Call_Channel,
a.Call_Type,
a.Call_CLM,
a.Call_Currency_Code,
a.Call_Group_Currency,
a.Call_Duration,
a.Call_Id,
a.is_parent_call,
a.Call_Attendee_Status,
a.Call_Status,
a.Call_Customer_Request_Number,
a.Call_Customer_Request_Veeva_Link,
a.Call_IspersonAccount,
a.Call_Expense_Type,
a.Call_Device,
a.Call,
a.Single_Call_to_Hospital,
a.Next_Call_Notes,
a.Pre_Call_Notes,
a.Call_Proactive,
a.Call_Record_Type,
a.Activity,
a.Activity_Type,
a.Call_Subtype,
a.Account_Visited_NonVisited_Technical,
a.Origin,
a.Aux_Owner_Id,
a.Aux_User_Id,
a.Aux_Parent_Call_Id,
a.Territory_Nominal_Id,
a.Employee_Id,
a.Call_Employee_Type,
cs.jj_Region__c as Region,
cs.jj_Region__c as Compliance_Region,
cs.name Compliance_Country,
cs.name Country,
a.veeva_remote_meeting_id,
a.Call_Proactive_Reactive_Flag,
a.average_call_expenses_lc,
a.total_call_expenses_lc,
a.call_attachments
FROM {{ ref('tmp_f_call_user') }} a
LEFT JOIN {{ source('raw', 'account') }} as ACC ON a.account_id = ACC.id
LEFT JOIN {{ source('raw', 'user') }} AS USR ON USR.id = a.aux_owner_id
LEFT JOIN {{ source('raw', 'country_settings') }} cs on A.Country_Code = cs.jj_Country_ISO_Code__c
LEFT JOIN {{ source('raw', 'country_settings') }} cs2 ON USR.jj_user_country__C = cs2.name
) rj on ch.call_id=rj.call_id