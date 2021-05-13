{{ config(
	post_hook="grant all privileges on {{ this }} to {{ var('snowflake_user_grant_privileges') }}")
}}

SELECT
FC.Account_Plan_Id,
FC.Call_Therapeutical_Area,
FC.Call_Counter_And_Expenses_Id,
FC.Account_Id,
FC.Brick_Code,
FC.Assignment_Id,
FC.Call_Territory,
FC.Country_Code, 
FC.Call_Attendee_Type,
FC.Call_Attendees,
FC.Call_Comments,
FC.Date,
FC.Call_Date,
FC.Call_Timestamp,
FC.Call_Objective,
FC.Call_Channel,
FC.Call_Type,
FC.Call_CLM,
FC.Call_Currency_Code,
FC.Call_Group_Currency,
FC.Call_Duration,
FC.Call_Id,
FC.is_parent_call,
FC.Call_Attendee_Status,
FC.Call_Status,
FC.Call_Customer_Request_Number,
FC.Call_Customer_Request_Veeva_Link,
FC.Call_IspersonAccount,
FC.Call_Expense_Type,
FC.Call_Device,
FC.Call,
FC.Single_Call_to_Hospital,
FC.Next_Call_Notes,
FC.pre_call_notes,
FC.Call_Proactive,
FC.Call_Record_Type,
FC.Activity,
FC.Activity_Type,
FC.Call_Subtype,
FC.Account_Visited_NonVisited_Technical,
FC.Origin,
FC.Aux_Owner_Id,
FC.Aux_User_Id,
FC.Aux_Parent_Call_Id,

---------------------------------------
--FC.Aux_Parent_Call_Id as Parent_Call_Id,
case when coalesce(FC.Aux_Parent_Call_Id, '') = '' then FC.Call_Id else FC.Aux_Parent_Call_Id end as Parent_Call_Id,	
---------------------------------------

c.TerritoryId as Territory_Nominal_Id,
c.Aux_User_Id as Employee_Id,
'Co-Attendee' as Call_Employee_Type,
FC.veeva_remote_meeting_id,
FC.Call_Proactive_Reactive_Flag,
FC.average_call_expenses_lc,
FC.total_call_expenses_lc,
FC.call_attachments
FROM {{ ref('tmp_f_call') }} FC 
INNER JOIN (
            SELECT case when coalesce(FC.aux_parent_call_id,'') = '' then FC.Call_Id else FC.aux_parent_call_id end as Parent_Call_Id,
                  FC.Aux_User_Id,
                  --nvl(u.TerritoryId ,'NM') as TerritoryId
                  case when trim(u.userid )='' or u.userid is null then 'NM' else u.Id end TerritoryId
              FROM {{ ref('tmp_f_call') }} FC
            LEFT JOIN (
                        SELECT max(id) as id, userid
                        FROM (
                              SELECT ut.territory2id as id, ut.UserId
                                FROM {{ source('raw', 'user_territory_association') }} ut
                                JOIN {{ source('raw', 'user') }} u on u.id = ut.userid AND u.isactive != 0
                                JOIN {{ ref('m_territory') }} mt on ut.territory2id=mt.territory_id		
                                          
                            )
                        GROUP BY userid
              
                      ) u on fc.Aux_User_Id = u.UserId
            WHERE 1=1
              and LOWER(FC.Call_Attendee_Type) = 'user_vod'

           ) c
  on case when coalesce(FC.Aux_Parent_Call_Id, '') = '' then FC.Call_Id else FC.Aux_Parent_Call_Id end = c.Parent_Call_Id


WHERE LOWER(FC.Call_Attendee_Type) <> 'user_vod'


UNION ALL

SELECT
FC.Account_Plan_Id,
FC.Call_Therapeutical_Area,
FC.Call_Counter_And_Expenses_Id,
FC.Account_Id,
FC.Brick_Code,
FC.Assignment_Id,
FC.Call_Territory,
FC.Country_Code, 
FC.Call_Attendee_Type,
FC.Call_Attendees,
FC.Call_Comments,
FC.Date,
FC.Call_Date,
FC.Call_Timestamp,
FC.Call_Objective,
FC.Call_Channel,
FC.Call_Type,
FC.Call_CLM,
FC.Call_Currency_Code,
FC.Call_Group_Currency,
FC.Call_Duration,
FC.Call_Id,
FC.is_parent_call,
FC.Call_Attendee_Status,
FC.Call_Status,
FC.Call_Customer_Request_Number,
FC.Call_Customer_Request_Veeva_Link,
FC.Call_IspersonAccount,
FC.Call_Expense_Type,
FC.Call_Device,
FC.Call,
FC.Single_Call_to_Hospital,
FC.Next_Call_Notes,
FC.pre_call_notes,
FC.Call_Proactive,
FC.Call_Record_Type,
FC.Activity,
FC.Activity_Type,
FC.Call_Subtype,
FC.Account_Visited_NonVisited_Technical,
FC.Origin,
FC.Aux_Owner_Id,
FC.Aux_User_Id,
FC.Aux_Parent_Call_Id,
'' as Parent_Call_Id,
FC.Territory_Nominal_Id,
FC.Employee_Id,
FC.Call_Employee_Type,
FC.veeva_remote_meeting_id,
FC.Call_Proactive_Reactive_Flag,
FC.average_call_expenses_lc,
FC.total_call_expenses_lc,
FC.call_attachments
FROM {{ ref('tmp_f_call_account') }} FC