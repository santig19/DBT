{{ config(
	post_hook="grant all privileges on {{ this }} to {{ var('snowflake_user_grant_privileges') }}")
}}

SELECT
FC.Account_Plan_Id 						          as Account_Plan_Id,
FC.Call_Therapeutical_Area 				      as Call_Therapeutical_Area,
FC.Call_Counter_And_Expenses_Id 			  as Call_Counter_And_Expenses_Id,
FC.Account_Id  						              as Account_Id,
FC.Brick_Code 							            as Brick_Code,
FC.Assignment_Id 						            as Assignment_Id,
FC.Call_Territory 						          as Call_Territory,
FC.Country_Code 						            as Country_Code, 
FC.Call_Attendee_Type 					        as Call_Attendee_Type,
FC.Call_Attendees  						          as Call_Attendees,
FC.Call_Comments 						            as Call_Comments,
FC.Date 								                as Date,
FC.Call_Date 							              as Call_Date,
FC.Call_Timestamp					              as Call_Timestamp,
FC.Call_Objective 				              as Call_Objective,
FC.Call_Channel 						            as Call_Channel,
FC.Call_Type							              as Call_Type,
FC.Call_CLM 							              as Call_CLM,
FC.Call_Currency_Code 					        as Call_Currency_Code,
FC.Call_Group_Currency 					        as Call_Group_Currency,
FC.Call_Duration 						            as Call_Duration,
FC.Call_Id							                as Call_Id,
FC.is_parent_call 						          as is_parent_call,
FC.Call_Attendee_Status 					      as Call_Attendee_Status,
FC.Call_Status							            as Call_Status,
FC.Call_Customer_Request_Number 			  as Call_Customer_Request_Number,
FC.Call_Customer_Request_Veeva_Link		  as Call_Customer_Request_Veeva_Link, 
FC.Call_IspersonAccount 					      as Call_IspersonAccount,
FC.Call_Expense_Type 					          as Call_Expense_Type,
FC.Call_Device 						              as Call_Device,
FC.Call							                	  as Call,
FC.Single_Call_to_Hospital 				      as Single_Call_to_Hospital,
FC.Next_Call_Notes 						          as Next_Call_Notes,
FC.pre_call_notes						            as pre_call_notes,
FC.Call_Proactive 						          as Call_Proactive,
FC.Call_Record_Type						          as Call_Record_Type,
FC.Activity 							              as Activity,
FC.Activity_Type 						            as Activity_Type,
FC.Call_Subtype 						            as Call_Subtype,
FC.Account_Visited_NonVisited_Technical	as Account_Visited_NonVisited_Technical,
FC.Origin								                as Origin,
FC.Aux_Owner_Id 						            as Aux_Owner_Id,
FC.Aux_User_Id 						              as Aux_User_Id,

--------------------------------------------------
--FC.Aux_Parent_Call_Id 					as Aux_Parent_Call_Id,
case
  when coalesce(FC.Aux_Parent_Call_Id, '') = '' 
    then FC.Call_Id 
  else FC.Aux_Parent_Call_Id 
end                                     as Aux_Parent_Call_Id,
--------------------------------------------------

--nvl(U.TerritoryId,'NM') 				as Territory_Nominal_Id,
case 
  when trim(u.userid )= '' or u.userid is null 
    then 'NM' 
  else u.Id 
end::varchar(255)                       as Territory_Nominal_Id,		
FC.Aux_Owner_Id 						            as Employee_Id,
'Owner'								                  as Call_Employee_Type,
veeva_remote_meeting_id,
FC.Call_Proactive_Reactive_Flag,
FC.average_call_expenses_lc,
FC.total_call_expenses_lc,
FC.call_attachments
FROM {{ ref('tmp_f_call') }} FC 
LEFT JOIN (
            SELECT max(id) as id, userid
            FROM (
                  SELECT ut.territory2id as id, ut.UserId
                  FROM {{ var('schema') }}.user_territory_association_raw ut
                  JOIN {{ var('schema') }}.user_raw u on u.id = ut.userid AND u.isactive != 0
                  JOIN {{ ref('m_territory') }} mt on ut.territory2id=mt.territory_id	
                 )
            GROUP BY userid
          ) u ON u.USERID = FC.Aux_Owner_Id 
WHERE FC.Call_Attendee_Type <> 'User_vod'