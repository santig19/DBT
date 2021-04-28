{{ config(
	post_hook="grant all privileges on {{ this }} to {{ var('snowflake_user_grant_privileges') }}")
}}

SELECT 
	Event_Avg_Cost_Per_Attendee_LC						::varchar(255) as Event_Avg_Cost_Per_Attendee_LC, 
	Event_Finalised_Date								::varchar(255) as Event_Finalised_Date,
	Event_Avg_Cost_Per_Attendee_GC 						::numeric(32,16) as Event_Avg_Cost_Per_Attendee_GC,
	Event_Total_Cost_LC 								::varchar(255) as Event_Total_Cost_LC, 
	Event_Total_Cost_GC 								::numeric(32,16) as Event_Total_Cost_GC,
	Event_Created_By_Id 								::varchar(255) as Event_Created_By_Id, 
	Event_Created_By_Employee 							::varchar(255) as Event_Created_By_Employee, 
	Event_Created_Date 									::varchar(255) as Event_Created_Date,
	Event_Created_Timestamp 							::varchar(255) as Event_Created_Timestamp,
	Event_Local_Currency 								::varchar(255) as Event_Local_Currency, 
	Event_Group_Currency 								::varchar(255) as Event_Group_Currency, 
	Event_End_Date 										::varchar(255) as Event_End_Date,
	Event_End_Timestamp 								::varchar(255) as Event_End_Timestamp,
	Event_MonthYear 									::varchar(255) as Event_MonthYear,
	Event_Month 										::varchar(255) as Event_Month,
	Event_Year 											::varchar(255) as Event_Year,
	Event_Date 											::varchar(255) as Event_Date,
	Event_Start_Timestamp 								::varchar(255) as Event_Start_Timestamp,
	Event_Number 										::varchar(255) as Event_Number, 
	Event_Requestor_Name 								::varchar(255) as Event_Requestor_Name, 
	Event_Status 										::varchar(255) as Event_Status,
	Event_Type 											::varchar(255) as Event_Type,
	Event_Expected_Janssen_Attendees 					::varchar(255) as Event_Expected_Janssen_Attendees,
	Event_Expected_Total_Attendees 						::varchar(255) as Event_Expected_Total_Attendees,
	Event_Expected_Speakers_Chairmen 					::varchar(255) as Event_Expected_Speakers_Chairmen,
	Event_Forecasted_Total_Cost_LC 						::varchar(255) as Event_Forecasted_Total_Cost_LC,
	Event_Forecasted_Total_Cost_GC 						::numeric(32,16) as Event_Forecasted_Total_Cost_GC,
	Event_Id 											::varchar(255) as Event_Id,
	Event_Account_Id 									::varchar(255) as Event_Account_Id,
	Event_Account 										::varchar(65535) as Event_Account,
	Event_Location_Country_Code 						::varchar(255) as Event_Location_Country_Code,
	Event_Location_City 								::varchar(1000) as Event_Location_City,
	Event_Location  									::varchar(255) as Event_Location,
	Event  												::varchar(255) as Event,
	Event_Objective 									::varchar(65535) as Event_Objective,
	Event_NPS 											::numeric(32,16) as Event_NPS,
	Event_Country   									::varchar(255) as Event_Country,
	Event_Region   										::varchar(255) as Event_Region,
	Event_Country_Code   								::varchar(255) as Event_Country_Code,
	Event_Confirm_Approved_Materials  					::varchar(1000) as Event_Confirm_Approved_Materials,
	Event_Within_Framework   							::varchar(1000) as Event_Within_Framework,
	Event_Unitas_Number   								::varchar(1000) as Event_Unitas_Number,
	Event_Received_Contract_Date   						::varchar(255) as Event_Received_Contract_Date,
	Event_Sent_to_HPR 									::varchar(255) as Event_Sent_to_HPR,
	Event_Medical_Approved_Materials   					::varchar(1000) as Event_Medical_Approved_Materials,
	Event_Provider_Id 									::varchar(255) as Event_Provider_Id,
	Date 												::varchar(255) as date,
	Employee_Id 										::varchar(255) as Employee_Id,
	Territory_Nominal_Id 								::varchar(255) as Territory_Nominal_Id,
	Product_Id 											::varchar(255) as Product_Id,
	Event_Record_Type 									::varchar(255) as Event_Record_Type,
	Event_Last_Modification_Date 						::varchar(255) as Event_Last_Modification_Date,
	Event_CME_CPD_Accredited 							::varchar(255) as Event_CME_CPD_Accredited,
	Event_Sub_Type   									::varchar(1000) as Event_Sub_Type,
	Event_Scientific_3rd_Party_Implicated 				::varchar(65535) as Event_Scientific_3rd_Party_Implicated,
	ISO_Country_Code 									::varchar(1000) as ISO_Country_Code,
	Event_ICD_Contract_Status 							::varchar(1000) as Event_ICD_Contract_Status,
	Event_ICD_Number 									::varchar(255) as Event_ICD_Number,
	Event_Counter  										::numeric(32,16) as Event_Counter,
	Event_Public_Name 									::varchar(1000) as Event_Public_Name,
	Event_External_Reference_Number 					::varchar(255) as Event_External_Reference_Number,
	Event_EMEA_Parent_Event_Number 						::varchar(1000) as Event_EMEA_Parent_Event_Number,
	Event_Approval_Starter_Employee 					::varchar(255) as Event_Approval_Starter_Employee,
	Event_Approval_Id 									::varchar(255) as Event_Approval_Id,
	Event_Approval_Status 								::varchar(255) as Event_Approval_Status,
	Event_Approval_Start_Timestamp 						::varchar(255) as Event_Approval_Start_Timestamp,
	Event_Approval_Step1_Employee 						::varchar(255) as Event_Approval_Step1_Employee, 
	Event_Approval_step1_Status 						::varchar(255) as Event_Approval_step1_Status,
	Event_Approval_step1_Timestamp 						::varchar(255) as Event_Approval_step1_Timestamp,
	Event_Approval_Step2_Employee 						::varchar(255) as Event_Approval_Step2_Employee,
	Event_Approval_step2_Status 						::varchar(255) as Event_Approval_step2_Status,
	Event_Approval_step2_Timestamp 						::varchar(255) as Event_Approval_step2_Timestamp,
	Event_Approval_Step3_Employee 						::varchar(255) as Event_Approval_Step3_Employee,
	Event_Approval_step3_Status 						::varchar(255) as Event_Approval_step3_Status,
	Event_Approval_step3_Timestamp 						::varchar(255) as Event_Approval_step3_Timestamp,
	(DATEDIFF(second, TO_TIMESTAMP(Event_Created_Timestamp, 'dd-mm-yyyy HH24:mi:ss')::TIMESTAMP, TO_TIMESTAMP(Event_Approval_Start_Timestamp, 'yyyymmdd HH24:mi:ss')::TIMESTAMP)::numeric(38,18)/86400)::numeric(38,18) as Event_Time_To_Approve,
	case when (Event_Approval_Step2_Status = 'Approved') then 'Event Approved by HCC and Line Manager' else case when (Event_Approval_Step1_Status = 'Approved') then 'Event Approved only by Line Manager' else Null end end ::varchar(255) as Event_Approved_By,
	case when (Event_Approval_Step1_Status = 'Approved') then (DATEDIFF(second, TO_TIMESTAMP(Event_Approval_Start_Timestamp, 'yyyymmdd HH24:mi:ss')::TIMESTAMP, TO_TIMESTAMP(Event_Approval_Step1_Timestamp, 'yyyymmdd HH24:mi:ss')::TIMESTAMP)::numeric(38,18)/86400)::numeric(38,18) else null end ::numeric(38,18) as Event_Time_To_Approve_Step1,
	case when (Event_Approval_Status = 'Approved') then case when (DATEDIFF(second,TO_TIMESTAMP(Event_Approval_Step1_Timestamp, 'yyyymmdd HH24:mi:ss')::TIMESTAMP,TO_TIMESTAMP(Event_Approval_Step2_Timestamp, 'yyyymmdd HH24:mi:ss')::TIMESTAMP)::numeric(38,18)/86400)::numeric(38,18) >0 then (DATEDIFF(second,TO_TIMESTAMP(Event_Approval_Step1_Timestamp, 'yyyymmdd HH24:mi:ss')::TIMESTAMP,TO_TIMESTAMP(Event_Approval_Step2_Timestamp, 'yyyymmdd HH24:mi:ss')::TIMESTAMP)::numeric(38,18)/86400)::numeric(38,18) else 0 end else 0 end ::numeric(38,18) as Event_Time_To_Approve_Step2,
	(DATEDIFF(second, TO_TIMESTAMP(Event_Start_Timestamp, 'DD-MM-YYYY HH24:MI:SS')::TIMESTAMP, TO_TIMESTAMP(Event_End_Timestamp, 'DD-MM-YYYY HH24:MI:SS')::TIMESTAMP) ::numeric(38,18)/3600)::numeric(38,18) as Event_Duration,
	case when (Event_Approval_Status = 'Approved') then Event_Approval_Step2_Timestamp else null end ::varchar(255) as MonthYear_Event_Approved,
	case when(Event_Approval_Status = 'Approved') then 
		case when(DATEDIFF(day, TO_DATE(Event_Last_Modification_Date::varchar(255), 'YYYYMMDD HH24:MI:SS'),to_date(Event_Date,'dd-mm-yyyy')) >=7) then 'Events approved more than one week before the event' else
			case when(DATEDIFF(day, TO_DATE(Event_Last_Modification_Date::varchar(255), 'YYYYMMDD HH24:MI:SS'),to_date(Event_Date,'dd-mm-yyyy')) <7 and DATEDIFF(day, TO_DATE(Event_Last_Modification_Date::varchar(255), 'YYYYMMDD HH24:MI:SS'),to_date(Event_Date,'dd-mm-yyyy')) >0 and (to_char(TO_date(Event_Approval_Step2_Timestamp,'YYYYMMDD HH24:MI:SS'),'YYYYMMDD') =  to_char(to_date(Event_Start_Timestamp,'DD-MM-YYYY HH24:MI:SS'),'YYYYMMDD'))) then 'Events approved in the same week as the event date' else
				case when( to_char(TO_date(Event_Approval_Step2_Timestamp,'YYYYMMDD HH24:MI:SS'),'YYYYMMDD') = to_char(to_date(Event_Start_Timestamp,'DD-MM-YYYY HH24:MI:SS'),'YYYYMMDD')) then 'Events approved on the same day as the event' else 
					case when( DATEDIFF(day, TO_DATE(Event_Last_Modification_Date::varchar(255), 'YYYYMMDD HH24:MI:SS'),to_date(Event_Date,'dd-mm-yyyy')) <0 and to_char(TO_date(Event_Approval_Step2_Timestamp,'YYYYMMDD HH24:MI:SS'),'YYYYMMDD') =  to_char(to_date(Event_Start_Timestamp,'DD-MM-YYYY HH24:MI:SS'),'YYYYMMDD')) then 'Events approved after event date' else
						case when(Event_Approval_Step2_Timestamp is Null) then 'Events without approval flow information'
						ELSE 'Events without approval flow information' 
						end
					end
				end
			end
		end
	end ::varchar(255) as Event_Approved_When,
	case when Event_Finalised_Date is not Null and Event_Finalised_Date != '' and (DATEDIFF(day, to_date(Event_Finalised_Date,'YYYYMMDD HH24:MI:SS'), TO_DATE(TO_TIMESTAMP(Event_End_Date, 'dd-mm-yyyy')::TIMESTAMP)) < 30) then 0 else
		case when (Event_Status = 'Finalised' and Event_Finalised_Date is not Null) then 1 else 
			case when (Event_Status = 'Approved' and (DATEDIFF(day,to_date(current_date()),to_date(Event_End_Date,'dd-mm-yyyy')) > 30)) then 1 else null end
		end
	end ::numeric(32) as Event_Not_Finalised_Thirty_Days_After_Counter,
	case when Event_Status = 'Finalised' and Event_Finalised_Date is not Null and Event_Finalised_Date != '' and (DATEDIFF(day, to_date(Event_End_Date,'dd-mm-yyyy'), TO_DATE(TO_TIMESTAMP(Event_Finalised_Date, 'YYYYMMDD HH24:MI:SS')::TIMESTAMP)) < 30) then 'Finalized before 30 days' else 
		case when (Event_Status = 'Finalised' and Event_Finalised_Date is not Null) then 'Not finalized in 30 days' else 
			case when (Event_Status = 'Approved' and (DATEDIFF(day,to_date(Event_End_Date,'dd-mm-yyyy'),to_date(current_date())) > 30)) then 'Not finalized in 30 days' 
			else Null end
		end
	end ::varchar(255) as Event_is_Finalised_Thirty_Days_After,
	case when (Event_Status = 'Finalised') 
		then case when Event_Finalised_Date != '' and DATEDIFF(second,TO_TIMESTAMP(Event_Finalised_Date, 'yyyymmdd HH24:mi:ss')::TIMESTAMP, to_date(Event_End_Date,'dd-mm-yyyy')::TIMESTAMP)>0 
				then ((DATEDIFF(second, to_date(Event_End_Date,'dd-mm-yyyy')::TIMESTAMP, TO_TIMESTAMP(Event_Finalised_Date, 'yyyymmdd HH24:mi:ss')::TIMESTAMP))::numeric(38,18)/86400)::numeric(38,18) 
				else 0 end 
		else 0 end ::numeric(38,18) as Event_Time_to_Close,
	case when(Event_NPS <0) then '< 0' else
		case when(Event_NPS >= 0 and Event_NPS <10) then '0 - 10 %' else 
			case when(Event_NPS >=10 and Event_NPS <25) then '10 - 25 %' else
				case when(Event_NPS >=25 and Event_NPS <50) then '25 - 50 %' else
					case when(Event_NPS >=50 and Event_NPS <75) then '50 - 75 %' else
						case when(Event_NPS >=75 and Event_NPS <=100) then '75 - 100 %'  
						else Null end
					end
				end
			end
		end
	end ::varchar(255) as Event_NPS_Percentage,
	Event_Meeting_Style						::varchar(1000) as Event_Meeting_Style,
	Event_Meeting_channel 						AS Event_Meeting_channel,
	Event_WPP_iMR_UNITAS_number 				AS Event_WPP_iMR_UNITAS_number,
	Event_WPP_I_will_publish_approved_materials AS Event_WPP_I_will_publish_approved_materials,
	Content_Topic::varchar(16777216) 				AS Content_Topic,
	Content_Sub_Topic::varchar(255) 			AS Content_Sub_Topic,
	Key_Message_Group::varchar(16777216) 			AS Key_Message_Group,
	event_original_creator_territory::varchar(510) as event_original_creator_territory,
	event_csat_score::decimal(5,0) as event_csat_score,
	event_csat_status::varchar(2600) as event_csat_status,
	event_publication_name_title::varchar(2600) as event_publication_name_title,
	event_jmc_event_name::varchar(510) as event_jmc_event_name,
	event_details::varchar(16777216) as event_details,
	event_quick_eevent::integer as event_quick_eevent,
	event_publish_to_web::varchar(510) as event_publish_to_web,
	event_automatic_web_publication_start_date::datetime as event_automatic_web_publication_start_date,
	event_automatic_web_publication_end_date::datetime as event_automatic_web_publication_end_date,
	event_start_hour::varchar(510) as event_start_hour,
	event_end_hour::varchar(510) as event_end_hour,
	event_live_event::integer as event_live_event,
	event_live_event_start::datetime as event_live_event_start,
	event_live_event_end::datetime as event_live_event_end,
	event_live_event_automatic_alert::integer as event_live_event_automatic_alert,
	event_published_on_jmc_ion::integer as event_published_on_jmc_ion,
	event_eloqua_access_token::varchar(510) as event_eloqua_access_token,
	event_automatic_registration::integer as event_automatic_registration,
	event_link::varchar(2600) as event_link
FROM {{ ref('tmp_f_event_1') }}
WHERE (Event_Approval_Step2_Timestamp='' OR Event_Approval_Step2_Timestamp is null)
AND (SELECT COUNT(*) FROM {{ ref('tmp_f_event_1') }} WHERE Event_Approval_Step2_Timestamp = '' OR Event_Approval_Step2_Timestamp is null) > 0

UNION

--line 193 from transformation excel
SELECT 
	Event_Avg_Cost_Per_Attendee_LC						::varchar(255) as Event_Avg_Cost_Per_Attendee_LC, 
	Event_Finalised_Date								::varchar(255) as Event_Finalised_Date,
	Event_Avg_Cost_Per_Attendee_GC 						::numeric(32,16) as Event_Avg_Cost_Per_Attendee_GC,
	Event_Total_Cost_LC 								::varchar(255) as Event_Total_Cost_LC, 
	Event_Total_Cost_GC 								::numeric(32,16) as Event_Total_Cost_GC,
	Event_Created_By_Id 								::varchar(255) as Event_Created_By_Id, 
	Event_Created_By_Employee 							::varchar(255) as Event_Created_By_Employee, 
	Event_Created_Date 									::varchar(255) as Event_Created_Date,
	Event_Created_Timestamp 							::varchar(255) as Event_Created_Timestamp,
	Event_Local_Currency 								::varchar(255) as Event_Local_Currency, 
	Event_Group_Currency 								::varchar(255) as Event_Group_Currency, 
	Event_End_Date 										::varchar(255) as Event_End_Date,
	Event_End_Timestamp 								::varchar(255) as Event_End_Timestamp,
	Event_MonthYear 									::varchar(255) as Event_MonthYear,
	Event_Month 										::varchar(255) as Event_Month,
	Event_Year 											::varchar(255) as Event_Year,
	Event_Date 											::varchar(255) as Event_Date,
	Event_Start_Timestamp 								::varchar(255) as Event_Start_Timestamp,
	Event_Number 										::varchar(255) as Event_Number, 
	Event_Requestor_Name 								::varchar(255) as Event_Requestor_Name, 
	Event_Status 										::varchar(255) as Event_Status,
	Event_Type 											::varchar(255) as Event_Type,
	Event_Expected_Janssen_Attendees 					::varchar(255) as Event_Expected_Janssen_Attendees,
	Event_Expected_Total_Attendees 						::varchar(255) as Event_Expected_Total_Attendees,
	Event_Expected_Speakers_Chairmen 					::varchar(255) as Event_Expected_Speakers_Chairmen,
	Event_Forecasted_Total_Cost_LC 						::varchar(255) as Event_Forecasted_Total_Cost_LC,
	Event_Forecasted_Total_Cost_GC 						::numeric(32,16) as Event_Forecasted_Total_Cost_GC,
	Event_Id 											::varchar(255) as Event_Id,
	Event_Account_Id 									::varchar(255) as Event_Account_Id,
	Event_Account 										::varchar(65535) as Event_Account,
	Event_Location_Country_Code 						::varchar(255) as Event_Location_Country_Code,
	Event_Location_City 								::varchar(1000) as Event_Location_City,
	Event_Location  									::varchar(255) as Event_Location,
	Event  												::varchar(255) as Event,
	Event_Objective 									::varchar(65535) as Event_Objective,
	Event_NPS 											::numeric(32,16) as Event_NPS,
	Event_Country   									::varchar(255) as Event_Country,
	Event_Region   										::varchar(255) as Event_Region,
	Event_Country_Code   								::varchar(255) as Event_Country_Code,
	Event_Confirm_Approved_Materials  					::varchar(1000) as Event_Confirm_Approved_Materials,
	Event_Within_Framework   							::varchar(1000) as Event_Within_Framework,
	Event_Unitas_Number   								::varchar(1000) as Event_Unitas_Number,
	Event_Received_Contract_Date   						::varchar(255) as Event_Received_Contract_Date,
	Event_Sent_to_HPR 									::varchar(255) as Event_Sent_to_HPR,
	Event_Medical_Approved_Materials   					::varchar(1000) as Event_Medical_Approved_Materials,
	Event_Provider_Id 									::varchar(255) as Event_Provider_Id,
	Date 												::varchar(255) as date,
	Employee_Id 										::varchar(255) as Employee_Id,
	Territory_Nominal_Id 								::varchar(255) as Territory_Nominal_Id,
	Product_Id 											::varchar(255) as Product_Id,
	Event_Record_Type 									::varchar(255) as Event_Record_Type,
	Event_Last_Modification_Date 						::varchar(255) as Event_Last_Modification_Date,
	Event_CME_CPD_Accredited 							::varchar(255) as Event_CME_CPD_Accredited,
	Event_Sub_Type   									::varchar(1000) as Event_Sub_Type,
	Event_Scientific_3rd_Party_Implicated 				::varchar(65535) as Event_Scientific_3rd_Party_Implicated,
	ISO_Country_Code 									::varchar(1000) as ISO_Country_Code,
	Event_ICD_Contract_Status 							::varchar(1000) as Event_ICD_Contract_Status,
	Event_ICD_Number 									::varchar(255) as Event_ICD_Number,
	Event_Counter  										::numeric(32,16) as Event_Counter,
	Event_Public_Name 									::varchar(1000) as Event_Public_Name,
	Event_External_Reference_Number 					::varchar(255) as Event_External_Reference_Number,
	Event_EMEA_Parent_Event_Number 						::varchar(1000) as Event_EMEA_Parent_Event_Number,
	Event_Approval_Starter_Employee 					::varchar(255) as Event_Approval_Starter_Employee,
	Event_Approval_Id 									::varchar(255) as Event_Approval_Id,
	Event_Approval_Status 								::varchar(255) as Event_Approval_Status,
	Event_Approval_Start_Timestamp 						::varchar(255) as Event_Approval_Start_Timestamp,
	Event_Approval_Step1_Employee 						::varchar(255) as Event_Approval_Step1_Employee, 
	Event_Approval_step1_Status 						::varchar(255) as Event_Approval_step1_Status,
	Event_Approval_step1_Timestamp 						::varchar(255) as Event_Approval_step1_Timestamp,
	Event_Approval_Step2_Employee 						::varchar(255) as Event_Approval_Step2_Employee,
	Event_Approval_step2_Status 						::varchar(255) as Event_Approval_step2_Status,
	Event_Approval_step2_Timestamp 						::varchar(255) as Event_Approval_step2_Timestamp,
	Event_Approval_Step3_Employee 						::varchar(255) as Event_Approval_Step3_Employee,
	Event_Approval_step3_Status 						::varchar(255) as Event_Approval_step3_Status,
	Event_Approval_step3_Timestamp 						::varchar(255) as Event_Approval_step3_Timestamp,

	(DATEDIFF(second, TO_TIMESTAMP(Event_Created_Timestamp, 'dd-mm-yyyy HH24:mi:ss')::TIMESTAMP, TO_TIMESTAMP(Event_Approval_Start_Timestamp, 'yyyymmdd HH24:mi:ss')::TIMESTAMP)::numeric(38,18)/86400)::numeric(38,18) as Event_Time_To_Approve,
	case when (Event_Approval_Step2_Status = 'Approved') then 'Event Approved by HCC and Line Manager' else case when (Event_Approval_Step1_Status = 'Approved') then 'Event Approved only by Line Manager' else Null end end ::varchar(255) as Event_Approved_By,
	case when (Event_Approval_Step1_Status = 'Approved') then (DATEDIFF(second, TO_TIMESTAMP(Event_Approval_Start_Timestamp, 'yyyymmdd HH24:mi:ss')::TIMESTAMP, TO_TIMESTAMP(Event_Approval_Step1_Timestamp, 'yyyymmdd HH24:mi:ss')::TIMESTAMP)::numeric(38,18)/86400)::numeric(38,18) else null end ::numeric(38,18) as Event_Time_To_Approve_Step1,
	case when (Event_Approval_Status = 'Approved') then case when (DATEDIFF(second,TO_TIMESTAMP(Event_Approval_Step1_Timestamp, 'yyyymmdd HH24:mi:ss')::TIMESTAMP,TO_TIMESTAMP(Event_Approval_Step2_Timestamp, 'yyyymmdd HH24:mi:ss')::TIMESTAMP)::numeric(38,18)/86400)::numeric(38,18) >0 then (DATEDIFF(second,TO_TIMESTAMP(Event_Approval_Step1_Timestamp, 'yyyymmdd HH24:mi:ss')::TIMESTAMP,TO_TIMESTAMP(Event_Approval_Step2_Timestamp, 'yyyymmdd HH24:mi:ss')::TIMESTAMP)::numeric(38,18)/86400)::numeric(38,18) else 0 end else 0 end ::numeric(38,18) as Event_Time_To_Approve_Step2,
	(DATEDIFF(second, TO_TIMESTAMP(Event_Start_Timestamp, 'DD-MM-YYYY HH24:MI:SS')::TIMESTAMP, TO_TIMESTAMP(Event_End_Timestamp, 'DD-MM-YYYY HH24:MI:SS')::TIMESTAMP)::numeric(38,18)/3600)::numeric(38,18) as Event_Duration,
	case when (Event_Approval_Status = 'Approved') then Event_Approval_Step2_Timestamp else null end ::varchar(255) as MonthYear_Event_Approved,
	case when(Event_Approval_Status = 'Approved') then 
		case when(DATEDIFF(day, TO_DATE(Event_Last_Modification_Date::varchar(255), 'YYYYMMDD HH24:MI:SS'), to_date(Event_Date,'dd-mm-yyyy')) >=7) then 'Events approved more than one week before the event' else
			case when(DATEDIFF(day, TO_DATE(Event_Last_Modification_Date::varchar(255), 'YYYYMMDD HH24:MI:SS'), to_date(Event_Date,'dd-mm-yyyy')) <7 and DATEDIFF(day, TO_DATE(Event_Last_Modification_Date::varchar(255), 'YYYYMMDD HH24:MI:SS'), to_date(Event_Date,'dd-mm-yyyy')) >0 and (to_char(TO_date(Event_Approval_Step2_Timestamp,'YYYYMMDD HH24:MI:SS'),'YYYYMMDD') =  to_char(to_date(Event_Start_Timestamp,'DD-MM-YYYY HH24:MI:SS'),'YYYYMMDD'))) then 'Events approved in the same week as the event date' else
				case when( to_char(TO_date(Event_Approval_Step2_Timestamp,'YYYYMMDD HH24:MI:SS'),'YYYYMMDD') = to_char(to_date(Event_Start_Timestamp,'DD-MM-YYYY HH24:MI:SS'),'YYYYMMDD')) then 'Events approved on the same day as the event' else 
					case when( DATEDIFF(day, TO_DATE(Event_Last_Modification_Date::varchar(255), 'YYYYMMDD HH24:MI:SS'), to_date(Event_Date,'dd-mm-yyyy')) <0 and to_char(TO_date(Event_Approval_Step2_Timestamp,'YYYYMMDD HH24:MI:SS'),'YYYYMMDD') =  to_char(to_date(Event_Start_Timestamp,'DD-MM-YYYY HH24:MI:SS'),'YYYYMMDD')) then 'Events approved after event date' else
						case when(Event_Approval_Step2_Timestamp is Null) then 'Events without approval flow information'
						ELSE 'Events without approval flow information' 
						end
					end
				end
			end
		end
	end ::varchar(255) as Event_Approved_When,
	case when Event_Finalised_Date is not Null and Event_Finalised_Date != '' and (DATEDIFF(day, to_date(Event_Finalised_Date,'YYYYMMDD HH24:MI:SS'), TO_DATE(TO_TIMESTAMP(Event_End_Date, 'dd-mm-yyyy')::TIMESTAMP)) < 30) then 0 else
		case when (Event_Status = 'Finalised' and Event_Finalised_Date is not Null) then 1 else 
			case when (Event_Status = 'Approved' and (DATEDIFF(day,to_date(current_date()),to_date(Event_End_Date,'dd-mm-yyyy')) > 30)) then 1 else null end
		end
	end ::numeric(32) as Event_Not_Finalised_Thirty_Days_After_Counter,
	case when Event_Status = 'Finalised' and Event_Finalised_Date is not Null and Event_Finalised_Date != '' and (DATEDIFF(day, to_date(Event_End_Date,'dd-mm-yyyy'), TO_DATE(TO_TIMESTAMP(Event_Finalised_Date, 'YYYYMMDD HH24:MI:SS')::TIMESTAMP)) < 30) then 'Finalized before 30 days' else 
		case when (Event_Status = 'Finalised' and Event_Finalised_Date is not Null) then 'Not finalized in 30 days' else 
			case when (Event_Status = 'Approved' and (DATEDIFF(day,to_date(Event_End_Date,'dd-mm-yyyy'),to_date(current_date())) > 30)) then 'Not finalized in 30 days' 
			else Null end
		end
	end ::varchar(255) as Event_is_Finalised_Thirty_Days_After,
	case when (Event_Status = 'Finalised') 
		then case when Event_Finalised_Date != '' and (DATEDIFF(second, TO_TIMESTAMP(Event_Finalised_Date, 'yyyymmdd HH24:mi:ss')::TIMESTAMP, to_date(Event_End_Date,'dd-mm-yyyy')::TIMESTAMP))>0 
				then ((DATEDIFF(second, to_date(Event_End_Date,'dd-mm-yyyy')::TIMESTAMP, TO_TIMESTAMP(Event_Finalised_Date, 'yyyymmdd HH24:mi:ss')::TIMESTAMP))::numeric(38,18)/86400)::numeric(38,18) 
				else 0 end 
		else 0 end ::numeric(38,18) as Event_Time_to_Close,
	case when(Event_NPS <0) then '< 0' else
		case when(Event_NPS >= 0 and Event_NPS <10) then '0 - 10 %' else 
			case when(Event_NPS >=10 and Event_NPS <25) then '10 - 25 %' else
				case when(Event_NPS >=25 and Event_NPS <50) then '25 - 50 %' else
					case when(Event_NPS >=50 and Event_NPS <75) then '50 - 75 %' else
						case when(Event_NPS >=75 and Event_NPS <=100) then '75 - 100 %'  
						else Null end
					end
				end
			end
		end
	end ::varchar(255) as Event_NPS_Percentage,
	Event_Meeting_Style::varchar(1000)          as Event_Meeting_Style,
	Event_Meeting_channel 						as Event_Meeting_channel,
	Event_WPP_iMR_UNITAS_number 				as Event_WPP_iMR_UNITAS_number,
	Event_WPP_I_will_publish_approved_materials as Event_WPP_I_will_publish_approved_materials,
	Content_Topic::varchar(16777216) 				as Content_Topic,
	Content_Sub_Topic::varchar(255) 			as Content_Sub_Topic,
	Key_Message_Group::varchar(16777216) 			as Key_Message_Group,
	event_original_creator_territory::varchar(510) as event_original_creator_territory,
	event_csat_score::decimal(5,0) as event_csat_score,
	event_csat_status::varchar(2600) as event_csat_status,
	event_publication_name_title::varchar(2600) as event_publication_name_title,
	event_jmc_event_name::varchar(510) as event_jmc_event_name,
	event_details::varchar(16777216) as event_details,
	event_quick_eevent::integer as event_quick_eevent,
	event_publish_to_web::varchar(510) as event_publish_to_web,
	event_automatic_web_publication_start_date::datetime as event_automatic_web_publication_start_date,
	event_automatic_web_publication_end_date::datetime as event_automatic_web_publication_end_date,
	event_start_hour::varchar(510) as event_start_hour,
	event_end_hour::varchar(510) as event_end_hour,
	event_live_event::integer as event_live_event,
	event_live_event_start::datetime as event_live_event_start,
	event_live_event_end::datetime as event_live_event_end,
	event_live_event_automatic_alert::integer as event_live_event_automatic_alert,
	event_published_on_jmc_ion::integer as event_published_on_jmc_ion,
	event_eloqua_access_token::varchar(510) as event_eloqua_access_token,
	event_automatic_registration::integer as event_automatic_registration,
	event_link::varchar(2600) as event_link
FROM {{ ref('tmp_f_event_1') }}
WHERE Event_Approval_Step2_Timestamp <> ''
