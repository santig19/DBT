{{ config(
	post_hook="grant all privileges on {{ this }} to {{ var('snowflake_user_grant_privileges') }}")
}}

SELECT
c.Account_vod__c as Account_Id,
case when (c.Account_vod__c = a.Id) 
	then case when a.Country_JJ__c <> '' 
        then a.Country_JJ__c 
        else COALESCE(a.JJ_Country__c,'') end 
		|| '_' || case when a.Brick_Number_JJ__c <> '' then a.Brick_Number_JJ__c  else 'No Value' end
	else 'NM' 
end as Brick_Code,
case when c.Account_vod__c <> '' then c.Account_vod__c else c.User_vod__c end as Event_Attendee_Integrated_Id,
'' as Brick_Id,
case when c.Status_vod__c = '' then 'No Value' else c.Status_vod__c end as Event_Attendee_Speaker_Status,
c.CurrencyIsoCode as Event_Attendee_Local_Currency,
'EUR' as Event_Attendee_Group_Currency,
c.ICD_Number_JJ__c as Event_Attendee_ICD_Number,
c.Id as Event_Attendee_Id,
c.name as Event_Attendee_Number,
c.Attendee_vod__c as Event_Attendee_Full_Name,
case when c.CreatedById = u.Id THEN u.Name ELSE 'NM' end as Event_Attendee_Created_By_Employee,
to_char(to_date(c.CreatedDate,'yyyymmdd hh24:mi:ss'),'YYYYMMDD') as Event_Attendee_Created_Date,
case when (c.Attached_Speaker_Fair_Mrkt_Value_Form_JJ__c IN ('True','true','1')) 
	THEN 'Yes'
	ELSE case when c.Attached_Speaker_Fair_Mrkt_Value_Form_JJ__c IN ('False','false','0') THEN 'No' ELSE 'NM' end 
end as Event_Attendee_Attached_Fair_Market_Value_Form_FFS,

((decode(trim(c.jj_Other_Costs__c),'',0,NULL,0,c.jj_Other_Costs__c::numeric(38,12)))::numeric(38,12) + (decode(trim(c.jj_Travel_Costs__c),'',0,NULL,0,(c.jj_Travel_Costs__c::numeric(38,12)))::numeric(38,12))::numeric(38,12)) as Event_Attendee_Cost_LC,
case when c.CurrencyIsoCode = ct.IsoCode THEN 
	(((decode(trim(c.jj_Other_Costs__c),'',0,NULL,0,(c.jj_Other_Costs__c::numeric(38,12)))::numeric(38,12)) +
	(decode(trim(c.jj_Travel_Costs__c),'',0,NULL,0,(c.jj_Travel_Costs__c::numeric(38,12)))::numeric(38,12)))
	/ (ct.conversionrate::numeric(38,12)))::numeric(38,12)::varchar(255)
	else 'NM'
end::varchar(255) as Event_Attendee_Cost_GC,
decode(trim(c.jj_Other_Costs__c),'',0,NULL,0,(c.jj_Other_Costs__c::numeric(38,12)))::numeric(38,12) as Event_Attendee_Other_Costs_LC,
case when c.CurrencyIsoCode = ct.IsoCode 
	then (decode(trim(c.jj_Other_Costs__c),'',0,NULL,0,(c.jj_Other_Costs__c::numeric(38,12)))::numeric(38,12) / (ct.conversionrate::numeric(38,12)))::numeric(38,12)::varchar(255)
	else 'NM'
end::varchar(255) as Event_Attendee_Other_Costs_GC,
decode(trim(c.JJ_Total_Fee_Including_Travel__c),'',0,NULL,0,(c.JJ_Total_Fee_Including_Travel__c::numeric(38,12)))::numeric(38,12) as Event_Attendee_Max_Service_Fee_Inc_Expenses_LC,
case when c.CurrencyIsoCode = ct.IsoCode 
	then ((decode(trim(c.JJ_Total_Fee_Including_Travel__c),'',0,NULL,0,c.JJ_Total_Fee_Including_Travel__c::numeric(38,12))::numeric(38,12)) / (ct.conversionrate::numeric(38,12)))::numeric(38,12)::varchar(255)
	else 'NM'
end::varchar(255) as Event_Attendee_Max_Service_Fee_Inc_Expenses_GC,
decode(trim(c.jj_Travel_Costs__c),'',0,NULL,0,c.jj_Travel_Costs__c::numeric(38,12))::numeric(38,12) as Event_Attendee_Travel_Costs_LC,
case when c.CurrencyIsoCode = ct.IsoCode THEN 
	(decode(trim(c.jj_Travel_Costs__c),'',0,NULL,0,c.jj_Travel_Costs__c::numeric(38,12))::numeric(38,12) / (ct.conversionrate::numeric(38,12)))::numeric(38,12)::varchar(255)
	else 'NM'
end::varchar(255) as Event_Attendee_Travel_Costs_GC,
decode(trim(c.Speaker_Fees__c),'',0,NULL,0,c.Speaker_Fees__c::numeric(38,12))::numeric(38,12) as Event_Attendee_Speaker_Fee_LC,
case when c.CurrencyIsoCode = ct.IsoCode THEN 
	(decode(trim(c.Speaker_Fees__c),'',0,NULL,0,c.Speaker_Fees__c::numeric(38,12))::numeric(38,12) / (ct.conversionrate::numeric(38,12)))::numeric(38,12)::varchar(255)
	else 'NM'
end::varchar(255) as Event_Attendee_Speaker_Fee_GC,

c.Medical_Event_vod__c as Event_Id,
case when c.Status_vod__c = '' then 'No Value' else c.Status_vod__c end as Event_Attendee_Invitee_Status,
case when c.Attended_JJ__c = '' then 'No Value' else c.Attended_JJ__c end as Event_Attendee_Attendance_Status,
case when c.RecordTypeId = rtea.Id THEN rtea.Name ELSE 'NM' end as Event_Attendee_Record_Type,
c.Attendee_Type_vod__c as Event_Attendee_Type,
'Attendee to Events' as Activity,
case when m.JJ_Event_Type__c = '' then 'No Value' else m.JJ_Event_Type__c end as Activity_Type,
case when m.Sub_Event_Type_JJ__c = '' then 'No Value' else m.Sub_Event_Type_JJ__c end as Activity_SubType,
case when lower(c.Request_Attendee_Approval_JJ__c) = 'true' then 'Yes' else 'No' end as Event_Attendee_Request_Approval,
c.Selection_Justification_JJ__c as Event_Attendee_Selection_Justification,
CASE WHEN Event_Attendee_Record_Type = 'Speaker' THEN 'Yes' ELSE 'No' END AS Event_Attendee_Speaker,
U.Id as User_Attendee,
1 as Event_Attendee_Counter,
c.JJ_Address_line_1__c as Event_Attendee_Address,
c.JJ_City__c as Event_Attendee_City,
case when 
		Event_Attendee_Record_Type not in ('Speaker','Consultant')
		OR m.Status_JJ__c IN ('Cancelled','Cancelled (Costs Incurred)')
		OR c.Attended_JJ__c IN ('Cancelled','Not Attended')
	then NULL
	else	case when Len(c.JJ_Attendee_Is_Within_Framework__c)>0 then c.JJ_Attendee_Is_Within_Framework__c ELSE 'No' end
end as Event_Attendee_Speaker_Within_Framework,

decode(trim(c.JJ_Total_Preparation_Hours__c),'','0.00',c.JJ_Total_Preparation_Hours__c) as Event_Attendee_Preparation_Hours,
decode(trim(c.JJ_Total_Time_Event_Presence__c),'','0.00',c.JJ_Total_Time_Event_Presence__c) as Event_Attendee_Service_Hours,

c.JJ_Meets_Selection_Criteria__c as Event_Attendee_Meet_Selection_Criteria,
case 
	when Event_Attendee_Record_Type not in ('Speaker','Consultant') then NULL
	when c.JJ_Received_Signed_Contract__c IN ('True','true','1') then 'Yes'
	when c.JJ_Received_Signed_Contract__c IN ('False','false','0') then 'No'
	else 'NM'
end as Event_Attendee_Received_Signed_Contract,
case when Event_Attendee_Record_Type not in ('Speaker','Consultant') then NULL else c.jj_Date_Received_Signed_Contract__c end as Event_Attendee_Date_Received_Signed_Contract,
case when Event_Attendee_Record_Type not in ('Speaker','Consultant') then 0 else 1 end::integer as Aux_Event_Attendee_Received_Signed_Contract_Counter,
case when rtme.Name = 'Promotional Event' then 1 else 0 end::integer as Aux_Promotional_Event_Attendee_Counter,
case when rtme.Name = 'Medical Educational Event' then 1 else 0 end::integer as Aux_Medical_Event_Attendee_Counter,
decode(trim(c.JJ_Cancellation_Costs_Incurred__c),'',0,NULL,0,c.JJ_Cancellation_Costs_Incurred__c::numeric(38,12))::numeric(38,12) as Event_Attendee_Cancellation_Costs_LC,
case when c.CurrencyIsoCode = ct.IsoCode 
	then	((decode(trim(c.JJ_Cancellation_Costs_Incurred__c),'',0,NULL,0,c.JJ_Cancellation_Costs_Incurred__c::numeric(38,12))::numeric(38,12)) / (ct.conversionrate::numeric(38,12)))::numeric(38,12)::varchar(255)
	else	'NM'
end as Event_Attendee_Cancellation_Costs_GC,
c.LastModifiedDate as Event_Attendee_Last_Modification_Date,
c.jj_ICD_Contract_Status__c as Event_Attendee_ICD_Contract_Status,
'Events' as Origin,

e.Event_Approval_Status as Event_Attendee_Approval_Status,
e.Event_Approval_Start_Timestamp as Event_Attendee_Approval_Start_Timestamp,
e.Event_Approval_Step1_Employee as Event_Attendee_Approval_Step1_Employee,
e.Event_Approval_Step2_Employee as Event_Attendee_Approval_Step2_Employee,
e.Event_Approval_Step3_Employee as Event_Attendee_Approval_Step3_Employee,
e.Event_Approval_step1_Status as Event_Attendee_Approval_Step1_Status,
e.Event_Approval_step2_Status as Event_Attendee_Approval_Step2_Status,
e.Event_Approval_step3_Status as Event_Attendee_Approval_Step3_Status,
e.Event_Approval_step1_Timestamp as Event_Attendee_Approval_Step1_Timestamp,
e.Event_Approval_step2_Timestamp as Event_Attendee_Approval_Step2_Timestamp,
e.Event_Approval_step3_Timestamp as Event_Attendee_Approval_Step3_Timestamp,

e.Event_Approval_Starter_Employee, /*Not in transformation sheet*/
e.Event_Approval_Id, /*Not in transformation sheet*/

datediff(day, to_date('1899-12-30','YYYY-MM-DD'),TO_DATE(TO_CHAR(TO_DATE(f.Date, 'YYYYMMDD'), 'YYYY-MM-DD'),'YYYY-MM-DD')) AS Date,
f.Aux_Year,
f.Employee_Id as Employee_Id,
f.Territory_Nominal_Id as Territory_Nominal_Id,
f.Product_Id as Product_Id,
f.ISO_Country_Code as ISO_Country_Code,
case when c.JJ_NPS_Score__c = '' then null else c.JJ_NPS_Score__c end::decimal(2,0) as event_attendee_nps

FROM {{ source('raw', 'event_attendee') }} c
LEFT OUTER JOIN {{ ref('f_event_approval_process') }} e on e.Event_Id = c.id
LEFT OUTER JOIN {{ source('raw', 'record_type') }} rtea on rtea.id = c.recordtypeid
LEFT OUTER JOIN {{ source('raw', 'currency_type') }} ct on ct.IsoCode = c.CurrencyIsoCode
LEFT OUTER JOIN {{ source('raw', 'medical_event') }} m on m.Id = c.Medical_Event_vod__c
LEFT OUTER JOIN {{ source('raw', 'record_type') }} rtme on rtme.id = m.recordtypeid
LEFT OUTER JOIN {{ source('raw', 'account') }} a on a.Id = c.Account_vod__c
LEFT OUTER JOIN {{ source('raw', 'user') }} u on u.Id = c.CreatedById
JOIN (
	SELECT 
		Event_Id,
		Date,
		to_char((to_date(Date,'yyyymmdd')),'yyyy') as Aux_Year,
		Employee_Id,
		Territory_Nominal_Id,
		Product_Id,
		ISO_Country_Code
	FROM {{ ref('tmp_f_event_2') }}
	WHERE to_char(to_date(Date,'yyyymmdd'),'yyyymmdd')>=to_char(add_months(CURRENT_DATE,-36),'YYYYMM')+'01' 
	AND to_char(to_date(event_created_date,'dd-mm-yyyy'),'YYYY')>=to_char(add_months(CURRENT_DATE,-36),'YYYY')
) f on f.Event_Id = c.Medical_Event_vod__c

WHERE COALESCE(c.country_iso_code, '') != '' 
AND to_char(to_date(c.createddate,'yyyymmdd hh24:mi:ss'),'YYYY')>=to_char(add_months(CURRENT_DATE,-36),'YYYY')
