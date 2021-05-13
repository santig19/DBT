{{ config(
	post_hook="grant all privileges on {{ this }} to {{ var('snowflake_user_grant_privileges') }}")
}}

select 
	a.*,
	f.Event_Date as Event_Date
from (
	select
	e.Account_Id,
	e.Brick_Code,
	e.Event_Attendee_Integrated_Id,
	e.Brick_Id,
	e.Event_Attendee_Speaker_Status,
	e.Event_Attendee_Local_Currency,
	e.Event_Attendee_Group_Currency,
	e.Event_Attendee_ICD_Number,
	e.Event_Attendee_Id,
	e.Event_Attendee_Number,
	e.Event_Attendee_Full_Name,
	e.Event_Attendee_Created_By_Employee,
	e.Event_Attendee_Created_Date,
	e.Event_Attendee_Attached_Fair_Market_Value_Form_FFS,
	e.Event_Attendee_Cost_LC,
	e.Event_Attendee_Cost_GC,
	e.Event_Attendee_Other_Costs_LC,
	e.Event_Attendee_Other_Costs_GC,
	e.Event_Attendee_Max_Service_Fee_Inc_Expenses_LC,
	e.Event_Attendee_Max_Service_Fee_Inc_Expenses_GC,
	e.Event_Attendee_Travel_Costs_LC,
	e.Event_Attendee_Travel_Costs_GC,
	e.Event_Attendee_Speaker_Fee_LC,
	e.Event_Attendee_Speaker_Fee_GC,
	e.Event_Id,
	e.Event_Attendee_Invitee_Status,
	e.Event_Attendee_Attendance_Status,
	e.Event_Attendee_Record_Type,
	e.Event_Attendee_Type,
	e.Activity,
	e.Activity_Type,
	e.Activity_SubType,
	e.Event_Attendee_Request_Approval,
	e.Event_Attendee_Selection_Justification,
	e.Event_Attendee_Speaker,
	e.User_Attendee,
	e.Event_Attendee_Counter,
	e.Event_Attendee_Address,
	e.Event_Attendee_City,
	e.Event_Attendee_Speaker_Within_Framework,
	e.Event_Attendee_Preparation_Hours,
	e.Event_Attendee_Service_Hours,
	e.Event_Attendee_Meet_Selection_Criteria,
	e.Event_Attendee_Received_Signed_Contract,
	e.Event_Attendee_Date_Received_Signed_Contract,
	e.Aux_Event_Attendee_Received_Signed_Contract_Counter,
	e.Aux_Promotional_Event_Attendee_Counter,
	e.Aux_Medical_Event_Attendee_Counter,
	e.Event_Attendee_Cancellation_Costs_LC,
	e.Event_Attendee_Cancellation_Costs_GC,
	e.Event_Attendee_Last_Modification_Date,
	e.Event_Attendee_ICD_Contract_Status,
	e.Origin,
	
	e.Event_Attendee_Approval_Status,
	e.Event_Attendee_Approval_Start_Timestamp,
	e.Event_Attendee_Approval_Step1_Employee,
	e.Event_Attendee_Approval_Step2_Employee,
	e.Event_Attendee_Approval_Step3_Employee,
	e.Event_Attendee_Approval_Step1_Status,
	e.Event_Attendee_Approval_Step2_Status,
	e.Event_Attendee_Approval_Step3_Status,
	e.Event_Attendee_Approval_Step1_Timestamp,
	e.Event_Attendee_Approval_Step2_Timestamp,
	e.Event_Attendee_Approval_Step3_Timestamp,
	
	e.Event_Approval_Starter_Employee, /*Not in transformation sheet*/
	e.Event_Approval_Id, /*Not in transformation sheet*/
	
	e.Date as Date,
	e.aux_year as Aux_Year,
	e.Employee_Id as Employee_Id,
	e.Territory_Nominal_Id as Territory_Nominal_Id,
	e.Product_Id as Product_Id,
	e.ISO_Country_Code as ISO_Country_Code,
	
	COALESCE(Aux_Year,'') || '-' || COALESCE(e.Account_Id,'') || '-' || COALESCE(cs1.Name,'') as Aux_Event_Speaker_Framework_Key,
	COALESCE(Aux_Year,'') || '-' || COALESCE(e.Account_Id,'') || '-' || COALESCE(cs1.Name,'') || COALESCE(rt.Name,'') as Aux_Event_Attendee_Framework_Key,
	case 
		when (e.Event_Attendee_Approval_Step3_Timestamp is not null) then e.Event_Attendee_Approval_Step3_Timestamp
		when (e.Event_Attendee_Approval_Step2_Timestamp is not Null) then e.Event_Attendee_Approval_Step2_Timestamp 
		else e.Event_Attendee_Approval_Step1_Timestamp
	end as Event_Attendee_Approval_Date,
	e.Event_Id as Event,
	e.Event_Id as Event_Cost_Id,
	COALESCE(e.Event_Attendee_Integrated_Id,'') || '-' || COALESCE(e.Employee_Id,'') as Security_Key,
	e.Event_Attendee_Integrated_Id as Assignment_Id,
	COALESCE(e.Account_Id,'') || '-' || DATEDIFF(day,TO_DATE('30-12-1899','DD-MM-YYYY'),date_trunc('month', DATEADD(day,e.date,TO_DATE('30-12-1899','DD-MM-YYYY'))))::varchar(255) as Account_SCD_Id,
	COALESCE(e.Account_Id,'') || '-' || COALESCE(mp.Product_Detail_Id,'')::varchar(255) as Product_Target_Class_Key,
	COALESCE(e.Account_Id,'') || '-' || COALESCE(e.Product_Id,'') || '-' || 
		DATEDIFF(day,TO_DATE('30-12-1899','DD-MM-YYYY'),date_trunc('month', DATEADD(day,e.date,TO_DATE('30-12-1899','DD-MM-YYYY'))))::varchar(255) as Product_Metrics_SCD_Id,
--used to be cs3
	COALESCE(cs5.name,'NM') as Country,
	COALESCE(cs5.jj_Region__c,'NM') as Region,
	cs1.jj_Country_ISO_Code__c as Spider_Country_Code,
	cs2.jj_Region__c as Compliance_Region,
	cs2.Name as Compliance_Country,
	e.ISO_Country_Code as Compliance_Country_Code,
	cs5.jj_Region__c as Aux_Compliance_Account_Region,
	cs5.name as Aux_Compliance_Account_Country,
	case 
		when (ac.Country_JJ__c is not Null) Then ac.Country_JJ__c 
		else case when (e.Account_Id = ac.Id) then ac.JJ_Country__c else case when e.employee_id = u.Id AND u.jj_User_Country__c = cs1.Name then cs1.jj_Country_ISO_Code__c end end
	end as Aux_Compliance_Account_Country_Code,
	'My Region' as Compliance_Visibility,
	'1' as Sys_Compliance_Visibility,
/*
	CASE 
		WHEN e.account_id = ac.Id AND LEN(ac.CODS_external_id__c)>0 
			THEN (CASE WHEN ac.Country_jj__C IS NULL THEN ac.jj_country__C ELSE ac.Country_jj__C END)
		WHEN U.id = e.employee_id 
			THEN cs4.country_iso_code
		ELSE 'NM' 
	END::varchar(255) AS Country_Code, 
*/ 
	COALESCE(cs5.jj_country_iso_code__c, 'NM') as Country_Code,
	'Non Visited' || '_' || Country_Code as Account_Visited_NonVisited_Technical,
	'Face to Face Contacts' as Multichannel_Type,
	'Attendee to Events' as Multichannel_Activity,
	e.Activity_Type as Multichannel_Activity_Type,
	e.event_attendee_nps,
	rt.name as Event_Record_Type,
	CASE WHEN m.sub_event_type_jj__c = '' THEN 'NoValue' ELSE m.sub_event_type_jj__c END as Event_Sub_Type,
	m.jj_meeting_style__c as Event_Meeting_Style,
	m.status_jj__c as Event_Status,
	NULL as channel_new

	from {{ ref('tmp_event_attendee') }} e
	left join (
		select max(product_id) as product_id,Product_Detail_Id 
		from {{ ref('m_product') }} 
		group by Product_Detail_Id,product_id
	) mp on mp.product_id=e.product_id
	left join {{ source('raw', 'user') }} u on e.Employee_Id = u.Id
	left join {{ source('raw', 'medical_event') }} m on e.Event_Id = m.Id
	left join {{ source('raw', 'record_type') }} rt on rt.id = m.recordtypeid
	left join {{ source('raw', 'product') }} p on p.Id=e.product_id
	left join {{ source('raw', 'account') }} ac on e.Account_Id = ac.ID 
	left join {{ source('raw', 'country_settings') }} cs1 on cs1.name=u.jj_User_Country__c
	left join {{ source('raw', 'country_settings') }} cs2 on cs2.jj_Country_ISO_Code__c=e.ISO_Country_Code
/*
	left join emea_mto.zz_fwk_icon_jj_country_settings__c_business cs3 on cs3.JJ_COUNTRY_ISO_CODE__C= 	
		CASE WHEN e.account_id = ac.Id AND LEN(ac.CODS_external_id__c)>0 
				THEN (CASE WHEN ac.Country_jj__C IS NULL THEN ac.jj_country__C ELSE ac.Country_jj__C END)
			WHEN U.id = e.employee_id 
				THEN cs1.country_iso_code
			ELSE 'NM' 
		END
	left join emea_mto.zz_fwk_icon_jj_country_settings__c_business cs4 ON cs4.JJ_COUNTRY_ISO_CODE__C = 
		CASE WHEN e.Account_Id=ac.Id 
			THEN CASE WHEN ac.COUNTRY_JJ__C IS NOT null THEN ac.COUNTRY_JJ__C ELSE  ac.JJ_Country__c END 
			ELSE CASE WHEN e.employee_id=u.id THEN u.jj_User_Country__c ELSE 'NM' END
		END
*/  		
	left join {{ source('raw', 'country_settings') }} cs5 ON cs5.JJ_COUNTRY_ISO_CODE__C = 
		CASE WHEN ac.Country_JJ__c is not null 
			THEN ac.Country_JJ__c 
			ELSE CASE WHEN e.Account_Id=ac.Id THEN ac.JJ_Country__c ELSE CASE WHEN e.employee_id=u.id AND u.jj_User_Country__c = cs5.Name THEN cs1.jj_Country_ISO_Code__c END END
		END  

UNION

SELECT
	case when F.Event_Type in ('Charitable Contribution','Educational Grant/Support','Patient Organisation Support','Sponsorship') 
		then F.Event_Account_Id 
		else 'No Value' 
	end as Account_Id,
	NULL as Brick_Code,
	NULL as Event_Attendee_Integrated_Id,
	NULL as Brick_Id,
	NULL as Event_Attendee_Speaker_Status,
	NULL as Event_Attendee_Local_Currency,
	NULL as Event_Attendee_Group_Currency,
	NULL as Event_Attendee_ICD_Number,
--f.Event_Id as Event_Attendee_Id,
	NULL as Event_Attendee_Id,
	NULL as Event_Attendee_Number,
	NULL as Event_Attendee_Full_Name,
	NULL as Event_Attendee_Created_By_Employee,
	NULL as Event_Attendee_Created_Date,
	NULL as Event_Attendee_Attached_Fair_Market_Value_Form_FFS,
	NULL as Event_Attendee_Cost_LC,
	NULL as Event_Attendee_Cost_GC,
	NULL as Event_Attendee_Other_Costs_LC,
	NULL as Event_Attendee_Other_Costs_GC,
	NULL as Event_Attendee_Max_Service_Fee_Inc_Expenses_LC,
	NULL as Event_Attendee_Max_Service_Fee_Inc_Expenses_GC,
	NULL as Event_Attendee_Travel_Costs_LC,
	NULL as Event_Attendee_Travel_Costs_GC,
	NULL as Event_Attendee_Speaker_Fee_LC,
	NULL as Event_Attendee_Speaker_Fee_GC,
	f.Event_Id as Event_Id,
	NULL as Event_Attendee_Invitee_Status,
	NULL as Event_Attendee_Attendance_Status,
	NULL as Event_Attendee_Record_Type,
	NULL as Event_Attendee_Type,
	'Attendee to Events' as Activity,
	case when m.JJ_Event_Type__c = '' then 'No Value' else m.JJ_Event_Type__c end as Activity_Type,
	case when m.Sub_Event_Type_JJ__c = '' then 'No Value' else m.Sub_Event_Type_JJ__c end as Activity_SubType,
	NULL as Event_Attendee_Request_Approval,
	NULL as Event_Attendee_Selection_Justification,
	NULL as Event_Attendee_Speaker,
	NULL as User_Attendee,
	NULL as Event_Attendee_Counter,
	NULL as Event_Attendee_Address,
	NULL as Event_Attendee_City,
	NULL as Event_Attendee_Speaker_Within_Framework,
	NULL as Event_Attendee_Preparation_Hours,
	NULL as Event_Attendee_Service_Hours,
	NULL as Event_Attendee_Meet_Selection_Criteria,
	NULL as Event_Attendee_Received_Signed_Contract,
	NULL as Event_Attendee_Date_Received_Signed_Contract,
	NULL as Aux_Event_Attendee_Received_Signed_Contract_Counter,
	NULL as Aux_Promotional_Event_Attendee_Counter,
	NULL as Aux_Medical_Event_Attendee_Counter,
	NULL as Event_Attendee_Cancellation_Costs_LC,
	NULL as Event_Attendee_Cancellation_Costs_GC,
	NULL as Event_Attendee_Last_Modification_Date,
	NULL as Event_Attendee_ICD_Contract_Status,
	'Event Null Value' as Origin,
	NULL as Event_Attendee_Approval_Status,
	NULL as Event_Attendee_Approval_Start_Timestamp,
	NULL as Event_Attendee_Approval_Step1_Employee,
	NULL as Event_Attendee_Approval_Step2_Employee,
	NULL as Event_Attendee_Approval_Step3_Employee,
	NULL as Event_Attendee_Approval_Step1_Status,
	NULL as Event_Attendee_Approval_Step2_Status,
	NULL as Event_Attendee_Approval_Step3_Status,
	NULL as Event_Attendee_Approval_Step1_Timestamp,
	NULL as Event_Attendee_Approval_Step2_Timestamp,
	NULL as Event_Attendee_Approval_Step3_Timestamp,
	NULL as Event_Approval_Starter_Employee,
	NULL as Event_Approval_Id,
	DATEDIFF(day,to_date('1899-12-30','YYYY-MM-DD'),TO_DATE(TO_CHAR(TO_DATE(f.Date, 'YYYYMMDD'), 'YYYY-MM-DD'),'YYYY-MM-DD')) as Date,
	to_char(TO_DATE(f.Date,'YYYYMMDD'),'YYYY') as Aux_Year,
	F.Employee_Id as Employee_Id,
	ut.Territory2Id as Territory_Nominal_Id,
	F.Product_Id as Product_Id,
	NULL as ISO_Country_Code,
	COALESCE(Aux_Year,'') || '-' || 'No Value' || '-' || COALESCE(cs1.Name,'') as Aux_Event_Speaker_Framework_Key,
	COALESCE(Aux_Year,'') || '-' || 'No Value' || '-' || COALESCE(cs1.Name,'') || COALESCE(rt.Name,'') as Aux_Event_Attendee_Framework_Key,
	NULL as Event_Attendee_Approval_Date,
	NULL as Event,
	f.Event_Id as Event_Cost_Id,
	case when f.event_Type in ('Charitable Contribution','Educational Grant/Support','Patient Organisation Support','Sponsorship') and f.Event_Account_Id is not Null 
		then f.Event_Account_Id else COALESCE(f.Employee_Id,'') || '-' || COALESCE(f.Employee_Id,'') 
	end as Security_Key,
	case when f.event_Type in ('Charitable Contribution','Educational Grant/Support','Patient Organisation Support','Sponsorship') and f.Event_Account_Id is not Null 
		then f.Event_Account_Id else f.Employee_Id 
	end as Assignment_Id,
	NULL as Account_SCD_Id,
	NULL as Product_Target_Class_Key,
	NULL as Product_Metrics_SCD_Id,
	COALESCE(cs3.Name, 'NM') as Country,
	COALESCE(cs3.jj_Region__c, 'NM') as Region,
	NULL as Spider_Country_Code,
	cs2.jj_Region__c as Compliance_Region,
	cs2.Name as Compliance_Country,
	f.ISO_Country_Code as Compliance_Country_Code,
	NULL as Aux_Compliance_Account_Region,
	NULL as Aux_Compliance_Account_Country,
	NULL as Aux_Compliance_Account_Country_Code,
	'My Region' as Compliance_Visibility,
	'1' as Sys_Compliance_Visibility,
	COALESCE(cs3.JJ_COUNTRY_ISO_CODE__C, 'NM') AS Country_Code, 
	'Non Visited' || '_' || case WHEN U.id = f.employee_id THEN cs1.country_iso_code ELSE 'NM' END as Account_Visited_NonVisited_Technical,
	NULL as Multichannel_Type,
	NULL as Multichannel_Activity,
	NULL as Multichannel_Activity_Type,
	NULL as event_attendee_nps,
	rt.name as Event_Record_Type,
	CASE WHEN m.sub_event_type_jj__c = '' THEN 'NoValue' ELSE m.sub_event_type_jj__c END as Event_Sub_Type,
	m.jj_meeting_style__c as Event_Meeting_Style,
	m.status_jj__c as Event_Status,
	NULL as channel_new

	from (
		SELECT * 
		FROM {{ ref('tmp_f_event_2') }}
		WHERE to_char(to_date(Date,'yyyymmdd'),'yyyymmdd')>=to_char(add_months(CURRENT_DATE,-36),'YYYYMM')+'01'
		and to_char(to_date(event_created_date,'dd-mm-yyyy'),'YYYY')>=to_char(add_months(CURRENT_DATE,-36),'YYYY')
	) F
	left join (
		SELECT max(id) as territory2id, userid
		FROM (
			SELECT ut.territory2id as id, ut.UserId
			FROM {{ source('raw', 'user_territory_association') }} ut
			JOIN {{ source('raw', 'user') }} u on u.id = ut.userid AND u.isactive != 0
			JOIN {{ ref('m_territory') }} mt on ut.territory2id=mt.territory_id
		)
		GROUP BY userid
	) ut on F.Employee_Id = ut.UserId
	left join {{ source('raw', 'user') }} u on f.employee_id=u.id
	left join {{ source('raw', 'medical_event') }} m on f.event_id=m.Id
	left join {{ source('raw', 'record_type') }} rt on m.RecordTypeId=rt.id
	left join {{ source('raw', 'account') }} a on f.event_account_id=a.id
	left join {{ source('raw', 'country_settings') }} cs1 on cs1.name=u.jj_User_Country__c
	left join {{ source('raw', 'country_settings') }} cs2 on cs2.jj_Country_ISO_Code__c=f.iso_country_code
	left join {{ source('raw', 'country_settings') }} cs3 on cs3.JJ_COUNTRY_ISO_CODE__C = 
		case when f.event_Type in ('Charitable Contribution','Educational Grant/Support','Patient Organisation Support','Sponsorship') 
			then case when (a.Country_JJ__c <> '') then a.Country_JJ__c else a.JJ_Country__c end
			else case when cs1.country_iso_code is not null then cs1.country_iso_code ELSE 'NM' end
		end								 
) a 

left join (
	select event_id, event_date 
	from {{ ref('tmp_f_event_2') }}
	where to_char(to_date(Date,'yyyymmdd'),'yyyymmdd')>=to_char(add_months(CURRENT_DATE,-36),'YYYYMM')+'01'
	and to_char(to_date(event_created_date,'dd-mm-yyyy'),'YYYY')>=to_char(add_months(CURRENT_DATE,-36),'YYYY')
) f on a.event_id=f.event_id
