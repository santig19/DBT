{{ config(
	post_hook="grant all privileges on {{ this }} to {{ var('snowflake_user_grant_privileges') }}")
}}

SELECT 
	E.Event_Avg_Cost_Per_Attendee_LC as Event_Avg_Cost_Per_Attendee_LC, 
	E.Event_Finalised_Date as Event_Finalised_Date,
	E.Event_Avg_Cost_Per_Attendee_GC as Event_Avg_Cost_Per_Attendee_GC,
	E.Event_Total_Cost_LC as Event_Total_Cost_LC, 
	E.Event_Total_Cost_GC as Event_Total_Cost_GC,
	E.Event_Created_By_Id as Event_Created_By_Id, 
	E.Event_Created_By_Employee as Event_Created_By_Employee, 
	E.Event_Created_Date as Event_Created_Date,
	E.Event_Created_Timestamp as Event_Created_Timestamp,
	E.Event_Local_Currency as Event_Local_Currency, 
	E.Event_Group_Currency as Event_Group_Currency, 
	E.Event_End_Date as Event_End_Date,
	E.Event_End_Timestamp as Event_End_Timestamp,
	E.date as date,
	E.Event_MonthYear as Event_MonthYear,
	E.Event_Month as Event_Month,
	E.Event_Year as Event_Year,
	E.Event_Date as Event_Date,
	E.Event_Start_Timestamp as Event_Start_Timestamp,
	E.Event_Number as Event_Number, 
	E.Event_Requestor_Name as Event_Requestor_Name, 
	E.Event_Status as Event_Status,
	E.Event_Type as Event_Type,
	E.Event_Expected_Janssen_Attendees as Event_Expected_Janssen_Attendees,
	E.Event_Expected_Total_Attendees as Event_Expected_Total_Attendees,
	E.Event_Expected_Speakers_Chairmen as Event_Expected_Speakers_Chairmen,
	E.Event_Forecasted_Total_Cost_LC as Event_Forecasted_Total_Cost_LC,
	E.Event_Forecasted_Total_Cost_GC as Event_Forecasted_Total_Cost_GC,
	E.Event_Id as Event_Id,
	E.Event_Account_Id as Event_Account_Id,
	E.Event_Account as Event_Account,
	E.Event_Location_Country_Code as Event_Location_Country_Code,
	E.Event_Location_City as Event_Location_City,
	E.Event_Location as Event_Location,
	E.Event as Event,
	E.Event_Objective as Event_Objective,
	E.Employee_Id as Employee_Id,
	E.Event_NPS as Event_NPS,
	E.Territory_Nominal_Id as Territory_Nominal_Id,
	E.Event_Country as Event_Country,
	E.Event_Region as Event_Region,
	E.Product_Id as Product_Id,
	E.Event_Country_Code as Event_Country_Code,
	E.Event_Confirm_Approved_Materials as Event_Confirm_Approved_Materials,
	E.Event_Within_Framework as Event_Within_Framework,
	E.Event_Unitas_Number as Event_Unitas_Number,
	E.Event_Received_Contract_Date as Event_Received_Contract_Date,
	E.Event_Sent_to_HPR as Event_Sent_to_HPR,
	E.Event_Medical_Approved_Materials as Event_Medical_Approved_Materials,
	E.Event_Provider_Id as Event_Provider_Id,
	E.Event_Record_Type as Event_Record_Type,
	E.Event_Last_Modification_Date as Event_Last_Modification_Date,
	E.Event_CME_CPD_Accredited as Event_CME_CPD_Accredited,
	E.Event_Sub_Type as Event_Sub_Type,
	E.Event_Scientific_3rd_Party_Implicated as Event_Scientific_3rd_Party_Implicated,
	E.ISO_Country_Code as ISO_Country_Code,
	E.Event_ICD_Contract_Status as Event_ICD_Contract_Status,
	E.Event_ICD_Number as Event_ICD_Number,
	E.Event_Counter as Event_Counter,
	E.Event_Public_Name as Event_Public_Name,
	E.Event_External_Reference_Number as Event_External_Reference_Number,
	E.Event_EMEA_Parent_Event_Number as Event_EMEA_Parent_Event_Number,
	P.Event_Approval_Starter_Employee as Event_Approval_Starter_Employee,
	P.Event_Approval_Id as Event_Approval_Id,
	P.Event_Approval_Status as Event_Approval_Status,
	P.Event_Approval_Start_Timestamp as Event_Approval_Start_Timestamp,
	P.Event_Approval_Step1_Employee as Event_Approval_Step1_Employee,
	P.Event_Approval_step1_Status as Event_Approval_step1_Status,
	P.Event_Approval_step1_Timestamp as Event_Approval_step1_Timestamp,
	P.Event_Approval_Step2_Employee as Event_Approval_Step2_Employee,
	P.Event_Approval_step2_Status as Event_Approval_step2_Status,
	P.Event_Approval_step2_Timestamp as Event_Approval_step2_Timestamp,
	P.Event_Approval_Step3_Employee as Event_Approval_Step3_Employee,
	P.Event_Approval_step3_Status as Event_Approval_step3_Status,
	P.Event_Approval_step3_Timestamp as Event_Approval_step3_Timestamp,
	E.Event_Meeting_Style as Event_Meeting_Style,
	E.Event_Meeting_channel                       as Event_Meeting_channel,
	E.Event_WPP_iMR_UNITAS_number                 as Event_WPP_iMR_UNITAS_number,
	E.Event_WPP_I_will_publish_approved_materials as Event_WPP_I_will_publish_approved_materials,
	E.Content_Topic                               as Content_Topic,
	E.Content_Sub_Topic                           as Content_Sub_Topic,
	E.Key_Message_Group                           as Key_Message_Group,
	E.event_original_creator_territory            as event_original_creator_territory,
	E.event_csat_score                            as event_csat_score,
	E.event_csat_status                           as event_csat_status,
	E.event_publication_name_title                as event_publication_name_title,
	E.event_jmc_event_name                        as event_jmc_event_name,
	E.event_details                               as event_details,
	E.event_quick_eevent                          as event_quick_eevent,
	E.event_publish_to_web                        as event_publish_to_web,
	E.event_automatic_web_publication_start_date  as event_automatic_web_publication_start_date,
	E.event_automatic_web_publication_end_date    as event_automatic_web_publication_end_date,
	E.event_start_hour                            as event_start_hour,
	E.event_end_hour                              as event_end_hour,
	E.event_live_event                            as event_live_event,
	E.event_live_event_start                      as event_live_event_start,
	E.event_live_event_end                        as event_live_event_end,
	E.event_live_event_automatic_alert            as event_live_event_automatic_alert,
	E.event_published_on_jmc_ion                  as event_published_on_jmc_ion,
	E.event_eloqua_access_token                   as event_eloqua_access_token,
	E.event_automatic_registration                as event_automatic_registration,
	E.event_link                                  as event_link
FROM (

	SELECT
		me.JJ_Forecasted_Cost_per_Attendee__c as Event_Avg_Cost_Per_Attendee_LC, 
		me.jj_date_event_finalised__c as Event_Finalised_Date,
		cast(decode(trim(me.JJ_Forecasted_Cost_per_Attendee__c),'',0,NULL,0,cast(me.JJ_Forecasted_Cost_per_Attendee__c as numeric(32,16))) as numeric(32,10)) / cast(ct.conversionrate as numeric(32,10)) as Event_Avg_Cost_Per_Attendee_GC,
		me.jj_actual_costs__c as Event_Total_Cost_LC, 
		cast(decode(trim(me.JJ_Actual_Costs__c),'',0,NULL,0,cast(me.JJ_Actual_Costs__c as numeric(32,16))) as numeric(32,16)) / cast(ct.conversionrate as numeric(32,16)) as Event_Total_Cost_GC,
		me.createdbyid as Event_Created_By_Id, 
		u.name as Event_Created_By_Employee, 
		TO_CHAR(TO_DATE(me.createddate, 'YYYYMMDD HH24:MI:SS'), 'DD-MM-YYYY') as Event_Created_Date,
		TO_CHAR(TO_TIMESTAMP(me.createddate, 'YYYYMMDD HH24:MI:SS'), 'DD-MM-YYYY HH24:MI:SS') as Event_Created_Timestamp,
		me.currencyisocode as Event_Local_Currency, 
		'EUR' as Event_Group_Currency, 
		TO_CHAR(TO_DATE(me.end_time_jj__c, 'YYYYMMDD HH24:MI:SS'), 'DD-MM-YYYY') as Event_End_Date,
		TO_CHAR(TO_TIMESTAMP(me.end_time_jj__c, 'YYYYMMDD HH24:MI:SS'), 'DD-MM-YYYY HH24:MI:SS') as Event_End_Timestamp,
		TO_CHAR(TO_DATE(me.start_time_jj__c, 'YYYYMMDD HH24:MI:SS'), 'YYYYMMDD') as date,
		TO_CHAR(TO_DATE(me.start_time_jj__c, 'YYYYMMDD HH24:MI:SS'), 'Mon-YYYY') as Event_MonthYear,
		TO_CHAR(TO_DATE(me.start_time_jj__c, 'YYYYMMDD HH24:MI:SS'), 'Month') as Event_Month,
		TO_CHAR(TO_DATE(me.start_time_jj__c, 'YYYYMMDD HH24:MI:SS'), 'Year') as Event_Year,
		TO_CHAR(TO_DATE(me.start_time_jj__c, 'YYYYMMDD HH24:MI:SS'), 'DD-MM-YYYY') as Event_Date,
		TO_CHAR(TO_TIMESTAMP(me.start_time_jj__c, 'YYYYMMDD HH24:MI:SS'), 'DD-MM-YYYY HH24:MI:SS') as Event_Start_Timestamp,
		me.event_number_jj__c as Event_Number, 
		CASE WHEN uu.name IS NOT NULL THEN uu.name ELSE 'NM' END as Event_Requestor_Name, 
		me.status_jj__c as Event_Status,
		CASE WHEN me.jj_event_type__c IS NOT NULL THEN me.jj_event_type__c ELSE 'NoValue' END as Event_Type,
		me.expected_jc_attendees_jj__c as Event_Expected_Janssen_Attendees,
		me.jj_expected_external_attendees__c as Event_Expected_Total_Attendees,
		me.jj_expected_speakers_chairmen__c as Event_Expected_Speakers_Chairmen,
		me.jj_total_forecast_cost__c as Event_Forecasted_Total_Cost_LC,
		cast(decode(trim(me.JJ_Total_Forecast_Cost__c),'',0,NULL,0,cast(me.JJ_Total_Forecast_Cost__c as numeric(32,16))) as numeric(32,10)) / cast(ct.conversionrate as numeric(32,10)) as Event_Forecasted_Total_Cost_GC,
		me.id as Event_Id,
		me.jj_account__c as Event_Account_Id,
		CASE WHEN me.jj_event_type__c IN ('Charitable Contribution', 'Educational Grant/Support', 'Patient Organisation Support', 'Sponsorship')
			THEN 
				CASE WHEN ac.ispersonaccount = 1 THEN concat(ac.lastname, ' ', ac.firstname) else
					CASE WHEN ac.ispersonaccount = 0 then ac.name else 'NM' end
				end
			else 'NA'	
		end as Event_Account,
		me.country_iso_code as Event_Location_Country_Code,
		me.jj_location_details__c as Event_Location_City,
		me.location__c as Event_Location,
		me.name as Event,
		me.objective_jj__c as Event_Objective,
		me.OwnerId as Employee_Id,
		round(cast(decode(trim(me.jj_nps__c),'',0,NULL,0,cast(me.jj_nps__c as numeric(32,16))) as numeric(32,16))) as Event_NPS,
		ut.territory2id as Territory_Nominal_Id,
		cs.name as Event_Country,
		cs.jj_region__c as Event_Region,
		SUBSTRING(JJ_Product__c,1,(case when position(';' in JJ_Product__c)=0 then len(JJ_Product__c) else position(';' in JJ_Product__c)-1 end)) as Product_Id,
		cs.country_iso_code as Event_Country_Code,
		CASE WHEN me.jj_confirm_approved_materials__c = '' THEN 'NoValue' ELSE me.jj_confirm_approved_materials__c END as Event_Confirm_Approved_Materials,
		CASE WHEN me.jj_confirm_approved_framework__c = '' THEN 'No' ELSE me.jj_confirm_approved_framework__c END as Event_Within_Framework,
		me.jj_unitas_number__c as Event_Unitas_Number,
		CASE WHEN me.jj_date_received_signed_contract__c = '' THEN '' ELSE me.jj_date_received_signed_contract__c END as Event_Received_Contract_Date,
		CASE WHEN me.jj_sent_to_hpr__c = 0 or  jj_sent_to_hpr__c='' THEN 'No' ELSE 'Yes' END as Event_Sent_to_HPR,
		CASE WHEN me.jj_medical_approved_materials__c IN ('', 'NA') THEN 'No' ELSE me.jj_medical_approved_materials__c END as Event_Medical_Approved_Materials,
		me.jj_provider__c as Event_Provider_Id,
		rt.name as Event_Record_Type,
		me.systemmodstamp as Event_Last_Modification_Date,
		CASE WHEN me.jj_cme_cpd_accredited_event__c = 1 THEN 'Yes' ELSE 'No' END as Event_CME_CPD_Accredited,
		CASE WHEN me.sub_event_type_jj__c = '' THEN 'NoValue' ELSE me.sub_event_type_jj__c END as Event_Sub_Type,
		me.jj_scientific_3rd_party_implicated__c as Event_Scientific_3rd_Party_Implicated,
		me.jj_country_iso_code__c as ISO_Country_Code,
		me.jj_icd_contract_status__c as Event_ICD_Contract_Status,
		me.icd_number_jj__c as Event_ICD_Number,
		1 as Event_Counter,
		me.jj_event_public_name__c as Event_Public_Name,
		me.jj_external_reference_number__c as Event_External_Reference_Number,
		me.jj_emea_parent_event_number__c as Event_EMEA_Parent_Event_Number,
		me.jj_meeting_style__c as Event_Meeting_Style,
		me.JJ_MEETING_CHANNEL__C                        AS Event_Meeting_channel,
		me.jj_ZINC_Reference__c                         AS Event_WPP_iMR_UNITAS_number,
		me.jj_All_publication_approvals_present__c	    AS Event_WPP_I_will_publish_approved_materials,
		CASE WHEN (POSITION(Concat(CHR(92),CHR(92)),me.JJ_Content_Topic__c) > 0) OR POSITION(Concat(CHR(124),CHR(34)), me.JJ_Content_Topic__c) > 0 OR POSITION(Concat(CHR(92),CHR(124)), me.JJ_Content_Topic__c) > 0 OR POSITION(Concat(CHR(92),CHR(34)), me.JJ_Content_Topic__c) > 0
			THEN REPLACE(REPLACE(REPLACE(REPLACE(me.JJ_Content_Topic__c, Concat(CHR(92),CHR(92)), CHR(92)), Concat(CHR(124),CHR(34)), CHR(34)), Concat(CHR(92),CHR(124)), CHR(124)), Concat(CHR(92),CHR(34)), CHR(34))
			ELSE me.JJ_Content_Topic__c 
		END AS Content_Topic,
		CASE WHEN (POSITION(Concat(CHR(92),CHR(92)),me.JJ_Content_Sub_Topic__c) > 0) OR POSITION(Concat(CHR(124),CHR(34)),me.JJ_Content_Sub_Topic__c) > 0 OR POSITION(Concat(CHR(92),CHR(124)),me.JJ_Content_Sub_Topic__c) > 0 OR POSITION(Concat(CHR(92),CHR(34)),me.JJ_Content_Sub_Topic__c) > 0
			THEN REPLACE(REPLACE(REPLACE(REPLACE(me.JJ_Content_Sub_Topic__c,Concat(CHR(92),CHR(92)), CHR(92)), Concat(CHR(124),CHR(34)), CHR(34)), Concat(CHR(92),CHR(124)), CHR(124)), Concat(CHR(92),CHR(34)), CHR(34))
			ELSE me.JJ_Content_Sub_Topic__c 
		END AS Content_Sub_Topic,
		me.JJ_Key_Message_Group__c 						AS Key_Message_Group,
		me.jj_original_creator_territory__c::varchar(510) as event_original_creator_territory,
		case when me.jj_event_csat_score__c = '' then null else me.jj_event_csat_score__c end::decimal(5,0) as event_csat_score,
		(CASE 
			WHEN (POSITION(Concat(CHR(92),CHR(92)),me.JJ_Event_CSAT_Status__c) > 0) OR POSITION(Concat(CHR(124),CHR(34)), me.JJ_Event_CSAT_Status__c) > 0 OR POSITION(Concat(CHR(92),CHR(124)), me.JJ_Event_CSAT_Status__c) > 0 OR POSITION(Concat(CHR(92),CHR(34)), me.JJ_Event_CSAT_Status__c) > 0
				THEN REPLACE(REPLACE(REPLACE(REPLACE(me.JJ_Event_CSAT_Status__c,Concat(CHR(92),CHR(92)), CHR(92)), Concat(CHR(124),CHR(34)), CHR(34)), Concat(CHR(92),CHR(124)), CHR(124)), Concat(CHR(92),CHR(34)), CHR(34))
			ELSE me.JJ_Event_CSAT_Status__c 
		END)::varchar(2600) as event_csat_status,				
		(CASE 
		    WHEN (POSITION(Concat(CHR(92),CHR(92)), me.jj_publication_name_title__c) > 0) OR POSITION(Concat(CHR(124),CHR(34)), me.jj_publication_name_title__c) > 0 OR POSITION(Concat(CHR(92),CHR(124)), me.jj_publication_name_title__c) > 0 OR POSITION(Concat(CHR(92),CHR(34)), me.jj_publication_name_title__c) > 0
				THEN REPLACE(REPLACE(REPLACE(REPLACE(me.jj_publication_name_title__c,Concat(CHR(92),CHR(92)), CHR(92)), Concat(CHR(124),CHR(34)), CHR(34)), Concat(CHR(92),CHR(124)), CHR(124)), Concat(CHR(92),CHR(34)), CHR(34))
			ELSE me.jj_publication_name_title__c 
		END)::varchar(2600) AS event_publication_name_title,
		(CASE 
		    WHEN (POSITION(Concat(CHR(92),CHR(92)), me.jj_jmc_event_name__c) > 0) OR POSITION(Concat(CHR(124),CHR(34)), me.jj_jmc_event_name__c) > 0 OR POSITION(Concat(CHR(92),CHR(124)), me.jj_jmc_event_name__c) > 0 OR POSITION(Concat(CHR(92),CHR(34)), me.jj_jmc_event_name__c) > 0
				THEN REPLACE(REPLACE(REPLACE(REPLACE(me.jj_jmc_event_name__c,Concat(CHR(92),CHR(92)), CHR(92)), Concat(CHR(124),CHR(34)), CHR(34)), Concat(CHR(92),CHR(124)), CHR(124)), Concat(CHR(92),CHR(34)), CHR(34))
			ELSE me.jj_jmc_event_name__c 
		END)::varchar(510) as event_jmc_event_name,
		(CASE 
		    WHEN (POSITION(Concat(CHR(92),CHR(92)),REGEXP_REPLACE(replace(me.JJ_Event_Details__c,'<br>', '      '), '<[^>]*>', '')) > 0) OR POSITION(Concat(CHR(124),CHR(34)),REGEXP_REPLACE(replace(me.JJ_Event_Details__c,'<br>', '      '), '<[^>]*>', '')) > 0 OR POSITION(Concat(CHR(92),CHR(124)),REGEXP_REPLACE(replace(me.JJ_Event_Details__c,'<br>', '      '), '<[^>]*>', '')) > 0 OR POSITION(Concat(CHR(92),CHR(34)),REGEXP_REPLACE(replace(me.JJ_Event_Details__c,'<br>', '      '), '<[^>]*>', '')) > 0
				THEN REPLACE(REPLACE(REPLACE(REPLACE(REGEXP_REPLACE(replace(me.JJ_Event_Details__c,'<br>', '      '), '<[^>]*>', ''),Concat(CHR(92),CHR(92)), CHR(92)), Concat(CHR(124),CHR(34)), CHR(34)), Concat(CHR(92),CHR(124)), CHR(124)), Concat(CHR(92),CHR(34)), CHR(34))
			ELSE REGEXP_REPLACE(replace(me.JJ_Event_Details__c,'<br>', '      '), '<[^>]*>', '')
		END)::varchar(16777216) as event_details, --replace break lines with 6 spaces*/
		case when me.jj_quick_eevent__c = '' then null else me.jj_quick_eevent__c end::integer as event_quick_eevent,
		me.jj_publish_to_web__c::varchar(510) as event_publish_to_web,
		case when me.jj_automatic_web_publication_start_date__c = '' then null else TO_DATE(me.jj_automatic_web_publication_start_date__c, 'YYYYMMDD HH24:MI:SS') end::datetime as event_automatic_web_publication_start_date,
		case when me.jj_automatic_web_publication_end_date__c = '' then null else TO_DATE(me.jj_automatic_web_publication_end_date__c, 'YYYYMMDD HH24:MI:SS') end::datetime as event_automatic_web_publication_end_date,
		me.jj_event_start_hour__c::varchar(510) as event_start_hour,
		me.jj_event_end_hour__c::varchar(510) as event_end_hour,
		case when me.jj_live_event__c = '' then null else me.jj_live_event__c end::integer as event_live_event,
		case when me.jj_live_event_start__c = '' then null else TO_DATE(me.jj_live_event_start__c, 'YYYYMMDD HH24:MI:SS') end::datetime as event_live_event_start,
		case when me.jj_live_event_end__c = '' then null else TO_DATE(me.jj_live_event_end__c, 'YYYYMMDD HH24:MI:SS') end::datetime as event_live_event_end,
		case when me.jj_live_event_automatic_alert__c = '' then null else me.jj_live_event_automatic_alert__c end::integer as event_live_event_automatic_alert,
		case when me.jj_publish_on_emea_meded_umbrella__c = '' then null else me.jj_publish_on_emea_meded_umbrella__c end::integer as event_published_on_jmc_ion,
		me.jj_eloqua_access_token__c::varchar(510) as event_eloqua_access_token,
		case when me.jj_automatic_registration__c = '' then null else me.jj_automatic_registration__c end::integer as event_automatic_registration,
		me.JJ_EVENT_LINK__C::varchar(2600) as event_link
	FROM {{ var('schema') }}.medical_event_vod__c_raw me 
	LEFT OUTER JOIN (
		SELECT max(id) as territory2id, userid
		FROM (
			SELECT ut.territory2id as id, ut.UserId
			FROM {{ var('schema') }}.user_territory_association_raw ut
			JOIN {{ var('schema') }}.user_raw u on u.id = ut.userid AND u.isactive != 0
			JOIN {{ ref('m_territory') }} mt on ut.territory2id=mt.territory_id
		)
		GROUP BY userid
	) ut on ut.UserId =  me.OwnerId
	LEFT OUTER JOIN {{ var('schema') }}.user_raw u ON u.id = me.createdbyid
	LEFT OUTER JOIN {{ var('schema') }}.user_raw uu ON uu.id = me.event_requestor_jj__c
	LEFT OUTER JOIN {{ var('schema') }}.account_raw ac ON ac.id = me.jj_account__c
	LEFT OUTER JOIN {{ var('schema') }}.country_settings_raw cs ON cs.country_iso_code = me.country_iso_code
	LEFT OUTER JOIN {{ var('schema') }}.record_type_raw rt ON rt.id = me.recordtypeid
	LEFT OUTER JOIN {{ var('schema') }}.currency_type_raw ct ON ct.isocode = me.currencyisocode
) E
LEFT OUTER JOIN (
	SELECT f.Event_Id::varchar(255) as Event_Id,
	case when ( f.Employee_Nominal_Id = U.Id) THEN U.Name ELSE 'NM' end::varchar(255) as Event_Approval_Starter_Employee,
	f.Process_Instance_Id::varchar(255) as			Event_Approval_Id,
	f.Process_Instance_Status::varchar(255)	 as		Event_Approval_Status,
	f.Process_Instance_Step_Date::varchar(255) as	Event_Approval_Start_Timestamp,
	case when ( f1.Employee_Nominal_Id = U1.Id) THEN U1.Name else 'NM' end::varchar(255) as Event_Approval_Step1_Employee,
	f1.Process_Instance_Step_Status::varchar(255) as	Event_Approval_step1_Status,
	f1.Process_Instance_Step_Date::varchar(255) as	Event_Approval_step1_Timestamp,
	case when ( f2.Employee_Nominal_Id = U2.Id) THEN U2.Name else 'NM' end::varchar(255) as Event_Approval_Step2_Employee,
	f2.Process_Instance_Step_Status::varchar(255) as	Event_Approval_step2_Status,
	f2.Process_Instance_Step_Date::varchar(255) as	Event_Approval_step2_Timestamp,
	case when ( f3.Employee_Nominal_Id = U3.Id) THEN U3.Name else 'NM' end::varchar(255) as Event_Approval_Step3_Employee,
	f3.Process_Instance_Step_Status::varchar(255) as	Event_Approval_step3_Status,
	f3.Process_Instance_Step_Date::varchar(255) as	Event_Approval_step3_Timestamp

	FROM {{ ref('tmp_f_process_instance') }} F	
	LEFT OUTER JOIN {{ var('schema') }}.user_raw u on f.Employee_Nominal_Id = u.Id	
	LEFT OUTER JOIN (
		select rowid,Event_Id,Employee_Nominal_Id,Process_Instance_Id,Process_Instance_Status,process_Instance_Step_Date,Process_Instance_Step_Status
			from (
				(select ROW_NUMBER() OVER ( partition by Event_Id order by Process_Instance_Step_Date asc) as rowid,
				Event_Id,
				Employee_Nominal_Id,
				Process_Instance_Id,
				Process_Instance_Status,
				process_Instance_Step_Date,
				Process_Instance_Step_Status  
				from {{ ref('tmp_f_process_instance') }}
				where Process_Instance_Step_Status not in ('Reassigned','Started')))f1
			where f1.rowid=1)f1 on f.Process_Instance_Id=f1.Process_Instance_Id
	LEFT OUTER JOIN {{ var('schema') }}.user_raw u1 on f1.Employee_Nominal_Id = u1.Id
	LEFT OUTER JOIN (
		select rowid,Event_Id,Employee_Nominal_Id,Process_Instance_Id,Process_Instance_Status,process_Instance_Step_Date,Process_Instance_Step_Status
			from (
				(select ROW_NUMBER() OVER ( partition by Event_Id order by Process_Instance_Step_Date asc) as rowid,
				Event_Id,
				Employee_Nominal_Id,
				Process_Instance_Id,
				Process_Instance_Status,
				process_Instance_Step_Date,
				Process_Instance_Step_Status  
				from {{ ref('tmp_f_process_instance') }}
				where Process_Instance_Step_Status not in ('Reassigned','Started')))f2
			where f2.rowid=2)f2 on F.Process_Instance_Id=F2.Process_Instance_Id
	LEFT OUTER JOIN {{ var('schema') }}.user_raw u2 on f2.Employee_Nominal_Id = u2.Id
	LEFT OUTER JOIN (
		select rowid,Event_Id,Employee_Nominal_Id,Process_Instance_Id,Process_Instance_Status,process_Instance_Step_Date,Process_Instance_Step_Status
			from (
				(select ROW_NUMBER() OVER ( partition by Event_Id order by Process_Instance_Step_Date asc) as rowid,
				Event_Id,
				Employee_Nominal_Id,
				Process_Instance_Id,
				Process_Instance_Status,
				process_Instance_Step_Date,
				Process_Instance_Step_Status  
				from {{ ref('tmp_f_process_instance') }}
				where Process_Instance_Step_Status not in ('Reassigned','Started')))f3
			where f3.rowid=3)f3 on F.Process_Instance_Id=F3.Process_Instance_Id
	LEFT OUTER JOIN {{ var('schema') }}.user_raw u3 on f3.Employee_Nominal_Id = u3.Id
	WHERE f.Process_Instance_Step_Status = 'Started'
) P on E.Event_id= P.Event_id
