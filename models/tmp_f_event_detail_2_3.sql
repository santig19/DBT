{{ config(
	post_hook="grant all privileges on {{ this }} to {{ var('snowflake_user_grant_privileges') }}")
}}

SELECT 
	ev.account_id,
	ev.brick_code,
	ev.event_attendee_integrated_id,
	ev.brick_id,
	ev.event_attendee_speaker_status,
	ev.event_attendee_local_currency,
	ev.event_attendee_group_currency,
	ev.event_attendee_icd_number,
	ev.event_attendee_id,
	ev.event_attendee_number,
	ev.event_attendee_full_name,
	ev.event_attendee_created_by_employee,
	ev.event_attendee_created_date,
	ev.event_attendee_attached_fair_market_value_form_ffs,
	ev.event_attendee_cost_lc,
	ev.event_attendee_cost_gc,
	ev.event_attendee_other_costs_lc,
	ev.event_attendee_other_costs_gc,
	ev.event_attendee_max_service_fee_inc_expenses_lc,
	ev.event_attendee_max_service_fee_inc_expenses_gc,
	ev.event_attendee_travel_costs_lc,
	ev.event_attendee_travel_costs_gc,
	ev.event_attendee_speaker_fee_lc,
	ev.event_attendee_speaker_fee_gc,
	ev.event_id,
	ev.event_attendee_invitee_status,
	ev.event_attendee_attendance_status,
	ev.event_attendee_record_type,
	ev.event_attendee_type,
	ev.activity,
	ev.activity_type,
	ev.activity_subtype,
	ev.event_attendee_request_approval,
	ev.event_attendee_selection_justification,
	ev.event_attendee_speaker,
	ev.user_attendee,
	ev.event_attendee_counter,
	ev.event_attendee_address,
	ev.event_attendee_city,
	ev.event_attendee_speaker_within_framework,
	ev.event_attendee_preparation_hours,
	ev.event_attendee_service_hours,
	ev.event_attendee_meet_selection_criteria,
	ev.event_attendee_received_signed_contract,
	ev.event_attendee_date_received_signed_contract,
	ev.aux_event_attendee_received_signed_contract_counter,
	ev.aux_promotional_event_attendee_counter,
	ev.aux_medical_event_attendee_counter,
	ev.event_attendee_cancellation_costs_lc,
	ev.event_attendee_cancellation_costs_gc,
	ev.event_attendee_last_modification_date,
	ev.event_attendee_icd_contract_status,
	ev.origin,
	ev.event_attendee_approval_status,
	ev.event_attendee_approval_start_timestamp,
	ev.event_attendee_approval_step1_employee,
	ev.event_attendee_approval_step2_employee,
	ev.event_attendee_approval_step3_employee,
	ev.event_attendee_approval_step1_status,
	ev.event_attendee_approval_step2_status,
	ev.event_attendee_approval_step3_status,
	ev.event_attendee_approval_step1_timestamp,
	ev.event_attendee_approval_step2_timestamp,
	ev.event_attendee_approval_step3_timestamp,
	ev.event_approval_starter_employee,
	ev.event_approval_id,
	ev.date,
	ev.aux_year,
	ev.employee_id,
	ev.territory_nominal_id,
	ev.product_id,
	ev.iso_country_code,
	ev.aux_event_speaker_framework_key,
	ev.aux_event_attendee_framework_key,
	ev.event_attendee_approval_date,
	ev.event,
	ev.event_cost_id,
	ev.security_key,
	ev.assignment_id,
	ev.account_scd_id,
	ev.product_target_class_key,
	ev.product_metrics_scd_id,
	ev.country,
	ev.region,
	ev.spider_country_code,
	ev.compliance_region,
	ev.compliance_country,
	ev.compliance_country_code,
	ev.aux_compliance_account_region,
	ev.aux_compliance_account_country,
	ev.aux_compliance_account_country_code,
	ev.compliance_visibility,
	ev.sys_compliance_visibility,
	ev.country_code,
	ev.account_visited_nonvisited_technical,
	ev.multichannel_type,
	ev.multichannel_activity,
	ev.multichannel_activity_type,
	ev.event_date,
	case when (lower(ev.Event_Attendee_Received_Signed_Contract) ='yes' and
			COALESCE(ev.Event_Attendee_Date_Received_Signed_Contract,'') != '' AND COALESCE(ev.Event_Date,'')!= '' AND
			DATEDIFF(day,TO_DATE(ev.Event_Attendee_Date_Received_Signed_Contract,'YYYYMMDD HH24:MI:SS'),TO_DATE(ev.Event_Date,'DD-MM-YYYY'))>0)
		then 'Signed contract received after the event date'
		when (lower(ev.Event_Attendee_Received_Signed_Contract)='yes' and COALESCE(ev.Event_Date,'')!= '' AND
			COALESCE(ev.Event_Attendee_Date_Received_Signed_Contract, '') != '' AND
			DATEDIFF(day,to_date(ev.Event_Date,'dd-mm-yyyy'), TO_DATE(ev.Event_Attendee_Date_Received_Signed_Contract, 'YYYYMMDD HH24:MI:SS'))>7)
		then 'Signed contract received at least one week before the event date' 
		when (lower(ev.Event_Attendee_Received_Signed_Contract)='yes' and COALESCE(ev.Event_Date,'')!= '' AND
			COALESCE(ev.Event_Attendee_Date_Received_Signed_Contract, '') != '' AND
			DATEDIFF(day,to_date(ev.Event_Date,'dd-mm-yyyy'), TO_DATE(ev.Event_Attendee_Date_Received_Signed_Contract, 'YYYYMMDD HH24:MI:SS'))<=7)
		then 'Signed contract received in the week of the event date'
		when (lower(ev.Event_Attendee_Received_Signed_Contract)='yes' and COALESCE(ev.Event_Attendee_Date_Received_Signed_Contract,'') = '')
		then 'Signed contract received without received date informed'
		when lower(ev.Event_Attendee_Received_Signed_Contract)='no' then 'Signed contract not received'
	end as Event_Attendee_Contract_Reception,
	CASE WHEN LEN(m.product_therapeutic_area_1)>0 THEN m.product_therapeutic_area_1 ELSE 'NM' END as Therapeutic_Area_Name,
	CASE WHEN LEN(CASE WHEN product_brand_therapeutic_area_1 = 'No Value' THEN NULL ELSE product_brand_therapeutic_area_1 END)>0 
		THEN CASE WHEN product_brand_therapeutic_area_1 = 'No Value' THEN NULL ELSE product_brand_therapeutic_area_1 END 
		ELSE 'NM' 
	END as Therapeutic_Area_Brand_Name,
	CASE WHEN rgb.RGB IS NULL THEN 'RGB(0,160,223)' ELSE rgb.RGB END as Event_Attendee_Contract_Reception_Colour,
	COALESCE(ev.Event_Id,'') || '-' || COALESCE(ev.Account_Id,'') as Customer_Feedback_Id,
	'Event Attendee' as Event_Attendee_Origin,
	ev.event_attendee_nps,
	ev.Event_Record_Type, 	
	ev.Event_Sub_Type,	
	ev.Event_Meeting_Style,
	ev.Event_Status,		
	ev.channel_new


FROM {{ ref('tmp_f_event_detail_1') }} ev
LEFT JOIN {{ ref('m_product') }} m on m.product_id=ev.Product_Id
LEFT JOIN {{ var('schema') }}.rgb_raw rgb on rgb.Field_Value = 
	--Event_Attendee_Contract_Reception
	case when (lower(ev.Event_Attendee_Received_Signed_Contract) ='yes' and
			COALESCE(ev.Event_Attendee_Date_Received_Signed_Contract,'') != '' AND COALESCE(ev.Event_Date,'')!= '' AND
			DATEDIFF(day,TO_DATE(ev.Event_Attendee_Date_Received_Signed_Contract,'YYYYMMDD HH24:MI:SS'),TO_DATE(ev.Event_Date,'DD-MM-YYYY'))>0)
		then 'Signed contract received after the event date'
		when (lower(ev.Event_Attendee_Received_Signed_Contract)='yes' and COALESCE(ev.Event_Date,'')!= '' AND
			COALESCE(ev.Event_Attendee_Date_Received_Signed_Contract, '') != '' AND
			DATEDIFF(day,to_date(ev.Event_Date,'dd-mm-yyyy'), TO_DATE(ev.Event_Attendee_Date_Received_Signed_Contract, 'YYYYMMDD HH24:MI:SS'))>7)
		then 'Signed contract received at least one week before the event date' 
		when (lower(ev.Event_Attendee_Received_Signed_Contract)='yes' and COALESCE(ev.Event_Date,'')!= '' AND
			COALESCE(ev.Event_Attendee_Date_Received_Signed_Contract, '') != '' AND
			DATEDIFF(day,to_date(ev.Event_Date,'dd-mm-yyyy'), TO_DATE(ev.Event_Attendee_Date_Received_Signed_Contract, 'YYYYMMDD HH24:MI:SS'))<=7)
		then 'Signed contract received in the week of the event date'
		when (lower(ev.Event_Attendee_Received_Signed_Contract)='yes' and COALESCE(ev.Event_Attendee_Date_Received_Signed_Contract,'') = '')
		then 'Signed contract received without received date informed'
		when lower(ev.Event_Attendee_Received_Signed_Contract)='no' then 'Signed contract not received'
	end
