{{ config(
	post_hook="grant all privileges on {{ this }} to {{ var('snowflake_user_grant_privileges') }}")
}}

SELECT *
FROM {{ ref('tmp_f_event_detail_2_3') }}

UNION ALL

SELECT
	TMP_ANON_SURV.account_id AS account_id,
	evdet3.brick_code AS brick_code,
	NULL AS event_attendee_integrated_id,
	NULL AS brick_id,
	NULL AS event_attendee_speaker_status,
	NULL AS event_attendee_local_currency,
	NULL AS event_attendee_group_currency,
	NULL AS event_attendee_icd_number,
	NULL AS event_attendee_id,
	NULL AS event_attendee_number,
	NULL AS event_attendee_full_name,
	NULL AS event_attendee_created_by_employee,
	NULL AS event_attendee_created_date,
	NULL AS event_attendee_attached_fair_market_value_form_ffs,
	NULL::numeric(20,2) AS event_attendee_cost_lc,
	NULL AS event_attendee_cost_gc,
	NULL::numeric(20,2) AS event_attendee_other_costs_lc,
	NULL AS event_attendee_other_costs_gc,
	NULL::numeric(20,2) AS event_attendee_max_service_fee_inc_expenses_lc,
	NULL AS event_attendee_max_service_fee_inc_expenses_gc,
	NULL::numeric(20,2) AS event_attendee_travel_costs_lc,
	NULL AS event_attendee_travel_costs_gc,
	NULL::numeric(20,2) AS event_attendee_speaker_fee_lc,
	NULL AS event_attendee_speaker_fee_gc,
	TMP_ANON_SURV.event_id AS event_id,
	NULL AS event_attendee_invitee_status,
	NULL AS event_attendee_attendance_status,
	NULL AS event_attendee_record_type,
	NULL AS event_attendee_type,
	evdet3.activity AS activity,
	evdet3.activity_type AS activity_type,
	evdet3.activity_subtype AS activity_subtype,
	NULL AS event_attendee_request_approval,
	NULL AS event_attendee_selection_justification,
	NULL AS event_attendee_speaker,
	NULL AS user_attendee,
	NULL::integer AS event_attendee_counter,
	NULL AS event_attendee_address,
	NULL AS event_attendee_city,
	NULL AS event_attendee_speaker_within_framework,
	NULL AS event_attendee_preparation_hours,
	NULL AS event_attendee_service_hours,
	NULL AS event_attendee_meet_selection_criteria,
	NULL AS event_attendee_received_signed_contract,
	NULL AS event_attendee_date_received_signed_contract,
	NULL::integer AS aux_event_attendee_received_signed_contract_counter,
	NULL::integer AS aux_promotional_event_attendee_counter,
	NULL::integer AS aux_medical_event_attendee_counter,
	NULL::numeric(20,2) AS event_attendee_cancellation_costs_lc,
	NULL AS event_attendee_cancellation_costs_gc,
	NULL AS event_attendee_last_modification_date,
	NULL AS event_attendee_icd_contract_status,
	evdet3.origin AS origin,
	NULL AS event_attendee_approval_status,
	NULL AS event_attendee_approval_start_timestamp,
	NULL AS event_attendee_approval_step1_employee,
	NULL AS event_attendee_approval_step2_employee,
	NULL AS event_attendee_approval_step3_employee,
	NULL AS event_attendee_approval_step1_status,
	NULL AS event_attendee_approval_step2_status,
	NULL AS event_attendee_approval_step3_status,
	NULL AS event_attendee_approval_step1_timestamp,
	NULL AS event_attendee_approval_step2_timestamp,
	NULL AS event_attendee_approval_step3_timestamp,
	NULL AS event_approval_starter_employee,
	NULL AS event_approval_id,
	evdet3.date AS date,
	NULL AS aux_year,
	evdet3.employee_id AS employee_id,
	evdet3.territory_nominal_id AS territory_nominal_id,
	evdet3.product_id AS product_id,
	NULL AS iso_country_code,
	NULL AS aux_event_speaker_framework_key,
	NULL AS aux_event_attendee_framework_key,
	NULL AS event_attendee_approval_date,
	NULL AS event,
	NULL AS event_cost_id,
	NULL AS security_key,
	NULL AS assignment_id,
	NULL AS account_scd_id,
	NULL AS product_target_class_key,
	evdet3.product_metrics_scd_id AS product_metrics_scd_id,
	evdet3.country AS country,
	evdet3.region AS region,
	evdet3.spider_country_code AS spider_country_code,
	NULL AS compliance_region,
	NULL AS compliance_country,
	NULL AS compliance_country_code,
	NULL AS aux_compliance_account_region,
	NULL AS aux_compliance_account_country,
	NULL AS aux_compliance_account_country_code,
	NULL AS compliance_visibility,
	NULL AS sys_compliance_visibility,
	evdet3.country_code AS country_code,
	NULL AS account_visited_nonvisited_technical,
	evdet3.multichannel_type AS multichannel_type,
	evdet3.multichannel_activity AS multichannel_activity,
	evdet3.multichannel_activity_type AS multichannel_activity_type,
	NULL AS event_date,
	NULL AS event_attendee_contract_reception,
	evdet3.therapeutic_area_name AS therapeutic_area_name,
	evdet3.therapeutic_area_brand_name AS therapeutic_area_brand_name,
	NULL AS event_attendee_contract_reception_colour,
	NULL AS customer_feedback_id,
	'Anonymous Survey' AS event_attendee_origin,
	evdet3.event_attendee_nps,
	NULL AS Event_Record_Type,
	NULL AS Event_Sub_Type,
	NULL AS Event_Meeting_Style,
	NULL AS Event_Status,
	NULL AS channel_new

FROM {{ ref('tmp_anonymous_surveys') }} AS TMP_ANON_SURV
JOIN {{ ref('tmp_f_event_detail_2_3') }} evdet3 ON evdet3.event_id = TMP_ANON_SURV.Event_id
	and LEN(evdet3.country_code)>0
