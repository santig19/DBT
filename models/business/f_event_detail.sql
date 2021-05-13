{{ config(
	post_hook="grant all privileges on {{ this }} to {{ var('snowflake_user_grant_privileges') }}")
}}

SELECT
     evdet4.account_id::varchar(20) AS account_id
    ,evdet4.brick_code::varchar(255) AS brick_code
    ,evdet4.event_attendee_integrated_id::varchar(20) AS event_attendee_integrated_id
    ,evdet4.brick_id::varchar(255) AS brick_id
    ,evdet4.event_attendee_speaker_status::varchar(255) AS event_attendee_speaker_status
    ,evdet4.event_attendee_local_currency::varchar(3) AS event_attendee_local_currency
    ,evdet4.event_attendee_group_currency::varchar(3) AS event_attendee_group_currency
    ,evdet4.event_attendee_icd_number::varchar(255) AS event_attendee_icd_number
    ,evdet4.event_attendee_id::varchar(20) AS event_attendee_id
    ,evdet4.event_attendee_number::varchar(20) AS event_attendee_number
    ,evdet4.event_attendee_full_name::varchar(255) AS event_attendee_full_name
    ,evdet4.event_attendee_created_by_employee::varchar(255) AS event_attendee_created_by_employee
    ,evdet4.event_attendee_created_date::varchar(8) AS event_attendee_created_date
    ,evdet4.event_attendee_attached_fair_market_value_form_ffs::varchar(3) AS event_attendee_attached_fair_market_value_form_ffs
    ,evdet4.event_attendee_cost_lc::numeric(20,2) AS event_attendee_cost_lc
    ,evdet4.event_attendee_cost_gc::varchar(20) AS event_attendee_cost_gc--includes NM values
    ,evdet4.event_attendee_other_costs_lc::numeric(20,2) AS event_attendee_other_costs_lc
    ,evdet4.event_attendee_other_costs_gc::varchar(20) AS event_attendee_other_costs_gc  --includes NM values
    ,evdet4.event_attendee_max_service_fee_inc_expenses_lc::numeric(20,2) AS event_attendee_max_service_fee_inc_expenses_lc
    ,evdet4.event_attendee_max_service_fee_inc_expenses_gc::varchar(25) AS event_attendee_max_service_fee_inc_expenses_gc --includes NM values
    ,evdet4.event_attendee_travel_costs_lc::numeric(20,2) AS event_attendee_travel_costs_lc 
    ,evdet4.event_attendee_travel_costs_gc::varchar(20) AS event_attendee_travel_costs_gc  --includes NM values
    ,evdet4.event_attendee_speaker_fee_lc::numeric(20,2) AS event_attendee_speaker_fee_lc
    ,evdet4.event_attendee_speaker_fee_gc::varchar(25) AS event_attendee_speaker_fee_gc  --includes NM values
    ,evdet4.event_id::varchar(20) AS event_id
    ,evdet4.event_attendee_invitee_status::varchar(255) AS event_attendee_invitee_status
    ,evdet4.event_attendee_attendance_status::varchar(255) AS event_attendee_attendance_status
    ,evdet4.event_attendee_record_type::varchar(255) AS event_attendee_record_type
    ,evdet4.event_attendee_type::varchar(255) AS event_attendee_type
    ,evdet4.activity::varchar(18) AS activity
    ,evdet4.activity_type::varchar(255) AS activity_type
    ,evdet4.activity_subtype::varchar(255) AS activity_subtype
    ,evdet4.event_attendee_request_approval::varchar(3) AS event_attendee_request_approval
    ,evdet4.event_attendee_selection_justification::varchar(255) AS event_attendee_selection_justification
    ,evdet4.event_attendee_speaker::varchar(3) AS event_attendee_speaker
    ,evdet4.user_attendee::varchar(20) AS user_attendee
    ,evdet4.event_attendee_counter::integer AS event_attendee_counter
    ,evdet4.event_attendee_address::varchar(255) AS event_attendee_address
    ,evdet4.event_attendee_city::varchar(255) AS event_attendee_city
    ,evdet4.event_attendee_speaker_within_framework::varchar(3) AS event_attendee_speaker_within_framework
    ,evdet4.event_attendee_preparation_hours::numeric(20,2) AS event_attendee_preparation_hours
    ,evdet4.event_attendee_service_hours::numeric(20,2) AS event_attendee_service_hours
    ,evdet4.event_attendee_meet_selection_criteria::varchar(255) AS event_attendee_meet_selection_criteria
    ,evdet4.event_attendee_received_signed_contract::varchar(3) AS event_attendee_received_signed_contract
    ,evdet4.event_attendee_date_received_signed_contract::varchar(255) AS event_attendee_date_received_signed_contract 
    ,evdet4.event_attendee_cancellation_costs_lc::numeric(20,2) AS event_attendee_cancellation_costs_lc
    ,evdet4.event_attendee_cancellation_costs_gc::varchar(20) AS event_attendee_cancellation_costs_gc    --includes NM values
    ,evdet4.event_attendee_last_modification_date::varchar(17) AS event_attendee_last_modification_date
    ,evdet4.event_attendee_icd_contract_status::varchar(255) AS event_attendee_icd_contract_status
    ,evdet4.origin::varchar(16) AS origin
    ,evdet4.event_attendee_approval_status::varchar(255) AS event_attendee_approval_status
    ,evdet4.event_attendee_approval_start_timestamp::varchar(17) AS event_attendee_approval_start_timestamp
    ,evdet4.event_attendee_approval_step1_employee::varchar(255) AS event_attendee_approval_step1_employee
    ,evdet4.event_attendee_approval_step2_employee::varchar(255) AS event_attendee_approval_step2_employee
    ,evdet4.event_attendee_approval_step3_employee::varchar(255) AS event_attendee_approval_step3_employee
    ,evdet4.event_attendee_approval_step1_status::varchar(255) AS event_attendee_approval_step1_status
    ,evdet4.event_attendee_approval_step2_status::varchar(255) AS event_attendee_approval_step2_status
    ,evdet4.event_attendee_approval_step3_status::varchar(255) AS event_attendee_approval_step3_status
    ,evdet4.event_attendee_approval_step1_timestamp::varchar(17) AS event_attendee_approval_step1_timestamp
    ,evdet4.event_attendee_approval_step2_timestamp::varchar(17) AS event_attendee_approval_step2_timestamp
    ,evdet4.event_attendee_approval_step3_timestamp::varchar(17) AS event_attendee_approval_step3_timestamp
    ,evdet4.event_approval_starter_employee::varchar(255) AS event_approval_starter_employee
    ,evdet4.event_approval_id::varchar(20) AS event_approval_id
    ,to_char(to_date('1899-12-30', 'YYYY-MM-DD') + evdet4.date::integer,'YYYYMMDD')::varchar(8) AS date
    ,evdet4.employee_id::varchar(20) AS employee_id
    ,evdet4.territory_nominal_id::varchar(20) AS territory_nominal_id
    ,evdet4.product_id::varchar(20) AS product_id
    ,evdet4.event_attendee_approval_date::varchar(17) AS event_attendee_approval_date
    ,evdet4.event::varchar(20) AS event
    ,evdet4.event_cost_id::varchar(20) AS event_cost_id
    ,evdet4.security_key::varchar(255) AS security_key
    ,evdet4.assignment_id::varchar(20) AS assignment_id
    ,evdet4.account_scd_id::varchar(255) AS account_scd_id
    ,evdet4.product_target_class_key::varchar(255) AS product_target_class_key
    ,evdet4.product_metrics_scd_id::varchar(255) AS product_metrics_scd_id
    ,evdet4.country::varchar(255) AS country
    ,evdet4.region::varchar(255) AS region
    ,evdet4.spider_country_code::varchar(2) AS spider_country_code
    ,evdet4.compliance_region::varchar(255) AS compliance_region
    ,evdet4.compliance_country::varchar(255) AS compliance_country
    ,evdet4.compliance_country_code::varchar(2) AS compliance_country_code
    ,evdet4.compliance_visibility::varchar(9) AS compliance_visibility
    ,evdet4.sys_compliance_visibility::varchar(1) AS sys_compliance_visibility
    ,evdet4.country_code::varchar(2) AS country_code
    ,evdet4.account_visited_nonvisited_technical::varchar(20) AS account_visited_nonvisited_technical
    ,evdet4.multichannel_type::varchar(21) AS multichannel_type
    ,evdet4.multichannel_activity::varchar(18) AS multichannel_activity
    ,evdet4.multichannel_activity_type::varchar(255) AS multichannel_activity_type
    ,to_char(to_date(evdet4.event_date,'dd-mm-yyyy'),'YYYYMMDD')::varchar(8) AS event_date
    ,evdet4.event_attendee_contract_reception::varchar(255) AS event_attendee_contract_reception
    ,evdet4.therapeutic_area_name::varchar(255) AS therapeutic_area_name
    ,evdet4.therapeutic_area_brand_name::varchar(255) AS therapeutic_area_brand_name
    ,evdet4.event_attendee_contract_reception_colour::varchar(255) AS event_attendee_contract_reception_colour
    ,evdet4.customer_feedback_id::varchar(255) AS Customer_Feedback_Id
    ,evdet4.event_attendee_origin::varchar(16) AS event_attendee_origin
--TMP_YEARLY_FFS
    ,CASE
        WHEN TMP_YEARLY_FFS.Aux_Annual_FFS_GC IS NULL then 'No Value'
         WHEN COALESCE(TMP_YEARLY_FFS.FFS_Fee_Year_Limit,'NM') = 'NM' then 'Within limit'
         WHEN TMP_YEARLY_FFS.Aux_Annual_FFS_GC::varchar > TMP_YEARLY_FFS.FFS_Fee_Year_Limit then 'Above limit'
        ELSE 'Within limit'
    END::varchar(12) AS Event_Attendee_Framework_Max_Annual_Fees_Violation,
    CASE
        WHEN TMP_YEARLY_FFS.Aux_Annual_Contracts_Received IS NULL then 'No Value'
         WHEN REGEXP_SUBSTR(trim(TMP_YEARLY_FFS.FFS_Contracts_Year_Limit),'^[0-9]+') = '' OR TMP_YEARLY_FFS.FFS_Contracts_Year_Limit IS NULL then 'Within limit'
         WHEN TMP_YEARLY_FFS.Aux_Annual_Contracts_Received::varchar > TMP_YEARLY_FFS.FFS_Contracts_Year_Limit then 'Above limit'
        ELSE 'Within limit'
    END::varchar(12) AS Event_Attendee_Framework_Max_Annual_FFS_Contracts_Violation,
--TMP_YEARLY_ATTENDANCE
    CASE WHEN REGEXP_SUBSTR(trim(TMP_YEARLY_ATTENDANCE.Aux_Annual_promotional_event_Attendee_counter),'^[0-9]+') > 0
        THEN CASE WHEN COALESCE(TMP_YEARLY_ATTENDANCE.Promo_Events_Year_Limit,'') = ''
                THEN 'Within Limit'
                ELSE CASE WHEN TMP_YEARLY_ATTENDANCE.Aux_Annual_promotional_event_Attendee_counter::varchar > REGEXP_SUBSTR(trim(TMP_YEARLY_ATTENDANCE.Promo_Events_Year_Limit),'^[0-9]+')
                        THEN 'Above Limit' ELSE 'Within Limit'
                    END
            END
        ELSE NULL
    END::varchar(12) AS Event_Attendee_Framework_Max_Annual_Promo_Events_Violation,
    CASE WHEN TMP_YEARLY_ATTENDANCE.Aux_Annual_medical_event_Attendee_counter > 0
        THEN CASE WHEN COALESCE(TMP_YEARLY_ATTENDANCE.Meded_Events_Year_Limit,'') = ''
                THEN 'Within Limit'
                ELSE CASE WHEN REGEXP_SUBSTR(trim(TMP_YEARLY_ATTENDANCE.Aux_Annual_medical_event_Attendee_counter),'^[0-9]+') > REGEXP_SUBSTR(trim(TMP_YEARLY_ATTENDANCE.meded_Events_Year_Limit),'^[0-9]+')
                        THEN 'Above Limit' ELSE 'Within Limit'
                    END
            END
        ELSE NULL
    END::varchar(12) AS Event_Attendee_Framework_Max_Annual_Meded_Events_Violation,
--TMP_YEARLY_ATTENDANCE_MAIN_KPI
    CASE WHEN TMP_YEARLY_KPI.aux_annual_promotional_event_attendee_counter_main_kpi > 0
        THEN (CASE WHEN COALESCE(TMP_YEARLY_KPI.promo_events_year_limit,'') = ''
                THEN 'Within Limit'
                ELSE CASE WHEN TMP_YEARLY_KPI.aux_annual_promotional_event_attendee_counter_main_kpi::varchar > REGEXP_SUBSTR(trim(TMP_YEARLY_KPI.promo_events_year_limit),'^[0-9]+')
                    THEN 'Above Limit' ELSE 'Within Limit' END
            END)
        ELSE 'Within Limit'
    END::varchar(12) AS Event_Attendee_Framework_Max_Annual_Promo_Events_Violation_Main_KPI,
    CASE WHEN TMP_YEARLY_KPI.aux_annual_Medical_event_attendee_counter_main_kpi > 0
        THEN (CASE WHEN COALESCE(TMP_YEARLY_KPI.meded_events_year_limit,'') = ''
                THEN 'Within Limit'
                ELSE CASE WHEN TMP_YEARLY_KPI.aux_annual_medical_event_attendee_counter_main_kpi::varchar > REGEXP_SUBSTR(trim(TMP_YEARLY_KPI.meded_events_year_limit),'^[0-9]+')
                    THEN 'Above Limit' ELSE 'Within Limit' END
            END)
        ELSE 'Within Limit'
    END::varchar(12) AS Event_Attendee_Framework_Max_Annual_MedEd_Events_Violation_Main_KPI,
--Add 2 additional fields
    CASE
        WHEN LOWER(evdet4.event_attendee_record_type) NOT IN ('speaker','consultant')
            THEN NULL
        WHEN evdet4.event_attendee_service_hours IN ('','0',' ','0.00') OR evdet4.event_attendee_service_hours IS NULL
            THEN 'No Value'
        WHEN ST.maximum_ratio_preparation_hours_service_hours = ''
            THEN 'Following the Framework'
        WHEN (evdet4.event_attendee_preparation_hours / evdet4.event_attendee_service_hours::numeric(38,12)) > ST.maximum_ratio_preparation_hours_service_hours
            THEN 'Outside of the Framework'
        ELSE 'Following the Framework'
    END::varchar(25) AS Event_Attendee_Framework_Preparation_Service_Hours_Violation,
    CASE
        WHEN LOWER(evdet4.event_attendee_record_type) NOT IN ('speaker','consultant')
            THEN NULL
        WHEN evdet4.event_attendee_speaker_fee_GC = ''
            THEN 'No Value'
        WHEN CASE WHEN ST.maximum_fee_for_service_per_hcp_per_event IS NULL OR ST.maximum_fee_for_service_per_hcp_per_event IN ('','NM')
                  THEN 'NM'
                  ELSE (REGEXP_SUBSTR(trim(ST.maximum_fee_for_service_per_hcp_per_event),'^[0-9]+')::numeric(38,12) / CT.conversionrate::numeric(38,12))::varchar(255)
                END = ''
            THEN 'Following the Framework'
        WHEN evdet4.event_attendee_speaker_fee_gc > CASE WHEN ST.maximum_fee_for_service_per_hcp_per_event IS NULL OR ST.maximum_fee_for_service_per_hcp_per_event IN ('','NM')
                  THEN 'NM'
                  ELSE (REGEXP_SUBSTR(trim(ST.maximum_fee_for_service_per_hcp_per_event),'^[0-9]+')::numeric(38,12) / CT.conversionrate::numeric(38,12))::varchar(255)
                END
            THEN 'Outside of the Framework'
        ELSE 'Following the Framework'
    END::varchar(25) AS Event_Attendee_Framework_FFS_HCP_Event_Violation,
     
CASE WHEN mncv.Account_Id IS NOT NULL AND mncv.Territory_Id IS NOT NULL
        THEN 1
            ELSE 0
                END AS flag_territory_belong,
CASE WHEN mncvs.Account_Id IS NOT NULL AND mncvs.Territory_Id IS NOT NULL AND yearmonth IS NOT NULL
        THEN 1
            ELSE 0
                END AS historical_flag_territory_belong, 
	evdet4.event_attendee_nps::decimal(2,0) AS event_attendee_nps,
	evdet4.Event_Record_Type::varchar(255) AS Event_Record_Type,
	evdet4.Event_Sub_Type::varchar(1000) AS Event_Sub_Type,
	evdet4.Event_Meeting_Style::varchar(1000) AS Event_Meeting_Style,
	evdet4.Event_Status::varchar(255) AS Event_Status,
	evdet4.channel_new::varchar(255) AS channel_new
          
FROM {{ ref('tmp_f_event_detail_4') }} AS evdet4
 
LEFT OUTER JOIN {{ source('raw', 'slider_threshold') }} AS ST ON ST.country = evdet4.Compliance_Country /*Excel File*/
LEFT OUTER JOIN {{ source('raw', 'currency_type') }} CT ON CT.isocode = ST.Currency
 
LEFT OUTER JOIN (
    SELECT DISTINCT *
    FROM {{ ref('tmp_yearly_ffs') }}
) TMP_YEARLY_FFS ON TMP_YEARLY_FFS.aux_event_speaker_framework_key = evdet4.aux_event_speaker_framework_key
  AND TMP_YEARLY_FFS.compliance_country = evdet4.compliance_country
LEFT OUTER JOIN (
    SELECT DISTINCT *
    FROM {{ ref('tmp_yearly_attendance') }}
) TMP_YEARLY_ATTENDANCE ON TMP_YEARLY_ATTENDANCE.aux_event_attendee_framework_key = evdet4.aux_event_attendee_framework_key
  AND TMP_YEARLY_ATTENDANCE.compliance_country = evdet4.compliance_country
LEFT OUTER JOIN (
    SELECT DISTINCT *
    FROM {{ ref('tmp_yearly_attendance_main_kpi') }}
) TMP_YEARLY_KPI ON TMP_YEARLY_KPI.aux_event_attendee_framework_key = evdet4.aux_event_attendee_framework_key
  AND TMP_YEARLY_KPI.compliance_country = evdet4.compliance_country
   
LEFT JOIN (SELECT Account_Id, Territory_Id FROM {{ ref('m_null_country_values') }}
            GROUP BY Account_Id, Territory_Id) mncv
       ON evdet4.account_id = mncv.Account_Id AND evdet4.territory_nominal_id = mncv.Territory_Id
   
LEFT JOIN (SELECT Account_Id, Territory_Id, yearmonth FROM {{ source('raw', 'm_null_country_values_snapshot_monthly_historical') }}
            GROUP BY Account_Id, Territory_Id, yearmonth) mncvs
       ON evdet4.account_id = mncvs.Account_Id AND evdet4.territory_nominal_id = mncvs.Territory_Id AND left(to_char(to_date('1899-12-30', 'YYYY-MM-DD') + evdet4.date::integer,'YYYYMMDD')::varchar(8),6) = mncvs.yearmonth