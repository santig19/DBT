{{ config(
	post_hook="grant all privileges on {{ this }} to {{ var('snowflake_user_grant_privileges') }}")
}}

SELECT
	 Compliance_Country
	,Aux_Event_Attendee_Framework_Key
    ,Aux_Annual_Promotional_Event_Attendee_Counter_Main_KPI::numeric(10, 0) AS Aux_Annual_Promotional_Event_Attendee_Counter_Main_KPI	
    ,Aux_Annual_Medical_Event_Attendee_Counter_Main_KPI::numeric(10, 0) AS Aux_Annual_Medical_Event_Attendee_Counter_Main_KPI	
	,ST.n_of_company_organized_promo_events_and_hcp_can_attend_per_year AS Promo_Events_Year_Limit
	,ST.n_of_company_organized_meded_events_and_hcp_can_attend_per_year AS MedEd_Events_Year_Limit

FROM (
	SELECT
		compliance_country AS Compliance_Country,
		aux_event_attendee_framework_key AS Aux_Event_Attendee_Framework_Key,
		SUM(CASE WHEN aux_promotional_event_attendee_counter IS NULL OR LEN(TRIM(aux_promotional_event_attendee_counter))=0 
			THEN 0 ELSE aux_promotional_event_attendee_counter 
		END) AS Aux_Annual_Promotional_Event_Attendee_Counter_Main_KPI,
		SUM(CASE WHEN aux_medical_event_attendee_counter IS NULL OR LEN(TRIM(aux_medical_event_attendee_counter))=0 
			THEN 0 ELSE aux_medical_event_attendee_counter 
		END) AS Aux_Annual_Medical_Event_Attendee_Counter_Main_KPI
	FROM {{ ref('tmp_f_event_detail_4') }} evdet4 
	LEFT OUTER JOIN (
		SELECT DISTINCT F.Event_Id, F.Event_Status
    		FROM {{ ref('f_event') }} F
	) F on F.Event_Id = evdet4.Event_Id
	WHERE LOWER(event_attendee_record_type) NOT IN ('speaker','consultant','internal attendee')
	AND LOWER(event_attendee_attendance_status) NOT IN ('not attended','cancelled')
	AND LOWER(f.event_status) NOT IN ('cancelled','cancelled (costs incurred)')
	GROUP BY compliance_country, aux_event_attendee_framework_key
)

LEFT OUTER JOIN {{ source('raw', 'slider_threshold') }} ST ON ST.Country=Compliance_Country /*Excel File*/
