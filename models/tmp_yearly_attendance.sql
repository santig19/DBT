{{ config(
	post_hook="grant all privileges on {{ this }} to {{ var('snowflake_user_grant_privileges') }}")
}}
	
SELECT 
	 Compliance_Country
	,Aux_Event_Attendee_Framework_Key
    ,Aux_Annual_Promotional_Event_Attendee_Counter::numeric(10, 0)AS Aux_Annual_Promotional_Event_Attendee_Counter
    ,Aux_Annual_Medical_Event_Attendee_Counter::numeric(10, 0) AS Aux_Annual_Medical_Event_Attendee_Counter
	,ST.n_of_company_organized_promo_events_and_hcp_can_attend_per_year AS Promo_Events_Year_Limit
	,ST.n_of_company_organized_meded_events_and_hcp_can_attend_per_year AS MedEd_Events_Year_Limit
FROM (
	SELECT Compliance_Country, Aux_Event_Attendee_Framework_Key, 
		SUM(CASE WHEN (aux_promotional_event_attendee_counter IS NULL OR LEN(trim(aux_promotional_event_attendee_counter)) = 0) 
			THEN 0 ELSE aux_promotional_event_attendee_counter 
		END) AS Aux_Annual_Promotional_Event_Attendee_Counter,
		SUM(CASE WHEN (aux_medical_event_attendee_counter IS NULL OR LEN(trim(aux_medical_event_attendee_counter))=0) 
			THEN 0 ELSE aux_medical_event_attendee_counter 
		END) AS Aux_Annual_Medical_Event_Attendee_Counter
	FROM {{ ref('tmp_f_event_detail_4') }} evdet4
	LEFT OUTER JOIN (
		SELECT DISTINCT F.Event_Id, F.Event_Status
    		FROM {{ ref('f_event') }} F
	) F on F.Event_Id = evdet4.Event_Id
	WHERE LOWER(Event_Attendee_Record_Type) NOT IN ('speaker','consultant','internal attendee')
	AND LOWER(Event_Attendee_Attendance_Status) NOT IN ('not attended', 'cancelled')
	AND CASE WHEN evdet4.Event_Id = F.Event_Id THEN LOWER(F.Event_Status) ELSE 'NM' END NOT IN ('cancelled','cancelled (costs incurred)')
	GROUP BY Compliance_Country, Aux_Event_Attendee_Framework_Key
)

LEFT OUTER JOIN {{ var('schema') }}.slider_threshold_raw ST ON ST.Country=Compliance_Country
