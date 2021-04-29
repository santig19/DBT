{{ config(
	post_hook="grant all privileges on {{ this }} to {{ var('snowflake_user_grant_privileges') }}")
}}

SELECT 
	 Compliance_Country
	,Aux_Event_Speaker_Framework_Key
	,Aux_Annual_FFS_GC
	,Aux_Annual_Contracts_Received

	,ST.Maximum_FFS_contracts_with_Janssen_per_year as FFS_Contracts_Year_Limit
	,CASE WHEN ST.Currency = CT.IsoCode
		THEN (CASE WHEN REGEXP_SUBSTR(trim(ST.Maximum_yearly_total_annual_fees_FFS),'^[0-9]+') = '' OR ST.Maximum_yearly_total_annual_fees_FFS IS NULL 
			THEN '0'
			ELSE REGEXP_SUBSTR(trim(ST.Maximum_yearly_total_annual_fees_FFS),'^[0-9]+') END::numeric(20,2) / CT.conversionrate::numeric(20,2))::varchar(255)
    		ELSE 'NM' 
	 END::varchar(255) as FFS_Fee_Year_Limit    
FROM (
	SELECT 
		Compliance_Country as Compliance_Country,
    		Aux_Event_Speaker_Framework_Key as Aux_Event_Speaker_Framework_Key,
    	sum(case when len(trim(Event_Attendee_Speaker_Fee_GC))=0 or Event_Attendee_Speaker_Fee_GC is null or Event_Attendee_Speaker_Fee_GC = 'NM'
			then '0' else Event_Attendee_Speaker_Fee_GC::varchar(255)
		end) as Aux_Annual_FFS_GC,
    	sum(case when len(trim(Aux_Event_Attendee_Received_Signed_Contract_Counter))=0 or Aux_Event_Attendee_Received_Signed_Contract_Counter is null 
			then '0' else Aux_Event_Attendee_Received_Signed_Contract_Counter::varchar(255) 
		end) as Aux_Annual_Contracts_Received
	FROM {{ ref('tmp_f_event_detail_4') }} evdet4
	LEFT OUTER JOIN (
		SELECT DISTINCT F.Event_Id, F.Event_Status
    		FROM {{ ref('f_event') }} F
	) F on F.Event_Id = evdet4.Event_Id
	WHERE LOWER(Event_Attendee_Record_Type) IN ('speaker','consultant') 
	AND LOWER(Event_Attendee_Attendance_Status) NOT IN ('not attended', 'cancelled')
	AND CASE WHEN evdet4.Event_Id = f.Event_Id THEN LOWER(f.Event_Status) ELSE 'NM' END NOT IN ('cancelled','cancelled (costs incurred)')
	GROUP BY Compliance_Country, Aux_Event_Speaker_Framework_Key
)

LEFT OUTER JOIN {{ var('schema') }}.slider_threshold_raw ST on ST.Country=Compliance_Country
LEFT OUTER JOIN {{ var('schema') }}.currency_type_raw CT ON CT.isocode = ST.Currency 
