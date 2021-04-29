{{ config(
	post_hook="grant all privileges on {{ this }} to {{ var('snowflake_user_grant_privileges') }}")
}}

SELECT
    st.id                          as survey_target_id
  , st.ACCOUNT_DISPLAY_NAME_VOD__C as survey_target_account_name
  , st.ACCOUNT_VOD__C              as account_id
  , st.STATUS_VOD__C               as survey_target_status
  , st.JJ_CALL__C                  as call_id
  , st.JJ_CUSTOMER_REQUEST__C      as customer_request_id
  , (CASE WHEN LEN(st.JJ_EVENT__C)>0 THEN st.JJ_EVENT__C ELSE NULL END)::varchar(255) as event_id
  , (CASE WHEN LEN(st.JJ_NPS_SCORE__C)>0 THEN st.JJ_NPS_SCORE__C ELSE '0' END)::numeric(10,0) as survey_target_nps_score
  , st.JJ_NPS_STATUS__C            as survey_nps_target_status
  , TO_CHAR(TO_DATE(st.LASTMODIFIEDDATE, 'YYYYMMDD HH24:MI:SS'), 'YYYYMMDD')::numeric(8,0) as date
  , st.name                        as survey_target_name
  , TO_CHAR(TO_DATE(st.LASTMODIFIEDDATE, 'YYYYMMDD HH24:MI:SS'), 'YYYY-MM-DD')::varchar(255) as survey_last_modified_date
  , st.LASTMODIFIEDDATE  as survey_last_modified_date_time
  , st.OWNERID            as survey_target_ownerid
  , st.LASTMODIFIEDBYID   as survey_target_last_modifiedby_id
  , st.SURVEY_VOD__C      as survey_id
  , st.JJ_CLOSE_STATUS__C as survey_target_close_status
  , '1'::numeric(10,0)    as survey_target_counter
  , CASE
        WHEN LEN(ac.country_iso_code) > 0
        THEN ac.country_iso_code
        ELSE 'NM'
    END as country_code
  , CASE
        WHEN LEN(cs.NAME) > 0
        THEN cs.NAME
        ELSE 'NM'
    END as country
  , CASE
        WHEN LEN (cs.JJ_REGION__C) > 0
        THEN cs.JJ_REGION__C
        ELSE 'NM'
    END as region
  , CASE
        WHEN st.id = qr.SURVEY_TARGET_VOD__C
        THEN qr.JJ_CLICKTOOLS_ANSWER__C
        ELSE NULL
    END as Survey_Clicktool_Answer
  , CASE
        WHEN qr.JJ_CLICKTOOLS_ANSWER__C <> ''
        THEN st.OWNERID
        ELSE st.LASTMODIFIEDBYID
    END as employee_id,
CASE WHEN mncv.Account_Id IS NOT NULL AND mncv.Territory_Id IS NOT NULL
        THEN 1
           ELSE 0
               END AS flag_territory_belong,
CASE WHEN mncvs.Account_Id IS NOT NULL AND mncvs.Territory_Id IS NOT NULL AND yearmonth IS NOT NULL
        THEN 1
            ELSE 0
                END AS historical_flag_territory_belong
  , st.JJ_Ext_Response_Id__c::varchar(60)   as Medallia_Response_ID
  , NULLIF(st.JJ_CSAT_Score__c,'')::decimal(2,0)    as CSAT_Score
  , NULLIF(st.JJ_NPS_Score__c,'')::decimal(2,0)         as NPS_Score
  , qr.ID::varchar(36)                      as Question_Response_id
FROM {{ var('schema') }}.survey_target_vod__c_raw st
LEFT OUTER JOIN {{ var('schema') }}.account_raw ac on ac.ID = st.ACCOUNT_VOD__C
LEFT OUTER JOIN {{ var('schema') }}.country_settings_raw cs ON cs.JJ_COUNTRY_ISO_CODE__C = ac.country_iso_code
LEFT OUTER JOIN {{ var('schema') }}.question_response_vod__c_raw qr ON st.ID = qr.SURVEY_TARGET_VOD__C
LEFT JOIN {{ ref('tmp_user_territory') }} ut 
	on ut.USERID = CASE WHEN qr.JJ_CLICKTOOLS_ANSWER__C <> '' THEN st.OWNERID ELSE st.LASTMODIFIEDBYID END
LEFT JOIN (SELECT Account_Id, Territory_Id FROM {{ ref('m_null_country_values') }}
            GROUP BY Account_Id, Territory_Id) mncv
       ON st.ACCOUNT_VOD__C = mncv.Account_Id AND CASE WHEN LEN(ut.territoryid) > 0 THEN ut.territoryid ELSE 'NM' END = mncv.Territory_Id
LEFT JOIN (SELECT Account_Id, Territory_Id, yearmonth FROM {{ var('schema') }}.buw_alignment_m_null_country_values_snapshot_monthly_historical
            GROUP BY Account_Id, Territory_Id, yearmonth) mncvs
       ON st.ACCOUNT_VOD__C = mncvs.Account_Id AND CASE WHEN LEN(ut.territoryid) > 0 THEN ut.territoryid ELSE 'NM' END = mncvs.Territory_Id AND left(TO_CHAR(TO_DATE(st.LASTMODIFIEDDATE, 'YYYYMMDD HH24:MI:SS'), 'YYYYMMDD')::numeric(8,0),6) = mncvs.yearmonth
WHERE LOWER(st.STATUS_VOD__C) IN ('saved_vod', 'pending_vod', 'late_submission_vod', 'submitted_vod')
	AND TO_CHAR(TO_DATE(st.LASTMODIFIEDDATE,'YYYYMMDD HH24:MI:SS'),'yyyymmdd') BETWEEN TO_CHAR(add_months(CURRENT_DATE,-24),'YYYYMM')+'01' AND TO_CHAR(current_date(), 'yyyymmdd')