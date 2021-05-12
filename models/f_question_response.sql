{{ config(
	post_hook="grant all privileges on {{ this }} to {{ var('snowflake_user_grant_privileges') }}")
}}

SELECT DISTINCT
    CASE WHEN (qr.DATE_VOD__C IS NULL OR qr.DATE_VOD__C='') THEN NULL ELSE TO_CHAR(TO_DATE(DATE_VOD__C, 'YYYYMMDD HH24:MI:SS'), 'YYYYMMDD')::varchar(255) END AS question_response_date
  , qr.ID                                                               AS question_response_id
  , qr.NAME                                                             AS question_response_name
  , qr.NUMBER_VOD__C                                                    AS question_response_number
  , qr.ORDER_VOD__C                                                     AS question_response_order
  , qr.RECORDTYPEID                                                     AS question_response_record_type
  , qr.TEXT_VOD__C                                                      AS question_response_text
  ,LEFT(qr.RESPONSE_VOD__C, 1000)::varchar(1000) AS response
  , qr.SCORE_VOD__C                                                     AS score
  , qr.SURVEY_QUESTION_VOD__C                                           AS survey_question_id
  , qr.SURVEY_TARGET_VOD__C                                             AS survey_target_id
  , CASE
        WHEN qr.SURVEY_TARGET_VOD__C=F_SURVEY_TARGET.survey_target_id
	   THEN F_SURVEY_TARGET.region
        ELSE 'NM'
    END                                                                 AS question_response_region
  , 1::numeric(10,0)                                                    AS response_counter
  , qr.TYPE_VOD__C                                                      AS question_response_type
  , qr.JJ_CLICKTOOLS_ANSWER__C                                          AS survey_clicktool_answer
  , CASE 
	  WHEN COALESCE(qr.DATETIME_VOD__C, '') = '' 
		THEN NULL 
		ELSE TO_CHAR(TO_DATE(qr.DATETIME_VOD__C, 'YYYYMMDD HH24:MI:SS'), 'YYYYMMDD') 
    END::varchar(255) AS question_response_datetime
FROM {{ source('raw', 'question_response') }} qr
LEFT OUTER JOIN
	(
	SELECT *, ROW_NUMBER() OVER (PARTITION BY survey_target_id ORDER BY survey_last_modified_date_time DESC) as rowid
	FROM (
		SELECT DISTINCT
	    		st.id as survey_target_id
	    		, CASE
	          	WHEN LEN (cs.JJ_REGION__C) > 0
	              		THEN cs.JJ_REGION__C
	              		ELSE 'NM'
	      	END AS region
			, st.LASTMODIFIEDDATE AS survey_last_modified_date_time
		FROM {{ source('raw', 'survey_target') }} st
		LEFT OUTER JOIN {{ source('raw', 'account') }} ac on ac.ID = st.ACCOUNT_VOD__C
		LEFT OUTER JOIN {{ source('raw', 'country_settings') }} cs ON cs.JJ_COUNTRY_ISO_CODE__C = ac.country_iso_code
		LEFT OUTER JOIN {{ source('raw', 'question_response') }} qr ON st.ID = qr.SURVEY_TARGET_VOD__C
		WHERE LOWER(st.STATUS_VOD__C) IN ('saved_vod', 'pending_vod', 'late_submission_vod', 'submitted_vod') 
		AND TO_DATE(st.LASTMODIFIEDDATE, 'YYYYMMDD HH24:MI:SS') BETWEEN TO_DATE((extract(year from GETDATE())-2)::varchar(4) || RIGHT('0' || RTRIM(extract(month from GETDATE())), 2) || '01', 'YYYYMMDD') AND GETDATE()
		AND TO_DATE(st.CREATEDDATE, 'YYYYMMDD HH24:MI:SS') >= TO_DATE((extract(year from GETDATE())-2)::varchar || '0101','YYYYMMDD')	
		) 
	) F_SURVEY_TARGET ON qr.SURVEY_TARGET_VOD__C = F_SURVEY_TARGET.survey_target_id AND F_SURVEY_TARGET.rowid=1
WHERE TO_DATE(qr.CREATEDDATE, 'YYYYMMDD HH24:MI:SS') >= TO_DATE((extract(year from GETDATE())-2)::varchar || '0101','YYYYMMDD')
