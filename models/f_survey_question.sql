{{ config(
	post_hook="grant all privileges on {{ this }} to {{ var('snowflake_user_grant_privileges') }}")
}}

SELECT DISTINCT
	 sq.id::varchar(20)					as survey_question_id
	,sq.name::varchar(255)				as survey_question_name
	,sq.QUESTION_VOD__C::varchar(20)		as question_id
	,sq.SURVEY_VOD__C::varchar(20)		as survey_id
	,LEFT(sq.TEXT_VOD__C, 1000)::varchar(1000)			as survey_question_text
	,LEFT(sq.ANSWER_CHOICE_VOD__C, 1000)::varchar(1000)	as survey_question_answer_choice
FROM {{ var('schema') }}.survey_question_raw sq
JOIN {{ var('schema') }}.question_response_vod__c_raw qr ON qr.SURVEY_QUESTION_VOD__C = sq.ID
