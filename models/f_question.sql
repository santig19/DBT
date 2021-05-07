{{ config(
	post_hook="grant all privileges on {{ this }} to {{ var('snowflake_user_grant_privileges') }}")
}}

SELECT DISTINCT
	 qu.ID::varchar(20) as Question_Id
	,qu.NAME::varchar(255) as Question_Name
	,LEFT(qu.TEXT_VOD__C, 1000)::varchar(1000) as Question_Text
	,qu.ACTIVE_VOD__C::varchar(1) as Question_Active
	,qu.LANGUAGE_VOD__C::varchar(255) as Question_Language
	,LEFT(qu.ANSWER_CHOICE_VOD__C, 1000)::varchar(1000) as Question_Answer_Choice
FROM {{ var('schema') }}.question_raw qu 
JOIN {{ var('schema') }}.survey_question_raw sq ON sq.QUESTION_VOD__C = qu.ID
JOIN {{ var('schema') }}.question_response_vod__c_raw qr ON qr.SURVEY_QUESTION_VOD__C = sq.ID
