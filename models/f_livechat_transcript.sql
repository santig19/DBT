{{ config(
	post_hook="grant all privileges on {{ this }} to {{ var('snowflake_user_grant_privileges') }}")
}}

select livechattranscript.id as live_chat_id,
       livechattranscript.ownerid as owner_id ,
       livechattranscript.name as live_chat_name,
       livechattranscript.createddate as live_chat_created_date,
       livechattranscript.createdbyid as live_chat_created_by_id,
       livechattranscript.systemmodstamp as live_chat_systemmodstamp,
       livechattranscript.livechatvisitorid as live_chat_visitor_id,
       livechattranscript.caseid as case_id,
       livechattranscript.accountid as account_id,
       livechattranscript.status as live_chat_status,
       livechattranscript.requesttime as live_chat_request_time,
       livechattranscript.waittime as wait_time_in_seconds,
       livechattranscript.starttime as live_chat_start_time,
       livechattranscript.endtime as live_chat_end_time,
       livechattranscript.chatduration as live_chat_duration_in_seconds,
       livechattranscript.endedby as live_chat_ended_by,
       livechattranscript.averageresponsetimevisitor as visitor_avg_response_time,
       livechattranscript.visitormessagecount as visitor_message_count,
       livechattranscript.maxresponsetimevisitor as visitor_max_response_time,
       livechattranscript.visitornetwork as visitor_network,
       livechattranscript.averageresponsetimeoperator as operator_avg_response_time,
       livechattranscript.operatormessagecount as operator_message_count,
       livechattranscript.maxresponsetimeoperator as operator_max_response_time,
       livechattranscript.chatkey as chat_key,
       livechattranscript.abandoned as abandoned,
       livechattranscript.browser as live_chat_browser,
       livechattranscript.platform as live_chat_platform,
       livechattranscript.referreruri as live_chat_referrer_uri,
       u.country as owner_country_code,
       u.jj_position_type__c as owner_position_type,
       u.division as owner_division,
       u.department as owner_department,
       livechatvisitor.sessionkey as live_chat_visitor_session_key,
       account.cods_external_id__c as account_cods_code,
       account.name as 	account_name,
       account.country_jj__c as account_country_code,     
       'Live Chat' as touch_point
 from {{ var('schema') }}.livechattranscript_raw as livechattranscript
 left join {{ var('schema') }}.user_raw as u
 on u.id = livechattranscript.ownerid  
 left join {{ var('schema') }}.livechatvisitor_raw as livechatvisitor
 on livechatvisitor.id = livechattranscript.livechatvisitorid 
 left join {{ var('schema') }}.account_raw as account
 on account.id = livechattranscript.accountid