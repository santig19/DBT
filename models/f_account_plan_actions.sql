{{ config(
	post_hook="grant all privileges on {{ this }} to {{ var('snowflake_user_grant_privileges') }}")
}}

-------- emea_mto.buw_calls_call_planning_f_account_plan_actions --------

select AI.account_plan_vod__c::varchar(255) as account_plan_id,
	   AI.account_tactic_vod__c::varchar(255) as account_plan_objective_id,
	   AI.id::varchar(255) as account_plan_action_id,
	   case when (Position(CHR(92) || CHR(92), AI.name) > 0) OR Position(CHR(124) || CHR(34), AI.name) > 0 OR Position(CHR(92) || CHR(124), AI.name) > 0 OR Position(CHR(92) || CHR(34), AI.name) > 0 then REPLACE(REPLACE(REPLACE(REPLACE(AI.name, CHR(92) || CHR(92), CHR(92)), CHR(124) || CHR(34), CHR(34)), CHR(92) || CHR(124), CHR(124)), CHR(92) || CHR(34), CHR(34)) ELSE AI.name end::varchar(255) as account_plan_action,
	   '1'::int as account_plan_action_counter,
	   AI.account_vod__c::varchar(255) as account_id,
	   AI.assignee_vod__c::varchar(255) as assignee_id,
	   UB.name::varchar(255) as account_plan_action_owner,
	   PB.name::varchar(255) as account_plan_action_owner_profile_name,
       AI.Country_ISO_Code::varchar(100) as account_plan_action_country_code,
	   CS.name::varchar(255) as account_plan_action_country,
	   CS.jj_region__c::varchar(255) as account_plan_action_region,
       AI.createdbyid::varchar(255) as account_plan_action_created_by_id,
	   left (AI.createddate, 8)::varchar(8) as account_plan_action_created_date,
	   left (AI.lastmodifieddate, 8)::varchar(8) as account_plan_action_last_modified_date,
	   left (AI.due_date_vod__c, 8)::varchar(8) as account_plan_action_due_date,
	   left (AI.start_date_vod__c, 8)::varchar(8) as account_plan_action_start_date,
	   left (AI.completed_date_vod__c, 8)::varchar(8) as account_plan_action_completed_date,
	   AI.jj_priority__c::varchar(510) as account_plan_action_priority,
	   AI.jj_status__c::varchar(510) as account_plan_action_status,
	   case when (Position(CHR(92) || CHR(92), AI.jj_status_formula__c) > 0) OR Position(CHR(124) || CHR(34), AI.jj_status_formula__c) > 0 OR Position(CHR(92) || CHR(124), AI.jj_status_formula__c) > 0 OR Position(CHR(92) || CHR(34), AI.jj_status_formula__c) > 0 then REPLACE(REPLACE(REPLACE(REPLACE(AI.jj_status_formula__c, CHR(92) || CHR(92), CHR(92)), CHR(124) || CHR(34), CHR(34)), CHR(92) || CHR(124), CHR(124)), CHR(92) || CHR(34), CHR(34)) ELSE AI.jj_status_formula__c end::varchar(255) as account_plan_action_traffic_light_status,
	   case when AI.jj_status_formula__c like '%/img/samples/light_green.gif%' then '1' else '0' end::varchar(1) as account_plan_action_traffic_light_status_green_counter,
	   case when AI.jj_status_formula__c like '%/img/samples/light_yellow.gif%' then '1' else '0' end::varchar(1) as account_plan_action_traffic_light_status_amber_counter,
	   case when AI.jj_status_formula__c like '%/img/samples/light_red.gif%' then '1' else '0' end::varchar(1) as account_plan_action_traffic_light_status_red_counter,
       AI.jj_Issue__c::varchar(510) as account_plan_action_issue,
       AI.jj_Solution__c::varchar(510) as account_plan_action_solution,
       AI.recordtypeid::varchar(36) as account_plan_action_record_type  
 from      {{ source('raw', 'action_item') }}     as AI
inner join {{ source('raw', 'account_tactic') }}  as AT
   on AI.ACCOUNT_TACTIC_VOD__C                                   = AT.ID
 left join {{ source('raw', 'user') }}                   as UB
   on AI.assignee_vod__c                                         = UB.id
 left join {{ source('raw', 'profile') }}                as PB
   on PB.id                                                      = UB.profileid
  and AI.assignee_vod__c                                         = UB.id
 left join {{ source('raw', 'country_settings') }} as CS
   on AI.Country_ISO_Code                                        = CS.jj_country_iso_code__c
group by account_plan_id,
	     account_plan_objective_id,
	     account_plan_action_id,
	     account_plan_action,
	     account_plan_action_counter,
	     account_id,
	     assignee_id,
	     account_plan_action_owner,
	     account_plan_action_owner_profile_name,
         account_plan_action_country_code,
	     account_plan_action_country,
	     account_plan_action_region,
         account_plan_action_created_by_id,
	     account_plan_action_created_date,
	     account_plan_action_last_modified_date,
	     account_plan_action_due_date,
	     account_plan_action_start_date,
	     account_plan_action_completed_date,
	     account_plan_action_priority,
	     account_plan_action_status,
	     account_plan_action_traffic_light_status,
	     account_plan_action_traffic_light_status_green_counter,
	     account_plan_action_traffic_light_status_amber_counter,
	     account_plan_action_traffic_light_status_red_counter,
	     account_plan_action_issue,
         account_plan_action_solution,
         account_plan_action_record_type  