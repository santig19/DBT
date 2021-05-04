{{ config(
	post_hook="grant all privileges on {{ this }} to {{ var('snowflake_user_grant_privileges') }}")
}}

select AT.Account_Plan_vod__c::varchar(255) as account_plan_id,
	   AT.Id::varchar(255) as account_plan_objective_id,
	   1::varchar(1) as account_plan_objective_counter,	   
	   case when (Position(CHR(92) || CHR(92), AT.Name) > 0) OR Position(CHR(124) || CHR(34), AT.Name) > 0 OR Position(CHR(92) || CHR(124), AT.Name) > 0 OR Position(CHR(92) || CHR(34), AT.Name) > 0 then REPLACE(REPLACE(REPLACE(REPLACE(AT.Name, CHR(92) || CHR(92), CHR(92)), CHR(124) || CHR(34), CHR(34)), CHR(92) || CHR(124), CHR(124)), CHR(92) || CHR(34), CHR(34)) ELSE AT.Name end::varchar(160) as account_plan_objective,	   	   
	   AT.Account_vod__c::varchar(100) as account_id,
	   AT.country_iso_code::varchar(100) as account_plan_objective_country_code,
	   CS.Name::varchar(50) as account_plan_objective_country,
	   CS.jj_Region__c::varchar(50) as account_plan_objective_region,
	   UB.name::varchar(255) as account_plan_objective_owner_name,
	   PB.name::varchar(255) as account_plan_objective_owner_profile_name,
	   case when AT.complete_vod__c = 1 then 'yes' else 'no' end::varchar (5) as account_plan_objective_is_complete,
	   case when (Position(CHR(92) || CHR(92), AT.description_vod__c) > 0) OR Position(CHR(124) || CHR(34), AT.description_vod__c) > 0 OR Position(CHR(92) || CHR(124), AT.description_vod__c) > 0 OR Position(CHR(92) || CHR(34), AT.description_vod__c) > 0 then REPLACE(REPLACE(REPLACE(REPLACE(AT.description_vod__c, CHR(92) || CHR(92), CHR(92)), CHR(124) || CHR(34), CHR(34)), CHR(92) || CHR(124), CHR(124)), CHR(92) || CHR(34), CHR(34)) ELSE AT.description_vod__c end::varchar(10000) as account_plan_objective_description,	   
	   AT.type_vod__c::varchar(510) as account_plan_objective_type,
	   case when AT.jj_medical__c = 1 then 'yes' else 'no' end::varchar (5) as account_plan_objective_is_medical,
	   AT.jj_percentage__c::varchar (5) as account_plan_objective_percentage,
	   left (AT.createddate,8)::varchar(8) as account_plan_objective_created_date,
	   left (AT.lastmodifieddate,8)::varchar(8) as account_plan_objective_last_modified_date,
	   case when (Position(CHR(92) || CHR(92), AT.jj_progress__c) > 0) OR Position(CHR(124) || CHR(34), AT.jj_progress__c) > 0 OR Position(CHR(92) || CHR(124), AT.jj_progress__c) > 0 OR Position(CHR(92) || CHR(34), AT.jj_progress__c) > 0 then REPLACE(REPLACE(REPLACE(REPLACE(AT.jj_progress__c, CHR(92) || CHR(92), CHR(92)), CHR(124) || CHR(34), CHR(34)), CHR(92) || CHR(124), CHR(124)), CHR(92) || CHR(34), CHR(34)) ELSE AT.jj_progress__c end::varchar(2600) as account_plan_objective_progress,   
	   AT.jj_priority__c::varchar (5) as account_plan_objective_priority,
	   AT.jj_impact__c::varchar(510) as account_plan_objective_impact,
	   AT.jj_key_strategic_drivers__c::varchar(510) as account_plan_objective_key_strategic_drivers,
	   case when (Position(CHR(92) || CHR(92), AT.jj_status__c) > 0) OR Position(CHR(124) || CHR(34), AT.jj_status__c) > 0 OR Position(CHR(92) || CHR(124), AT.jj_status__c) > 0 OR Position(CHR(92) || CHR(34), AT.jj_status__c) > 0 then REPLACE(REPLACE(REPLACE(REPLACE(AT.jj_status__c, CHR(92) || CHR(92), CHR(92)), CHR(124) || CHR(34), CHR(34)), CHR(92) || CHR(124), CHR(124)), CHR(92) || CHR(34), CHR(34)) ELSE AT.jj_status__c end::varchar(2600) as account_plan_objective_traffic_light_status,
	   case when AT.jj_status__c like '%/img/samples/light_green.gif%' then '1' else '0' end::varchar(1) as account_plan_objective_traffic_light_status_green_counter,
	   case when AT.jj_status__c like '%/img/samples/light_yellow.gif%' then '1' else '0' end::varchar(1) as account_plan_objective_traffic_light_status_amber_counter,
	   case when AT.jj_status__c like '%/img/samples/light_red.gif%' then '1' else '0' end::varchar(1) as account_plan_objective_traffic_light_status_red_counter,
	   AT.jj_key_actions_with_status_amber__c::varchar(13) as account_plan_objective_key_actions_with_status_amber,
	   AT.jj_key_actions_with_status_green_comp__c::varchar(13) as account_plan_objective_key_actions_with_status_green,
	   AT.jj_key_actions_with_status_red__c::varchar(13) as account_plan_objective_key_actions_with_status_red,
	   AT.jj_number_of_key_actions__c::varchar(13) as account_plan_objective_number_of_key_actions,
	   left (AT.jj_Actual_End_Date__c, 8)::varchar(8) as account_plan_objective_actual_end_date,
	   AT.Status_vod__c::varchar(510) as account_plan_objective_status,
	   left (AT.jj_Start_Date__c, 8)::varchar(8) as account_plan_objective_start_date,
	   AT.recordtypeid::varchar(36) as account_plan_objective_record_type
  from {{ var('schema') }}.account_tactic_raw       as AT
 inner join {{ var('schema') }}.account_plan_raw    as AP
    on  AT.Account_Plan_vod__c                                    = AP.Id
  left join {{ var('schema') }}.country_settings_raw as CS
	on AT.country_iso_code                                        = CS.jj_Country_ISO_Code__c
  left join {{ var('schema') }}.user_raw                   as UB
	on AT.createdbyid                                             = UB.id	
  left join {{ var('schema') }}.profile_raw                as PB
	on PB.id                                                      = UB.profileid 
   and AT.createdbyid                                             = UB.id
 group by account_plan_id,
	      account_plan_objective_id,
	      account_plan_objective_counter,	   
	      account_plan_objective,	   	   
	      account_id,
	      account_plan_objective_country_code,
	      account_plan_objective_country,
	      account_plan_objective_region,
	      account_plan_objective_owner_name,
	      account_plan_objective_owner_profile_name,
	      account_plan_objective_is_complete,
	      account_plan_objective_description,	   
	      account_plan_objective_type,
	      account_plan_objective_is_medical,
	      account_plan_objective_percentage,
	      account_plan_objective_created_date,
	      account_plan_objective_last_modified_date,
	      account_plan_objective_progress,   
	      account_plan_objective_priority,
	      account_plan_objective_impact,
	      account_plan_objective_key_strategic_drivers,
	      account_plan_objective_traffic_light_status,
	      account_plan_objective_traffic_light_status_green_counter,
	      account_plan_objective_traffic_light_status_amber_counter,
	      account_plan_objective_traffic_light_status_red_counter,
	      account_plan_objective_key_actions_with_status_amber,
	      account_plan_objective_key_actions_with_status_green,
	      account_plan_objective_key_actions_with_status_red,
	      account_plan_objective_number_of_key_actions,
	      account_plan_objective_actual_end_date,
	      account_plan_objective_status,
	      account_plan_objective_start_date,
	      account_plan_objective_record_type 	