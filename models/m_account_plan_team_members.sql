{{ config(
	post_hook="grant all privileges on {{ this }} to {{ var('snowflake_user_grant_privileges') }}")
}}

select distinct
	   TM.JJ_Account_Plan__c::varchar(100) as account_plan_id,
       TM.jj_team_member__c::varchar(50) as employee_id,
	   US.Name::varchar(100) as account_plan_team_member,
	   case when TM.JJ_Account_Plan_Coordinator__c = 1 then 'yes' else 'no' end::varchar(3) as account_plan_team_member_is_coordinator,
	   AP.account_vod__c::varchar(255) as account_id,
	   AP.country_iso_code::varchar(5) as account_country_code,
	   CS.name::varchar(255) as account_country,
	   CS.jj_region__c::varchar(20) as account_region,
	   TM.jj_system_unique_team_member__c::varchar(255) as account_plan_system_unique_team_member   
  from      {{ source('raw', 'account_plan_team_member') }} as TM
 inner join {{ source('raw', 'account_plan') }}            as AP
    on  TM.JJ_Account_Plan__c                                                = AP.Id
  left join {{ source('raw', 'user') }} 			                as US
    on TM.JJ_Team_Member__c 					                             = US.Id
  left join {{ source('raw', 'country_settings') }}         as CS
    on AP.country_iso_code                                                   = CS.JJ_COUNTRY_ISO_CODE__C