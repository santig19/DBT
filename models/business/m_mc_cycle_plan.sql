{{ config(
	post_hook="grant all privileges on {{ this }} to {{ var('snowflake_user_grant_privileges') }}")
}}

select distinct
       left(end_date_vod__c, 8)::varchar(8) as mc_cycle_plan_end_date,
	   ifnull(id,'')::varchar(50) as mc_cycle_plan_id,
       ifnull(name,'')::varchar(255) as mc_cycle_plan_name,
       left(start_date_vod__c, 8)::varchar(8) as mc_cycle_plan_start_date,
       ifnull(status_vod__c,'')::varchar(50) as mc_cycle_plan_status,
       ifnull(territory_vod__c,'')::varchar(50) as mc_cycle_plan_territory
from {{ source('raw', 'mc_cycle_plan') }}