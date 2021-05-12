{{ config(
	post_hook="grant all privileges on {{ this }} to {{ var('snowflake_user_grant_privileges') }}")
}}

with cte_aux_m_mc_cycle_plan_target_channel as
(
  select distinct
         isnull(cplan.Id,'') as mc_cycle_plan_id,
         isnull(cptarget.Id,'') as mc_cycle_plan_target_id,
         isnull(cptarget.target_vod__c,'') as account_id,
         
         cast(case when isnull(cpchannel.Channel_Activity_Actual_vod__c,'') = '' then '0' else cpchannel.Channel_Activity_Actual_vod__c end as float8) as mc_cycle_plan_channel_activity_actual,
         cast(case when isnull(cpchannel.Channel_Activity_Goal_vod__c,'') = '' then '0' else cpchannel.Channel_Activity_Goal_vod__c end as float8) as mc_cycle_plan_channel_activity_goal,
         cast(case when isnull(cpchannel.Channel_Activity_Remaining_vod__c,'') = '' then '0' else cpchannel.Channel_Activity_Remaining_vod__c end as float8) as mc_cycle_plan_channel_activity_remaining,

         case 
           when (cast(case when isnull(cpchannel.Channel_Activity_Actual_vod__c,'') = '' then '0' else cpchannel.Channel_Activity_Actual_vod__c end as float8)) 
              <= (cast(case when isnull(cpchannel.Channel_Activity_Goal_vod__c,'') = '' then '0' else cpchannel.Channel_Activity_Goal_vod__c end as float8))
             then (cast(case when isnull(cpchannel.Channel_Activity_Actual_vod__c,'') = '' then '0' else cpchannel.Channel_Activity_Actual_vod__c end as float8)) 
           else (cast(case when isnull(cpchannel.Channel_Activity_Goal_vod__c,'') = '' then '0' else cpchannel.Channel_Activity_Goal_vod__c end as float8))
         end as mc_cycle_plan_channel_activity_valid,

         isnull(cpchannel.Channel_vod__c,'') as mc_cycle_plan_channel,
         isnull(cpchannel.id,'') as mc_cycle_plan_channel_id,
         isnull(cpchannel.Cycle_Channel_vod__c,'') as mc_cycle_channel_id,
         cast(case when isnull(cpchannel.Channel_Activity_Max_vod__c,'') = '' then '0' else cpchannel.Channel_Activity_Max_vod__c end as float8) as mc_cycle_plan_channel_activity_max,

         case
           when (cast(case when isnull(cpchannel.Channel_Activity_Actual_vod__c,'') = '' then '0' else cpchannel.Channel_Activity_Actual_vod__c end as float8)) 
               > (cast(case when isnull(cpchannel.Channel_Activity_Max_vod__c,'') = '' then '0' else cpchannel.Channel_Activity_Max_vod__c end as float8))
             then (cast(case when isnull(cpchannel.Channel_Activity_Max_vod__c,'') = '' then '0' else cpchannel.Channel_Activity_Max_vod__c end as float8))
           else (cast(case when isnull(cpchannel.Channel_Activity_Actual_vod__c,'') = '' then '0' else cpchannel.Channel_Activity_Actual_vod__c end as float8))
         end as mc_cycle_plan_channel_activity_actual_compared_to_max,

         cast(case when isnull(cchannel.Channel_Weight_vod__c,'') = '' then '0' else cchannel.Channel_Weight_vod__c end as float8) as mc_cycle_channel_weight

    from      {{ source('raw', 'mc_cycle_plan') }}         as cplan
    left join {{ source('raw', 'mc_cycle_plan_target') }}  as cptarget
      on cptarget.cycle_plan_vod__c 											                = cplan.id
    left join {{ source('raw', 'mc_cycle_plan_channel') }} as cpchannel
      on cpchannel.cycle_plan_target_vod__c                               = cptarget.id
    left join {{ source('raw', 'mc_cycle_channel') }}      as cchannel
      on cchannel.id                                                      = cpchannel.Cycle_Channel_vod__c
)
select (mc_cycle_plan_channel_id || '-' || account_id:):varchar(50) as mc_channel_account_id,
       (mc_cycle_plan_target_id || '-' || account_id)::varchar(50) as mc_cycle_plan_target_account_id,
       mc_cycle_plan_channel_activity_goal::smallint as mc_cycle_plan_channel_activity_goal,
       mc_cycle_plan_channel_activity_actual::smallint as mc_cycle_plan_channel_activity_actual,
       (mc_cycle_plan_channel_activity_actual * mc_cycle_channel_weight)::numeric(38,18) as mc_cycle_plan_weighted_activity_actual,
       mc_cycle_plan_channel_activity_remaining::smallint as mc_cycle_plan_channel_activity_remaining,
       (mc_cycle_plan_channel_activity_remaining * mc_cycle_channel_weight)::numeric(38,18) as mc_cycle_plan_weighted_activity_remaining,
       mc_cycle_plan_channel_activity_valid::numeric(38,18) as mc_cycle_plan_channel_activity_valid,
       mc_cycle_plan_channel::varchar(50) as mc_cycle_plan_channel,
       mc_cycle_plan_channel_activity_max::varchar(50) as mc_cycle_plan_channel_activity_max,
       mc_cycle_channel_weight::numeric(38,18) as mc_cycle_channel_weight,
       mc_cycle_plan_channel_activity_actual_compared_to_max::smallint as mc_cycle_plan_channel_activity_actual_compared_to_max,
       (mc_cycle_plan_channel_activity_actual_compared_to_max * mc_cycle_channel_weight)::numeric(38,18) as mc_cycle_plan_channel_weighted_activity_actual_compared_to_max,
       case when mc_cycle_plan_channel_activity_goal = '0' then '0' else (mc_cycle_plan_channel_activity_actual_compared_to_max / mc_cycle_plan_channel_activity_goal) end ::numeric(38,18) as mc_cycle_plan_channel_attainment,
       (mc_cycle_plan_channel_activity_valid * mc_cycle_channel_weight)::numeric(38,18) as mc_cycle_plan_channel_weighted_activity_valid,
       case when mc_cycle_plan_channel_activity_goal = '0' then '0' else (mc_cycle_plan_channel_activity_valid / mc_cycle_plan_channel_activity_goal) end::numeric(38,18)  as mc_cycle_plan_channel_completion,
       (mc_cycle_plan_channel_activity_goal * mc_cycle_channel_weight)::numeric(38,18) as mc_cycle_plan_weighted_activity_goal
  from cte_aux_m_mc_cycle_plan_target_channel;