{{ config(
	post_hook="grant all privileges on {{ this }} to {{ var('snowflake_user_grant_privileges') }}")
}}

with cte_aux_m_mc_cycle_plan_detail as
(
  select distinct
         ifnull(cplan.Id,'') as mc_cycle_plan_id,
         ifnull(cptarget.Id,'') as mc_cycle_plan_target_id,
         ifnull(cptarget.target_vod__c,'') as account_id,
         ifnull(cpchannel.id,'') as mc_cycle_plan_channel_id,
         ifnull(cpproduct.Cycle_Product_vod__c,'') as mc_cycle_product_id,
         ifnull(cpproduct.Id,'') as mc_cycle_plan_product_id,
         
         cast(case when ifnull(cpproduct.Product_Activity_Goal_vod__c,'') = '' then '0' else cpproduct.Product_Activity_Goal_vod__c end as float8) as mc_cycle_plan_product_activity_goal,
         cast(case when ifnull(cpproduct.Product_Actual_Activity_vod__c,'') = '' then '0' else cpproduct.Product_Actual_Activity_vod__c end as float8) as mc_cycle_plan_product_activity_actual,
         cast(case when ifnull(cpproduct.Product_Activity_Remaining_vod__c,'') = '' then '0' else cpproduct.Product_Activity_Remaining_vod__c end as float8) as mc_cycle_plan_product_activity_remaining,

         case
           when (cast(case when ifnull(cpproduct.Product_Actual_Activity_vod__c,'') = '' then '0' else cpproduct.Product_Actual_Activity_vod__c end as float8))
              <= (cast(case when ifnull(cpproduct.Product_Activity_Goal_vod__c,'') = '' then '0' else cpproduct.Product_Activity_Goal_vod__c end as float8))
             then (cast(case when ifnull(cpproduct.Product_Actual_Activity_vod__c,'') = '' then '0' else cpproduct.Product_Actual_Activity_vod__c end as float8))
           else (cast(case when ifnull(cpproduct.Product_Activity_Goal_vod__c,'') = '' then '0' else cpproduct.Product_Activity_Goal_vod__c end as float8))
         end as mc_cycle_plan_product_activity_valid,    
         

         ifnull(cpproduct.Product_Name_vod__c, '') as mc_cycle_plan_product,
         
         cast(case when ifnull(cpproduct.Product_Activity_Max_vod__c,'') = '' then '0' else cpproduct.Product_Activity_Max_vod__c end as float8) as mc_cycle_plan_product_activity_max,
         
         case
            when (cast(case when ifnull(cpproduct.Product_Actual_Activity_vod__c,'') = '' then '0' else cpproduct.Product_Actual_Activity_vod__c end as float8))
               >  (cast(case when ifnull(cpproduct.Product_Activity_Max_vod__c,'') = '' then '0' else cpproduct.Product_Activity_Max_vod__c end as float8))
              then  (cast(case when ifnull(cpproduct.Product_Activity_Max_vod__c,'') = '' then '0' else cpproduct.Product_Activity_Max_vod__c end as float8))
            else (cast(case when ifnull(cpproduct.Product_Actual_Activity_vod__c,'') = '' then '0' else cpproduct.Product_Actual_Activity_vod__c end as float8))
         end as mc_cycle_plan_product_activity_actual_compared_to_max,
         
         
         ifnull(cproduct.Product_vod__c, '') as product_id
    from      {{ source('raw', 'mc_cycle_plan') }}         as cplan
    left join {{ source('raw', 'mc_cycle_plan_target') }}  as cptarget
      on cptarget.cycle_plan_vod__c 											                = cplan.id
    left join {{ source('raw', 'mc_cycle_plan_channel') }} as cpchannel
      on cpchannel.cycle_plan_target_vod__c                               = cptarget.id
    left join {{ source('raw', 'mc_cycle_plan_product') }} as cpproduct
      on cpproduct.cycle_plan_channel_vod__c                              = cpchannel.id
    left join {{ source('raw', 'mc_cycle_product') }}      as cproduct
      on cproduct.id                                                      = cpproduct.cycle_product_vod__c 
)
select mc_cycle_plan_channel_id || '-' || account_id::varchar(50) as mc_channel_account_id,
       mc_cycle_plan_id || '-' || account_id || '-' || product_id::varchar(100) as mc_cycle_plan_account_product_id,
       mc_cycle_plan_product_activity_goal::smallint as mc_cycle_plan_product_activity_goal,
       mc_cycle_plan_product_activity_actual::smallint as mc_cycle_plan_product_activity_actual,
       mc_cycle_plan_product_activity_remaining::smallint as mc_cycle_plan_product_activity_remaining,
       mc_cycle_plan_product_activity_valid::smallint as mc_cycle_plan_product_activity_valid,
       mc_cycle_plan_product::varchar(50) as mc_cycle_plan_product,
       mc_cycle_plan_product_activity_max::smallint as mc_cycle_plan_product_activity_max,
       mc_cycle_plan_product_activity_actual_compared_to_max::smallint as mc_cycle_plan_product_activity_actual_compared_to_max,
       case when mc_cycle_plan_product_activity_goal = '0' then '0' else (mc_cycle_plan_product_activity_actual_compared_to_max / mc_cycle_plan_product_activity_goal) end::numeric(18,0) as mc_cycle_plan_product_attainment,
       case when mc_cycle_plan_product_activity_goal = '0' then '0' else (mc_cycle_plan_product_activity_valid / mc_cycle_plan_product_activity_goal) end::numeric(18,0) as mc_cycle_plan_product_completion      
  from cte_aux_m_mc_cycle_plan_detail