{{ config(
	post_hook="grant all privileges on {{ this }} to {{ var('snowflake_user_grant_privileges') }}")
}}

select nvl(cp.id,'')                           as id,
       nvl(cp.isdeleted,'')                    as isdeleted,
       nvl(cp.jj_account__c,'')                as jj_account__c,
       nvl(cp.jj_channel_preferences__c,'')    as jj_channel_preferences__c,
       nvl(cp.jj_last_confirmation_date__c,'') as jj_last_confirmation_date__c,
       nvl(cp.jj_status_flag__c,'')            as jj_status_flag__c
  from {{ source('raw', 'channel_preferences') }} as cp
 where nvl(cp.isdeleted,'') = '0'
 group by nvl(cp.id,''),
          nvl(cp.isdeleted,''),
          nvl(cp.jj_account__c,''),
          nvl(cp.jj_channel_preferences__c,''),
          nvl(cp.jj_last_confirmation_date__c,''),
          nvl(cp.jj_status_flag__c,'')  
