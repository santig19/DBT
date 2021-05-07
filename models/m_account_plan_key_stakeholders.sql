{{ config(
	post_hook="grant all privileges on {{ this }} to {{ var('snowflake_user_grant_privileges') }}")
}}

select distinct
       key_stakeholder.ACCOUNT_PLAN_VOD__C::varchar(50)          as account_plan_id,
       key_stakeholder.KEY_STAKEHOLDER_VOD__C::varchar(50)       as account_plan_key_stakeholder_id,
       key_stakeholder.KEY_STAKEHOLDER_NAME_VOD__C::varchar(256) as account_plan_key_stakeholder_name,
       key_stakeholder.ROLE_VOD__C::varchar(50)                  as account_plan_key_stakeholder_role,
       left(key_stakeholder.CREATEDDATE, 8)::varchar(8)          as account_plan_key_stakeholder_created_date,
       left(key_stakeholder.LASTMODIFIEDDATE, 8)::varchar(8)     as account_plan_key_stakeholder_last_modified_date,
       key_stakeholder.OWNERID::varchar(25)                       as account_plan_key_stakeholder_owner_id,
       user_b.Name::varchar(256)                                 as account_plan_key_stakeholder_owner_name,
       country_settings.JJ_COUNTRY_ISO_CODE__C::varchar(10)      as account_country_code,
       country_settings.Name::varchar(50)                        as account_country,
       country_settings.JJ_Region__c::varchar(50)                as account_region
  from      {{ var('schema') }}.key_stakeholder_raw as key_stakeholder    
 inner join {{ var('schema') }}.account_plan_raw    as account_plan
    on  key_stakeholder.account_plan_vod__c                          = account_plan.ID
  left join {{ var('schema') }}.user_raw                   as user_b
    on user_b.Id                                                     = key_stakeholder.OwnerId
  left join {{ var('schema') }}.account_raw                as account
    on account.Id                                                    = key_stakeholder.KEY_STAKEHOLDER_VOD__C
  left join {{ var('schema') }}.country_settings_raw as country_settings
    on ifnull(account.COUNTRY_JJ__C, account.JJ_COUNTRY__C)          = country_settings.JJ_COUNTRY_ISO_CODE__C

