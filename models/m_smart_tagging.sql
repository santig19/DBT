{{ config(
	post_hook="grant all privileges on {{ this }} to {{ var('snowflake_user_grant_privileges') }}")
}}

select Id::varchar(255)                                                                                    as smart_tagging_Id,
       OwnerId::varchar(255)                                                                               as OwnerId,
       Name::varchar(255)                                                                                  as Name,
       CurrencyIsoCode::varchar(255)                                                                       as CurrencyIsoCode,
       case when CreatedDate = '' then null else cast(CreatedDate as datetime) end::datetime               as CreatedDate,
       CreatedById::varchar(255)                                                                           as CreatedById,
       case when LastModifiedDate = '' then null else cast(LastModifiedDate as datetime) end::datetime     as LastModifiedDate,
       LastModifiedById::varchar(255)                                                                      as LastModifiedById,
       case when SystemModstamp = '' then null else cast(SystemModstamp as datetime) end::datetime         as SystemModstamp,
       case when MayEdit = '' then null else MayEdit end::integer                                          as MayEdit,
       case when IsLocked = '' then null else IsLocked end::integer                                        as IsLocked,
       case when LastViewedDate = '' then null else cast(LastViewedDate as datetime) end::datetime         as LastViewedDate,
       case when LastReferencedDate = '' then null else cast(LastReferencedDate as datetime) end::datetime as LastReferencedDate,
       ConnectionReceivedId::varchar(255)                                                                  as ConnectionReceivedId,
       ConnectionSentId::varchar(255)                                                                      as ConnectionSentId,
       JJ_Key_Message_Group__c::varchar(512)                                                               as Key_Message_Group,
       JJ_Key_Message_Sub_Group__c::varchar(4000)                                                          as Key_Message_Sub_Group,
       JJ_Patient_Group__c::varchar(512)                                                                   as Patient_Group,
       JJ_Product_Catalog__c::varchar(255)                                                                 as Product_Catalog
  from {{ source('raw', 'smart_tagging') }}	