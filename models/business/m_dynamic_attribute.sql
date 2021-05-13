{{ config(
	post_hook="grant all privileges on {{ this }} to {{ var('snowflake_user_grant_privileges') }}")
}}

select da.account_vod__c::varchar(255)                                                                                                     as account_id,
	   case when da.active_vod__c = '' then null else da.active_vod__c end::integer                                                        as active,
	   da.connectionreceivedid::varchar(255)                                                                                               as connectionreceivedid,
	   da.connectionsentid::varchar(255)                                                                                                   as connectionsentid,
	   da.createdbyid::varchar(255)                                                                                                        as createdbyid,
	   case when da.createddate = '' then null else TO_TIMESTAMP(da.createddate, 'YYYYMMDD HH24:MI:SS') end::datetime                                                           as createddate,
	   da.currencyisocode::varchar(255)                                                                                                    as currencyisocode,
	   da.dynamic_attribute_configuration_vod__c::varchar(255)                                                                             as dynamic_attribute_configuration_id,
	   da.dynamic_attribute_description_vod__c::varchar(10000)                                                                             as dynamic_attribute_description,
	   da.dynamic_attribute_help_text_vod__c::varchar(10000)                                                                               as dynamic_attribute_help_text,
	   da.dynamic_attribute_label_vod__c::varchar(255)                                                                                     as dynamic_attribute_label,
	   da.dynamic_attribute_name_vod__c::varchar(10000)                                                                                    as dynamic_attribute_name,
	   da.dynamic_attribute_record_type_vod__c::varchar(255)                                                                               as dynamic_attribute_record_type,
	   case when da.dynamic_attribute_value_checkbox_vod__c = '' then null else da.dynamic_attribute_value_checkbox_vod__c end::integer    as dynamic_attribute_value_checkbox,
	   case when da.dynamic_attribute_value_date_time_vod__c = '' then null else da.dynamic_attribute_value_date_time_vod__c end::datetime as dynamic_attribute_value_date_time,
	   case when da.dynamic_attribute_value_date_vod__c = '' then null else da.dynamic_attribute_value_date_vod__c end::datetime           as dynamic_attribute_value_date,
	   case when da.dynamic_attribute_value_number_vod__c = '' then null else da.dynamic_attribute_value_number_vod__c end::numeric(14,0)  as dynamic_attribute_value_number,
	   da.dynamic_attribute_value_text_area_vod__c::varchar(10000)                                                                         as dynamic_attribute_value_text_area,
	   da.dynamic_attribute_value_text_vod__c::varchar(10000)                                                                              as dynamic_attribute_value_text,
	   da.id::varchar(255)                                                                                                                 as dynamic_attribute_id,
	   da.lastmodifiedbyid::varchar(255)                                                                                                   as lastmodifiedbyid,
	   case when da.lastmodifieddate = '' then null else TO_TIMESTAMP(da.lastmodifieddate, 'YYYYMMDD HH24:MI:SS') end::datetime                                                 as lastmodifieddate,
	   da.mobile_id_vod__c::varchar(255)                                                                                                   as mobile_id,
	   da.name::varchar(255)                                                                                                               as name,
	   da.recordtypeid::varchar(255)                                                                                                       as recordtypeid,
	   case when da.systemmodstamp = '' then null else TO_TIMESTAMP(da.systemmodstamp, 'YYYYMMDD HH24:MI:SS') end::datetime                                                     as systemmodstamp
  from {{ source('raw', 'dynamic_attribute') }} as da
