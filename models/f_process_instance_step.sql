{{ config(
	post_hook="grant all privileges on {{ this }} to {{ var('snowflake_user_grant_privileges') }}")
}}

select country_iso_code::varchar(4) as country_iso_code,
       id::varchar(36) as process_instance_step_id,
       processinstanceid::varchar(36) as process_instance_id,
       stepstatus::varchar(80) as step_status,
       originalactorid::varchar(36) as original_actor_id,
       actorid::varchar(36) as actor_id,
       case when (Position(CHR(92) || CHR(92), comments) > 0) OR Position(CHR(124) || CHR(34), comments) > 0 OR Position(CHR(92) || CHR(124), comments) > 0 OR Position(CHR(92) || CHR(34), comments) > 0 then REPLACE(REPLACE(REPLACE(REPLACE(comments, CHR(92) || CHR(92), CHR(92)), CHR(124) || CHR(34), CHR(34)), CHR(92) || CHR(124), CHR(124)), CHR(92) || CHR(34), CHR(34)) ELSE comments end as comments,     
       stepnodeid::varchar(36) as step_node_id,
       case when elapsedtimeindays = '' then null else elapsedtimeindays end::decimal(18,0) as elapsed_time_in_days,
       case when elapsedtimeinhours = '' then null else elapsedtimeinhours end::decimal(7,2) as elapsed_time_in_hours,
       case when elapsedtimeinminutes = '' then null else elapsedtimeinminutes end::decimal(8,0) as elapsed_time_in_minutes,
       case when createddate = '' then null else to_timestamp_ntz(createddate,'YYYYMMDD HH24:MI:SS') end::datetime as created_date,
       createdbyid::varchar(36) as created_by_id,
       case when systemmodstamp = '' then null else to_timestamp_ntz(systemmodstamp,'YYYYMMDD HH24:MI:SS') end::datetime as system_mod_stamp
  from {{ var('schema') }}.process_instance_step_raw