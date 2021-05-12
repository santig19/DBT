{{ config(
	post_hook="grant all privileges on {{ this }} to {{ var('snowflake_user_grant_privileges') }}")
}}

SELECT
    ins.Event_Id::varchar(255)                as Event_Id
  , ins.Process_Instance_Id::varchar(255)     as Process_Instance_Id
  , ins.Process_Instance_Status::varchar(255) as Process_Instance_Status
  , ins_step.ActorId::varchar(255)            as Employee_Nominal_Id
  , ins_step.PROCESSINSTANCEID::varchar(255)  as Process_Instance_Step_Id
  , ins_step.originalActorId::varchar(255)    as Original_Employee_Nominal_Id
  , ins_step.Stepstatus::varchar(255)         as Process_Instance_Step_Status
  , ins_step.SystemModstamp::varchar(255)     as Process_Instance_Step_Date
FROM
    (
        SELECT
            ROW_NUMBER() OVER (PARTITION BY ins.TargetObjectId ORDER BY ins.lastmodifieddate DESC) as rowid
          , ins.TargetObjectId                           as Event_Id
          , ins.STATUS                                   as Process_Instance_Status
          , ins.id                                       as Process_Instance_Id
        FROM {{ source('raw', 'process_instance') }} ins
    ) ins
LEFT OUTER JOIN {{ source('raw', 'process_instance_step') }} ins_step ON ins.Process_Instance_Id=ins_step.PROCESSINSTANCEID
WHERE ins.rowid=1
