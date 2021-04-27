{{ config(
	post_hook="grant all privileges on {{ this }} to {{ var('snowflake_user_grant_privileges') }}")
}}

SELECT 

f.Event_Id::varchar(255) as Event_Id,
case when ( f.Employee_Nominal_Id = U.Id) THEN U.Name ELSE 'NM' end::varchar(255) as Event_Approval_Starter_Employee,
f.Process_Instance_Id::varchar(255) as Event_Approval_Id,
f.Process_Instance_Status::varchar(255) as Event_Approval_Status,
f.Process_Instance_Step_Date::varchar(255) as	Event_Approval_Start_Timestamp,
case when ( f1.Employee_Nominal_Id = U1.Id) THEN U1.Name else 'NM' end::varchar(255) as Event_Approval_Step1_Employee,
f1.Process_Instance_Step_Status::varchar(255) as	Event_Approval_step1_Status,
f1.Process_Instance_Step_Date::varchar(255) as	Event_Approval_step1_Timestamp,
case when ( f2.Employee_Nominal_Id = U2.Id) THEN U2.Name else 'NM' end::varchar(255) as Event_Approval_Step2_Employee,
f2.Process_Instance_Step_Status::varchar(255) as	Event_Approval_step2_Status,
f2.Process_Instance_Step_Date::varchar(255) as	Event_Approval_step2_Timestamp,
case when ( f3.Employee_Nominal_Id = U3.Id) THEN U3.Name else 'NM' end::varchar(255) as Event_Approval_Step3_Employee,
f3.Process_Instance_Step_Status::varchar(255) as	Event_Approval_step3_Status,
f3.Process_Instance_Step_Date::varchar(255) as	Event_Approval_step3_Timestamp

FROM {{ ref('tmp_f_process_instance') }} F	
LEFT OUTER JOIN {{ var('schema') }}.user_raw u on f.Employee_Nominal_Id = u.Id

LEFT OUTER JOIN (
	select rowid,Event_Id,Employee_Nominal_Id,Process_Instance_Id,Process_Instance_Status,process_Instance_Step_Date,Process_Instance_Step_Status
		from (
			(select ROW_NUMBER() OVER ( partition by Event_Id order by Process_Instance_Step_Date asc) as rowid,
			Event_Id,
			Employee_Nominal_Id,
			Process_Instance_Id,
			Process_Instance_Status,
			process_Instance_Step_Date,
			Process_Instance_Step_Status  
			from {{ ref('tmp_f_process_instance') }}
			where Process_Instance_Step_Status not in ('Reassigned','Started'))) f1
		where f1.rowid=1) f1 on f.Process_Instance_Id=f1.Process_Instance_Id
LEFT OUTER JOIN emea_mto.ZZ_fwk_icon_user_business u1 on f1.Employee_Nominal_Id = u1.Id

LEFT OUTER JOIN (
	select rowid,Event_Id,Employee_Nominal_Id,Process_Instance_Id,Process_Instance_Status,process_Instance_Step_Date,Process_Instance_Step_Status
		from (
			(select ROW_NUMBER() OVER ( partition by Event_Id order by Process_Instance_Step_Date asc) as rowid,
			Event_Id,
			Employee_Nominal_Id,
			Process_Instance_Id,
			Process_Instance_Status,
			process_Instance_Step_Date,
			Process_Instance_Step_Status  
			from {{ ref('tmp_f_process_instance') }}
			where Process_Instance_Step_Status not in ('Reassigned','Started'))) f2
		where f2.rowid=2) f2 on F.Process_Instance_Id=F2.Process_Instance_Id
LEFT OUTER JOIN emea_mto.ZZ_fwk_icon_user_business u2 on f1.Employee_Nominal_Id = u2.Id

LEFT OUTER JOIN (
	select rowid,Event_Id,Employee_Nominal_Id,Process_Instance_Id,Process_Instance_Status,process_Instance_Step_Date,Process_Instance_Step_Status
		from (
			(select ROW_NUMBER() OVER ( partition by Event_Id order by Process_Instance_Step_Date asc) as rowid,
			Event_Id,
			Employee_Nominal_Id,
			Process_Instance_Id,
			Process_Instance_Status,
			process_Instance_Step_Date,
			Process_Instance_Step_Status  
			from {{ ref('tmp_f_process_instance') }}
			where Process_Instance_Step_Status not in ('Reassigned','Started'))) f3
		where f3.rowid=3) f3 on F.Process_Instance_Id=F3.Process_Instance_Id
LEFT OUTER JOIN {{ var('schema') }}.user_raw u3 on f1.Employee_Nominal_Id = u3.Id

WHERE f.Process_Instance_Step_Status = 'Started'