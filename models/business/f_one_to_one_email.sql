{{ config(
	post_hook="grant all privileges on {{ this }} to {{ var('snowflake_user_grant_privileges') }}")
}}

with cte_counts as
(
select f_oto_email.one_to_one_email_subject || '-' || f_oto_email.account_id || '-' || f_oto_email.employee_id as one_to_one_key_f_oto_email_counter,
       sum(f_oto_email.one_to_one_email_sent_counter_temp) as one_to_one_email_sent_counter_f_oto_email_counter,
       sum(f_oto_email.one_to_one_email_opened_counter_temp) as one_to_one_email_total_opened_counter_f_oto_email_counter,
       max(f_oto_email.one_to_one_email_opened_counter_temp) as one_to_one_email_opened_counter_f_oto_email_counter,
       sum(f_oto_email.one_to_one_email_clikthrough_counter_temp) as one_to_one_email_total_clikthrough_counter_f_oto_email_counter,
       max(f_oto_email.one_to_one_email_clikthrough_counter_temp) as one_to_one_email_clikthrough_counter_f_oto_email_counter,
       f_oto_email.one_to_one_email_country as one_to_one_email_counters_country_f_oto_email_counter,
       f_oto_email.one_to_one_email_region as one_to_one_email_counters_region_f_oto_email_counter
  from (  
        select aux2.accountid AS account_id,
        			 aux2.ownerid AS employee_id,
        			 case when (lower(aux2.jj_Response_Type__c) = 'email sent') then trim(ltrim(ltrim(trim(REPLACE(aux2.subject,'E-Mail','Email')),'Email:'),'-mail :')) else trim(aux2.subject) end as one_to_one_email_subject,
        			 case when coalesce(aux2.createddate,'') = '' then null else TO_CHAR(TO_TIMESTAMP(aux2.createddate, 'YYYYMMDD HH24:MI:SS'), 'DD-MM-YYYY HH24:MI:SS') end as one_to_one_email_created_date,
        			 case when lower(aux2.jj_Response_Type__c)='email sent' then 1 else 0 end as one_to_one_email_sent_counter_temp,
        			 case when lower(aux2.jj_Response_Type__c)='email opened' then 1 else 0 end as one_to_one_email_opened_counter_temp,
        			 case when lower(aux2.jj_Response_Type__c)='clickthrough' then 1 else 0 end as one_to_one_email_clikthrough_counter_temp,
        			 case when aux2.country_iso_code=cs2.jj_Country_ISO_Code__c then cs2.name else 'NM' end as one_to_one_email_country,
        			 case when aux2.country_iso_code=cs2.jj_Country_ISO_Code__c then cs2.jj_Region__c else 'NM' end as one_to_one_email_region
          from      {{ ref('tmp_f_one_to_one_email') }} aux2
          left join {{ source('raw', 'country_settings') }} cs2 ON aux2.country_iso_code=cs2.jj_Country_ISO_Code__c       
       ) as f_oto_email        
 group by f_oto_email.one_to_one_email_subject || '-' || f_oto_email.account_id || '-' || f_oto_email.employee_id,
          f_oto_email.one_to_one_email_country,
          f_oto_email.one_to_one_email_region
                 
)

select (case when (aux.ownerid=us.id) then prf.name else 'NM' end)::varchar(255) as one_to_one_email_owner_profile_name,
       (aux.accountid || '-' || aux.ownerid)::varchar(255) as security_key,
       aux.accountid as account_id,
       aux.id as one_to_one_email_id,
       case when substring(aux.whatid,1,3) ='a00' then aux.whatid else null end as product_id,
       case when lower(aux.jj_response_type__c) ='email sent' Then trim(ltrim(ltrim(trim(replace(aux.subject,'E-Mail','Email')),'Email:'),'-mail :')) else trim(aux.subject) end as one_to_one_email_subject,
       (case when len(ac.country_jj__c)>0 then ac.country_jj__c else ac.jj_country__c end || '_' || case when len(ac.brick_number_jj__c)>0 then ac.brick_number_jj__c else 'No Value' end)::varchar(255) as brick_code,
       aux.accountid as assignment_id,
       (case when aux.createddate  is null or aux.createddate='' then null else to_char(to_date(aux.createddate, 'YYYYMMDD HH24:MI:SS'), 'DD-MM-YYYY') end)::varchar(255) as date,
       (case when aux.activitydate is null or aux.activitydate='' then null else to_char(to_date(aux.activitydate, 'YYYYMMDD HH24:MI:SS'), 'DD-MM-YYYY') end)::varchar(255) as one_to_one_email_due_date,
       aux.status as one_to_one_email_status,
       aux.priority as one_to_one_email_priority,
       aux.ownerid as employee_id,
       case when (aux.ownerid = ut.userid) then ut.territoryid else 'NM' end as territory_nominal_id,
       case when (aux.ownerid = us.id) then us.name else 'NM' end as one_to_one_email_owner_name,
       case when (aux.isclosed = '1' or aux.isclosed ilike 'TRUE') then 'Yes' when (aux.isclosed='0' or aux.isclosed ilike 'FALSE') then 'No' end as one_to_one_email_is_closed,
       (case when aux.createddate is null or aux.createddate='' then null else to_char(to_timestamp(aux.createddate, 'YYYYMMDD HH24:MI:SS'), 'DD-MM-YYYY HH24:MI:SS') end)::varchar(255) as one_to_one_email_created_date,
       case when (aux.jj_medical__c = '1' or aux.jj_medical__c ilike 'TRUE') THEN 'Yes' WHEN (aux.jj_medical__c='0' OR aux.jj_medical__c ILIKE 'FALSE') THEN 'No' END AS one_to_one_email_is_medical,
       case when (aux.accountid = ac.id) then ( (case when len(ac.country_jj__c)>0 then ac.country_jj__c else ac.jj_country__c end)) else 'NM' end as country_code,
       case when (aux.country_iso_code = cs.jj_country_iso_code__c) then cs.name else 'NM' end as country,
       case when (aux.country_iso_code = cs.jj_country_iso_code__c) then cs.jj_region__c else 'NM' end as region,
       aux.type as one_to_one_email_type,
       '1to1 Email'::varchar(255) as origin,
       (case when (aux.accountid=ac.id) then (case when len(ac.country_jj__c)>0 then ('Non Visited' || '_' || ac.jj_country__c) end ) else 'Non Visted_NM' end)::varchar(255) as account_visited_nonvisited_technical,
       'One To One Email'::varchar(255) as activity,
       'One To One Email'::varchar(255) as activity_type,
       '1'::numeric(10,0) as one_to_one_email_counter,
       'Non Face to Face Contacts'::varchar(255) as multichannel_type,
       '1 to 1 Emails sent'::varchar(255) as multichannel_activity,
       null as multichannel_activity_type,
       aux.jj_response_type__c as one_to_one_email_response_type,
       (case when aux.jj_response_type__c = 'Email Sent' then trim(ltrim(ltrim(trim(replace(aux.subject,'E-Mail','Email')),'Email:'),'-mail :')) || '-' || aux.accountid /*+ '-' + case when (aux.createddate is null or aux.createddate='') then null else to_char(to_timestamp(aux.createddate, 'YYYYMMDD HH24:MI:SS'), 'DD-MM-YYYY HH24:MI:SS') end */|| '-' || aux.ownerid else (trim(aux.subject) || '-' || aux.accountid /*+ '-' + case when (aux.createddate is null or aux.createddate='') then null else to_char(to_timestamp(aux.createddate, 'YYYYMMDD HH24:MI:SS'), 'DD-MM-YYYY HH24:MI:SS') end */|| '-' || aux.ownerid) end)::varchar(1000) as one_to_one_key,
       case when ((case when substring(aux.whatid,1,3)='a00' then aux.whatid else null end)=mpag.product_id) then mpag.product_analytic_group_id else null end as product_analytic_group_id,
       (case when ((case when substring(aux.whatid,1,3)='a00' then aux.whatid else null end)=mpag.product_id) then ( aux.accountid || '-' || (case when ((case when substring(aux.whatid,1,3)='a00' then aux.whatid else null end)=mpag.product_id) then mpag.product_analytic_group_id else null end) || '-' || (case when (case when substring(aux.whatid,1,3)='a00' then aux.whatid else null end)=mprd.product_id then mprd.product_country_code else 'NM' end)) else ( (aux.accountid) || '-' || (case when (case when substring(aux.whatid,1,3)='a00' then aux.whatid else null end)=mprd.product_id then mprd.product_detail_id else 'NM' end)) end)::varchar(255) as product_target_class_key,
       f_oto_email_counter.one_to_one_email_sent_counter_f_oto_email_counter::numeric(10, 0) AS one_to_one_email_sent_counter,
       f_oto_email_counter.one_to_one_email_total_opened_counter_f_oto_email_counter::numeric(10, 0) AS one_to_one_email_total_opened_counter,
       f_oto_email_counter.one_to_one_email_opened_counter_f_oto_email_counter::numeric(10, 0) AS one_to_one_email_opened_counter,
       f_oto_email_counter.one_to_one_email_total_clikthrough_counter_f_oto_email_counter::numeric(10, 0) AS one_to_one_email_total_clikthrough_counter,
       f_oto_email_counter.one_to_one_email_clikthrough_counter_f_oto_email_counter::numeric(10, 0) AS one_to_one_email_clikthrough_counter,
       f_oto_email_counter.one_to_one_email_counters_country_f_oto_email_counter AS one_to_one_email_counters_country,
       f_oto_email_counter.one_to_one_email_counters_region_f_oto_email_counter AS one_to_one_email_counters_region,
	   aux.isarchived
  from      {{ ref('tmp_f_one_to_one_email') }} aux
  left join {{ ref('tmp_user_territory') }} ut on aux.ownerid=ut.userid
  left join {{ source('raw', 'user') }} us on aux.ownerid=us.id
  left join {{ source('raw', 'profile') }} prf on us.profileid=prf.id
  left join {{ source('raw', 'account') }} ac on aux.accountid = ac.id
  left join {{ source('raw', 'country_settings') }} cs on aux.country_iso_code=cs.jj_Country_ISO_Code__c
  left join {{ ref('m_product') }} mprd on (case when substring(aux.whatid,1,3)='a00' then aux.whatid else null end) = mprd.product_id
  left join
        		(
              	select an.id             as product_analytic_group_id ,
                       pg.product_vod__c as product_id,
                       1::varchar(1)     as Indication_Flag
              	  from {{ source('raw', 'analytics_product_group') }} an
                 inner join {{ source('raw', 'product_group_map') }} pg on an.id = pg.analytics_product_group_vod__c 
			       ) mpag on (case when substring(aux.whatid,1,3)='a00' then aux.whatid else null end)= mpag.product_id
  left join cte_counts as f_oto_email_counter on (case when lower(aux.jj_response_type__c)='email sent' then trim(ltrim(ltrim(trim(replace(aux.subject,'E-Mail','Email')),'Email:'),'-mail :')) || '-' || aux.accountid || '-' || aux.ownerid else (trim(aux.subject) || '-' || aux.accountid || '-' || aux.ownerid) end) = f_oto_email_counter.one_to_one_key_f_oto_email_counter
 group by (case when (aux.ownerid=us.id) then prf.name else 'NM' end)::varchar(255),
       (aux.accountid || '-' || aux.ownerid)::varchar(255),
       aux.accountid,
       aux.id,
       case when substring(aux.whatid,1,3) ='a00' then aux.whatid else null end,
       case when lower(aux.jj_response_type__c) ='email sent' Then trim(ltrim(ltrim(trim(replace(aux.subject,'E-Mail','Email')),'Email:'),'-mail :')) else trim(aux.subject) end,
       (case when len(ac.country_jj__c)>0 then ac.country_jj__c else ac.jj_country__c end || '_' || case when len(ac.brick_number_jj__c)>0 then ac.brick_number_jj__c else 'No Value' end)::varchar(255),
       aux.accountid ,
       (case when aux.createddate  is null or aux.createddate='' then null else to_char(to_date(aux.createddate, 'YYYYMMDD HH24:MI:SS'), 'DD-MM-YYYY') end)::varchar(255),
       (case when aux.activitydate is null or aux.activitydate='' then null else to_char(to_date(aux.activitydate, 'YYYYMMDD HH24:MI:SS'), 'DD-MM-YYYY') end)::varchar(255),
       aux.status,
       aux.priority,
       aux.ownerid,
       case when (aux.ownerid = ut.userid) then ut.territoryid else 'NM' end,
       case when (aux.ownerid = us.id) then us.name else 'NM' end,
       case when (aux.isclosed = '1' or aux.isclosed ilike 'TRUE') then 'Yes' when (aux.isclosed='0' or aux.isclosed ilike 'FALSE') then 'No' end,
       (case when aux.createddate is null or aux.createddate='' then null else to_char(to_timestamp(aux.createddate, 'YYYYMMDD HH24:MI:SS'), 'DD-MM-YYYY HH24:MI:SS') end)::varchar(255),
       case when (aux.jj_medical__c = '1' or aux.jj_medical__c ilike 'TRUE') THEN 'Yes' WHEN (aux.jj_medical__c='0' OR aux.jj_medical__c ILIKE 'FALSE') THEN 'No' END,
       case when (aux.accountid = ac.id) then ( (case when len(ac.country_jj__c)>0 then ac.country_jj__c else ac.jj_country__c end)) else 'NM' end,
       case when (aux.country_iso_code = cs.jj_country_iso_code__c) then cs.name else 'NM' end,
       case when (aux.country_iso_code = cs.jj_country_iso_code__c) then cs.jj_region__c else 'NM' end,
       aux.type,
       (case when (aux.accountid=ac.id) then (case when len(ac.country_jj__c)>0 then ('Non Visited' || '_' || ac.jj_country__c) end ) else 'Non Visted_NM' end)::varchar(255),
       aux.jj_response_type__c,
       (case when aux.jj_response_type__c = 'Email Sent' then trim(ltrim(ltrim(trim(replace(aux.subject,'E-Mail','Email')),'Email:'),'-mail :')) || '-' || aux.accountid || '-' || aux.ownerid else (trim(aux.subject) || '-' || aux.accountid || '-' || aux.ownerid) end)::varchar(1000),
       case when ((case when substring(aux.whatid,1,3)='a00' then aux.whatid else null end)=mpag.product_id) then mpag.product_analytic_group_id else null end,
       (case when ((case when substring(aux.whatid,1,3)='a00' then aux.whatid else null end)=mpag.product_id) then ( aux.accountid || '-' || (case when ((case when substring(aux.whatid,1,3)='a00' then aux.whatid else null end)=mpag.product_id) then mpag.product_analytic_group_id else null end) || '-' || (case when (case when substring(aux.whatid,1,3)='a00' then aux.whatid else null end)=mprd.product_id then mprd.product_country_code else 'NM' end)) else ( (aux.accountid) || '-' || (case when (case when substring(aux.whatid,1,3)='a00' then aux.whatid else null end)=mprd.product_id then mprd.product_detail_id else 'NM' end)) end)::varchar(255),
       f_oto_email_counter.one_to_one_email_sent_counter_f_oto_email_counter::numeric(10, 0),
       f_oto_email_counter.one_to_one_email_total_opened_counter_f_oto_email_counter::numeric(10, 0),
       f_oto_email_counter.one_to_one_email_opened_counter_f_oto_email_counter::numeric(10, 0),
       f_oto_email_counter.one_to_one_email_total_clikthrough_counter_f_oto_email_counter::numeric(10, 0),
       f_oto_email_counter.one_to_one_email_clikthrough_counter_f_oto_email_counter::numeric(10, 0),
       f_oto_email_counter.one_to_one_email_counters_country_f_oto_email_counter,
       f_oto_email_counter.one_to_one_email_counters_region_f_oto_email_counter,
       aux.isarchived
