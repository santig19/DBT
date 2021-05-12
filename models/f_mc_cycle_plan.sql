{{ config(
	post_hook="grant all privileges on {{ this }} to {{ var('snowflake_user_grant_privileges') }}")
}}

select case when LEN(cs.jj_Region__c) > 0 then cs.jj_Region__c else 'NM' end::varchar(255)                                           as region,
         TO_CHAR(TO_DATE(cplan.LASTMODIFIEDDATE, 'YYYYMMDD HH24:MI:SS'), 'YYYYMMDD')::varchar(8)                                       as date,
         cpt.TARGET_VOD__C::varchar(20)                                                                                                as account_id,
         cplan.OWNERID::varchar(20)                                                                                                      as employee_id,
         cprod.PRODUCT_VOD__C::varchar(20)                                                                                             as product_id,
         cplan.ID::varchar(20)                                                                                                             as MC_Cycle_Plan_id,
         'My Cycle Plan'::varchar(13)                                                                                                      as origin,
         (
        case when cplan.ID is null then '' else cplan.ID end || '-' ||
        case when cpt.TARGET_VOD__C is null then '' else cpt.TARGET_VOD__C end || '-' ||
        case when cprod.PRODUCT_VOD__C is null then '' else cprod.PRODUCT_VOD__C end
       )::varchar(255)                                                                                                               as MC_Cycle_Plan_Account_Product_Id,
         (
        case when cpt.TARGET_VOD__C is null then '' else cpt.TARGET_VOD__C end || '-' ||
        case when prod.ID = cprod.PRODUCT_VOD__C then mprod.product_detail_id else 'NM' end
       )::varchar(255)                                                                                                               as Product_Target_Class_Key,
         case when LEN(ut.territoryid) > 0 then ut.territoryid else 'NM' end::varchar(20)                                              as Territory_Nominal_Id,
         case when LEN(cpt.country_iso_code) > 0 then cpt.country_iso_code else 'NM' end::varchar(2)                                   as country_code,
         case when LEN(cs.name) > 0 then cs.name else 'NM' end::varchar(255)                                                           as country,
         cpt.TARGET_VOD__C::varchar(20)                                                                                                as assignment_id,
         (
        case when cpt.TARGET_VOD__C is null then '' else cpt.TARGET_VOD__C end || '-' ||
        case when cplan.OWNERID is null then '' else cplan.OWNERID end
       )::varchar(255)                                                                                                               as security_key,
       CASE WHEN mncv.Account_Id IS NOT NULL AND mncv.Territory_Id IS NOT NULL
    THEN 1
            ELSE 0
                END AS flag_territory_belong,
       CASE WHEN mncvs.Account_Id IS NOT NULL AND mncvs.Territory_Id IS NOT NULL AND yearmonth IS NOT NULL
        THEN 1
            ELSE 0
                END AS historical_flag_territory_belong
  from      {{ source('raw', 'mc_cycle_pla') }}         as cplan
  left join {{ source('raw', 'mc_cycle_plan_target') }}  as cpt
    on cpt.CYCLE_PLAN_VOD__C                                            = cplan.ID
  left join {{ source('raw', 'mc_cycle_plan_channel') }} as cpc
    on cpc.Cycle_Plan_Target_vod__c                                     = cpt.ID
  left join {{ source('raw', 'mc_cycle_plan_product') }} as cpp
    on cpp.Cycle_Plan_Channel_vod__c                                    = cpc.ID
  left join {{ source('raw', 'mc_cycle_product') }}      as cprod
    on cprod.ID                                                         = cpp.Cycle_Product_vod__c
  left join {{ source('raw', 'product') }}               as prod 
    on prod.ID                                                          = cprod.PRODUCT_VOD__C
  left join {{ source('raw', 'country_settings') }}       as cs
    on cs.JJ_COUNTRY_ISO_CODE__C                                        = cpt.country_iso_code
  left join {{ ref('m_product') }}                          as mprod
    on mprod.PRODUCT_ID                                                 = cprod.PRODUCT_VOD__C
left join {{ ref('tmp_user_territory') }}                    as ut
    on ut.USERID                                                        = cplan.OWNERID
  LEFT JOIN (SELECT Account_Id, Territory_Id FROM {{ ref('m_null_country_values') }}
            GROUP BY Account_Id, Territory_Id) mncv
    ON cpt.TARGET_VOD__C = mncv.Account_Id AND CASE WHEN LEN(ut.territoryid) > 0 THEN ut.territoryid ELSE 'NM' END = mncv.Territory_Id
  LEFT JOIN (SELECT Account_Id, Territory_Id, yearmonth FROM {{ source('raw', 'm_null_country_values_snapshot_monthly_historical') }}
            GROUP BY Account_Id, Territory_Id, yearmonth) mncvs
    ON cpt.TARGET_VOD__C = mncvs.Account_Id AND CASE WHEN LEN(ut.territoryid) > 0 THEN ut.territoryid ELSE 'NM' END = mncvs.Territory_Id AND substring(cplan.LASTMODIFIEDDATE,1,6) = mncvs.yearmonth
 group by cs.jj_Region__c,
		cplan.LASTMODIFIEDDATE,
		cpt.TARGET_VOD__C,
		cplan.OWNERID,
		cprod.PRODUCT_VOD__C,
		cplan.ID,prod.ID,
		mprod.product_detail_id,
		ut.territoryid,
		cpt.country_iso_code,
		cs.name,
		mncv.Account_Id,
		mncv.Territory_Id,
		mncvs.Account_Id,
		mncvs.Territory_Id,
		mncvs.yearmonth