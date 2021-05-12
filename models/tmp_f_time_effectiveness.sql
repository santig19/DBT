{{ config(
	post_hook="grant all privileges on {{ this }} to {{ var('snowflake_user_grant_privileges') }}")
}}

SELECT
    (time_off.ownerid || '-' || time_off.ownerid)::VARCHAR(255) AS Security_Key
  , time_off.country_iso_code                                 AS Country_Code
  , CASE
        WHEN time_off.country_iso_code = country_settings.jj_country_iso_code__c
            THEN country_settings.jj_Region__c
            ELSE 'NM'
    END::VARCHAR(255) AS Region
  , CASE
        WHEN time_off.country_iso_code = country_settings.jj_country_iso_code__c
            THEN country_settings.Name
            ELSE 'NM'
    END::VARCHAR(255)                                                                                       AS Country
  , TO_CHAR(TO_DATE(time_off.Date_vod__c,'YYYYMMDD HH24:MI:SS'),'DD/MM/YYYY')::varchar(255)                 AS Date
  , TO_CHAR(TO_DATE(time_off.Date_vod__c,'YYYYMMDD HH24:MI:SS'),'YYYYMMDD')::varchar(255)                   AS Time_Effectiveness_Date
  , TO_CHAR(TO_TIMESTAMP(time_off.Date_vod__c,'YYYYMMDD HH24:MI:SS'),'DD/MM/YYYY HH24:MI:SS')::varchar(255) AS Time_Effectiveness_Created_Timestamp
  , time_off.id                                                                                             AS Time_Effectiveness_Id
  , time_off.JJ_Comments__c                                                                                 AS Time_Effectiveness_Comments
  , CASE
        WHEN LEN(time_off.JJ_Type__c) > 0
            THEN time_off.JJ_Type__c
            ELSE 'No Value'
    END::VARCHAR(255) AS Time_Effectiveness_Type
  , time_off.Name     AS Time_Effectiveness
  , time_off.OwnerId  AS Employee_Id
  , CASE
        WHEN time_off.OwnerId = UT.UserId
            THEN UT.TerritoryId
            ELSE 'NM'
    END::VARCHAR(255)      AS Territory_Nominal_Id
  , time_off.OwnerId       AS Assignment_Id
  , time_off.OwnerId       AS Assignment_Nominal_Id
  , time_off.Reason_vod__c AS Time_Effectiveness_Reason
  , CASE
        WHEN time_off.RecordTypeId = recordtype.Id
            THEN recordtype.Name
            ELSE 'NM'
    END::varchar(255)                                               AS Time_Effectiveness_Record_Type
  , time_off.Status_vod__c                                          AS Time_Effectiveness_Status
  , 'Time Effectiveness'::VARCHAR(255)                              AS Origin
  , ('Non Visited' || '_' || time_off.country_iso_code)::VARCHAR(255) AS Account_Visited_NonVisited_Technical
  , time_off.Time_vod__c                                            AS Time_Effectiveness_Time
  , (time_off.ownerid ||'-'|| TO_CHAR(TO_DATE(time_off.date_vod__c, 'YYYYMMDD HH24:MI:SS'), 'DD-MM-YYYY'))::varchar(255)     AS Aux_Employee_Date
  , time_off.jj_product_catalog__c                                  AS Aux_Product_Id
  , time_off.jj_therapeutical_area__c                               AS Aux_Therapeutic_Area
  , time_off.hours_vod__c                                           AS Time_Effectiveness_Hours
  , NULL::varchar(255)                                              AS Time_Effectiveness_Percentage
FROM {{ source('raw', 'time_off_territory') }} AS time_off
LEFT OUTER JOIN {{ source('raw', 'country_settings') }} AS country_settings ON time_off.country_iso_code = country_settings.jj_country_iso_code__c
LEFT OUTER JOIN {{ source('raw', 'record_type') }} AS recordtype ON time_off.recordtypeid = recordtype.id
LEFT OUTER JOIN {{ ref('tmp_user_territory') }} AS UT ON time_off.ownerid = UT.userid
WHERE time_off.country_iso_code != 'DE' AND extract(year from TO_DATE(time_off.createddate, 'YYYYMMDD HH24:MI:SS')) >= extract(year from GETDATE())-2

UNION

SELECT
    (TR.OwnerId || '-' || TR.OwnerId)::VARCHAR(255) AS Security_Key
  , CASE
        WHEN TR.ownerid = USR.id AND USR.jj_user_country__c = CS.name
            THEN CS.jj_country_iso_code__c
            ELSE 'NM'
    END::varchar(255) AS Country_Code
  , CASE
        WHEN
          (CASE
               WHEN TR.ownerid = USR.id AND USR.jj_user_country__c = CS.name
				THEN CS.jj_country_iso_code__c
				ELSE 'NM'
               END
          ) = CS.jj_country_iso_code__c
          THEN CS.jj_Region__c
          ELSE 'NM'
    END::varchar(255) AS Region
  , CASE
        WHEN
          (CASE
               WHEN TR.ownerid = USR.id AND USR.jj_user_country__c = CS.name
				THEN CS.jj_country_iso_code__c
				ELSE 'NM'
               END
          ) = CS.jj_country_iso_code__c
          THEN CS.name
          ELSE 'NM'
    END::varchar(255)                                                                AS Country
  , TO_CHAR(TO_DATE(TR.JJ_Date__C,'YYYYMMDD HH24:MI:SS'),'DD/MM/YYYY')::varchar(255) AS Date
  , TO_CHAR(TO_DATE(TR.JJ_Date__C,'YYYYMMDD HH24:MI:SS'),'YYYYMMDD')::varchar(255)   AS Time_Effectiveness_Date
  ---------------------------------------------
  , TO_CHAR(TO_TIMESTAMP(TR.JJ_Date__C,'YYYYMMDD HH24:MI:SS'),'DD/MM/YYYY HH24:MI:SS')::varchar(255) AS Time_Effectiveness_Created_Timestamp
  ---------------------------------------------
  , TR.Id                                                                            AS Time_Effectiveness_Id
  , TR.JJ_Comment__c                                                                 AS Time_Effectiveness_Comments
  ---------------------------------------------
  , CASE
      WHEN TR.jj_time_off__c = '' THEN 'Field Day'
      WHEN TR.jj_time_off__c = TET.time_effectiveness_reason THEN TET.time_effectiveness_type
      ELSE TR.jj_time_off__c
    END::varchar(255) AS Time_Effectiveness_Type
  ---------------------------------------------  
  , TR.name           AS Time_Effectiveness
  , TR.OwnerId        AS Employee_Id
  , CASE
        WHEN TR.ownerid = UT.userid
            THEN UT.territoryid
            ELSE 'NM'
    END::varchar(255) AS Territory_Nominal_Id
  , TR.OwnerId        AS Assignment_Id
  , TR.OwnerId        AS Assignment_Nominal_Id
  , TR.jj_time_off__c AS Time_Effectiveness_Reason 
  ---------------------------------------------
  , CASE
      WHEN TR.jj_time_off__c = '' THEN 'Time On Territory'
      WHEN TR.jj_time_off__c = TERT.time_effectiveness_reason THEN TERT.time_effectiveness_record_type       
      ELSE TR.jj_time_off__c
    END::varchar(255) AS Time_Effectiveness_Record_Type
  ---------------------------------------------     
  , NULL::varchar(255)                 AS Time_Effectiveness_Status
  , ('Time Effectiveness')::varchar(255) AS Origin
  , ('Non Visited_' ||
    CASE
        WHEN (TR.ownerid = USR.id AND USR.jj_user_country__c = CS.name)
            THEN CS.jj_country_iso_code__c
            ELSE 'NM'
    END)::varchar(255)                             AS Account_Visited_NonVisited_Technical
  , NULL::varchar(255)                             AS Time_Effectiveness_Time
  , (TR.ownerid ||'-'|| TO_CHAR(TO_DATE(TR.jj_date__c, 'YYYYMMDD HH24:MI:SS'), 'DD-MM-YYYY'))::varchar(255) AS Aux_Employee_Date
  ---------------------------------------------  
  , TR.jj_product__c::varchar(255)                 AS Aux_Product_Id
  ---------------------------------------------  
  , NULL::varchar(255)                             AS Aux_Therapeutic_Area
  , NULL::varchar(255)                             AS Time_Effectiveness_Hours
  , TR.jj_system_percentage__c                     AS Time_Effectiveness_Percentage
FROM {{ source('raw', 'time_registration') }} AS TR
LEFT OUTER JOIN {{ source('raw', 'user') }} USR ON TR.ownerid = USR.id
LEFT OUTER JOIN {{ source('raw', 'country_settings') }} AS CS ON USR.JJ_User_Country__c = CS.name
LEFT OUTER JOIN {{ source('raw', 'time_effectiveness_type') }} AS TET ON TR.jj_time_off__c = TET.time_effectiveness_reason
LEFT OUTER JOIN {{ source('raw', 'time_effectiveness_record_type') }} AS TERT ON TR.jj_time_off__c = TERT.time_effectiveness_reason
LEFT OUTER JOIN {{ ref('tmp_user_territory') }} AS UT ON TR.ownerid = UT.userid
WHERE CASE
          WHEN TR.ownerid = USR.id AND USR.jj_user_country__c = CS.name
               THEN CS.jj_country_iso_code__c
               ELSE 'NM'
	END = 'DE' 
AND extract(year from TO_DATE(TR.createddate, 'YYYYMMDD HH24:MI:SS')) >= extract(year from GETDATE())-2
