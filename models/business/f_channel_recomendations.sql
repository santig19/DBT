{{ config(
	post_hook="grant all privileges on {{ this }} to {{ var('snowflake_user_grant_privileges') }}")
}}

SELECT COUNTRY_ISO_CODE::varchar(100) AS COUNTRY_ISO_CODE
      ,ID::varchar(36) AS ID
      ,CASE WHEN ISDELETED = '' THEN NULL ELSE ISDELETED END::int AS ISDELETED 
      ,NAME::varchar(160) AS NAME
      ,CURRENCYISOCODE::varchar(6) AS CURRENCYISOCODE
      ,CASE WHEN CREATEDDATE = '' THEN NULL ELSE TO_TIMESTAMP(CREATEDDATE,'YYYYMMDD HH24:MI:SS') END::datetime AS CREATEDDATE
      ,CREATEDBYID::varchar(36) AS CREATEDBYID
      ,CASE WHEN LASTMODIFIEDDATE = '' THEN NULL ELSE TO_TIMESTAMP(LASTMODIFIEDDATE,'YYYYMMDD HH24:MI:SS') END::datetime AS LASTMODIFIEDDATE 
      ,LASTMODIFIEDBYID::varchar(36) AS LASTMODIFIEDBYID
      ,CASE WHEN SYSTEMMODSTAMP = '' THEN NULL ELSE TO_TIMESTAMP(SYSTEMMODSTAMP,'YYYYMMDD HH24:MI:SS') END::datetime AS SYSTEMMODSTAMP 
      ,CASE WHEN MAYEDIT = '' THEN NULL ELSE MAYEDIT END::int AS MAYEDIT 
      ,CASE WHEN ISLOCKED = '' THEN NULL ELSE ISLOCKED END::int AS ISLOCKED 
      ,CONNECTIONRECEIVEDID::varchar(36) AS CONNECTIONRECEIVEDID
      ,CONNECTIONSENTID::varchar(36) AS CONNECTIONSENTID
      ,CODS_EXTERNAL_ID__C::varchar(36) AS CODS_EXTERNAL_ID__C
      ,JJ_CHANNEL__C::varchar(16777216) AS JJ_CHANNEL__C
      ,JJ_JOE_EXTERNAL_ID__C::varchar(510) AS JJ_JOE_EXTERNAL_ID__C
      ,JJ_PRODUCT__C::varchar(36) AS JJ_PRODUCT__C
      ,JJ_PRODUCT_DESCRIPTION__C::varchar(510) AS JJ_PRODUCT_DESCRIPTION__C
      ,CASE WHEN JJ_RECOMMENDATION__C = '' THEN NULL ELSE JJ_RECOMMENDATION__C END::decimal(18,0) AS JJ_RECOMMENDATION__C
      ,JJ_SALESLINE__C::varchar(200) AS JJ_SALESLINE__C
      ,JJ_MC_CYCLE__C::varchar(36) AS JJ_MC_CYCLE__C
      ,JJ_RUN_ID__C::varchar(100) AS JJ_RUN_ID__C
      ,JJ_COMMENTS__C::varchar(510) AS JJ_COMMENTS__C
      ,JJ_FEEDBACK__C::varchar(510) AS JJ_FEEDBACK__C
      ,JJ_REASON__C::varchar(510) AS JJ_REASON__C
      ,JJ_CONCATENATED_PRODUCT__C::varchar(200) AS JJ_CONCATENATED_PRODUCT__C
      ,JJ_TERRITORY__C::varchar(16777216) AS JJ_TERRITORY__C
      ,JJ_TRAFFIC_LIGHT_SOURCE__C::varchar(510) AS JJ_TRAFFIC_LIGHT_SOURCE__C
      ,JJ_TRAFFIC_LIGHT__C::varchar(510) AS JJ_TRAFFIC_LIGHT__C
      ,JJ_ADDITIONAL_FILTERS__C::varchar(510) AS JJ_ADDITIONAL_FILTERS__C
      ,JJ_FURTHER_POTENTIAL__C::varchar(510) AS JJ_FURTHER_POTENTIAL__C
  FROM {{ source('raw', 'channel_recommendations') }}