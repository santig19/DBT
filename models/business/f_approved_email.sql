{{ config(
	post_hook="grant all privileges on {{ this }} to {{ var('snowflake_user_grant_privileges') }}")
}}

SELECT DISTINCT
    SE.id::varchar(20) AS Approved_Email_Counter_Id,
    SE.account_vod__c::varchar(20) AS Account_Id,
     
    'Non Visited_' ||
        CASE WHEN SE.account_vod__c = ACC.id
            THEN CASE WHEN LEN(ACC.Country_jj__c)>0 THEN ACC.country_jj__c ELSE ACC.jj_country__c END
            ELSE 'NM'
    END::varchar(14) AS Account_Visited_NonVisited_Technical,
    'One to One Email'::varchar(16) AS Activity_Type,
    'One to One Email'::varchar(16) AS Activity,
    1::integer AS Approved_Email_Counter,
    CASE WHEN LEN(SE.country_iso_code)>0 THEN SE.country_iso_code ELSE 'NM' END::varchar(2) AS Approved_Email_Country_Code,
    CASE WHEN LEN(cs.name)>0 THEN cs.name ELSE 'NM' END::varchar(255) AS Approved_Email_Country,
    CASE
        WHEN TO_CHAR(TO_TIMESTAMP(SE.createddate,'YYYYMMDD HH24:MI:SS'),'MI') >= '30'
        THEN TO_CHAR(DATEADD(hour,1,TO_TIMESTAMP(SE.createddate, 'YYYYMMDD HH24:MI:SS')),'DD/MM/YYYY HH24:00:00')
        ELSE TO_CHAR(TO_TIMESTAMP(SE.createddate,'YYYYMMDD HH24:MI:SS'),'DD/MM/YYYY HH24:00:00')
    END::varchar(19) AS Approved_Email_Created_Time,
    CASE
        WHEN TRIM(SE.last_activity_date_vod__c) = '' THEN SE.last_activity_date_vod__c
        ELSE TO_CHAR(TO_DATE(SE.last_activity_date_vod__c,'YYYYMMDD HH24:MI:SS'), 'YYYYMMDD')
    END::varchar(8) AS Approved_Email_Due_Date,
    SE.id::varchar(20) AS Approved_Email_Id,
    CASE WHEN SE.ownerid = USR.Id THEN USR.name ELSE 'NM' END::varchar(255) AS Approved_Email_Owner_Name,
    CASE WHEN USR.Profileid = PRF.Id THEN PRF.name ELSE 'NM' END::varchar(255) AS Approved_Email_Owner_Profile_Name,
    CASE WHEN LEN(cs.jj_region__c)>0 THEN cs.jj_region__c ELSE 'NM' END::varchar(255) AS Approved_Email_Region,
    CASE
        WHEN LOWER(SE.status_vod__c) IN ('delivered_vod','marked_spam_vod') 
        THEN CASE
                WHEN SE.clicked_vod__c = '1'
                THEN 'ClickThrough'
                ELSE CASE WHEN SE.opened_vod__c = '1' THEN 'Opened' ELSE 'Sent' END
            END
        ELSE NULL
    END::varchar(12) AS Approved_Email_Response_Type,      
    SE.status_vod__c::varchar(255) AS Approved_Email_Status,
    CASE WHEN APPDOC.id = SE.approved_email_template_vod__c THEN APPDOC.email_subject_vod__c ELSE 'NM' END::varchar(2000) AS Approved_Email_Subject,
    'Email'::varchar(5) AS Approved_Email_Type,
    SE.account_vod__c::varchar(20) AS Assignment_Id,
    CASE
        WHEN SE.account_vod__c = ACC.id
        THEN Approved_Email_Country_Code ||'_'||
            CASE WHEN LEN(ACC.brick_number_jj__c)>0 THEN ACC.brick_number_jj__c ELSE 'No Value' END
        ELSE 'NM'
    END::varchar(255) AS Brick_Code,
     
    Approved_Email_Country_Code::varchar(2) AS Country_Code,
    Approved_Email_Country::varchar(255) AS Country,
         
    --datediff(day, to_date('1899-12-30','YYYY-MM-DD'),TO_DATE(SE.createddate,'YYYYMMDD HH24:MI:SS'))::varchar(255) AS Date,
    left(SE.createddate,8)::varchar(8) as Date,
   
    SE.ownerid::varchar(20) AS Employee_Id,
    NULL AS Multichannel_Activity_Type,
    '1 to 1 Emails Sent'::varchar(18) AS Multichannel_Activity,
    'Non Face to Face Contacts'::varchar(25) AS Multichannel_Type,
    'Approved Email'::varchar(14) AS Origin,
    PROD_GROUP_MAP.analytics_product_group_vod__c::varchar(20) AS Product_Analytic_Group_Id,
    SE.product_vod__c::varchar(20) AS Product_Id,
    CASE
        WHEN PROD_GROUP_MAP.product_vod__c = SE.product_vod__c
        THEN SE.account_vod__c ||'-'|| PROD_GROUP_MAP.analytics_product_group_vod__c ||'-'|| PRODUCT.country_iso_code
        ELSE SE.account_vod__c ||'-'|| M_PRODUCT.product_detail_id
    END::varchar(255) AS Product_Target_Class_Key,
    Approved_Email_Region::varchar(255) AS Region,
    (SE.account_vod__c ||'-'|| SE.ownerid)::varchar(255) AS Security_Key,
    CASE WHEN SE.ownerid = UT.USERID THEN UT.TERRITORYID ELSE 'NM' END::varchar(20) AS Territory_Nominal_Id,
    CASE WHEN SE.Approved_Email_Template_vod__c = APPDOC.ID THEN APPDOC.name ELSE 'NM' END AS Approved_email_template_name,
    APPDOC.Key_Message_Category__c  as Key_Message_Category,
    APPDOC.JJ_Purpose__c AS Purpose,
        APPDOC.Key_Message_Sub_Group__c as Key_Message_Sub_Group,
    APPDOC.Content_Topic__c as Content_Topic,
    CASE WHEN (Position(CHR(92) || CHR(92), APPDOC.Content_Sub_Topic__c) > 0) OR Position(CHR(124) || CHR(34), APPDOC.Content_Sub_Topic__c) > 0 OR Position(CHR(92) || CHR(124), APPDOC.Content_Sub_Topic__c) > 0 OR Position(CHR(92) || CHR(34), APPDOC.Content_Sub_Topic__c) > 0
        THEN REPLACE(REPLACE(REPLACE(REPLACE(APPDOC.Content_Sub_Topic__c, CHR(92) || CHR(92), CHR(92)), CHR(124) || CHR(34), CHR(34)), CHR(92) || CHR(124), CHR(124)), CHR(92) || CHR(34), CHR(34))
        ELSE APPDOC.Content_Sub_Topic__c
        END AS Content_Sub_Topic,
    RT.name as record_type,
    CASE WHEN mncv.Account_Id IS NOT NULL AND mncv.Territory_Id IS NOT NULL
        THEN 1
            ELSE 0
                END AS flag_territory_belong,
    CASE WHEN mncvs.Account_Id IS NOT NULL AND mncvs.Territory_Id IS NOT NULL AND yearmonth IS NOT NULL
        THEN 1
            ELSE 0
                END AS historical_flag_territory_belong,
    sha2('approved_email' || approved_email_subject, 256) as eevent_key,
    NULL as channel_new
  
     
FROM {{ source('raw', 'sent_email') }} AS SE
LEFT OUTER JOIN {{ source('raw', 'user') }} USR ON SE.ownerid = USR.Id
LEFT OUTER JOIN {{ source('raw', 'account') }} AS ACC ON SE.account_vod__c = ACC.id
LEFT OUTER JOIN {{ source('raw', 'country_settings') }} AS CS ON CS.country_iso_code = SE.country_iso_code
LEFT OUTER JOIN {{ source('raw', 'profile') }} AS PRF ON USR.Profileid = PRF.Id
LEFT OUTER JOIN {{ source('raw', 'approved_document') }} AS APPDOC ON APPDOC.id = SE.approved_email_template_vod__c
LEFT OUTER JOIN {{ source('raw', 'product_group_map') }} AS PROD_GROUP_MAP ON SE.product_vod__c = PROD_GROUP_MAP.product_vod__c
LEFT OUTER JOIN {{ source('raw', 'product') }} AS PRODUCT ON SE.product_vod__c = PRODUCT.id
LEFT OUTER JOIN {{ ref('m_product') }} AS M_PRODUCT ON PRODUCT.id = M_PRODUCT.product_id
LEFT OUTER JOIN {{ ref('tmp_user_territory') }} UT ON UT.USERID = SE.ownerid
LEFT OUTER JOIN {{ source('raw', 'record_type') }} RT ON RT.id = SE.recordtypeid 
LEFT JOIN (SELECT Account_Id, Territory_Id FROM {{ ref('m_null_country_values') }}
            GROUP BY Account_Id, Territory_Id) mncv
       ON SE.account_vod__c = mncv.Account_Id AND CASE WHEN SE.ownerid = UT.USERID THEN UT.TERRITORYID ELSE 'NM' END = mncv.Territory_Id
LEFT JOIN (SELECT Account_Id, Territory_Id, yearmonth FROM {{ source('raw', 'm_null_country_values_snapshot_monthly_historical') }}
            GROUP BY Account_Id, Territory_Id, yearmonth) mncvs
       ON SE.account_vod__c = mncvs.Account_Id AND CASE WHEN SE.ownerid = UT.USERID THEN UT.TERRITORYID ELSE 'NM' END = mncvs.Territory_Id AND LEFT(SE.createddate,6) = mncvs.yearmonth
WHERE SE.originating_from_1_to_1_email__c IN ('0', 'false')