{{ config(
	post_hook="grant all privileges on {{ this }} to {{ var('snowflake_user_grant_privileges') }}")
}}

SELECT
    TMP.campaign_id::varchar(20) as Campaign_Id,
    TMP.Employee_Id::varchar(20) AS Employee_Id,
    TMP.Product_Id::varchar(20) AS Product_Id,
    TMP.Therapeutic_Area_Name::varchar(255) AS Therapeutic_Area_Name,
    TMP.Therapeutic_Area_Brand_Name::varchar(255) AS Therapeutic_Area_Brand_Name,
    CASE WHEN TRIM(utn.userid)='' OR utn.userid IS NULL THEN 'NM' ELSE utn.Id END::varchar(20) Territory_Nominal_Id,
    CASE WHEN TRIM(TMP.date)='' THEN '' ELSE TO_CHAR(TO_DATE(TMP.date, 'DD-MM-YYYY'), 'YYYY-MM-DD') END::varchar(10) as Date,
    F_CAMPAIGN_TARGET.Campaign_Target_Id::varchar(20) AS Campaign_Target_Id,
    F_CAMPAIGN_TARGET.Account_Id::varchar(20) AS Account_Id,
    F_CAMPAIGN_RESPONSE.Campaign_Response_Id::varchar(20) AS Campaign_Response_Id,
    CASE
        WHEN F_CAMPAIGN_TARGET.Account_Id = ACC.id THEN ACC.country_iso_code   -- ACC.country_jj__c
        WHEN TMP.Employee_Id = USR.id THEN USR.country_iso_code -- USR.jj_user_country__c
        ELSE 'NM'
    END::varchar(2) AS Country_Code,
    ('')::varchar(1) AS Brick_Id,
    CASE WHEN LEN(F_CAMPAIGN_TARGET.Account_Id) > 0 THEN F_CAMPAIGN_TARGET.Account_Id||'-'||TMP.Employee_Id ELSE TMP.Employee_Id||'-'||TMP.Employee_Id END::varchar(255) AS Security_Key,
    'Campaign'::varchar(14) AS Origin,
    CASE WHEN LOWER(Campaign.jj_status__c) IN ('active', 'inactive') THEN 'Campaign' ELSE NULL END::varchar(8) AS Activity,
    CASE    WHEN LOWER(Campaign.jj_status__c) IN ('active', 'inactive') THEN TMP.campaign_record_type ELSE NULL END::varchar(255) AS Activity_Type,
    CASE WHEN LEN(F_CAMPAIGN_TARGET.Account_Id)>0 THEN F_CAMPAIGN_TARGET.Account_Id ELSE TMP.Employee_Id END::varchar(20) AS Assignment_Id,
    (COALESCE(F_CAMPAIGN_TARGET.Account_Id,'')||'-'||        
        CASE WHEN LEN(TMP.Date) = 0 THEN NULL ELSE DATEDIFF(day, to_date('1899-12-30','YYYY-MM-DD'),TO_DATE(Date,'DD-MM-YYYY')) END::varchar(255))::varchar(255) AS Account_SCD_Id,
    (COALESCE(F_CAMPAIGN_TARGET.Account_Id,'')||'-'||TMP.Product_Id||'-'||        
        CASE WHEN LEN(TMP.Date) = 0 THEN NULL ELSE DATEDIFF(day, to_date('1899-12-30','YYYY-MM-DD'),TO_DATE(Date,'DD-MM-YYYY')) END::varchar(255))::varchar(255) AS Product_Metrics_SCD_Id,
    CASE
        WHEN (TMP.campaign_record_type='Phone' AND F_CAMPAIGN_TARGET.Call_Stat='Completed') OR
             (TMP.campaign_record_type='Traditional Mail' AND (Campaign.jj_status__c='Inactive' OR Campaign.jj_status__c='Active') AND
             LEN(F_CAMPAIGN_TARGET.Campaign_Target_Id)>0) OR
             (TMP.campaign_record_type='Fax' AND (Campaign.jj_status__c='Inactive' OR Campaign.jj_status__c='Active') AND
             LEN(F_CAMPAIGN_TARGET.Campaign_Target_Id)>0) THEN 'Non Face to Face Contacts' ELSE NULL
    END::varchar(25) AS Multichannel_Type,
    CASE
        WHEN (TMP.campaign_record_type='Phone' AND F_CAMPAIGN_TARGET.Call_Stat='Completed') THEN 'Phone Campaigns'
        WHEN (TMP.campaign_record_type='Traditional Mail' AND (Campaign.jj_status__c='Inactive' OR Campaign.jj_status__c='Active') AND
             LEN(F_CAMPAIGN_TARGET.Campaign_Target_Id)>0) THEN 'Traditional Mail Campaigns'
        WHEN (TMP.campaign_record_type='Fax' AND (Campaign.jj_status__c='Inactive' OR Campaign.jj_status__c='Active') AND
             LEN(F_CAMPAIGN_TARGET.Campaign_Target_Id)>0) THEN 'Fax Campaigns'
        ELSE NULL
    END::varchar(26) AS Multichannel_Activity,
    NULL::varchar(30) AS Multichannel_Activity_Type,
    CASE
        WHEN (CASE WHEN F_CAMPAIGN_TARGET.Account_Id = ACC.id THEN ACC.country_iso_code WHEN TMP.Employee_Id = USR.id THEN USR.country_iso_code ELSE 'NM' END) = COUNTRY.jj_Country_ISO_Code__C
        THEN COUNTRY.Name ELSE 'NM'
    END::varchar(255) AS Country,
    CASE
        WHEN (CASE WHEN F_CAMPAIGN_TARGET.Account_Id = ACC.id THEN ACC.country_iso_code WHEN TMP.Employee_Id = USR.id THEN USR.country_iso_code ELSE 'NM' END) = COUNTRY.jj_Country_ISO_Code__C
        THEN COUNTRY.jj_Region__c ELSE 'NM'
    END::varchar(255) AS Region,
    P_GROUP_MAP.product_analytic_group_id::varchar(20) AS Product_Analytic_Group_Id,
    CASE
        WHEN P_GROUP_MAP.Product_id = TMP.Product_Id
        THEN COALESCE(F_CAMPAIGN_TARGET.Account_Id,'')||'-'||COALESCE(P_GROUP_MAP.product_analytic_group_id,'')||'-'||CASE WHEN M_PRODUCT.Product_Id = Campaign.jj_product__c THEN M_PRODUCT.product_country_code ELSE 'NM' END
        ELSE COALESCE(F_CAMPAIGN_TARGET.Account_Id,'')||'-'||CASE WHEN M_PRODUCT.Product_Id = TMP.Product_Id THEN M_PRODUCT.product_detail_id ELSE 'NM' END
    END::varchar(255) AS Product_Target_Class_Key,
     
    NULL::varchar(255) AS Campaign_Email_Status,
    NULL::varchar(10) AS Activity_Date,
    NULL::varchar(255) AS Campaign_ClickThrough_Id,
    NULL::varchar(255) AS Campaign_Open_Id,
    NULL::integer AS Campaign_Email_Send_Counter,
    NULL::integer AS Campaign_Email_Bounced_Counter,
    NULL::varchar(255) AS Campaign_Pageview_Url,
    NULL::varchar(255) AS Campaign_WebSite_Url,
    NULL::varchar(255) AS Campaign_Form_Name,
    NULL::integer AS Campaign_Pageview_Counter,
    NULL::integer AS Campaign_Form_Counter,
    NULL::numeric(10,2) AS Campaign_Pageview_Duration,
    NULL AS Campaign_Email_Link,
    TMP.campaign_type,
    TMP.Content_Topic::varchar(510) AS Content_Topic,
    TMP.Content_Sub_Topic::varchar(510) AS Content_Sub_Topic,
    TMP.Key_Message_Category::varchar(16777216) AS Key_Message_Category,
    TMP.Key_Message_Sub_Group::varchar(510) AS Key_Message_Sub_Group,
    TMP.Purpose::varchar(3000) AS Purpose,
    CASE WHEN mncv.Account_Id IS NOT NULL AND mncv.Territory_Id IS NOT NULL
    THEN 1
        ELSE 0
            END AS flag_territory_belong,
  CASE WHEN mncvs.Account_Id IS NOT NULL AND mncvs.Territory_Id IS NOT NULL AND yearmonth IS NOT NULL
    THEN 1
        ELSE 0
            END AS historical_flag_territory_belong,
    NULL as channel_new
FROM ( select * from (
        select * , ROW_NUMBER() OVER(PARTITION BY Campaign_Id ORDER BY Campaign_Last_Modified_Date DESC) AS row1
          from {{ ref('tmp_f_campaign') }} ) a
         where a.row1 = 1
     ) AS TMP
 
LEFT OUTER JOIN ( select * from (
                    select * , ROW_NUMBER() OVER(PARTITION BY ID ORDER BY SYSTEMMODSTAMP DESC) AS row1
                      from {{ var('schema') }}.campaign_raw ) a
                     where a.row1 = 1
                ) AS Campaign ON TMP.campaign_id = Campaign.id
 
LEFT OUTER JOIN {{ var('schema') }}.product_raw AS Product ON Campaign.jj_product__c = Product.id 
 
LEFT OUTER JOIN {{ var('schema') }}.user_raw USR ON Campaign.ownerid = USR.id
 
LEFT OUTER JOIN {{ ref('m_product') }} AS M_PRODUCT ON M_PRODUCT.Product_Id = Campaign.jj_product__c
 
LEFT OUTER JOIN (
    SELECT an.id as product_analytic_group_id, pg.product_vod__c as product_id
    FROM {{ var('schema') }}.analytics_product_group_raw an   
    JOIN {{ var('schema') }}.product_group_map_raw pg ON an.id = pg.analytics_product_group_vod__c
    WHERE COALESCE(pg.product_vod__c, '') != ''
) AS P_GROUP_MAP ON Campaign.jj_product__c = P_GROUP_MAP.Product_id
 
LEFT OUTER JOIN (
    SELECT Target.campaign_vod__c as Campaign_Id, Target.target_account_vod__c as Account_Id, Target.JJ_Call_Status__c as Call_Stat, target.id as Campaign_Target_Id
    FROM {{ var('schema') }}.campaign_target_raw AS Target
    LEFT OUTER JOIN {{ var('schema') }}.account_raw AS Acc ON Acc.Id = Target.target_account_vod__c
    LEFT OUTER JOIN {{ var('schema') }}.country_settings_raw AS CS ON CS.jj_country_ISO_Code__c = (CASE WHEN Acc.Id = Target.target_account_vod__c THEN ACC.country_iso_code ELSE 'NM' END)
  LEFT OUTER JOIN( select * from (
                                  select * , ROW_NUMBER() OVER(PARTITION BY ID ORDER BY SYSTEMMODSTAMP DESC) AS row1
                                  from {{ var('schema') }}.campaign_raw
                                  ) a
                    where a.row1 = 1
                 ) AS Campaign ON Campaign.Id = Target.campaign_vod__c
    LEFT OUTER JOIN {{ var('schema') }}.record_type_raw AS RT ON Campaign.recordtypeid = RT.id
    WHERE LOWER(RT.name) NOT IN ('customer journey', 'email', 'sms')
 ) AS F_CAMPAIGN_TARGET ON TMP.Campaign_Id = F_CAMPAIGN_TARGET.Campaign_Id 
  
LEFT OUTER JOIN {{ var('schema') }}.account_raw AS ACC ON ACC.id = F_CAMPAIGN_TARGET.Account_Id
 
LEFT OUTER JOIN (
    SELECT Ass.id as Campaign_Response_Id, Ass.jj_Campaign_Target__c as Campaign_Target_Id
    FROM {{ var('schema') }}.assessment_raw AS Ass
    LEFT OUTER JOIN {{ var('schema') }}.account_raw AS Acc ON Ass.jj_Campaign_Target__c = Acc.Id
    LEFT OUTER JOIN {{ var('schema') }}.country_settings_raw AS CS ON CS.jj_country_ISO_Code__c = (CASE WHEN Ass.jj_campaign_target__c = Acc.Id THEN Acc.country_iso_code ELSE 'NM' END)
) AS F_CAMPAIGN_RESPONSE ON F_CAMPAIGN_TARGET.Campaign_Target_Id = F_CAMPAIGN_RESPONSE.Campaign_Target_Id
 
LEFT OUTER JOIN {{ var('schema') }}.country_settings_raw AS COUNTRY ON
    CASE
        WHEN F_CAMPAIGN_TARGET.Account_Id = ACC.id THEN ACC.country_iso_code
        WHEN Campaign.ownerid = USR.id THEN USR.country_iso_code
        ELSE 'NM'
    END = COUNTRY.jj_Country_ISO_Code__C
LEFT OUTER JOIN (
    SELECT max(id) as id, userid
    FROM (
        SELECT ut.territory2id as id, ut.UserId
        FROM {{ var('schema') }}.user_territory_association_raw ut
        JOIN {{ var('schema') }}.user_raw u on u.id = ut.userid AND u.isactive != 0
        JOIN {{ ref('m_territory') }} mt on ut.territory2id=mt.territory_id          
    )
    GROUP BY userid
) utn on TMP.employee_id = utn.UserId
LEFT JOIN (SELECT Account_Id, Territory_Id FROM {{ ref('m_null_country_values') }}
            GROUP BY Account_Id, Territory_Id
          ) mncv
  ON F_CAMPAIGN_TARGET.Account_Id = mncv.Account_Id AND CASE WHEN TRIM(utn.userid)='' OR utn.userid IS NULL THEN 'NM' ELSE utn.Id END = mncv.Territory_Id
LEFT JOIN (SELECT Account_Id, Territory_Id, yearmonth FROM {{ var('schema') }}.buw_alignment_m_null_country_values_snapshot_monthly_historical
            GROUP BY Account_Id, Territory_Id, yearmonth
          ) mncvs
  ON F_CAMPAIGN_TARGET.Account_Id = mncvs.Account_Id AND CASE WHEN TRIM(utn.userid)='' OR utn.userid IS NULL THEN 'NM' ELSE utn.Id END = mncvs.Territory_Id AND CASE WHEN LEN(TMP.Date) = 0 THEN NULL ELSE (split_part(TO_CHAR(TO_DATE(TMP.date, 'DD-MM-YYYY'), 'YYYY-MM-DD'),'-',1) || split_part(TO_CHAR(TO_DATE(TMP.date, 'DD-MM-YYYY'), 'YYYY-MM-DD'),'-',2)) END = mncvs.yearmonth 
 
UNION ALL
 
SELECT
 
  F_ACTIVITY_EMAIL_ELOQUA.Campaign_Id::varchar(20) AS Campaign_Id,
  COALESCE(TMP.Employee_Id, 'NM')::varchar(20) AS Employee_Id,
  COALESCE(TMP.Product_Id, 'NM')::varchar(20) AS Product_Id,
    CASE WHEN LEN(mp.product_therapeutic_area_1)>0 THEN mp.product_therapeutic_area_1 ELSE 'NM' END::varchar(255) as Therapeutic_Area_Name,
    CASE WHEN LEN(mp.product_brand_therapeutic_area_1_null)>0 THEN mp.product_brand_therapeutic_area_1_null ELSE 'NM' END::varchar(255) as Therapeutic_Area_Brand_Name,
  CASE WHEN F_ACTIVITY_EMAIL_ELOQUA.Campaign_Id = TMP.Campaign_Id THEN CASE WHEN TRIM(utn.userid)='' OR utn.userid IS NULL THEN 'NM' ELSE utn.Id END ELSE 'NM' END::varchar(20) AS Territory_Nominal_Id,
    CASE WHEN TRIM(F_ACTIVITY_EMAIL_ELOQUA.date) = ''
        THEN ''
        ELSE TO_CHAR(TO_DATE(F_ACTIVITY_EMAIL_ELOQUA.date, 'YYYYMMDD HH24:MI:SS'), 'YYYY-MM-DD')
    END::varchar(10) as Date,
    NULL::varchar(20) AS Campaign_Target_Id,
    F_ACTIVITY_EMAIL_ELOQUA.Account_Id::varchar(20) AS Account_Id,
    NULL::varchar(20) AS Campaign_Response_Id,
    CASE WHEN F_ACTIVITY_EMAIL_ELOQUA.Campaign_Email_Status = 'Bounceback'
        THEN CASE WHEN F_ACTIVITY_EMAIL_ELOQUA.Campaign_Id = TMP.Campaign_id THEN TMP.Campaign_Country_Code END
        ELSE CASE
                WHEN F_ACTIVITY_EMAIL_ELOQUA.Account_Id = ACCOUNT.Id AND LEN(ACCOUNT.Country_JJ__c)>0 THEN ACCOUNT.Country_JJ__c
                WHEN F_ACTIVITY_EMAIL_ELOQUA.Account_Id = ACCOUNT.Id THEN ACCOUNT.JJ_Country__c
                ELSE 'NM'
            END
    END::varchar(2) AS Country_Code,
    NULL::varchar(1) AS Brick_Id,
    F_ACTIVITY_EMAIL_ELOQUA.Account_Id::varchar(255) AS Security_Key,
    F_ACTIVITY_EMAIL_ELOQUA.Origin::varchar(14) AS Origin,
    CASE WHEN TMP.campaign_record_type IN ('SMS','Email','Customer Journey') THEN 'Campaign'
        WHEN TMP.Campaign_Status IN ('Active','Inactive') THEN 'Campaign' ELSE NULL
    END::varchar(8) AS Activity,
    CASE WHEN TMP.campaign_record_type IN ('SMS','Email','Customer Journey')
               THEN CASE WHEN F_ACTIVITY_EMAIL_ELOQUA.Campaign_Id = TMP.Campaign_Id THEN TMP.campaign_record_type ELSE 'NM' END
           WHEN TMP.Campaign_Status IN ('Active', 'Inactive')
               THEN CASE WHEN F_ACTIVITY_EMAIL_ELOQUA.Campaign_Id = TMP.Campaign_Id THEN TMP.campaign_record_type ELSE 'NM' END
           ELSE NULL
    END::varchar(255) AS Activity_Type,
    F_ACTIVITY_EMAIL_ELOQUA.Account_Id::varchar(20) AS Assignment_Id,
    NULL::varchar(255) AS Account_SCD_Id,
    NULL::varchar(255) AS Product_Metrics_SCD_Id,
    CASE
        WHEN (TMP.campaign_record_type = 'SMS' AND LEN(F_ACTIVITY_EMAIL_ELOQUA.Campaign_Email_Send_Counter)>0)
            OR (TMP.campaign_record_type IN ('Email','Customer Journey') AND F_ACTIVITY_EMAIL_ELOQUA.Campaign_Email_Open_Counter = 1)
        THEN 'Non Face to Face Contacts'
        ELSE NULL
    END::varchar(25) AS Multichannel_Type,
    CASE
        WHEN (TMP.campaign_record_type = 'SMS' AND LEN(F_ACTIVITY_EMAIL_ELOQUA.Campaign_Email_Send_Counter)>0) THEN 'SMS Campaigns'
        WHEN (TMP.campaign_record_type IN ('Email','Customer Journey') AND F_ACTIVITY_EMAIL_ELOQUA.Campaign_Email_Open_Counter = 1) THEN 'Mass Email Campaigns'
        ELSE NULL
    END::varchar(26) AS Multichannel_Activity,
    CASE WHEN TMP.campaign_record_type IN ('Email','Customer Journey') AND F_ACTIVITY_EMAIL_ELOQUA.Campaign_Email_Open_Counter = 1
        THEN 'Email Campaign Sent by Janssen'
        ELSE NULL
    END::varchar(30) AS Multichannel_Activity_Type,
    CASE WHEN LEN(Country_Code) > 0 THEN cs.name ELSE NULL END::varchar(255) AS Country,
    CASE WHEN LEN(Country_Code) > 0 THEN cs.jj_region__c ELSE NULL END::varchar(255) AS Region,
    P_GROUP_MAP.product_analytic_group_id::varchar(20) AS Product_Analytic_Group_Id,
    CASE WHEN P_GROUP_MAP.Product_id = COALESCE(TMP.Product_Id, 'NM')
        THEN COALESCE(F_ACTIVITY_EMAIL_ELOQUA.Account_Id,'') ||'-'|| COALESCE(P_GROUP_MAP.product_analytic_group_id,'') ||'-'||
            CASE WHEN mp.Product_Id = COALESCE(TMP.Product_Id, 'NM') THEN mp.product_country_code ELSE 'NM' END
        ELSE COALESCE(F_ACTIVITY_EMAIL_ELOQUA.Account_Id,'') ||'-'|| CASE WHEN mp.Product_Id = COALESCE(TMP.Product_Id, 'NM') THEN mp.product_detail_id ELSE 'NM' END
    END::varchar(255) AS Product_Target_Class_Key, 
     
    F_ACTIVITY_EMAIL_ELOQUA.Campaign_Email_Status::varchar(255) AS Campaign_Email_Status,
    F_ACTIVITY_EMAIL_ELOQUA.Activity_Date::varchar(10) AS Activity_Date,
    F_ACTIVITY_EMAIL_ELOQUA.Campaign_ClickThrough_Id::varchar(255) AS Campaign_ClickThrough_Id,
    F_ACTIVITY_EMAIL_ELOQUA.Campaign_Open_Id::varchar(255) AS Campaign_Open_Id,
    F_ACTIVITY_EMAIL_ELOQUA.Campaign_Email_Send_Counter::integer AS Campaign_Email_Send_Counter,
    F_ACTIVITY_EMAIL_ELOQUA.Campaign_Email_Bounced_Counter::integer AS Campaign_Email_Bounced_Counter,
    F_ACTIVITY_EMAIL_ELOQUA.Campaign_Pageview_Url::varchar(1000) AS Campaign_Pageview_Url,
    F_ACTIVITY_EMAIL_ELOQUA.Campaign_WebSite_Url::varchar(1000) AS Campaign_WebSite_Url,
    F_ACTIVITY_EMAIL_ELOQUA.Campaign_Form_Name::varchar(255) AS Campaign_Form_Name,
    F_ACTIVITY_EMAIL_ELOQUA.Campaign_pageview_counter::integer AS Campaign_Pageview_Counter,
    F_ACTIVITY_EMAIL_ELOQUA.Campaign_form_counter::integer AS Campaign_Form_Counter,
    F_ACTIVITY_EMAIL_ELOQUA.Campaign_Pageview_Duration::numeric(10,2) AS Campaign_Pageview_Duration,
    F_ACTIVITY_EMAIL_ELOQUA.Campaign_Email_Link AS Campaign_Email_Link,
    F_ACTIVITY_EMAIL_ELOQUA.campaign_type,
     
    TMP.Content_Topic::varchar(510),
    TMP.Content_Sub_Topic::varchar(510),
    TMP.Key_Message_Category::varchar(16777216),
    TMP.Key_Message_Sub_Group::varchar(510),
    TMP.Purpose::varchar(3000),
CASE WHEN mncv.Account_Id IS NOT NULL AND mncv.Territory_Id IS NOT NULL
    THEN 1
        ELSE 0
            END AS flag_territory_belong,
CASE WHEN mncvs.Account_Id IS NOT NULL AND mncvs.Territory_Id IS NOT NULL AND mncvs.yearmonth IS NOT NULL
        THEN 1
            ELSE 0
                END AS historical_flag_territory_belong,
    NULL as channel_new
 
FROM (
    SELECT
        AUX_ELOQUA.campaign_id AS Campaign_id,
        AUX_ELOQUA.campaign_email_status AS campaign_email_status,
        COALESCE(campaign.JJ_Start_date__c, '') AS Date,
        LEFT(aux_eloqua.date, 10) AS Activity_Date,
        COALESCE(AUX_ELOQUA.campaign_id,'') || '-' || COALESCE(AUX_ELOQUA.account_id,'') || '-' || COALESCE(AUX_ELOQUA.assetId,'') AS Campaign_Clickthrough_id,
        COALESCE(AUX_ELOQUA.campaign_id,'') || '-' || COALESCE(AUX_ELOQUA.account_id,'') || '-' || COALESCE(AUX_ELOQUA.campaign_email_name,'') AS Campaign_Open_id,
        AUX_ELOQUA.account_id AS Account_Id,
        AUX_ELOQUA.Origin AS origin,
        AUX_ELOQUA.Campaign_Pageview_url AS Campaign_Pageview_url,
        AUX_ELOQUA.campaign_website_url AS Campaign_website_url,
        AUX_ELOQUA.campaign_form_name AS Campaign_form_name,
        AUX_ELOQUA.campaign_email_name AS Campaign_email_name,
        AUX_ELOQUA.EmailClickedThroughLink AS Campaign_email_link,
         
        SUM(AUX_ELOQUA.Campaign_email_send_counter)::integer AS Campaign_email_send_counter, -- UPDATED 20201116 PASS FROM SUM() TO MAX
        MAX(AUX_ELOQUA.campaign_email_open_counter)::integer AS Campaign_email_open_counter,
        MAX(AUX_ELOQUA.campaign_email_bounced_counter)::integer AS Campaign_email_bounced_counter,
        SUM(AUX_ELOQUA.campaign_pageview_counter)::integer AS Campaign_pageview_counter,
        SUM(AUX_ELOQUA.campaign_form_counter)::integer AS Campaign_form_counter,
        AVG(AUX_ELOQUA.campaign_pageview_duration)::numeric(10,2) AS Campaign_pageview_duration,
         
        CASE WHEN campaign.Id IS NOT NULL THEN campaign.JJ_CAMPAIGN_TYPE__C ELSE NULL END AS campaign_type
 
    FROM {{ ref('tmp_f_activity_email_eloqua') }} AS AUX_ELOQUA
    LEFT OUTER JOIN ( select * from (
                    select * , ROW_NUMBER() OVER(PARTITION BY ID ORDER BY SYSTEMMODSTAMP DESC) AS row1
                      from {{ var('schema') }}.campaign_raw ) a
                     where a.row1 = 1
                  ) AS campaign ON campaign.Id = aux_eloqua.campaign_id
    GROUP BY aux_eloqua.campaign_id,campaign.id,aux_eloqua.campaign_email_status,campaign.jj_start_date__c,LEFT(aux_eloqua.date, 10),
        aux_eloqua.account_id,aux_eloqua.assetid,aux_eloqua.campaign_email_name,aux_eloqua.origin,aux_eloqua.campaign_pageview_url,
        aux_eloqua.campaign_website_url,aux_eloqua.campaign_form_name,aux_eloqua.emailclickedthroughlink, campaign.JJ_CAMPAIGN_TYPE__C
        
) AS F_ACTIVITY_EMAIL_ELOQUA
 
 
LEFT OUTER JOIN (select *
                   from (
                         select * , ROW_NUMBER() OVER(PARTITION BY Campaign_Id ORDER BY Campaign_Last_Modified_Date DESC) AS row1
                           from {{ ref('tmp_f_campaign') }}
                        ) a
                  where a.row1 = 1
                 )
 AS TMP ON F_ACTIVITY_EMAIL_ELOQUA.Campaign_Id = TMP.Campaign_Id
 
LEFT OUTER JOIN {{ var('schema') }}.account_raw AS ACCOUNT ON F_ACTIVITY_EMAIL_ELOQUA.Account_Id = Account.Id
 
LEFT OUTER JOIN {{ var('schema') }}.country_settings_raw AS CS ON
    (CASE WHEN (F_ACTIVITY_EMAIL_ELOQUA.Campaign_Email_Status = 'Bounceback')
        THEN (CASE WHEN F_ACTIVITY_EMAIL_ELOQUA.Campaign_Id = TMP.Campaign_id THEN TMP.Campaign_Country_Code ELSE 'NM' END)
        ELSE (CASE WHEN (F_ACTIVITY_EMAIL_ELOQUA.Account_Id = ACCOUNT.Id AND LEN(ACCOUNT.country_iso_code)>0) THEN ACCOUNT.country_iso_code
                   WHEN F_ACTIVITY_EMAIL_ELOQUA.Account_Id = ACCOUNT.Id THEN ACCOUNT.country_iso_code ELSE 'NM' END)
    END) = CS.jj_Country_ISO_Code__C
     
LEFT OUTER JOIN {{ var('schema') }}.product_raw AS P ON (CASE WHEN F_ACTIVITY_EMAIL_ELOQUA.Campaign_Id = TMP.Campaign_Id THEN TMP.Product_Id ELSE 'NM' END) = P.Id
 
LEFT OUTER JOIN (
    SELECT an.id as product_analytic_group_id, pg.product_vod__c as product_id
    FROM {{ var('schema') }}.analytics_product_group_raw an   
    JOIN {{ var('schema') }}.product_group_map_raw pg ON an.id = pg.analytics_product_group_vod__c
    WHERE COALESCE(pg.product_vod__c, '') != ''
) AS P_GROUP_MAP ON P_GROUP_MAP.Product_id = COALESCE(TMP.Product_Id,'NM')
 
LEFT OUTER JOIN (
                    SELECT *, CASE WHEN product_brand_therapeutic_area_1 = 'No Value' THEN NULL ELSE product_brand_therapeutic_area_1 END as product_brand_therapeutic_area_1_null
                    FROM {{ ref('m_product') }}
                  ) mp ON mp.product_id = (CASE WHEN F_ACTIVITY_EMAIL_ELOQUA.Campaign_Id = TMP.Campaign_Id THEN TMP.Product_Id ELSE 'NM' END)
 
LEFT OUTER JOIN (
    SELECT max(id) as id, userid
    FROM (
        SELECT ut.territory2id as id, ut.UserId
        FROM {{ var('schema') }}.user_territory_association_raw ut
        JOIN {{ var('schema') }}.user_raw u on u.id = ut.userid AND u.isactive != 0
        JOIN {{ ref('m_territory') }} mt on ut.territory2id=mt.territory_id          
        )
    GROUP BY userid
) utn on TMP.employee_id = utn.UserId
 
LEFT JOIN (SELECT Account_Id, Territory_Id FROM {{ ref('m_null_country_values') }}
            GROUP BY Account_Id, Territory_Id
          ) mncv
       ON F_ACTIVITY_EMAIL_ELOQUA.Account_Id = mncv.Account_Id AND CASE WHEN F_ACTIVITY_EMAIL_ELOQUA.Campaign_Id = TMP.Campaign_Id THEN CASE WHEN TRIM(utn.userid)='' OR utn.userid IS NULL THEN 'NM' ELSE utn.Id END ELSE 'NM' END = mncv.Territory_Id
 
LEFT JOIN (SELECT Account_Id, Territory_Id, yearmonth FROM {{ var('schema') }}.buw_alignment_m_null_country_values_snapshot_monthly_historical
            GROUP BY Account_Id, Territory_Id, yearmonth
          ) mncvs
       ON F_ACTIVITY_EMAIL_ELOQUA.Account_Id = mncvs.Account_Id AND CASE WHEN F_ACTIVITY_EMAIL_ELOQUA.Campaign_Id = TMP.Campaign_Id THEN CASE WHEN TRIM(utn.userid)='' OR utn.userid IS NULL THEN 'NM' ELSE utn.Id END ELSE 'NM' END = mncvs.Territory_Id AND CASE WHEN LEN(F_ACTIVITY_EMAIL_ELOQUA.date) = 0 THEN NULL ELSE (split_part(TO_CHAR(TO_DATE(F_ACTIVITY_EMAIL_ELOQUA.date, 'YYYYMMDD HH24:MI:SS'), 'YYYY-MM-DD')::varchar(10),'-',1) || split_part(TO_CHAR(TO_DATE(F_ACTIVITY_EMAIL_ELOQUA.date, 'YYYYMMDD HH24:MI:SS'), 'YYYY-MM-DD')::varchar(10),'-',2)) END = mncvs.yearmonth 