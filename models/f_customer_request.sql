{{ config(
	post_hook="grant all privileges on {{ this }} to {{ var('snowflake_user_grant_privileges') }}")
}}

SELECT
    t.Account_Id,
    t.Country_Code,
    t.Brick_Code,
    t.Customer_Request_Number,
    t.Customer_Request_Code,
    t.Customer_Request_SKM_Time_Registration,
    t.Customer_Request_Question_to_SKM,
    t.Customer_Request_Closed_Date,
    t."Date",
    t.Customer_Request_Date,
    t.Customer_Request_Start_Date,
    t.Customer_Request_Created_Timestamp,
    t.Customer_Request_Hours_To_Close,
    t.Customer_Request_Working_Hours_To_Close,
    t.Customer_Request_Id,
    t.Digital_Signature,
    t.Activity,
    t.Activity_Type,
    t.Customer_Request_Veeva_Link,
    t.Customer_Request_Related_Veeva_Link,
    t.Customer_Request_Closed,
    t.Customer_Request_Answer,
    t.Customer_Request_Approval_Status,
    t.Customer_Request_Category,
    t.Product_Id,
    t.Customer_Request_Reason,
    t.Customer_Request_Source_Out,
    t.Customer_Request_Subtype,
    t.Customer_Request_Subcategory,
    t.Customer_Request_Origin,
    t.Customer_Request_Assigned_To_Id,
    t.Customer_Request_CSC_Assigned_Flag,
    t.Customer_Request_Assigned_Employee,
    t.Customer_Request_Owner,
    t.Employee_Id,
    t.Customer_Request_Department,
    t.Customer_Request_Employee_Department,
    t.Customer_Request_Created_Employee,
    t.Customer_Request_Priority,
    t.Customer_Request_Off_On_Label,
    t.Customer_Request_Question,
    t.Customer_Request_Rejection_Reason,
    t.Customer_Request_Survey_Feedback_Id,
    t.Customer_Request_Record_Type,
    t.Customer_Request_Status,
    t.Customer_Request_Subject,
    t.Customer_Request_Description,
    t.Customer_Request_Cost,
    t.Customer_Request_Date_Assigned,
    t.Customer_Request_End_Date,
    t.Origin,
    t.Account_Visited_NonVisited_Technical,
    t.Sys_Compliance_Visibility,
    t.Compliance_Visibility,
    t.Customer_Request_PV_Reference_1,
    t.Customer_Request_PV_Reference_2,
    t.Customer_Request_QA_Reference,
    t.Customer_Request_Control_PV,
    t.Customer_Request_Control_QA,
    t.Customer_Request_Date_Awareness,
    t.Multichannel_Type,
    t.Multichannel_Activity,
    t.Multichannel_Activity_Type,
    t.Creator_Owner,
    t.Therapeutic_Area_Name,
    t.Therapeutic_Area_Brand_Name,
    t.Product_Target_Class_Key,
    t.Security_Key,
    t.Account_SCD_Id,
    t.Product_Metrics_SCD_Id,
    t.Assignment_Id,
    t.Region,
    t.Compliance_Region,
    t.Territory_Nominal_Id,
    t.Customer_Request_Threshold_Time,
    t.Customer_Request_Threshold_Working_Time,
    t.Country,
    t.Compliance_Country,
    t.Customer_Request_1st_2nd_Line,
    t.Customer_Request_Line_Of_Response,
    t.Customer_Request_Link_Id,
    t.parent_id,
    t.number_of_questions,
    t.customer_request_closed_timestamp,
    t.country_1,
    t.global_content_usage,
    CASE WHEN mncv.Account_Id IS NOT NULL AND mncv.Territory_Id IS NOT NULL
        THEN 1
            ELSE 0
                END AS flag_territory_belong,
    CASE WHEN mncvs.Account_Id IS NOT NULL AND mncvs.Territory_Id IS NOT NULL AND yearmonth IS NOT NULL
        THEN 1
            ELSE 0
                END AS historical_flag_territory_belong,
    t.date_received_by_ezreporter,
    t.ezreporter_ref,
    t.ae_external,
    t.pqc_external,
    t.report_type,
    t.answer_from_skm,
    NULL as channel_new        
     
FROM (
SELECT
    Account_Id,
    Country_Code_c as Country_Code,
    Brick_Code,
    Customer_Request_Number,
    Customer_Request_Code,
    Customer_Request_SKM_Time_Registration,
    Customer_Request_Question_to_SKM,
    Customer_Request_Closed_Date,
    "Date",
    Customer_Request_Date,
    Customer_Request_Start_Date,
    Customer_Request_Created_Timestamp,
    Customer_Request_Hours_To_Close,
    Customer_Request_Working_Hours_To_Close,
    Customer_Request_Id,
    Digital_Signature,
    Activity,
    Activity_Type,
    Customer_Request_Veeva_Link,
    Customer_Request_Related_Veeva_Link,
    Customer_Request_Closed,
    Customer_Request_Answer,
    Customer_Request_Approval_Status,
    Customer_Request_Category,
    Product_Id,
    Customer_Request_Reason,
    Customer_Request_Source_Out,
    Customer_Request_Subtype,
    Customer_Request_Subcategory,
    Customer_Request_Origin,
    Customer_Request_Assigned_To_Id,
    Customer_Request_CSC_Assigned_Flag,
    Customer_Request_Assigned_Employee,
    Customer_Request_Owner,
    Employee_Id,
    Customer_Request_Department,
    Customer_Request_Employee_Department,
    Customer_Request_Created_Employee,
    Customer_Request_Priority,
    Customer_Request_Off_On_Label,
    Customer_Request_Question,
    Customer_Request_Rejection_Reason,
    Customer_Request_Survey_Feedback_Id,
    Customer_Request_Record_Type,
    Customer_Request_Status,
    Customer_Request_Subject,
    Customer_Request_Description,
    Customer_Request_Cost,
    Customer_Request_Date_Assigned,
    Customer_Request_End_Date,
    Origin,
    Account_Visited_NonVisited_Technical,
    Sys_Compliance_Visibility,
    Compliance_Visibility,
    Customer_Request_PV_Reference_1,
    Customer_Request_PV_Reference_2,
    Customer_Request_QA_Reference,
    Customer_Request_Control_PV,
    Customer_Request_Control_QA,
    Customer_Request_Date_Awareness,
    Multichannel_Type,
    Multichannel_Activity,
    Multichannel_Activity_Type,
    Creator_Owner,
    Therapeutic_Area_Name,
    Therapeutic_Area_Brand_Name,
    Product_Target_Class_Key,
    Security_Key,
    Account_SCD_Id,
    Product_Metrics_SCD_Id,
    Assignment_Id,
    Region,
    Compliance_Region,
    Territory_Nominal_Id,
    Customer_Request_Threshold_Time,
    Customer_Request_Threshold_Working_Time,
    Country,
    Compliance_Country,
    Customer_Request_1st_2nd_Line,
    Customer_Request_Line_Of_Response,
    Customer_Request_Link_Id,
    parent_id,
    number_of_questions,
    customer_request_closed_timestamp,
    country_1,
    global_content_usage,
    date_received_by_ezreporter,
    ezreporter_ref,
    ae_external,
    pqc_external,
    report_type,
    answer_from_skm
FROM (
    SELECT
        C.AccountId AS "Account_Id",
        CASE
            WHEN C.AccountId = A.Id AND LEN(A.CODS_external_id__c)>0 THEN (CASE WHEN A.country_jj__c IS NULL THEN A.jj_country__c ELSE A.country_jj__c END)
            WHEN (U2.Id = CASE WHEN C.JJ_Non_CSC_Assignee__c <> '' THEN C.JJ_Non_CSC_Assignee__c ELSE C.OwnerId END) THEN CS2.jj_Country_iso_code__c
            WHEN C.AccountId = A.Id AND LEN(A.CODS_external_id__c)<1 THEN (CASE WHEN A.country_jj__c IS NULL THEN A.jj_country__c ELSE A.country_jj__c END)
            ELSE 'NM'
        END::varchar(255) AS "Country_Code_c",
        CASE
            WHEN LEN(A.Country_JJ__c)>0
                THEN A.Country_JJ__c
                  ELSE A.JJ_Country__c || '_' || CASE WHEN LEN(A.Brick_Number_JJ__c)>0 THEN A.Brick_Number_JJ__c ELSE 'No Value' END
        END::varchar(255) AS "Brick_Code",
        C.CaseNumber AS "Customer_Request_Number",
        C.JJ_Non_CSC_Request_Number__c AS "Customer_Request_Code",
        C.JJ_SKM_Time_Registration__c AS "Customer_Request_SKM_Time_Registration",
        C.JJ_SKM_comments__c AS "Customer_Request_Question_to_SKM",
        CASE
            WHEN C.ClosedDate = ''
                THEN ''
                ELSE TO_CHAR(TO_DATE(C.ClosedDate, 'YYYYMMDD HH24:MI:SS'), 'YYYYMMDD')
        END::varchar(255) AS "Customer_Request_Closed_Date",
        TO_CHAR(TO_DATE(C.CreatedDate, 'YYYYMMDD HH24:MI:SS'), 'YYYYMMDD')::varchar(255) AS "Date",
        TO_CHAR(TO_DATE(C.CreatedDate, 'YYYYMMDD HH24:MI:SS'), 'YYYYMMDD')::varchar(255) AS "Customer_Request_Date",
        CASE
            WHEN C.JJ_Start_Date__c = ''
                THEN ''
                ELSE TO_CHAR(TO_DATE(C.JJ_Start_Date__c, 'YYYYMMDD HH24:MI:SS'), 'YYYYMMDD')
        END::varchar(255) AS "Customer_Request_Start_Date",
        TO_CHAR(TO_TIMESTAMP(C.CreatedDate,'YYYYMMDD HH24:MI:SS'), 'YYYYMMDD HH24:MI:SS')::varchar(255) AS "Customer_Request_Created_Timestamp",
        CASE
            WHEN C.closeddate = ''
                THEN NULL
                ELSE (DATEDIFF(minute,C.CreatedDate::timestamp,C.ClosedDate::timestamp)/60.0)
        END::numeric(10,3) AS "Customer_Request_Hours_To_Close",
        CASE
            WHEN C.ClosedDate = '' OR C.CreatedDate > C.ClosedDate
                THEN NULL
                ELSE ((DATEDIFF(D,C.CreatedDate::timestamp,C.ClosedDate::timestamp) + 1) - (DATEDIFF(week, C.CreatedDate::timestamp,C.ClosedDate::timestamp)*2)
                - (CASE WHEN DATE_PART(dow,C.CreatedDate::timestamp) = '0' THEN 1 ELSE 0 END)
                - (CASE WHEN DATE_PART(dow,C.ClosedDate::timestamp) = '6' THEN 1 ELSE 0 END)) * 24
        END::NUMERIC(10,3) AS "Customer_Request_Working_Hours_To_Close",
        C.Id AS Customer_Request_Id,
        C.JJ_Signature_Date__c AS "Digital_Signature",
        'Customer Request'::varchar(255) AS "Activity",
        C.Origin AS "Activity_Type",
        ('https://iconnect-emea.my.salesforce.com/'|| C.Id)::varchar(255) AS "Customer_Request_Veeva_Link",
        ('https://iconnect-emea.my.salesforce.com/'|| C.JJ_Related_Customer_Request__c )::varchar(255) AS "Customer_Request_Related_Veeva_Link",
        CASE
             WHEN C.IsClosed IN ('true', '1')
                THEN 'yes'
            WHEN C.IsClosed IN ('false', '0')
                THEN 'no'
        END::varchar(255) AS "Customer_Request_Closed",
        C.JJ_Answer__c AS "Customer_Request_Answer",
        C.JJ_Approval_Status__c AS "Customer_Request_Approval_Status",
        CASE
            WHEN C.JJ_Category__c  <> ''
                THEN C.JJ_Category__c
                ELSE 'No Value'
        END::varchar(255) AS "Customer_Request_Category",
        C.JJ_Product__c AS "Product_Id",
        C.JJ_Reason__c  AS "Customer_Request_Reason",
        CASE
            WHEN C.JJ_Source_Out__c <> ''
                THEN C.JJ_Source_Out__c
                ELSE 'No Value'
        END::varchar(255) AS "Customer_Request_Source_Out",
        C.JJ_Sub_Type__c AS "Customer_Request_Subtype",
        C.JJ_Sub_category__c AS "Customer_Request_Subcategory",
        c.origin AS "Customer_Request_Origin",
        CASE
            WHEN C.JJ_Non_CSC_Assignee__c <> ''
                THEN C.JJ_Non_CSC_Assignee__c
                ELSE C.OwnerId
        END::varchar(255) AS "Customer_Request_Assigned_To_Id",
        CASE
            WHEN C.JJ_Non_CSC_Assignee__c <> ''
                THEN 'Yes'
                ELSE 'No'
        END::varchar(255) AS "Customer_Request_CSC_Assigned_Flag",
        U2.Name AS "Customer_Request_Assigned_Employee",
        U1.Name AS "Customer_Request_Owner",
        C.CreatedById AS "Employee_Id",
        P.Name AS "Customer_Request_Department",
        CASE
            WHEN (P1.Id = U.ProfileId AND ((CASE WHEN(c.JJ_Non_CSC_Assignee__c <> '') THEN c.JJ_Non_CSC_Assignee__c ELSE c.OwnerId END)= U.Id))
                THEN P1.Name
                ELSE NULL
        END AS "Customer_Request_Employee_Department",
        U.Id AS "Customer_Request_Created_Employee",
        C.Priority AS "Customer_Request_Priority",
        CASE
            WHEN C.JJ_Off_Label__c IN ('true' , '1')
                THEN 'yes'
            WHEN C.JJ_Off_Label__c IN ('false' , '0')
                THEN 'no'
        END::varchar(255) AS "Customer_Request_Off_On_Label",
        C.JJ_Question__c AS "Customer_Request_Question",
        C.JJ_Rejection_reason__c AS "Customer_Request_Rejection_Reason",
        (C.Id || '-' || C.AccountId)::varchar(255) AS "Customer_Request_Survey_Feedback_Id",
        R.Name AS   "Customer_Request_Record_Type",
        C.Status AS "Customer_Request_Status",
        C.Subject AS "Customer_Request_Subject",
        C.Description AS "Customer_Request_Description",
        C.JJ_CR_Cost__c AS "Customer_Request_Cost",
        TO_CHAR(TO_DATE(C.JJ_Date_Assigned__c, 'YYYYMMDD HH24:MI:SS'), 'YYYYMMDD')::varchar(255) AS "Customer_Request_Date_Assigned",
        CASE
            WHEN C.JJ_End_Date__C = ''
                THEN ''
                ELSE TO_CHAR(TO_DATE(C.JJ_End_Date__C, 'YYYYMMDD HH24:MI:SS'), 'YYYYMMDD')
        END::varchar(255) AS "Customer_Request_End_Date",
        'Customer Request'::varchar(255) AS "Origin",            
        ('Non Visited' || '_' || Country_Code_c)::varchar(255) AS "Account_Visited_NonVisited_Technical",    
        '1'::varchar(255) AS "Sys_Compliance_Visibility",
        'My Region'::varchar(255) AS "Compliance_Visibility",
        C.JJ_PV_Reference__c    AS "Customer_Request_PV_Reference_1",
        C.JJ_PV_Reference_2__c  AS "Customer_Request_PV_Reference_2",
        C.JJ_QA_Reference__c AS "Customer_Request_QA_Reference",
        C.JJ_Control_PV__c   AS "Customer_Request_Control_PV",
        C.jj_Control_QA__c   AS "Customer_Request_Control_QA",
        C.JJ_Date_of_Awareness_by_J_J__c AS "Customer_Request_Date_Awareness",
        CASE
            WHEN C.Origin NOT IN ('Internal','JAMIE','Web','Visit','9')
                THEN 'Non Face to Face Contacts'
                ELSE ''
        END::varchar(255) AS "Multichannel_Type",
        CASE
            WHEN C.Origin NOT IN ('Internal','JAMIE','Web','Visit','9')
                THEN 'Customer Request(' || C.Origin ||')'
                ELSE ''
        END::varchar(255) AS "Multichannel_Activity",
        'Null'::varchar(255) AS "Multichannel_Activity_Type",
        'Creator'::varchar(255) AS "Creator_Owner",
        CASE WHEN COALESCE(mp.product_therapeutic_area_1,'') = ''
            THEN 'NM' ELSE mp.product_therapeutic_area_1
        END AS "Therapeutic_Area_Name",
        CASE WHEN COALESCE(mp.product_brand_therapeutic_area_1,'') = ''
            THEN 'NM' ELSE mp.product_brand_therapeutic_area_1
        END AS "Therapeutic_Area_Brand_Name",
        CASE WHEN COALESCE(mp.product_detail_id,'') = ''
            THEN (C.AccountId || '-' || 'NM') ELSE (C.AccountId || '-' || mp.product_detail_id)
        END::varchar(255) AS "Product_Target_Class_Key",
        (C.AccountId || '-' || C.CreatedById)::varchar(255) AS "Security_Key",
        (C.AccountId || '-' || TO_CHAR(TO_DATE(C.CreatedDate, 'YYYYMMDD HH24:MI:SS'), 'YYYYMMDD'))::varchar(255) AS "Account_SCD_Id",
        (C.AccountId || '-' || Product_Id || '-' || TO_CHAR(TO_DATE(C.CreatedDate, 'YYYYMMDD HH24:MI:SS'), 'YYYYMMDD'))::varchar(255) AS "Product_Metrics_SCD_Id",
        CASE
            WHEN C.AccountId <> ''
                THEN C.AccountId
                ELSE C.CreatedById
        END::varchar(255) AS "Assignment_Id",
        REPLACE(REPLACE(jC.jj_Region__c, ' ', ''), '/', '-') AS "Region",
        REPLACE(REPLACE(jC.jj_Region__c, ' ', ''), '/', '-') AS "Compliance_Region",
        ut.TerritoryId AS "Territory_Nominal_Id",
        CASE
            WHEN Customer_Request_Hours_To_Close IS NULL THEN ''
            WHEN Customer_Request_Status = 'Closed'
            THEN (CASE
                WHEN Customer_Request_Hours_To_Close <= '24' THEN '<= 1 day'
                WHEN Customer_Request_Hours_To_Close <= '48' THEN '2 days'
                WHEN Customer_Request_Hours_To_Close <= '120' THEN '3-5 days'
                WHEN Customer_Request_Hours_To_Close <= '240' THEN '6-10 days'
                ELSE '>10 days' END)
            ELSE ''
        END::varchar(255) AS "Customer_Request_Threshold_Time",
        CASE
            WHEN Customer_Request_Hours_To_Close IS NULL OR Customer_Request_Working_Hours_To_Close IS NULL THEN ''
            WHEN Customer_Request_Hours_To_Close > Customer_Request_Working_Hours_To_Close
                THEN (CASE WHEN Customer_Request_Status = 'Closed'
                    THEN (CASE
                        WHEN Customer_Request_Working_Hours_To_Close <= '24' THEN '<= 1 day'
                        WHEN Customer_Request_Working_Hours_To_Close <= '48' THEN '1-2 days'
                        WHEN Customer_Request_Working_Hours_To_Close <= '120' THEN '3-5 days'
                        WHEN Customer_Request_Working_Hours_To_Close <= '240' THEN '6-10 days' ELSE '>10 days' END)
                    ELSE ''
                    END)
                ELSE (CASE WHEN Customer_Request_Status = 'Closed'
                    THEN (CASE
                        WHEN Customer_Request_Hours_To_Close <= '24' THEN '<= 1 day'
                        WHEN Customer_Request_Hours_To_Close <= '48' THEN '1-2 days'
                        WHEN Customer_Request_Hours_To_Close <= '120' THEN '3-5 days'
                        WHEN Customer_Request_Hours_To_Close <= '240' THEN '6-10 days' ELSE '>10 days' END)
                    ELSE ''
                    END)
        END::varchar(255) AS "Customer_Request_Threshold_Working_Time",
        jC.Name AS "Country",
        jC.Name AS "Compliance_Country",
        CASE
            WHEN Customer_Request_SKM_Time_Registration > 0
                THEN 'Third Line'
            WHEN Customer_Request_Department = 'EMEA_iConnect_CustomerServices'
                THEN 'First Line'
                ELSE 'Second Line'
        END::varchar(255) AS "Customer_Request_1st_2nd_Line",
        CASE
            WHEN Customer_Request_SKM_Time_Registration > 0
                THEN 'SKM'
            WHEN Customer_Request_Department = 'EMEA_iConnect_CustomerServices'
                THEN 'Customer Services'
                ELSE 'Non Customer Services'
        END::varchar(255) AS "Customer_Request_Line_Of_Response",
        (Customer_Request_Id || '-' || C.CreatedById)::varchar(255) AS "Customer_Request_Link_Id",
        C.PARENTID AS parent_id,
        C.JJ_NUMBER_OF_QUESTIONS__C AS number_of_questions,
        CASE
            WHEN C.ClosedDate = ''
                THEN ''
                ELSE TO_CHAR(TO_TIMESTAMP(C.ClosedDate,'YYYYMMDD HH24:MI:SS'), 'YYYYMMDD HH24:MI:SS')::varchar(255)
        END::varchar(255) AS customer_request_closed_timestamp,
        C.JJ_Country_1__c AS country_1,
        C.jj_sending_out_global_content__c AS global_content_usage,
        case when C.JJ_Date_Received_by_EZReporter__c = '' then null else TO_TIMESTAMP(C.JJ_Date_Received_by_EZReporter__c,'YYYYMMDD HH24:MI:SS') end as date_received_by_ezreporter,
        C.JJ_EZReporter_Ref__c::varchar(510) as ezreporter_ref,
        case when C.JJ_AE_EXTERNAL__c = '' then null else C.JJ_AE_EXTERNAL__c end::integer as ae_external,
        case when C.JJ_PQC_EXTERNAL__c = '' then null else C.JJ_PQC_EXTERNAL__c end::integer as pqc_external,
        C.JJ_REPORT_TYPE__c::varchar(510) as report_type,
        CASE WHEN (Position(CHR(92) || CHR(92), C.JJ_ANSWER_FROM_SKM__C) > 0) OR Position(CHR(124) || CHR(34), C.JJ_ANSWER_FROM_SKM__C) > 0 OR Position(CHR(92) || CHR(124), C.JJ_ANSWER_FROM_SKM__C) > 0 OR Position(CHR(92) || CHR(34), C.JJ_ANSWER_FROM_SKM__C) > 0
            THEN REPLACE(REPLACE(REPLACE(REPLACE(C.JJ_ANSWER_FROM_SKM__C, CHR(92) || CHR(92), CHR(92)), CHR(124) || CHR(34), CHR(34)), CHR(92) || CHR(124), CHR(124)), CHR(92) || CHR(34), CHR(34))
            ELSE C.JJ_ANSWER_FROM_SKM__C
        END::varchar(10000) AS answer_from_skm
    FROM {{ var('schema') }}.case_raw C
    LEFT OUTER JOIN {{ var('schema') }}.account_raw A ON C.AccountId = A.Id
    LEFT OUTER JOIN {{ var('schema') }}.user_raw U ON C.CreatedById = U.Id
    LEFT OUTER JOIN {{ var('schema') }}.user_raw U1 on C.OwnerId = U1.Id
    LEFT OUTER JOIN {{ var('schema') }}.user_raw U2 ON U2.Id =
    CASE
        WHEN C.JJ_Non_CSC_Assignee__c <> ''
            THEN C.JJ_Non_CSC_Assignee__c
            ELSE C.OwnerId
    END
    LEFT OUTER JOIN {{ var('schema') }}.profile_raw P ON U1.ProfileId = P.Id AND C.OwnerId = U1.Id
    LEFT OUTER JOIN {{ var('schema') }}.profile_raw P1 ON P1.Id = U.ProfileId
    LEFT OUTER JOIN {{ var('schema') }}.record_type_raw R ON C.RecordTypeId = R.Id
    LEFT OUTER JOIN {{ var('schema') }}.country_settings_raw jC ON jC.jj_Country_ISO_Code__c = C.country_iso_code
    LEFT OUTER JOIN {{ var('schema') }}.country_settings_raw cs ON cs.name = U.JJ_user_country__c
    LEFT OUTER JOIN {{ var('schema') }}.country_settings_raw cs2 ON cs2.name = U2.JJ_user_country__c
    LEFT OUTER JOIN {{ var('schema') }}.product_raw pv ON pv.id = C.JJ_Product__c
    LEFT OUTER JOIN {{ ref('tmp_user_territory') }} ut ON C.CreatedById= ut.USERID
    LEFT OUTER JOIN {{ ref('m_product') }} mp ON pv.id = mp.product_id
)
GROUP BY
    Account_Id,
    Country_Code,
    Brick_Code,
    Customer_Request_Number,
    Customer_Request_Code,
    Customer_Request_SKM_Time_Registration,
    Customer_Request_Question_to_SKM,
    Customer_Request_Closed_Date,
    "Date",
    Customer_Request_Date,
    Customer_Request_Start_Date,
    Customer_Request_Created_Timestamp,
    Customer_Request_Hours_To_Close,
    Customer_Request_Working_Hours_To_Close,
    Customer_Request_Id,
    Digital_Signature,
    Activity,
    Activity_Type,
    Customer_Request_Veeva_Link,
    Customer_Request_Related_Veeva_Link,
    Customer_Request_Closed,
    Customer_Request_Answer,
    Customer_Request_Approval_Status,
    Customer_Request_Category,
    Product_Id,
    Customer_Request_Reason,
    Customer_Request_Source_Out,
    Customer_Request_Subtype,
    Customer_Request_Subcategory,
    Customer_Request_Origin,
    Customer_Request_Assigned_To_Id,
    Customer_Request_CSC_Assigned_Flag,
    Customer_Request_Assigned_Employee,
    Customer_Request_Owner,
    Employee_Id,
    Customer_Request_Department,
    Customer_Request_Employee_Department,
    Customer_Request_Created_Employee,
    Customer_Request_Priority,
    Customer_Request_Off_On_Label,
    Customer_Request_Question,
    Customer_Request_Rejection_Reason,
    Customer_Request_Survey_Feedback_Id,
    Customer_Request_Record_Type,
    Customer_Request_Status,
    Customer_Request_Subject,
    Customer_Request_Description,
    Customer_Request_Cost,
    Customer_Request_Date_Assigned,
    Customer_Request_End_Date,
    Origin,
    Account_Visited_NonVisited_Technical,
    Sys_Compliance_Visibility,
    Compliance_Visibility,
    Customer_Request_PV_Reference_1,
    Customer_Request_PV_Reference_2,
    Customer_Request_QA_Reference,
    Customer_Request_Control_PV,
    Customer_Request_Control_QA,
    Customer_Request_Date_Awareness,
    Multichannel_Type,
    Multichannel_Activity,
    Multichannel_Activity_Type,
    Creator_Owner,
    Therapeutic_Area_Name,
    Therapeutic_Area_Brand_Name,
    Product_Target_Class_Key,
    Security_Key,
    Account_SCD_Id,
    Product_Metrics_SCD_Id,
    Assignment_Id,
    Region,
    Compliance_Region,
    Territory_Nominal_Id,
    Customer_Request_Threshold_Time,
    Customer_Request_Threshold_Working_Time,
    Country,
    Compliance_Country,
    Customer_Request_1st_2nd_Line,
    Customer_Request_Line_Of_Response,
    Customer_Request_Link_Id,
    parent_id,
    number_of_questions,
    customer_request_closed_timestamp,
    country_1,
    global_content_usage,
    date_received_by_ezreporter,
    ezreporter_ref,
    ae_external,
    pqc_external,
    report_type,
    answer_from_skm
 
UNION
  
SELECT
    Account_Id,
    Country_Code_c as Country_Code,
    Brick_Code,
    Customer_Request_Number,
    Customer_Request_Code,
    Customer_Request_SKM_Time_Registration,
    Customer_Request_Question_to_SKM,
    Customer_Request_Closed_Date,
    "Date",
    Customer_Request_Date,
    Customer_Request_Start_Date,
    Customer_Request_Created_Timestamp,
    Customer_Request_Hours_To_Close,
    Customer_Request_Working_Hours_To_Close,
    Customer_Request_Id,
    Digital_Signature,
    Activity,
    Activity_Type,
    Customer_Request_Veeva_Link,
    Customer_Request_Related_Veeva_Link,
    Customer_Request_Closed,
    Customer_Request_Answer,
    Customer_Request_Approval_Status,
    Customer_Request_Category,
    Product_Id,
    Customer_Request_Reason,
    Customer_Request_Source_Out,
    Customer_Request_Subtype,
    Customer_Request_Subcategory,
    Customer_Request_Origin,
    Customer_Request_Assigned_To_Id,
    Customer_Request_CSC_Assigned_Flag,
    Customer_Request_Assigned_Employee,
    Customer_Request_Owner,
    Employee_Id_c as Employee_Id,
    Customer_Request_Department,
    Customer_Request_Employee_Department,
    Customer_Request_Created_Employee,
    Customer_Request_Priority,
    Customer_Request_Off_On_Label,
    Customer_Request_Question,
    Customer_Request_Rejection_Reason,
    Customer_Request_Survey_Feedback_Id,
    Customer_Request_Record_Type,
    Customer_Request_Status,
    Customer_Request_Subject,
    Customer_Request_Description,
    Customer_Request_Cost,
    Customer_Request_Date_Assigned,
    Customer_Request_End_Date,
    Origin,
    Account_Visited_NonVisited_Technical,
    Sys_Compliance_Visibility,
    Compliance_Visibility,
    Customer_Request_PV_Reference_1,
    Customer_Request_PV_Reference_2,
    Customer_Request_QA_Reference,
    Customer_Request_Control_PV,
    Customer_Request_Control_QA,
    Customer_Request_Date_Awareness,
    Multichannel_Type,
    Multichannel_Activity,
    Multichannel_Activity_Type,
    Creator_Owner,
    Therapeutic_Area_Name,
    Therapeutic_Area_Brand_Name,
    Product_Target_Class_Key,
    Security_Key,
    Account_SCD_Id,
    Product_Metrics_SCD_Id,
    Assignment_Id,
    Region,
    Compliance_Region,
    Territory_Nominal_Id,
    Customer_Request_Threshold_Time,
    Customer_Request_Threshold_Working_Time,
    Country,
    Compliance_Country,
    Customer_Request_1st_2nd_Line,
    Customer_Request_Line_Of_Response,
    Customer_Request_Link_Id,
    parent_id,
    number_of_questions,
    customer_request_closed_timestamp,
    country_1,
    global_content_usage,
    date_received_by_ezreporter,
    ezreporter_ref,
    ae_external,
    pqc_external,
    report_type,
    answer_from_skm
 
FROM (
    SELECT
        C.AccountId  AS "Account_Id",
        CASE
            WHEN C.AccountId = A.Id AND LEN(A.CODS_external_id__c)>0 THEN (CASE WHEN A.country_jj__c IS NULL THEN A.jj_country__c ELSE A.country_jj__c END)
            WHEN (U2.Id = CASE WHEN C.JJ_Non_CSC_Assignee__c <> '' THEN C.JJ_Non_CSC_Assignee__c ELSE C.OwnerId END) THEN CS2.jj_Country_iso_code__c
            WHEN C.AccountId = A.Id AND LEN(A.CODS_external_id__c)<1 THEN (CASE WHEN A.country_jj__c IS NULL THEN A.jj_country__c ELSE A.country_jj__c END)
            ELSE 'NM'
        END::varchar(255) AS "Country_Code_c",
        CASE
            WHEN LEN(A.Country_JJ__c)>0
                THEN A.Country_JJ__c
                  ELSE A.JJ_Country__c || '_' || CASE WHEN LEN(A.Brick_Number_JJ__c)>0 THEN A.Brick_Number_JJ__c ELSE 'No Value' END
        END::varchar(255) AS "Brick_Code",
        C.CaseNumber AS "Customer_Request_Number",
        C.JJ_Non_CSC_Request_Number__c AS "Customer_Request_Code",
        C.JJ_SKM_Time_Registration__c AS "Customer_Request_SKM_Time_Registration",
        C.JJ_SKM_comments__c AS "Customer_Request_Question_to_SKM",
        CASE
            WHEN C.ClosedDate = ''
                THEN ''
                ELSE TO_CHAR(TO_DATE(C.ClosedDate, 'YYYYMMDD HH24:MI:SS'), 'YYYYMMDD')
        END::varchar(255) AS "Customer_Request_Closed_Date",
        TO_CHAR(TO_DATE(C.CreatedDate, 'YYYYMMDD HH24:MI:SS'), 'YYYYMMDD')::varchar(255) AS "Date",
        TO_CHAR(TO_DATE(C.CreatedDate, 'YYYYMMDD HH24:MI:SS'), 'YYYYMMDD')::varchar(255) AS "Customer_Request_Date",
        CASE
            WHEN C.JJ_Start_Date__c = ''
                THEN ''
                ELSE TO_CHAR(TO_DATE(C.JJ_Start_Date__c, 'YYYYMMDD HH24:MI:SS'), 'YYYYMMDD')
        END::varchar(255) AS "Customer_Request_Start_Date",
        TO_CHAR(TO_TIMESTAMP(C.CreatedDate,'YYYYMMDD HH24:MI:SS'), 'YYYYMMDD HH24:MI:SS')::varchar(255) AS "Customer_Request_Created_Timestamp",
        CASE
            WHEN C.closeddate = ''
                THEN NULL
                ELSE (DATEDIFF(minute,C.CreatedDate::timestamp,C.ClosedDate::timestamp)/60.0)
        END::numeric(10,3) AS "Customer_Request_Hours_To_Close",
        CASE
            WHEN C.ClosedDate = '' OR C.CreatedDate > C.ClosedDate
                THEN NULL
                ELSE ((DATEDIFF(D,C.CreatedDate::timestamp,C.ClosedDate::timestamp) + 1) - (DATEDIFF(week, C.CreatedDate::timestamp,C.ClosedDate::timestamp)*2)
                    - (CASE WHEN DATE_PART(dow,C.CreatedDate::timestamp) = '0' THEN 1 ELSE 0 END)
                    - (CASE WHEN DATE_PART(dow,C.ClosedDate::timestamp) = '6' THEN 1 ELSE 0 END)) * 24
        END::NUMERIC(10,3) AS "Customer_Request_Working_Hours_To_Close",
        C.Id AS "Customer_Request_Id",
        C.JJ_Signature_Date__c AS "Digital_Signature",
        'Customer Request'::varchar(255) AS "Activity",
        C.Origin AS "Activity_Type",
        ('https://iconnect-emea.my.salesforce.com/'|| C.Id)::varchar(255) AS "Customer_Request_Veeva_Link",
        ('https://iconnect-emea.my.salesforce.com/'|| C.JJ_Related_Customer_Request__c )::varchar(255) AS "Customer_Request_Related_Veeva_Link",
        CASE
             WHEN C.IsClosed IN ('true', '1')
                THEN 'yes'
            WHEN C.IsClosed IN ('false', '0')
                THEN 'no'
        END::varchar(255) AS "Customer_Request_Closed",
        C.JJ_Answer__c AS "Customer_Request_Answer",
        C.JJ_Approval_Status__c AS "Customer_Request_Approval_Status",
        CASE
            WHEN C.JJ_Category__c  <> ''
                THEN C.JJ_Category__c
                ELSE 'No Value'
        END::varchar(255) AS "Customer_Request_Category",
        C.JJ_Product__c AS "Product_Id",
        C.JJ_Reason__c  AS "Customer_Request_Reason",
        CASE
            WHEN C.JJ_Source_Out__c <> ''
                THEN C.JJ_Source_Out__c
                ELSE 'No Value'
        END::varchar(255) AS "Customer_Request_Source_Out",
        C.JJ_Sub_Type__c AS "Customer_Request_Subtype",
        C.JJ_Sub_category__c AS "Customer_Request_Subcategory",
        c.origin AS "Customer_Request_Origin",
        CASE
            WHEN C.JJ_Non_CSC_Assignee__c <> ''
                THEN C.JJ_Non_CSC_Assignee__c
                ELSE C.OwnerId
        END::varchar(255) AS "Customer_Request_Assigned_To_Id",
        CASE
            WHEN C.JJ_Non_CSC_Assignee__c <> ''
                THEN 'Yes'
                ELSE 'No'
        END::varchar(255) AS "Customer_Request_CSC_Assigned_Flag",
        U2.Name AS "Customer_Request_Assigned_Employee",
        U1.Name AS "Customer_Request_Owner",
        CASE
            WHEN C.JJ_Non_CSC_Assignee__c <> ''
                THEN C.JJ_Non_CSC_Assignee__c
                ELSE C.OwnerId
        END::varchar(255) AS "Employee_Id_c",
        P.Name AS "Customer_Request_Department",
        CASE
            WHEN (P1.Id = U.ProfileId AND ((CASE WHEN(c.JJ_Non_CSC_Assignee__c <> '') THEN c.JJ_Non_CSC_Assignee__c ELSE c.OwnerId END)= U.Id))
                THEN P1.Name
                ELSE NULL
        END AS "Customer_Request_Employee_Department",
        U.Id AS "Customer_Request_Created_Employee",
        C.Priority AS "Customer_Request_Priority",
        CASE
            WHEN C.JJ_Off_Label__c IN ('true' , '1')
                THEN 'yes'
            WHEN C.JJ_Off_Label__c IN ('false' , '0')
                THEN 'no'
        END::varchar(255) AS "Customer_Request_Off_On_Label",
        C.JJ_Question__c AS "Customer_Request_Question",
        C.JJ_Rejection_reason__c AS "Customer_Request_Rejection_Reason",
        C.Id || '-' || C.AccountId AS "Customer_Request_Survey_Feedback_Id",
        R.Name AS   "Customer_Request_Record_Type",
        C.Status  AS "Customer_Request_Status",
        C.Subject AS "Customer_Request_Subject",
        C.Description       AS "Customer_Request_Description",
        C.JJ_CR_Cost__c     AS "Customer_Request_Cost",
        TO_CHAR(TO_DATE(C.JJ_Date_Assigned__c, 'YYYYMMDD HH24:MI:SS'), 'YYYYMMDD')::varchar(255)    AS "Customer_Request_Date_Assigned",
        CASE
            WHEN C.JJ_End_Date__C = ''
                THEN ''
                ELSE TO_CHAR(TO_DATE(C.JJ_End_Date__C, 'YYYYMMDD HH24:MI:SS'), 'YYYYMMDD')
        END::varchar(255) AS "Customer_Request_End_Date",
        'Customer Request'::varchar(255) AS "Origin",
        ('Non Visited' || '_' || Country_Code_c)::varchar(255) AS "Account_Visited_NonVisited_Technical",
        '1'::varchar(255) AS "Sys_Compliance_Visibility",
        'My Region'::varchar(255) AS "Compliance_Visibility",
        C.JJ_PV_Reference__c     AS "Customer_Request_PV_Reference_1",
        C.JJ_PV_Reference_2__c   AS "Customer_Request_PV_Reference_2",
        C.JJ_QA_Reference__c     AS "Customer_Request_QA_Reference",
        C.JJ_Control_PV__c   AS "Customer_Request_Control_PV",
        C.jj_Control_QA__c   AS "Customer_Request_Control_QA",
        C.JJ_Date_of_Awareness_by_J_J__c AS "Customer_Request_Date_Awareness",
        CASE
            WHEN C.Origin NOT IN ('Internal','JAMIE','Web','Visit','9')
                THEN 'Non Face to Face Contacts'
                ELSE ''
        END::varchar(255) AS "Multichannel_Type",
        CASE
            WHEN C.Origin NOT IN ('Internal','JAMIE','Web','Visit','9')
                THEN 'Customer Request(' || C.Origin ||')'
                ELSE ''
        END::varchar(255) AS "Multichannel_Activity",
        'Null'::varchar(255) AS "Multichannel_Activity_Type",
        'Owner'::varchar(255) AS "Creator_Owner",
        CASE WHEN COALESCE(mp.product_therapeutic_area_1,'') = ''
            THEN 'NM' ELSE mp.product_therapeutic_area_1
        END AS "Therapeutic_Area_Name",
        CASE WHEN COALESCE(mp.product_brand_therapeutic_area_1,'') = ''
            THEN 'NM' ELSE mp.product_brand_therapeutic_area_1
        END AS "Therapeutic_Area_Brand_Name",
        CASE WHEN COALESCE(mp.product_detail_id,'') = ''
            THEN (C.AccountId || '-' || 'NM') ELSE (C.AccountId || '-' || mp.product_detail_id)
        END::varchar(255) AS "Product_Target_Class_Key",
        (C.AccountId || '-' || Employee_Id_c)::varchar(255) AS "Security_Key",
        (C.AccountId || '-' || TO_CHAR(TO_DATE(C.CreatedDate, 'YYYYMMDD HH24:MI:SS'), 'YYYYMMDD'))::varchar(255) AS "Account_SCD_Id",
        (C.AccountId || '-' || Product_Id || '-' || TO_CHAR(TO_DATE(C.CreatedDate, 'YYYYMMDD HH24:MI:SS'), 'YYYYMMDD'))::varchar(255) AS "Product_Metrics_SCD_Id",
        CASE
            WHEN C.AccountId <> ''
                THEN C.AccountId
                ELSE Employee_Id_c
        END::varchar(255) AS "Assignment_Id",
        REPLACE(REPLACE(jC.jj_Region__c, ' ', ''), '/', '-') AS "Region",
        REPLACE(REPLACE(jC.jj_Region__c, ' ', ''), '/', '-') AS "Compliance_Region",
        ut.TerritoryId AS "Territory_Nominal_Id",
        CASE
            WHEN Customer_Request_Hours_To_Close IS NULL THEN ''
            WHEN Customer_Request_Status = 'Closed'
            THEN (CASE
                WHEN Customer_Request_Hours_To_Close <= '24' THEN '<= 1 day'
                WHEN Customer_Request_Hours_To_Close <= '48' THEN '2 days'
                WHEN Customer_Request_Hours_To_Close <= '120' THEN '3-5 days'
                WHEN Customer_Request_Hours_To_Close <= '240' THEN '6-10 days'
                ELSE '>10 days' END)
            ELSE ''
        END::varchar(255) AS "Customer_Request_Threshold_Time",
        CASE
            WHEN Customer_Request_Hours_To_Close IS NULL OR Customer_Request_Working_Hours_To_Close IS NULL THEN ''
            WHEN Customer_Request_Hours_To_Close > Customer_Request_Working_Hours_To_Close
                THEN (CASE WHEN Customer_Request_Status = 'Closed'
                    THEN (CASE
                        WHEN Customer_Request_Working_Hours_To_Close <= '24' THEN '<= 1 day'
                        WHEN Customer_Request_Working_Hours_To_Close <= '48' THEN '1-2 days'
                        WHEN Customer_Request_Working_Hours_To_Close <= '120' THEN '3-5 days'
                        WHEN Customer_Request_Working_Hours_To_Close <= '240' THEN '6-10 days' ELSE '>10 days' END)
                    ELSE ''
                    END)
                ELSE (CASE WHEN Customer_Request_Status = 'Closed'
                    THEN (CASE
                        WHEN Customer_Request_Hours_To_Close <= '24' THEN '<= 1 day'
                        WHEN Customer_Request_Hours_To_Close <= '48' THEN '1-2 days'
                        WHEN Customer_Request_Hours_To_Close <= '120' THEN '3-5 days'
                        WHEN Customer_Request_Hours_To_Close <= '240' THEN '6-10 days' ELSE '>10 days' END)
                    ELSE ''
                    END)
        END::varchar(255) AS "Customer_Request_Threshold_Working_Time",
        jC.Name AS "Country",
        jC.Name AS "Compliance_Country",
        CASE
            WHEN Customer_Request_SKM_Time_Registration > 0
                THEN 'Third Line'
            WHEN Customer_Request_Department = 'EMEA_iConnect_CustomerServices'
                THEN 'First Line'
                ELSE 'Second Line'
        END::varchar(255) AS "Customer_Request_1st_2nd_Line",
        CASE
            WHEN Customer_Request_SKM_Time_Registration > 0
                THEN 'SKM'
            WHEN Customer_Request_Department = 'EMEA_iConnect_CustomerServices'
                THEN 'Customer Services'
                ELSE 'Non Customer Services'
        END::varchar(255) AS "Customer_Request_Line_Of_Response",
        (Customer_Request_Id || '-' || Employee_Id_c)::varchar(255) AS "Customer_Request_Link_Id",
        C.PARENTID AS parent_id,
        C.JJ_NUMBER_OF_QUESTIONS__C AS number_of_questions,
        CASE
            WHEN C.ClosedDate = ''
                THEN ''
                ELSE TO_CHAR(TO_TIMESTAMP(C.ClosedDate,'YYYYMMDD HH24:MI:SS'), 'YYYYMMDD HH24:MI:SS')::varchar(255)
        END::varchar(255) AS customer_request_closed_timestamp,
        C.JJ_Country_1__c AS country_1,
        C.JJ_sending_out_global_content__c AS global_content_usage,
        case when C.JJ_Date_Received_by_EZReporter__c = '' then null else TO_TIMESTAMP(C.JJ_Date_Received_by_EZReporter__c,'YYYYMMDD HH24:MI:SS') end as date_received_by_ezreporter,
        C.JJ_EZReporter_Ref__c::varchar(510) as ezreporter_ref,
        case when C.JJ_AE_EXTERNAL__c = '' then null else C.JJ_AE_EXTERNAL__c end::integer as ae_external,
        case when C.JJ_PQC_EXTERNAL__c = '' then null else C.JJ_PQC_EXTERNAL__c end::integer as pqc_external,
        C.JJ_REPORT_TYPE__c::varchar(510) as report_type,
        CASE WHEN (Position(CHR(92) || CHR(92), C.JJ_ANSWER_FROM_SKM__C) > 0) OR Position(CHR(124) || CHR(34), C.JJ_ANSWER_FROM_SKM__C) > 0 OR Position(CHR(92) || CHR(124), C.JJ_ANSWER_FROM_SKM__C) > 0 OR Position(CHR(92) || CHR(34), C.JJ_ANSWER_FROM_SKM__C) > 0
            THEN REPLACE(REPLACE(REPLACE(REPLACE(C.JJ_ANSWER_FROM_SKM__C, CHR(92) || CHR(92), CHR(92)), CHR(124) || CHR(34), CHR(34)), CHR(92) || CHR(124), CHR(124)), CHR(92) || CHR(34), CHR(34))
            ELSE C.JJ_ANSWER_FROM_SKM__C
        END::varchar(10000) AS answer_from_skm
         
    FROM {{ var('schema') }}.case_raw C
    LEFT OUTER JOIN {{ var('schema') }}.account_raw A ON C.AccountId = A.Id
    LEFT OUTER JOIN {{ var('schema') }}.user_raw U ON C.CreatedById = U.Id
    LEFT OUTER JOIN {{ var('schema') }}.user_raw U1 on C.OwnerId = U1.Id
    LEFT OUTER JOIN {{ var('schema') }}.user_raw U2 ON U2.Id =
    CASE
        WHEN C.JJ_Non_CSC_Assignee__c <> ''
            THEN C.JJ_Non_CSC_Assignee__c
            ELSE C.OwnerId
    END
    LEFT OUTER JOIN {{ var('schema') }}.profile_raw P ON U1.Profileid = P.Id
    LEFT OUTER JOIN {{ var('schema') }}.profile_raw P1 ON P1.Id = U.ProfileId
    LEFT OUTER JOIN {{ var('schema') }}.record_type_raw R ON C.RecordTypeId = R.Id
    LEFT OUTER JOIN {{ var('schema') }}.country_settings_raw jC ON jC.jj_Country_ISO_Code__c = C.country_iso_code
    LEFT OUTER JOIN {{ var('schema') }}.country_settings_raw cs ON cs.name = U.jj_user_country__c
    LEFT OUTER JOIN {{ var('schema') }}.country_settings_raw cs2 ON cs2.name = U2.jj_user_country__c
    LEFT OUTER JOIN {{ var('schema') }}.product_raw pv ON pv.id = C.JJ_Product__c
    LEFT OUTER JOIN {{ ref('tmp_user_territory') }} ut ON C.CreatedById= ut.USERID
    LEFT OUTER JOIN {{ ref('m_product') }} mp ON pv.id = mp.product_id
)
GROUP BY
    Account_Id,
    Country_Code,
    Brick_Code,
    Customer_Request_Number,
    Customer_Request_Code,
    Customer_Request_SKM_Time_Registration,
    Customer_Request_Question_to_SKM,
    Customer_Request_Closed_Date,
    "Date",
    Customer_Request_Date,
    Customer_Request_Start_Date,
    Customer_Request_Created_Timestamp,
    Customer_Request_Hours_To_Close,
    Customer_Request_Working_Hours_To_Close,
    Customer_Request_Id,
    Digital_Signature,
    Activity,
    Activity_Type,
    Customer_Request_Veeva_Link,
    Customer_Request_Related_Veeva_Link,
    Customer_Request_Closed,
    Customer_Request_Answer,
    Customer_Request_Approval_Status,
    Customer_Request_Category,
    Product_Id,
    Customer_Request_Reason,
    Customer_Request_Source_Out,
    Customer_Request_Subtype,
    Customer_Request_Subcategory,
    Customer_Request_Origin,
    Customer_Request_Assigned_To_Id,
    Customer_Request_CSC_Assigned_Flag,
    Customer_Request_Assigned_Employee,
    Customer_Request_Owner,
    Employee_Id,
    Customer_Request_Department,
    Customer_Request_Employee_Department,
    Customer_Request_Created_Employee,
    Customer_Request_Priority,
    Customer_Request_Off_On_Label,
    Customer_Request_Question,
    Customer_Request_Rejection_Reason,
    Customer_Request_Survey_Feedback_Id,
    Customer_Request_Record_Type,
    Customer_Request_Status,
    Customer_Request_Subject,
    Customer_Request_Description,
    Customer_Request_Cost,
    Customer_Request_Date_Assigned,
    Customer_Request_End_Date,
    Origin,
    Account_Visited_NonVisited_Technical,
    Sys_Compliance_Visibility,
    Compliance_Visibility,
    Customer_Request_PV_Reference_1,
    Customer_Request_PV_Reference_2,
    Customer_Request_QA_Reference,
    Customer_Request_Control_PV,
    Customer_Request_Control_QA,
    Customer_Request_Date_Awareness,
    Multichannel_Type,
    Multichannel_Activity,
    Multichannel_Activity_Type,
    Creator_Owner,
    Therapeutic_Area_Name,
    Therapeutic_Area_Brand_Name,
    Product_Target_Class_Key,
    Security_Key,
    Account_SCD_Id,
    Product_Metrics_SCD_Id,
    Assignment_Id,
    Region,
    Compliance_Region,
    Territory_Nominal_Id,
    Customer_Request_Threshold_Time,
    Customer_Request_Threshold_Working_Time,
    Country,
    Compliance_Country,
    Customer_Request_1st_2nd_Line,
    Customer_Request_Line_Of_Response,
    Customer_Request_Link_Id,
    parent_id,
    number_of_questions,
    customer_request_closed_timestamp,
    country_1,
    global_content_usage,
    date_received_by_ezreporter,
    ezreporter_ref,
    ae_external,
    pqc_external,
    report_type,
    answer_from_skm
)t
LEFT JOIN (SELECT Account_Id, Territory_Id FROM {{ ref('m_null_country_values') }}
            GROUP BY Account_Id, Territory_Id
          ) mncv
    ON t.Account_Id = mncv.Account_Id AND t.Territory_Nominal_Id = mncv.Territory_Id
LEFT JOIN (SELECT Account_Id, Territory_Id, yearmonth FROM {{ var('schema') }}.buw_alignment_m_null_country_values_snapshot_monthly_historical
            GROUP BY Account_Id, Territory_Id, yearmonth
          ) mncvs
    ON t.Account_Id = mncvs.Account_Id AND t.Territory_Nominal_Id = mncvs.Territory_Id AND LEFT(TO_CHAR(TO_DATE(t."Date", 'YYYYMMDD HH24:MI:SS'), 'YYYYMMDD')::varchar(255),6) = mncvs.yearmonth