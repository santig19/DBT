{{ config(
	post_hook="grant all privileges on {{ this }} to {{ var('snowflake_user_grant_privileges') }}")
}}

SELECT DISTINCT
    B.Account_Id
  , B.Account_SCD_Id
  , B.Account_Visited_NonVisited_Technical
  , B.Assignment_Id
  , B.Brick_Code
  , B.Country
  , B.Country_Code
  , B.Cycle_Plan_Actual_Calls
  , B.Cycle_Plan_Attainment
  , B.Cycle_Plan_Coverage_Calls
  , B.Cycle_Plan_Date::varchar(255) as Cycle_Plan_Date
  , B.Cycle_Plan_Id
  , B.Cycle_Plan_Global_Id
  , B.Cycle_Plan_Last_Modified_By
  , 'Detail'::varchar(255) as Cycle_Plan_Origin
  , null::varchar(255) as Cycle_Plan_Original_Calls
  , B.Cycle_Plan_Planned_Calls
  , null::varchar(255) as Cycle_Plan_POA_Fulfilled
  , B.Cycle_Plan_Remaining_Calls
  , B.Cycle_Plan_Scheduled_Calls
  , B.Cycle_Plan_Total_Actual_Calls
  , B.Cycle_Plan_Total_Attainment
  , B.Cycle_Plan_Total_Coverage_Calls
  , B.Cycle_Plan_Total_Coverage_Calls_Percentage
  , B.Cycle_Plan_Total_Planned_Calls
  , B.Cycle_Plan_Total_Remaining_Calls
  , B.Cycle_Plan_Total_Scheduled_Calls
  , B.Cycle_Plan_Total_Valid_Calls
  , B.Cycle_Plan_Valid_Calls
  , B.DateTime::varchar(255) as DateTime
  , B.Employee_Id
  , 'Cycle Plan Detail'::varchar(255) as Origin
  , B.Product_Id
  , B.Product_Metrics_SCD_Id
  , B.Product_Target_Class_Key
  , B.Region
  , B.Security_Key
  , B.Territory_Nominal_Id
FROM
    (
        SELECT
            cpd.attainment_vod__c        	  AS Cycle_Plan_Attainment
          , cpd.id                       	  AS Cycle_Plan_Global_Id
          , cpd.scheduled_details_vod__c 	  AS Cycle_Plan_Scheduled_Calls          
          , cpd.total_actual_details_vod__c    AS Cycle_Plan_Total_Actual_Calls
          , cpd.total_attainment_vod__c        AS Cycle_Plan_Total_Attainment
          , cpd.total_planned_details_vod__c   AS Cycle_Plan_Total_Planned_Calls
          , cpd.total_remaining_vod__c         AS Cycle_Plan_Total_Remaining_Calls
          , cpd.total_scheduled_details_vod__c AS Cycle_Plan_Total_Scheduled_Calls
          , CASE                
                WHEN  regexp_like(Trim(cpd.total_planned_details_vod__c),'^[0-9]+$')
                    THEN Trim(cpd.total_planned_details_vod__c)
                    ELSE '0'
            END :: Numeric(10,2) AS Total_Planned_Details_vod__c_1
          , Total_Planned_Details_vod__c_1 as Total_Planned_Details_vod__c
          , CASE
                WHEN regexp_like(Trim(cpd.total_actual_details_vod__c),'^[0-9]+$')                 
                    THEN Trim(cpd.total_actual_details_vod__c)
                    ELSE '0'
            END  :: Numeric(10,2) AS Total_Actual_Details_vod__c_1
          , Total_Actual_Details_vod__c_1 as Total_Actual_Details_vod__c
          , (total_planned_details_vod__c_1 - total_actual_details_vod__c_1) :: NUMERIC(10, 2) AS Cycle_Plan_Total_Coverage_Calls
          , CASE
                WHEN Len(total_planned_details_vod__c_1)  = 0
                    OR Len(total_actual_details_vod__c_1) = 0
                    OR total_actual_details_vod__c_1      = 0
                    THEN 0
                    ELSE total_planned_details_vod__c_1 / total_actual_details_vod__c_1
            END :: NUMERIC(10, 2) AS Cycle_Plan_Total_Coverage_Calls_Percentage
          , CASE
                WHEN Len(cpt.cycle_plan_vod__c) > 0
                    THEN cpt.cycle_plan_vod__c
                    ELSE 'NM'
            END :: varchar(255) AS Cycle_Plan_Id
          , datediff(day, to_date('1899-12-30','YYYY-MM-DD'),to_date(cpd.LastModifiedDate, 'YYYYMMDD hh24:mi:ss'))::varchar(255) AS DateTime
          , CASE
                WHEN Len(cpt.cycle_plan_account_vod__c) > 0
                    THEN cpt.cycle_plan_account_vod__c
                    ELSE 'NM'
            END :: varchar(255) AS Account_Id
			, CASE 
                WHEN cpd.total_actual_details_vod__c <= cpd.total_planned_details_vod__c
                    THEN cpd.total_actual_details_vod__c
                    ELSE cpd.total_planned_details_vod__c
            END      AS Cycle_Plan_Total_Valid_Calls
          , to_char(to_date(cpd.LastModifiedDate,'YYYYMMDD hh24:mi:ss'),'YYYYMMDD') AS Cycle_Plan_Date
          , CASE
                WHEN cpd.actual_details_vod__c <= cpd.planned_details_vod__c
                    THEN cpd.actual_details_vod__c
                    ELSE cpd.planned_details_vod__c
            END AS Cycle_Plan_Valid_Calls
          , cpd.remaining_vod__c      AS Cycle_Plan_Remaining_Calls
          , cpd.actual_details_vod__c AS Cycle_Plan_Actual_Calls
          , CASE
                WHEN Len(cpd.actual_details_vod__c) = 0
                    THEN '0'
                    ELSE cpd.actual_details_vod__c
            END :: NUMERIC(10, 2) AS actual_details_vod__c_1
          , actual_details_vod__c_1 AS actual_details_vod__c
          , CASE
                WHEN Len(cpd.planned_details_vod__c) = 0
                    THEN '0'
                    ELSE cpd.planned_details_vod__c
            END :: NUMERIC(10, 2)    AS Planned_Details_vod__c_1
          , Planned_Details_vod__c_1 AS Planned_Details_vod__c
          , cpd.Product_vod__c            as Product_Id
          , cpt.Cycle_Plan_Account_vod__c as Assignment_Id
          , cpd.Planned_Details_vod__c    as Cycle_Plan_Planned_Calls
          , CASE
                WHEN Len(ur.name) = 0
                    THEN 'NM'
                    ELSE ur.name
            END :: varchar(255) AS Cycle_Plan_Last_Modified_By
          , CASE
                WHEN Len(mp.product_detail_id) = 0
                    THEN account_id || '-' || 'NM'
                    ELSE account_id || '-' || mp.product_detail_id
            END :: varchar(255) AS Product_Target_Class_Key
          , (Account_Id || '-' || Cycle_Plan_Date) :: varchar(255) as Account_SCD_Id          
          , (Account_Id || '-' || Product_Id || '-' || Cycle_Plan_Date) :: varchar(255) as Product_Metrics_SCD_Id
          , CASE
                When Len(cpd.country_iso_code)>0
                    then cpd.country_iso_code
                    else 'NM'
            end :: varchar(255) as Country_Code
		  , CASE 
		  		WHEN (Account_Id = ac.Id)
					THEN Country_Code || '_' || 
					  	CASE
							WHEN Len(ac.Brick_Number_JJ__c)>0
			                    THEN ac.Brick_Number_JJ__c
								ELSE 'No Value'
							END
					ELSE 'NM'
			END :: varchar(255) as Brick_Code
          , case
                when Len(cp.Territory_vod__c)>0
                    then cp.Territory_vod__c
                    else 'NM'
            end:: varchar(255) as Territory_Nominal_Id
          , CASE
                When Len(cp.OwnerId)>0
                    then cp.OwnerId
                    else 'NM'
            end :: varchar(255) as Employee_Id
          , case
                when Len(cs.Name)>0
                    then cs.name
                    else 'NM'
            end :: varchar(255) as Country
          , (actual_details_vod__c_1 - planned_details_vod__c_1) :: NUMERIC(10, 2) AS Cycle_Plan_Coverage_Calls
          , case
                when Len(cs.jj_Region__c)>0
                    then cs.jj_region__c
                    else 'NM'
            end :: varchar(255) as Region
          ,(Account_Id || '-' || Employee_Id) :: varchar(255) as Security_Key
          ,('Non Visited' || '_' || Country_Code) :: varchar(255) as Account_Visited_NonVisited_Technical
        FROM {{ source('raw', 'cycle_plan_detail') }} cpd
        LEFT JOIN {{ source('raw', 'cycle_plan_target') }} cpt ON cpd.cycle_plan_target_vod__c = cpt.id
        LEFT JOIN {{ source('raw', 'cycle_plan') }} cp ON cp.id = cpt.cycle_plan_vod__c
        LEFT JOIN {{ source('raw', 'account') }} ac ON ac.id=cpt.cycle_plan_account_vod__c
        LEFT JOIN {{ source('raw', 'country_settings') }} cs ON cs.jj_Country_ISO_Code__c=cpd.country_iso_code
	   LEFT JOIN {{ source('raw', 'user') }} ur ON cpt.lastmodifiedbyid = ur.id
        LEFT JOIN {{ ref('m_product') }} mp ON cpd.product_vod__c = mp.product_id
    )
    B
	
UNION

SELECT DISTINCT
    C.Account_Id
  , C.Account_SCD_Id
  , C.Account_Visited_NonVisited_Technical
  , C.Assignment_Id
  , C.Brick_Code
  , C.Country
  , C.Country_Code
  , C.Cycle_Plan_Actual_Calls
  , C.Cycle_Plan_Attainment
  , C.Cycle_Plan_Coverage_Calls
  , C.Cycle_Plan_Date::varchar(255) as Cycle_Plan_Date
  , C.Cycle_Plan_Id
  , C.Cycle_Plan_Global_Id
  , C.Cycle_Plan_Last_Modified_By
  , 'Target'::Varchar(255) as Cycle_Plan_Origin
  , C.Cycle_Plan_Original_Calls
  , C.Cycle_Plan_Planned_Calls
  , C.Cycle_Plan_POA_Fulfilled
  , C.Cycle_Plan_Remaining_Calls
  , C.Cycle_Plan_Scheduled_Calls
  , C.Cycle_Plan_Total_Actual_Calls
  , C.Cycle_Plan_Total_Attainment
  , C.Cycle_Plan_Total_Coverage_Calls
  , C.Cycle_Plan_Total_Coverage_Calls_Percentage
  , C.Cycle_Plan_Total_Planned_Calls
  , C.Cycle_Plan_Total_Remaining_Calls
  , C.Cycle_Plan_Total_Scheduled_Calls
  , C.Cycle_Plan_Total_Valid_Calls
  , C.Cycle_Plan_Valid_Calls
  , C.DateTime::varchar(255)
  , C.Employee_Id
  , 'Cycle Plan Detail'::Varchar(255) AS Origin
  , NULL::Varchar(255) AS Product_Id
  , NULL::Varchar(255) AS Product_Metrics_SCD_Id
  , NULL::Varchar(255) AS Product_Target_Class_Key
  , C.Region
  , C.Security_Key
  , C.Territory_Nominal_Id
  FROM
  	  (
	  SELECT 
	   cpt.Cycle_Plan_Account_vod__c AS Acount_Id,        
	   to_char(date_trunc('month', to_date(cpt.LastModifiedDate,'YYYYMMDD hh24:mi:ss')),'YYYYMMDD') AS LastModifiedDate_1,
       (cpt.Cycle_Plan_Account_vod__c || '-' || LastModifiedDate_1)::Varchar(255) AS Account_SCD_Id,
       CASE
           WHEN Len(ac.Country_JJ__c) > 0 THEN ac.Country_JJ__c
           WHEN Len(ac.Country_JJ__c) = 0 THEN ac.JJ_Country__c
           ELSE 'NM'
       END :: Varchar(255) AS Country_Code,
       ('Non Visited' || '_' || Country_Code)::Varchar(255) AS Account_Visited_NonVisited_Technical,
       cpt.Cycle_Plan_Account_vod__c AS Assignment_Id,
       CASE
           WHEN Len(cs.Name) > 0 
			THEN cs.name
           	ELSE 'NM'
       END :: Varchar(255) AS Country,
       cpt.actual_calls_vod__c AS Cycle_Plan_Actual_Calls,
       cpt.Attainment_vod__c AS Cycle_Plan_Attainment,
       to_char(to_date(cpt.LastModifiedDate,'YYYYMMDD hh24:mi:ss'),'YYYYMMDD') AS Cycle_Plan_Date,
       cpt.Cycle_Plan_vod__c AS Cycle_Plan_Id,
       CASE
           WHEN cpt.actual_calls_vod__c <= cpt.planned_calls_vod__c THEN cpt.actual_calls_vod__c
           ELSE cpt.planned_calls_vod__c
       END AS Cycle_Plan_Valid_Calls,
       CASE
           WHEN Len(cpt.actual_calls_vod__c) = 0 THEN '0'
           ELSE cpt.actual_calls_vod__c
       END :: Numeric(10,2) AS actual_calls_vod__c_1,
       actual_calls_vod__c_1 AS actual_calls_vod__c,
       CASE
           WHEN Len(cpt.planned_calls_vod__c) = 0 THEN '0'
           ELSE cpt.planned_calls_vod__c
       END :: Numeric(10,2) AS Planned_calls_vod__c_1,
       Planned_calls_vod__c_1 AS Planned_calls_vod__c,
       (actual_calls_vod__c_1 - planned_calls_vod__c_1)::Numeric(10,2) AS Cycle_Plan_Coverage_Calls,
       cpt.id AS Cycle_Plan_Global_Id,
       CASE
           WHEN Len(ur.name) = 0 THEN 'NM'
           ELSE ur.name
       END::Varchar(255) AS Cycle_Plan_Last_Modified_By,
       cpt.JJ_Original__c AS Cycle_Plan_Original_Calls,
       cpt.total_planned_calls_vod__c AS Cycle_Plan_Planned_Calls,
       CASE
           WHEN regexp_like(Trim(cpt.total_planned_calls_vod__c),'^[0-9]+$') THEN Trim(cpt.total_planned_calls_vod__c)
           ELSE '0'
       END :: Numeric(10,2) AS Total_Planned_calls_vod__c_1,
       Total_Planned_calls_vod__c_1 AS Total_Planned_calls_vod__c,
       CASE
           WHEN regexp_like(Trim(cpt.total_actual_calls_vod__c),'^[0-9]+$') THEN Trim(cpt.total_actual_calls_vod__c)
           ELSE '0'
       END :: Numeric(10,2) AS Total_Actual_calls_vod__c_1,
       Total_Actual_calls_vod__c_1 AS Total_Actual_calls_vod__c,
       (total_planned_calls_vod__c_1 - total_actual_calls_vod__c_1)::Numeric(10,2) AS Cycle_Plan_Total_Coverage_Calls,
       CASE
           WHEN Len(total_planned_calls_vod__c_1) = 0
                OR Len(total_actual_calls_vod__c_1) = 0
                OR total_actual_calls_vod__c_1 = 0 THEN 0
           ELSE total_planned_calls_vod__c_1 / total_actual_calls_vod__c_1
       END::Numeric(10,2)  AS Cycle_Plan_Total_Coverage_Calls_Percentage,
       CASE
           WHEN Cycle_Plan_Total_Coverage_Calls_Percentage >= 1 THEN 'Fullfilled'
           ELSE 'Non-Fulfilled'
       END::Varchar(255) AS Cycle_Plan_POA_Fulfilled,
       cpt.Remaining_vod__c AS Cycle_Plan_Remaining_Calls,
       cpt.scheduled_calls_vod__c AS Cycle_Plan_Scheduled_Calls,
       cpt.total_actual_calls_vod__c AS Cycle_Plan_Total_Actual_Calls,
       cpt.total_attainment_vod__c AS Cycle_Plan_Total_Attainment,
       cpt.total_planned_calls_vod__c AS Cycle_Plan_Total_Planned_Calls,
       cpt.total_remaining_vod__c AS Cycle_Plan_Total_Remaining_Calls,
       cpt.total_scheduled_calls_vod__c AS Cycle_Plan_Total_Scheduled_Calls,
       CASE
           WHEN cpt.total_actual_calls_vod__c <= cpt.total_planned_calls_vod__c THEN cpt.total_actual_calls_vod__c
           ELSE cpt.total_planned_calls_vod__c
       END AS Cycle_Plan_Total_Valid_Calls,
       datediff(day, to_date('1899-12-30','YYYY-MM-DD'),to_date(cpt.LastModifiedDate, 'YYYYMMDD hh24:mi:ss'))::varchar(255) AS DateTime,
       CASE
           WHEN Len(cp.OwnerId) > 0 THEN cp.OwnerId
           ELSE 'NM'
       END::Varchar(255) AS Employee_Id,
       CASE
           WHEN Len(cs.jj_Region__c) > 0 THEN cs.jj_region__c
           ELSE 'NM'
       END::Varchar(255) AS Region,
       CASE
           WHEN Len(cpt.cycle_plan_account_vod__c) > 0 THEN cpt.cycle_plan_account_vod__c
           ELSE 'NM'
       END::Varchar(255) AS Account_Id,
       (Account_Id || '-' || Employee_Id)::Varchar(255) AS Security_Key,
       CASE
           WHEN Len(cp.Territory_vod__c) > 0 THEN cp.Territory_vod__c
           ELSE 'NM'
       END::Varchar(255) AS Territory_Nominal_Id,
       CASE
           WHEN (Account_Id = ac.Id) THEN Country_Code || '_' || CASE WHEN Len(ac.Brick_Number_JJ__c) > 0 THEN ac.Brick_Number_JJ__c ELSE 'No Value' END
           ELSE 'NM'
       END::Varchar(255) AS Brick_Code
	FROM {{ source('raw', 'action_item') }} cpt
	LEFT JOIN {{ source('raw', 'account') }} ac ON ac.id = cpt.cycle_plan_account_vod__c
	LEFT JOIN {{ source('raw', 'country_settings') }} cs ON cs.jj_Country_ISO_Code__c = cpt.country_iso_code
	LEFT JOIN {{ source('raw', 'cycle_plan') }} cp ON cp.id = cpt.cycle_plan_vod__c
	LEFT JOIN {{ source('raw', 'user') }} ur ON cpt.lastmodifiedbyid = ur.id
	) C