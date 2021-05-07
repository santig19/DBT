{{ config(
	post_hook="grant all privileges on {{ this }} to {{ var('snowflake_user_grant_privileges') }}")
}}

SELECT 
    AUX.Security_Key::varchar(255) AS Security_Key,
    AUX.Country_code::varchar(2) AS Country_code,
    AUX.Region::varchar(255) AS Region,
    AUX.Country::varchar(255) AS Country,
    AUX.Date::varchar(10) AS Date,
    AUX.Time_Effectiveness_Date::varchar(8) AS Time_Effectiveness_Date,
    AUX.Time_Effectiveness_Created_Timestamp::varchar(21) AS Time_Effectiveness_Created_Timestamp,
    AUX.Time_Effectiveness_Id::varchar(20) AS Time_Effectiveness_Id,
    AUX.Time_Effectiveness_Comments::varchar(1000) AS Time_Effectiveness_Comments,
    AUX.Time_Effectiveness_Type::varchar(255) AS Time_Effectiveness_Type,
    AUX.Time_Effectiveness::varchar(255) AS Time_Effectiveness,
    AUX.Employee_Id::varchar(20) AS Employee_Id,
    AUX.Territory_Nominal_Id::varchar(20) AS Territory_Nominal_Id,
    AUX.Assignment_Id::varchar(20) AS Assignment_Id,
    AUX.Assignment_Nominal_Id::varchar(20) AS Assignment_Nominal_Id,
    AUX.Time_Effectiveness_Reason::varchar(255) AS Time_Effectiveness_Reason,
    AUX.Time_Effectiveness_Record_Type::varchar(255) AS Time_Effectiveness_Record_Type,
    AUX.Time_Effectiveness_Status::varchar(255) AS Time_Effectiveness_Status,
    AUX.Origin::varchar(18) AS Origin,
    AUX.Account_Visited_NonVisited_Technical::varchar(14) AS Account_Visited_NonVisited_Technical,
    AUX.Time_Effectiveness_Time::varchar(255) AS Time_Effectiveness_Time,
    AUX.Aux_Product_Id::varchar(255) AS Product_Id,
    AUX.Aux_Therapeutic_Area::varchar(255) AS Therapeutic_Area_Name,
    AUX.Aux_Therapeutic_Area::varchar(255) AS Therapeutic_Area_Brand_Name
FROM {{ ref('tmp_f_time_effectiveness') }} AS AUX
WHERE LOWER(AUX.Time_Effectiveness_Type) = 'non field day'

UNION

SELECT 
    AUX.Security_Key::varchar(255) AS Security_Key,
    AUX.Country_code::varchar(2) AS Country_code,
    AUX.Region::varchar(255) AS Region,
    AUX.Country::varchar(255) AS Country,
    AUX.Date::varchar(10) AS Date,
    AUX.Time_Effectiveness_Date::varchar(8) AS Time_Effectiveness_Date,
    AUX.Time_Effectiveness_Created_Timestamp::varchar(21) AS Time_Effectiveness_Created_Timestamp,
    AUX.Time_Effectiveness_Id::varchar(20) AS Time_Effectiveness_Id,
    AUX.Time_Effectiveness_Comments::varchar(1000) AS Time_Effectiveness_Comments,
    AUX.Time_Effectiveness_Type::varchar(255) AS Time_Effectiveness_Type,
    AUX.Time_Effectiveness::varchar(255) AS Time_Effectiveness,
    AUX.Employee_Id::varchar(20) AS Employee_Id,
    AUX.Territory_Nominal_Id::varchar(20) AS Territory_Nominal_Id,
    AUX.Assignment_Id::varchar(20) AS Assignment_Id,
    AUX.Assignment_Nominal_Id::varchar(20) AS Assignment_Nominal_Id,
    AUX.Time_Effectiveness_Reason::varchar(255) AS Time_Effectiveness_Reason,
    AUX.Time_Effectiveness_Record_Type::varchar(255) AS Time_Effectiveness_Record_Type,
    AUX.Time_Effectiveness_Status::varchar(255) AS Time_Effectiveness_Status,
    AUX.Origin::varchar(18) AS Origin,
    AUX.Account_Visited_NonVisited_Technical::varchar(14) AS Account_Visited_NonVisited_Technical,
    AUX.Time_Effectiveness_Time::varchar(255) AS Time_Effectiveness_Time,
    NULL::varchar(255) AS Product_Id,
    NULL::varchar(255) AS Therapeutic_Area_Name,
    NULL::varchar(255) AS Therapeutic_Area_Brand_Name
FROM {{ ref('tmp_f_time_effectiveness') }} AS AUX
WHERE LOWER(AUX.Time_Effectiveness_Type) NOT IN ('field day', 'non field day')

UNION

SELECT 
    AUX.Security_Key::varchar(255) AS Security_Key,
    AUX.Country_code::varchar(2) AS Country_code,
    AUX.Region::varchar(255) AS Region,
    AUX.Country::varchar(255) AS Country,
    AUX.Date::varchar(10) AS Date,
    AUX.Time_Effectiveness_Date::varchar(8) AS Time_Effectiveness_Date,
    AUX.Time_Effectiveness_Created_Timestamp::varchar(21) AS Time_Effectiveness_Created_Timestamp,
    AUX.Time_Effectiveness_Id::varchar(20) AS Time_Effectiveness_Id,
    AUX.Time_Effectiveness_Comments::varchar(1000) AS Time_Effectiveness_Comments,
    AUX.Time_Effectiveness_Type::varchar(255) AS Time_Effectiveness_Type,
    AUX.Time_Effectiveness::varchar(255) AS Time_Effectiveness,
    AUX.Employee_Id::varchar(20) AS Employee_Id,
    AUX.Territory_Nominal_Id::varchar(20) AS Territory_Nominal_Id,
    AUX.Assignment_Id::varchar(20) AS Assignment_Id,
    AUX.Assignment_Nominal_Id::varchar(20) AS Assignment_Nominal_Id,
    AUX.Time_Effectiveness_Reason::varchar(255) AS Time_Effectiveness_Reason,
    AUX.Time_Effectiveness_Record_Type::varchar(255) AS Time_Effectiveness_Record_Type,
    AUX.Time_Effectiveness_Status::varchar(255) AS Time_Effectiveness_Status,
    AUX.Origin::varchar(18) AS Origin,
    AUX.Account_Visited_NonVisited_Technical::varchar(14) AS Account_Visited_NonVisited_Technical,
    AUX.Time_Effectiveness_Time::varchar(255) AS Time_Effectiveness_Time,
    F_CALL.Product_Id::varchar(255) AS Product_Id,
    F_CALL.Therapeutic_Area_Name::varchar(255) AS Therapeutic_Area_Name,
    F_CALL.Therapeutic_Area_Brand_Name::varchar(255) AS Therapeutic_Area_Brand_Name    
FROM {{ ref('tmp_f_time_effectiveness') }} AS AUX
LEFT OUTER JOIN {{ ref('f_call') }} AS F_CALL 
  ON AUX.AUX_Employee_Date = F_CALL.Employee_Id ||'-'|| F_CALL.Date
 AND AUX.Aux_Product_Id    = F_CALL.Product_Id
WHERE LOWER(AUX.Time_Effectiveness_Type) = 'field day'