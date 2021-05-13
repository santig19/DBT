{{ config(
	post_hook="grant all privileges on {{ this }} to {{ var('snowflake_user_grant_privileges') }}")
}}

SELECT  
COUNTRY_ISO_CODE							AS COUNTRY_ISO_CODE
,ID							                AS ID							            
,OWNERID                                    AS OWNERID                                
,ISDELETED                                  AS ISDELETED                              
,NAME                                       AS NAME                                   
,CURRENCYISOCODE                            AS CURRENCYISOCODE                        
,RECORDTYPEID                               AS RECORDTYPEID                           
,CREATEDDATE                                AS CREATEDDATE                            
,CREATEDBYID                                AS CREATEDBYID                            
,LASTMODIFIEDDATE                           AS LASTMODIFIEDDATE                       
,LASTMODIFIEDBYID                           AS LASTMODIFIEDBYID                       
,SYSTEMMODSTAMP                             AS SYSTEMMODSTAMP                         
,MAYEDIT                                    AS MAYEDIT                                
,ISLOCKED                                   AS ISLOCKED                               
,CONNECTIONRECEIVEDID                       AS CONNECTIONRECEIVEDID                   
,CONNECTIONSENTID                           AS CONNECTIONSENTID                       
,MEETING_ID_VOD__C                          AS MEETING_ID_VOD                       
,MEETING_NAME_VOD__C                        AS MEETING_NAME_VOD                     
,MOBILE_ID_VOD__C                           AS MOBILE_ID_VOD                        
,SCHEDULED_DATETIME_VOD__C                  AS SCHEDULED_DATETIME_VOD               
,SCHEDULED_VOD__C                           AS SCHEDULED_VOD                        
,ASSIGNED_HOST_VOD__C                       AS ASSIGNED_HOST_VOD                    
,ATTENDANCE_REPORT_PROCESS_STATUS_VOD__C    AS ATTENDANCE_REPORT_PROCESS_STATUS_VOD 
,DESCRIPTION_VOD__C                         AS DESCRIPTION_VOD                      
,LATEST_MEETING_START_DATETIME_VOD__C       AS LATEST_MEETING_START_DATETIME_VOD    
,WEBINAR_ALTERNATIVE_HOST_1_VOD__C          AS WEBINAR_ALTERNATIVE_HOST_1_VOD       
,WEBINAR_ALTERNATIVE_HOST_2_VOD__C          AS WEBINAR_ALTERNATIVE_HOST_2_VOD       
,WEBINAR_ALTERNATIVE_HOST_3_VOD__C          AS WEBINAR_ALTERNATIVE_HOST_3_VOD       
,MEETING_OUTCOME_STATUS_VOD__C              AS MEETING_OUTCOME_STATUS_VOD           
,MEETING_PASSWORD_VOD__C                    AS MEETING_PASSWORD_VOD                 
FROM {{ source('raw', 'elq_remote_meeting') }}