{{ config(
	post_hook="grant all privileges on {{ this }} to {{ var('snowflake_user_grant_privileges') }}")
}}

SELECT  
COUNTRY_ISO_CODE					AS COUNTRY_ISO_CODE
,ID                                  AS ID                            
,ISDELETED                           AS ISDELETED                     
,NAME                                AS NAME                          
,CURRENCYISOCODE                     AS CURRENCYISOCODE               
,RECORDTYPEID                        AS RECORDTYPEID                  
,CREATEDDATE                         AS CREATEDDATE                   
,CREATEDBYID                         AS CREATEDBYID                   
,LASTMODIFIEDDATE                    AS LASTMODIFIEDDATE              
,LASTMODIFIEDBYID                    AS LASTMODIFIEDBYID              
,SYSTEMMODSTAMP                      AS SYSTEMMODSTAMP                
,MAYEDIT                             AS MAYEDIT                       
,ISLOCKED                            AS ISLOCKED                      
,CONNECTIONRECEIVEDID                AS CONNECTIONRECEIVEDID          
,CONNECTIONSENTID                    AS CONNECTIONSENTID              
,REMOTE_MEETING_VOD__C               AS REMOTE_MEETING_VOD         
,CALL_VOD__C                         AS CALL_VOD                   
,JOIN_TIME_VOD__C                    AS JOIN_TIME_VOD              
,LEAVE_TIME_VOD__C                   AS LEAVE_TIME_VOD             
,MEETING_END_TIME_VOD__C             AS MEETING_END_TIME_VOD       
,MEETING_START_TIME_VOD__C           AS MEETING_START_TIME_VOD     
,NAME_VOD__C                         AS NAME_VOD                   
,PARTICIPANT_ID_VOD__C               AS PARTICIPANT_ID_VOD         
,VEXTERNAL_ID_VOD__C                 AS VEXTERNAL_ID_VOD           
,VEEVA_REMOTE_MEETING_ID_VOD__C      AS VEEVA_REMOTE_MEETING_ID_VOD
,DEVICE_VOD__C                       AS DEVICE_VOD                 
,EM_ATTENDEE_VOD__C                  AS EM_ATTENDEE_VOD            
,EM_EVENT_SPEAKER_VOD__C             AS EM_EVENT_SPEAKER_VOD       
,EVENT_VOD__C                        AS EVENT_VOD                  
,NETWORK_TYPE_VOD__C                 AS NETWORK_TYPE_VOD           
,ATTENDEE_TYPE_VOD__C                AS ATTENDEE_TYPE_VOD          
,JJ_ACCOUNT__C                       AS ACCOUNT                 
FROM {{ source('raw', 'elq_remote_meeting_attendee') }}



















