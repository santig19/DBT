{{ config(
	post_hook="grant all privileges on {{ this }} to {{ var('snowflake_user_grant_privileges') }}")
}}

SELECT
	EEA.Id::varchar(32)	as eligible_event_attendee_Id,
	EEA.IsDeleted::int	as IsDeleted,
	EEA.Name::varchar(160)	as Name,
	EEA.CurrencyIsoCode::varchar(6)	as CurrencyIsoCode,
	CASE WHEN EEA.CreatedDate = '' THEN null ELSE to_timestamp_ntz(EEA.CreatedDate,'YYYYMMDD HH24:MI:SS') END::datetime	as CreatedDate,
	EEA.CreatedById::varchar(32)	as CreatedById,
	CASE WHEN EEA.LastModifiedDate = '' THEN null ELSE to_timestamp_ntz(EEA.LastModifiedDate,'YYYYMMDD HH24:MI:SS') END::datetime	as LastModifiedDate,
	EEA.LastModifiedById::varchar(32)	as LastModifiedById,
	CASE WHEN EEA.SystemModstamp = '' THEN null ELSE to_timestamp_ntz(EEA.SystemModstamp,'YYYYMMDD HH24:MI:SS') END::datetime	as SystemModstamp,
	CASE WHEN EEA.LastActivityDate = '' THEN null ELSE EEA.LastActivityDate END::datetime	as LastActivityDate,
	EEA.MayEdit::int	as MayEdit,
	EEA.IsLocked::int	as IsLocked,
	CASE WHEN EEA.LastViewedDate = '' THEN null ELSE EEA.LastViewedDate END::datetime	as LastViewedDate,
	CASE WHEN EEA.LastReferencedDate = '' THEN null ELSE EEA.LastReferencedDate END::datetime	as LastReferencedDate,
	EEA.ConnectionReceivedId::varchar(32)	as Connection_Received_Id,
	EEA.ConnectionSentId::varchar(32)	as Connection_Sent_Id,
	EEA.JJ_Event__c::varchar(32)	as Event,
	EEA.JJ_Account__c::varchar(32)	as Account,
	EEA.JJ_Attendee_CODS_External_Id__c::varchar(2600)	as Attendee_CODS_External_Id,
	EEA.JJ_Attendee_First_Name__c::varchar(2600)	as Attendee_First_Name,
	EEA.JJ_Attendee_Full_Name__c::varchar(2600)	as Attendee_Full_Name,
	EEA.JJ_Attendee_Last_Name__c::varchar(2600)	as Attendee_Last_Name,
	EEA.JJ_Can_HCP_Contact__c::varchar(2600)	as Can_HCP_Contact,
	EEA.JJ_Can_HCP_Signup__c::varchar(2600)	as Can_HCP_Signup,
	EEA.JJ_Government_Official__c::int	as Government_Official,
	EEA.JJ_Publish_to_Web__c::varchar(2600)	as Publish_to_Web,
	EEA.JJ_Status__c::varchar(16)	as Status,
	EEA.JJ_Event_Attendee__c::varchar(32)	as Event_Attendee,
	CASE WHEN (POSITION(chr(92) || chr(92), EEA.JJ_Account_Email_Address__c) > 0) or POSITION(chr(124) || chr(34), EEA.JJ_Account_Email_Address__c) > 0 or POSITION(chr(92) || chr(124), EEA.JJ_Account_Email_Address__c) > 0 or POSITION(chr(92) || chr(34), EEA.JJ_Account_Email_Address__c) > 0
        THEN replace(replace(replace(replace(EEA.JJ_Account_Email_Address__c, chr(92) || chr(92), chr(92)), chr(124) || chr(34), chr(34)), chr(92) || chr(124), chr(124)), chr(92) || chr(34), chr(34))
        ELSE nvl(EEA.JJ_Account_Email_Address__c,'')
    END::varchar(2600) as Account_Email_Address,
	EEA.JJ_External_Id__c::varchar(100)	as External_Id,
	EEA.JJ_System_Unique_Eligible_Event_Attendee__c::varchar(300)	as System_Unique_Eligible_Event_Attendee,
	CASE WHEN EEA.JJ_Confirmation_Email_Date__c = '' THEN null ELSE EEA.JJ_Confirmation_Email_Date__c END::datetime	as Confirmation_Email_Date,
	CASE WHEN EEA.JJ_Final_Confirmation_Email_Date__c = '' THEN null ELSE EEA.JJ_Final_Confirmation_Email_Date__c END::datetime	as Final_Confirmation_Email_Date,
	EEA.JJ_Final_Confirmation_Email_Status__c::varchar(2600)	as Final_Confirmation_Email_Status,
	CASE WHEN EEA.JJ_Invitation_Email_Date__c = '' THEN null ELSE EEA.JJ_Invitation_Email_Date__c END::datetime	as Invitation_Email_Date,
	EEA.JJ_Invitation_Email_Status__c::varchar(2600)	as Invitation_Email_Status,
	CASE WHEN EEA.JJ_Post_Event_Email_Date__c = '' THEN null ELSE to_timestamp_ntz(EEA.JJ_Post_Event_Email_Date__c,'YYYYMMDD HH24:MI:SS') END::datetime	as Post_Event_Email_Date,
	EEA.JJ_Post_Event_Email_Status__c::varchar(2600)	as Post_Event_Email_Status,
	EEA.JJ_Registration_Confirmation_Email_Statu__c::varchar(2600)	as Registration_Confirmation_Email_Status,
	CASE WHEN EEA.JJ_Reminder_Email_Date__c = '' THEN null ELSE EEA.JJ_Reminder_Email_Date__c END::datetime	as Reminder_Email_Date,
	EEA.JJ_Reminder_Email_Status__c::varchar(2600)	as Reminder_Email_Status,
	CASE WHEN EEA.JJ_Save_The_Date_Email_Date__c = '' THEN null ELSE EEA.JJ_Save_The_Date_Email_Date__c END::datetime	as Save_The_Date_Email_Date,
	EEA.JJ_Save_The_Date_Email_Status__c::varchar(2600)	as Save_The_Date_Email_Status,
	EEA.JJ_Authorisation_Form_Attached__c::int	as Authorisation_Form_Attached,
	EEA.JJ_Authorisation_Rejection_Reason__c::varchar(510)	as Authorisation_Rejection_Reason,
	EEA.JJ_Authorisation_Status__c::varchar(510)	as Authorisation_Status,
	EEA.JJ_Authorisation_required__c::varchar(2600)	as Authorisation_required,
	EEA.JJ_Nb_Of_Authorisation_Forms__c::decimal(18, 0)	as Nb_Of_Authorisation_Forms
FROM {{ var('schema') }}.jj_eligible_event_attendee__c_raw EEA