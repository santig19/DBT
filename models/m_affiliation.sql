{{ config(
	post_hook="grant all privileges on {{ this }} to {{ var('snowflake_user_grant_privileges') }}")
}}

SELECT
	nvl(mc.child_affiliation_vod__c,'') as child_affiliation_id,
	nvl(mc.comments_vod__c,'') as comments,
	nvl(mc.connectionreceivedid,'') as connectionreceivedid,
	nvl(mc.connectionsentid,'') as connectionsentid,
	nvl(mc.createdbyid,'') as createdbyid,
	case when mc.createddate = '' then null else TO_TIMESTAMP(mc.createddate, 'YYYYMMDD HH24:MI:SS') END::datetime as createddate,
	nvl(mc.currencyisocode,'') as currencyisocode,
	nvl(mc.destroy_vod__c,'') as destroy,
	nvl(mc.disable_trigger_vod__c,'') as disable_trigger,
	nvl(mc.external_id_vod__c,'') as external_id,
	nvl(mc.from_account_vod__c,'') as from_account_id,
	nvl(mc.from_contact_vod__c,'') as from_contact_id,
	nvl(mc.id,'') as affiliation_id,
	nvl(mc.influence_vod__c,'') as influence,
	nvl(mc.isdeleted,'') as isdeleted,
	nvl(mc.islocked,'') as islocked,
	nvl(mc.jj_dataloadid__c,'') as dataloadid,
	nvl(mc.jj_dm_external_id__c,'') as dm_external_id,
	nvl(mc.lastmodifiedbyid,'') as lastmodifiedbyid,
	case when mc.lastmodifieddate = '' then null else TO_TIMESTAMP(mc.lastmodifieddate, 'YYYYMMDD HH24:MI:SS') END::datetime as lastmodifieddate,
	nvl(mc.mayedit,'') as mayedit,
	nvl(mc.mobile_id_vod__c,'') as mobile_id,
	nvl(mc.name,'') as name,
	nvl(mc.ownerid,'') as ownerid,
	nvl(mc.parent_vod__c,'') as parent,
	nvl(mc.relationship_strength_vod__c,'') as relationship_strength,
	nvl(mc.role_vod__c,'') as role,
	case when mc.systemmodstamp = '' then null else TO_TIMESTAMP(mc.systemmodstamp, 'YYYYMMDD HH24:MI:SS') END::datetime as systemmodstamp,
	nvl(mc.therapeutic_area_vod__c,'') as therapeutic_area,
	nvl(mc.to_account_affiliation_count_vod__c,'') as to_account_affiliation_count,
	nvl(mc.to_account_identifier_vod__c,'') as to_account_identifier,
	case when (Position(chr(92) || chr(92), mc.to_account_name_vod__c) > 0) or Position(chr(124) || chr(34), mc.to_account_name_vod__c) > 0 or Position(chr(92) || chr(124), mc.to_account_name_vod__c) > 0 or Position(chr(92) || chr(34), mc.to_account_name_vod__c) > 0
		then replace(replace(replace(replace(mc.to_account_name_vod__c, chr(92) || chr(92), chr(92)), chr(124) || chr(34), chr(34)), chr(92) || chr(124), chr(124)), chr(92) || chr(34), chr(34))
		else nvl(mc.to_account_name_vod__c,'') 
	end as to_account_name,	
	nvl(mc.to_account_record_type_vod__c,'') as to_account_record_type_id,
	nvl(mc.to_account_vod__c,'') as to_account_id,
	nvl(mc.to_contact_vod__c,'') as to_contact_id
FROM {{ var('schema') }}.affiliation_raw as mc