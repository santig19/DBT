{{ config(
	post_hook="grant all privileges on {{ this }} to {{ var('snowflake_user_grant_privileges') }}")
}}

select  country_iso_code, 
	id, 
	knowledgearticleid, 
	ownerid, 
	isdeleted, 
	publishstatus, 
	versionnumber, 
	islatestversion, 
	isvisibleinapp, 
	isvisibleinpkb, 
	isvisibleincsp, 
	isvisibleinprm, 
	createddate, 
	createdbyid, 
	lastmodifieddate, 
	lastmodifiedbyid, 
	systemmodstamp, 
	ismasterlanguage, 
	language, 
	case when (Position(CHR(92) || CHR(92), title) > 0) or Position(CHR(124) || CHR(34), title) > 0 or Position(CHR(92) || CHR(124), title) > 0 or Position(CHR(92) || CHR(34), title) > 0
                    then REPLACE(REPLACE(REPLACE(REPLACE(title, CHR(92) || CHR(92), CHR(92)), CHR(124) || CHR(34), CHR(34)), CHR(92) || CHR(124), CHR(124)), CHR(92) || CHR(34), CHR(34))
                    else title END AS title, 
	urlname, 
	currencyisocode, 
	archiveddate, 
	archivedbyid,
        case when (Position(CHR(92) || CHR(92), summary) > 0) or Position(CHR(124) || CHR(34), summary) > 0 or Position(CHR(92) || CHR(124), summary) > 0 or Position(CHR(92) || CHR(34), summary) > 0
                    then REPLACE(REPLACE(REPLACE(REPLACE(summary, CHR(92) || CHR(92), CHR(92)), CHR(124) || CHR(34), CHR(34)), CHR(92) || CHR(124), CHR(124)), CHR(92) || CHR(34), CHR(34))
                    else summary END AS summary, 
	articlenumber, 
	firstpublisheddate, 
	lastpublisheddate,
	articlearchivedbyid, 
	articlearchiveddate, 
	articlecaseattachcount, 
	articlecreatedbyid, 
	articlecreateddate, 
	articletotalviewcount,
	sourceid, 
	masterversionid, 
	recordtypeid,
	case when (Position(CHR(92) || CHR(92), jj_sss_sst_jj_sss_keywords__c) > 0) or Position(CHR(124) || CHR(34), jj_sss_sst_jj_sss_keywords__c) > 0 or Position(CHR(92) || CHR(124), jj_sss_sst_jj_sss_keywords__c) > 0 or Position(CHR(92) || CHR(34), jj_sss_sst_jj_sss_keywords__c) > 0
                    then REPLACE(REPLACE(REPLACE(REPLACE(jj_sss_sst_jj_sss_keywords__c, CHR(92) || CHR(92), CHR(92)), CHR(124) || CHR(34), CHR(34)), CHR(92) || CHR(124), CHR(124)), CHR(92) || CHR(34), CHR(34))
                    else jj_sss_sst_jj_sss_keywords__c END AS jj_sss_sst_jj_sss_keywords__c,
	jj_midas_jj_target_audience__c, 
	jj_midas_jj_midas_version__c,
	jj_smpc_pil_jj_emea_brand_name__c, 
	jj_sss_sst_jj_sss_community_name__c,
	case when (Position(CHR(92) || CHR(92), jj_midas_jj_keywords__c) > 0) or Position(CHR(124) || CHR(34), jj_midas_jj_keywords__c) > 0 or Position(CHR(92) || CHR(124), jj_midas_jj_keywords__c) > 0 or Position(CHR(92) || CHR(34), jj_midas_jj_keywords__c) > 0
                    then REPLACE(REPLACE(REPLACE(REPLACE(jj_midas_jj_keywords__c, CHR(92) || CHR(92), CHR(92)), CHR(124) || CHR(34), CHR(34)), CHR(92) || CHR(124), CHR(124)), CHR(92) || CHR(34), CHR(34))
                    else jj_midas_jj_keywords__c END AS jj_midas_jj_keywords__c,
	jj_sss_sst_jj_sss_answer_status__c,
        jj_midas_jj_periodic_review_date__c,
        case when (Position(CHR(92) || CHR(92), jj_sss_sst_jj_sss_body__c) > 0) or Position(CHR(124) || CHR(34), jj_sss_sst_jj_sss_body__c) > 0 or Position(CHR(92) || CHR(124), jj_sss_sst_jj_sss_body__c) > 0 or Position(CHR(92) || CHR(34), jj_sss_sst_jj_sss_body__c) > 0
                    then REPLACE(REPLACE(REPLACE(REPLACE(jj_sss_sst_jj_sss_body__c, CHR(92) || CHR(92), CHR(92)), CHR(124) || CHR(34), CHR(34)), CHR(92) || CHR(124), CHR(124)), CHR(92) || CHR(34), CHR(34))
                    else jj_sss_sst_jj_sss_body__c END AS jj_sss_sst_jj_sss_body__c,
        jj_midas_jj_local_brand_name__c, 
	jj_midas_jj_midas_id__c,
        jj_midas_jj_document_sub_type__c, 
	jj_sss_sst_jj_sss_user_story__c,
        jj_smpc_pil_jj_summary__c, 
	jj_midas_jj_category__c, 
	jj_midas_jj_therapeutic_area__c,
        case when (Position(CHR(92) || CHR(92), jj_sss_sst_jj_sss_answer_text__c) > 0) or Position(CHR(124) || CHR(34), jj_sss_sst_jj_sss_answer_text__c) > 0 or Position(CHR(92) || CHR(124), jj_sss_sst_jj_sss_answer_text__c) > 0 or Position(CHR(92) || CHR(34), jj_sss_sst_jj_sss_answer_text__c) > 0
                    then REPLACE(REPLACE(REPLACE(REPLACE(jj_sss_sst_jj_sss_answer_text__c, CHR(92) || CHR(92), CHR(92)), CHR(124) || CHR(34), CHR(34)), CHR(92) || CHR(124), CHR(124)), CHR(92) || CHR(34), CHR(34))
                    else jj_sss_sst_jj_sss_answer_text__c END AS jj_sss_sst_jj_sss_answer_text__c,
        jj_sss_sst_jj_sss_attachment_id__c, 
	jj_midas_sub_category__c,
        jj_midas_jj_emea_brand_name__c, 
	jj_midas_jj_available_for__c,
        jj_smpc_pil_jj_channel__c, 
	jj_midas_jj_document_type__c,
        jj_midas_jj_level_1_flag__c, 
	jj_midas_jj_enclosure_details__c,
        case when (Position(CHR(92) || CHR(92), jj_midas_jj_summary__c) > 0) or Position(CHR(124) || CHR(34), jj_midas_jj_summary__c) > 0 or Position(CHR(92) || CHR(124), jj_midas_jj_summary__c) > 0 or Position( CHR(92) || CHR(34), jj_midas_jj_summary__c) > 0
                    then REPLACE(REPLACE(REPLACE(REPLACE(jj_midas_jj_summary__c, CHR(92) || CHR(92), CHR(92)), CHR(124) || CHR(34), CHR(34)), CHR(92) || CHR(124), CHR(124)), CHR(92) || CHR(34), CHR(34))
                    else jj_midas_jj_summary__c END AS jj_midas_jj_summary__c,
        jj_sss_sst_jj_sss_sub_process__c, 
	jj_sss_sst_jj_sss_business_process__c,
        case when (Position(CHR(92) || CHR(92), jj_midas_jj_detailed_title__c) > 0) or Position(CHR(124) || CHR(34), jj_midas_jj_detailed_title__c) > 0 or Position(CHR(92) || CHR(124), jj_midas_jj_detailed_title__c) > 0 or Position(CHR(92) || CHR(34), jj_midas_jj_detailed_title__c) > 0
                    then REPLACE(REPLACE(REPLACE(REPLACE(jj_midas_jj_detailed_title__c, CHR(92) || CHR(92), CHR(92)), CHR(124) || CHR(34), CHR(34)), CHR(92) || CHR(124), CHR(124)), CHR(92) || CHR(34), CHR(34))
                    else jj_midas_jj_detailed_title__c END AS jj_midas_jj_detailed_title__c,
        jj_midas_jj_document_id__c, 
	jj_smpc_pil_jj_indication__c, 
	jj_midas_jj_indication__c,
        jj_sss_sst_jj_sss_userrole__c, 
	jj_midas_jj_vault_document_id__c, 
	jj_sss_sst_jj_sss_question_escalated__c 
from {{ var('schema') }}.knowledge_kav_raw