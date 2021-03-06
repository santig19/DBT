{{ config(
	post_hook="grant all privileges on {{ this }} to {{ var('snowflake_user_grant_privileges') }}")
}}

select approved_vod__c,
       copied_from_vod__c,
       copy_date_vod__c,
       country_iso_code,
       createdbyid,
       createddate,
       currencyisocode,
       default_presentation_vod__c,
       hidden_vod__c,
       id,
       isdeleted,
       islocked,
       jj_campaign__c,
       jj_country__c,
       jj_in_line_with_hcbi_sops__c,
       jj_unitas_number__c,
       lastmodifiedbyid,
       lastmodifieddate,
       mayedit,
       mobile_id_vod__c,
       name,
       original_record_id_vod__c,
       parentid_vod__c,
       presentation_id_vod__c,
       product_vod__c,
       systemmodstamp,
       training_vod__c
  from {{ source('raw', 'clm_presentation') }}     
 group by approved_vod__c,
          copied_from_vod__c,
          copy_date_vod__c,
          country_iso_code,
          createdbyid,
          createddate,
          currencyisocode,
          default_presentation_vod__c,
          hidden_vod__c,
          id,
          isdeleted,
          islocked,
          jj_campaign__c,
          jj_country__c,
          jj_in_line_with_hcbi_sops__c,
          jj_unitas_number__c,
          lastmodifiedbyid,
          lastmodifieddate,
          mayedit,
          mobile_id_vod__c,
          name,
          original_record_id_vod__c,
          parentid_vod__c,
          presentation_id_vod__c,
          product_vod__c,
          systemmodstamp,
          training_vod__c
