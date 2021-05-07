{{ config(
	post_hook="grant all privileges on {{ this }} to {{ var('snowflake_user_grant_privileges') }}")
}}

select base.account_subtype,	      
       base.account_type, 	      
	   base.account_msl_contact,
	   base.account_position,
	   base.account_profession,
	   base.account_treatment_guidelines,
	   base.account_specialty_1_int,
	   base.account_specialty_2_int,
	   base.account_specialty_3_int,      
	   base.account_id,
	   base.account_veeva_id,
	   base.parent_account_id,
	   base.account_postal_code,
	   base.account_city,
	   base.account_cods_code,
	   base.external_id,	      
	   base.account_email,
	   base.account_government_official_flag,
	   base.account_has_email,
       base.account_brick,	      
       base.account_leading_specialist,	      
       base.account_group_specialty,
	   base.account_group_specialty_2,
	   base.account_group_specialty_3, 	      
       base.account_emea_international_speakers,
	   base.account_infotype,
	   base.account_gender,   
       base.account_target_class,
	   base.account_channel_profile_segmentation,
	   base.account_country_code, 
       base.account_name,
	   base.account_record_type,
       base.account_status,
	   base.account_individual_organization,
	   base.account_type_emea,	      
       base.account_ultimate_account_name,
	   base.account_ultimate_account_id,
	   base.account_primary_parent_id,
       base.account_country, 
       base.account_vod_c, 
       base.account_traditional_mail_optin_out,
       base.account_additional_email,
       base.account_address_completeness,
       base.account_address,
       base.account_brick_number, 
       base.account_date_of_birth, 
       base.account_do_not_email, 
       base.account_email_bounced, 
       base.account_email_optin_completeness,
       base.account_email_optin_out,
       base.account_f2f_optin_out,
       base.account_fax_optin_out,
       base.account_field_of_interests_completeness,
       base.account_fields_of_interest,
       base.account_first_name,
       base.account_fixed_phone,
       base.account_ispersonaccount,
       base.account_last_name,
       base.account_local_identifier_1,
       base.account_login_for_janssen_websites,
       base.account_person_mobile_phone,
       base.account_phone_1,
       base.account_phone_completeness,
       base.account_phone_optin_out,
       base.account_phone,
       base.account_postal_mail_optin_out,
       base.account_province, 
       base.account_region,
       base.account_salutation,
       base.account_sms_optin_out,
       base.account_specialty_1,
       base.account_specialty_2,
       base.account_title,
       base.account_veeva_link,
       base.account_additional_brick_number,
       base.account_additional_brick,
       base.account_disclosure_transfer_of_value,
       base.account_ultimate_account,
       base.account_hospital_name,
       base.account_mobile_phone,
       base.account_cv,
       base.account_hcp_status,
       base.account_hcps_gos_requiring_hcco_approval,
       base.account_msl_target,
       base.account_specialty_3,
	   base.account_Level_1,
	   base.account_primary_parent,
	   base.account_departments,
	   base.patient_organization,
	   base.veeva_link_url,
	   base.tov_disclosure,
	   base.account_available_email
  from (
        select  case 
        	      	when lower(account.ispersonaccount) in ('yes','1','true') and len(account.individual_sub_type_jj__c)>0 
                    then account.individual_sub_type_jj__c 
        	      		else (case when len(account.jj_account_sub_type__c)>0 then account.jj_account_sub_type__c else 'No value' end)
                end::varchar(255) as account_subtype,
 	      
                case 
        	      	when lower(account.ispersonaccount) in ('yes','1','true') and len(account.individual_type__c)>0
        	      		then account.individual_type__c 
        	      		else case when len(account.account_type__c)>0 then account.account_type__c else 'No Value' end
                end::varchar(255) as account_type,
        	      
        	    case when lower(account.jj_msl_target__c) in ('yes','1','true') then 'True'                             else 'No Value' end::varchar(255) as account_msl_contact,
        	    case when len(account.jj_position__c)>0                         then account.jj_position__c             else 'No Value' end::varchar(255) as account_position,
        	    case when len(account.jj_profession__c)>0                       then account.jj_profession__c           else 'No Value' end::varchar(255) as account_profession,
        	    case when len(account.treatment_guidelines_jj__c)>0             then account.treatment_guidelines_jj__c else 'No Value' end::varchar(255) as account_treatment_guidelines,
        	    case when len(account.specialty_1_int_jj__c)>0                  then account.specialty_1_int_jj__c      else 'No Value' end::varchar(255) as account_specialty_1_int,
        	    case when len(account.specialty_2_int_jj__c)>0                  then account.specialty_2_int_jj__c      else 'No Value' end::varchar(255) as account_specialty_2_int,
        	    case when len(account.specialty_3_int_jj__c)>0                  then account.specialty_3_int_jj__c      else 'No Value' end::varchar(255) as account_specialty_3_int,
	      
        	    account.id                      as account_id,
        	    account.id                      as account_veeva_id,
        	    account.primary_parent_vod__c   as parent_account_id,
        	    account.jj_postal_code__c       as account_postal_code,
        	    account.city_jj__c              as account_city,
        	    account.cods_external_id__c     as account_cods_code,
        	    account.external_id_jj__c       as external_id,
	      
        	    case when lower(account.ispersonaccount)                in ('yes','1','true')                                                          then account.personemail else account.email_jj__c end::varchar(255)  as account_email,
        	    case when lower(account.jj_government_official__c)      in ('yes','1','true')                                                          then 'Yes'               else 'No'                end::varchar(255)  as account_government_official_flag,
                case when len(case when lower(account.ispersonaccount)  in ('yes','1','true') then account.personemail else account.email_jj__c end)>0 then 'Yes'               else 'No' end::varchar(255)                 as account_has_email,
	      	
        	    account.brick_jj__c as account_brick,
	      
                case when lower(account.leading_specialist_jj__c) in ('yes','1','true') then 'Yes' else 'No' end::varchar(255) as account_leading_specialist,
	      
                gsss1.jj_group_specialty__c as account_group_specialty,
				gsss2.jj_group_specialty__c as account_group_specialty_2,
        	    gsss3.jj_group_specialty__c as account_group_specialty_3,
       	      
                case when len(account.jj_cp_status__c)>0 then account.jj_cp_status__c                                             else 'No Value' end::varchar(255) as account_emea_international_speakers,
				case when len(account.jj_infotype__c)>0  then account.jj_infotype__c                                              else 'No Value' end::varchar(255) as account_infotype,
				case when len (account.gender_vod__c)>0  then (case when account.gender_vod__c='M' then 'Male' else 'Female' end) else 'No Value' end::varchar(255) as account_gender,
	      
                case when account.jj_target_class__c=''     then 'No Value'                 else account.jj_target_class__c end::varchar(255) as account_target_class,
				case when len(account.jj_channel_pref__c)>0 then account.jj_channel_pref__c else 'No Value' end::varchar(255)                 as account_channel_profile_segmentation,
				case when len(account.country_jj__c)>0      then account.country_jj__c      else 'No Value' end::varchar(255)                 as account_country_code,
	      
                account.name           as account_name,
				rt.account_record_type as account_record_type,
 	      
                case when len(account.ok_status_jj__c )>0                          then account.ok_status_jj__c  else 'No Value' end::varchar(255)     as account_status,
				case when lower(account.ispersonaccount) in ('yes','1','true')     then 'Individual'             else 'Organization' end::varchar(255) as account_individual_organization,
				nvl(case when lower(account.ispersonaccount) in ('yes','1','true') then trim(mp1.name)           else trim(mp2.name) end,'No Value')   as account_type_emea,
	      
                account.ultimate_account_jj__c      as account_ultimate_account_name,
				account.jj_ultimate_account_cods__c as account_ultimate_account_id,
				account.primary_parent_vod__c       as account_primary_parent_id,

                country.name                                                                   as account_country, 
                account.id                                                                     as account_vod_c,
                campaign.jj_number_of_traditional_mails_sent__c                                as account_traditional_mail_optin_out,
                account.jj_additional_email__c                                                 as account_additional_email,
                case when len(trim(account.jj_address_line_1__c)) > 0 then 'Yes' else 'No' end as account_address_completeness,
                account.jj_address_line_1__c                                                   as account_address,
                account.brick_number_jj__c                                                     as account_brick_number, 
                account.personbirthdate                                                        as account_date_of_birth,
                account.do_not_email_jj__c                                                     as account_do_not_email, 
                account.jj_emailbounced__c                                                     as account_email_bounced,
                'N/A'                                                                          as account_email_optin_completeness,
                'N/A'                                                                          as account_email_optin_out,
                account.jj_f2f__c                                                              as account_f2f_optin_out,
                account.fax                                                                    as account_fax_optin_out,
                account.jj_areas_of_interests__c                                               as account_field_of_interests_completeness,
                account.jj_areas_of_interests__c                                               as account_fields_of_interest,
                account.first_name_latin_jj__c                                                 as account_first_name,
                'N/A'                                                                          as account_fixed_phone,
                account.ispersonaccount                                                        as account_ispersonaccount,
                account.last_name_latin_jj__c                                                  as account_last_name,
                account.account_identifier_vod__c                                              as account_local_identifier_1,
                account.jj_login_for_janssen_websites__c                                       as account_login_for_janssen_websites,
                account.personmobilephone                                                      as account_person_mobile_phone,
                account.jj_phone_1__c                                                          as account_phone_1,
                account.jj_additional_mobile_phone__c                                          as account_phone_completeness,
                'N/A'                                                                          as account_phone_optin_out,
                account.phone                                                                  as account_phone,
                'N/A'                                                                          as account_postal_mail_optin_out,
                account.jj_region_province__c                                                  as account_province, 
                account.jj_region__c                                                           as account_region,
                account.salutation                                                             as account_salutation,
                account.jj_sms__c                                                              as account_sms_optin_out,

                case when len(account.specialty_1_jj__c)>0 then account.specialty_1_jj__c else 'No Value' end::varchar(255) as account_specialty_1,
                case when len(account.specialty_2_jj__c)>0 then account.specialty_2_jj__c else 'No Value' end::varchar(255) as account_specialty_2,

                account.persontitle                                                            as account_title,
                account.veevaid_vod__c                                                         as account_veeva_link,
                account.additional_brick_number_jj__c                                          as account_additional_brick_number,
                account.additional_brick_jj__c                                                 as account_additional_brick,
                account.jj_transfer_of_value__c                                                as account_disclosure_transfer_of_value,
                account.ultimate_account_jj__c                                                 as account_ultimate_account,
                account.primary_parent_vod__c                                                  as account_hospital_name,
                account.personmobilephone                                                      as account_mobile_phone,
                'N/A'                                                                          as account_cv,
                account.jj_cp_status__c                                                        as account_hcp_status,
                account.jj_government_official__c                                              as account_hcps_gos_requiring_hcco_approval,
                account.jj_msl_target__c                                                       as account_msl_target,
                
                case when len(account.jj_specialty_3__c)>0 then account.jj_specialty_3__c else 'No Value' end::varchar(255) as account_specialty_3,
				
				account.ultimate_account_jj__c      as Account_Level_1,
				account_name.name as account_primary_parent,
				case when account.departments__c = '' then null else cast(account.departments__c as numeric(18,0)) end::numeric(18,0) as account_departments,
				case when account.JJ_IS_PATIENT_ORGANISATION__C = '' then null else account.JJ_IS_PATIENT_ORGANISATION__C end::integer as patient_organization,
				CASE 
					WHEN (Position(CHR(92) || CHR(92), account.VL_URL_VOD__C) > 0) OR Position(CHR(124) || CHR(34), account.VL_URL_VOD__C) > 0 OR Position(CHR(92) || CHR(124), account.VL_URL_VOD__C) > 0 OR Position(CHR(92) || CHR(34), account.VL_URL_VOD__C) > 0
						THEN REPLACE(REPLACE(REPLACE(REPLACE(account.VL_URL_VOD__C, CHR(92) || CHR(92), CHR(92)), CHR(124) || CHR(34), CHR(34)), CHR(92) || CHR(124), CHR(124)), CHR(92) || CHR(34), CHR(34))
					ELSE account.VL_URL_VOD__C
				END::varchar(510) as veeva_link_url,
				account.JJ_ToV_disclosure_current_year__c::varchar(510) as tov_disclosure,
				case 
					when len(account.PersonEmail) > 0 then (case when (Position(CHR(92) || CHR(92), account.PersonEmail) > 0) OR Position(CHR(124) || CHR(34), account.PersonEmail) > 0 OR Position(CHR(92) || CHR(124), account.PersonEmail) > 0 OR Position(CHR(92) || CHR(34), account.PersonEmail) > 0 then REPLACE(REPLACE(REPLACE(REPLACE(account.PersonEmail, CHR(92) || CHR(92), CHR(92)), CHR(124) || CHR(34), CHR(34)), CHR(92) || CHR(124), CHR(124)), CHR(92) || CHR(34), CHR(34)) ELSE account.PersonEmail end)
					when len(account.Email_JJ__c) > 0 then (case when (Position(CHR(92) || CHR(92), account.Email_JJ__c) > 0) OR Position(CHR(124) || CHR(34), account.Email_JJ__c) > 0 OR Position(CHR(92) || CHR(124), account.Email_JJ__c) > 0 OR Position(CHR(92) || CHR(34), account.Email_JJ__c) > 0 then REPLACE(REPLACE(REPLACE(REPLACE(account.Email_JJ__c, CHR(92) || CHR(92), CHR(92)), CHR(124) || CHR(34), CHR(34)), CHR(92) || CHR(124), CHR(124)), CHR(92) || CHR(34), CHR(34)) ELSE account.Email_JJ__c end)
					when len(account.JJ_Additional_Email__c) > 0 then (case when (Position(CHR(92) || CHR(92), account.JJ_Additional_Email__c) > 0) OR Position(CHR(124) || CHR(34), account.JJ_Additional_Email__c) > 0 OR Position(CHR(92) || CHR(124), account.JJ_Additional_Email__c) > 0 OR Position(CHR(92) || CHR(34), account.JJ_Additional_Email__c) > 0 then REPLACE(REPLACE(REPLACE(REPLACE(account.JJ_Additional_Email__c, CHR(92) || CHR(92), CHR(92)), CHR(124) || CHR(34), CHR(34)), CHR(92) || CHR(124), CHR(124)), CHR(92) || CHR(34), CHR(34)) ELSE account.JJ_Additional_Email__c end)
					else 'No Value'
				end::varchar(255) as account_available_email
								
          from {{ var('schema')}}.account_raw as account
          left join (
                      select  min(jj_group_specialty__c) as jj_group_specialty__c,
                              jj_codsid__c as jj_codsid__c 
                        from {{ var('schema')}}.group_speciality_segment_setting_raw
                       group by jj_codsid__c
                    ) as gsss1 
                on account.group_specialty_1_vod__c = gsss1.jj_codsid__c
          left join (
                      select  min(jj_group_specialty__c) as jj_group_specialty__c,
                              jj_codsid__c as jj_codsid__c 
                        from {{ var('schema')}}.group_speciality_segment_setting_raw
                       group by jj_codsid__c
                    ) as gsss2
                 on account.group_specialty_2_vod__c = gsss2.jj_codsid__c
          left join (
                      select  min(jj_group_specialty__c) as jj_group_specialty__c,
                              jj_codsid__c as jj_codsid__c 
                        from {{ var('schema')}}.group_speciality_segment_setting_raw
                       group by jj_codsid__c
                    ) as gsss3
                 on account.jj_group_specialty_3__c = gsss3.jj_codsid__c
          left join {{ var('schema')}}.map_account_type_account_type_description_raw as mp1
				on trim(mp1.code) = trim(account.jj_individual_type_code__c)
          left join {{ var('schema')}}.map_account_type_account_type_description_raw as mp2 
				on trim(mp2.code) = trim(account.jj_account_type_code__c)
          left join (
                      select id,
                             account_record_type
                        from (
                              select id, case when len(max(mappedname))>0 then max(mappedname) else max(name) end::varchar(255) as account_record_type 
                                from (
                                      select rt.id, 
                                             value as mappedname, 
                                             rt.name
                                        from      {{ var('schema')}}.record_type_raw as rt
                                        left join {{ var('schema')}}.record_type_localization_raw as rtl 
                                          on rtl.parentid = rt.id
                                       where rtl.language = 'en_US'
                                      ) as rt 
                               group by id
                       )rt
                       group by id,account_record_type
                    ) as rt 
				on rt.id = account.recordtypeid
          left join (
                      select sum(c.jj_number_of_traditional_mails_sent__c) as jj_number_of_traditional_mails_sent__c,
                             ct.target_account_vod__c                      as target_account_vod__c
                        from      {{ var('schema')}}.campaign_target_raw as ct
                        left join {{ var('schema')}}.campaign_raw        as c
                          on c.id                                                       = ct.campaign_vod__c
                       group by ct.target_account_vod__c                       
                    ) as campaign
                 on campaign.target_account_vod__c = account.id     
          left join (
                     select id, name 
                     from {{ var('schema')}}.account_raw as account_name
                    ) as  account_name
                  on account_name.id  = account.primary_parent_vod__c
          left join (
                     select country_iso_code, name 
                       from {{ var('schema')}}.country_settings_raw as country
					  group by country_iso_code, name
                    ) as  country
                  on country.country_iso_code  = ifnull(account.country_jj__c, account.jj_country__c)
       ) base
 group by base.account_subtype,	      
          base.account_type, 	      
          base.account_msl_contact,
          base.account_position,
          base.account_profession,
          base.account_treatment_guidelines,
          base.account_specialty_1_int,
          base.account_specialty_2_int,
          base.account_specialty_3_int,      
          base.account_id,
          base.account_veeva_id,
          base.parent_account_id,
          base.account_postal_code,
          base.account_city,
          base.account_cods_code,
          base.external_id,	      
          base.account_email,
          base.account_government_official_flag,
          base.account_has_email,
          base.account_brick,	      
          base.account_leading_specialist,	      
          base.account_group_specialty,
          base.account_group_specialty_2,
          base.account_group_specialty_3, 	      
          base.account_emea_international_speakers,
          base.account_infotype,
          base.account_gender,   
          base.account_target_class,
          base.account_channel_profile_segmentation,
          base.account_country_code, 
          base.account_name,
          base.account_record_type,
          base.account_status,
          base.account_individual_organization,
          base.account_type_emea,	      
          base.account_ultimate_account_name,
          base.account_ultimate_account_id,
          base.account_primary_parent_id,
          base.account_country, 
          base.account_vod_c, 
          base.account_traditional_mail_optin_out,
          base.account_additional_email,
          base.account_address_completeness,
          base.account_address,
          base.account_brick_number, 
          base.account_date_of_birth, 
          base.account_do_not_email, 
          base.account_email_bounced, 
          base.account_email_optin_completeness,
          base.account_email_optin_out,
          base.account_f2f_optin_out,
          base.account_fax_optin_out,
          base.account_field_of_interests_completeness,
          base.account_fields_of_interest,
          base.account_first_name,
          base.account_fixed_phone,
          base.account_ispersonaccount,
          base.account_last_name,
          base.account_local_identifier_1,
          base.account_login_for_janssen_websites,
          base.account_person_mobile_phone,
          base.account_phone_1,
          base.account_phone_completeness,
          base.account_phone_optin_out,
          base.account_phone,
          base.account_postal_mail_optin_out,
          base.account_province, 
          base.account_region,
          base.account_salutation,
          base.account_sms_optin_out,
          base.account_specialty_1,
          base.account_specialty_2,
          base.account_title,
          base.account_veeva_link,
          base.account_additional_brick_number,
          base.account_additional_brick,
          base.account_disclosure_transfer_of_value,
          base.account_ultimate_account,
          base.account_hospital_name,
          base.account_mobile_phone,
          base.account_cv,
          base.account_hcp_status,
          base.account_hcps_gos_requiring_hcco_approval,
          base.account_msl_target,
          base.account_specialty_3,
          base.account_Level_1,
          base.account_primary_parent,
          base.account_departments,
		  base.patient_organization,
	      base.veeva_link_url,
		  base.tov_disclosure,
	      base.account_available_email