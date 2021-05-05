{{ config(
	post_hook="grant all privileges on {{ this }} to {{ var('snowflake_user_grant_privileges') }}")
}}

SELECT 
Campaign_open_id,
campaign_email_open_counter,
campaign_email_total_open_counter,
campaign_email_open_region
FROM
(
SELECT
	 (aux_eloqua.campaign_id ||'-'|| aux_eloqua.account_id ||'-'|| aux_eloqua.campaign_email_name)::varchar(255) AS Campaign_open_id
	,COALESCE(aggr2.campaign_email_open_counter, aggr.campaign_email_open_counter, 0)::numeric(10, 0) as campaign_email_open_counter
	,COALESCE(aggr2.campaign_email_total_open_counter, aggr.campaign_email_total_open_counter, 0)::numeric(10, 0) as campaign_email_total_open_counter
	,(CASE 
    	WHEN aux_eloqua.account_id = account.id
    	THEN CASE
            	WHEN account.country_JJ__c = jj_country_settings.jj_Country_ISO_code__c
               THEN jj_country_settings.jj_region__c 
               ELSE 'NM'
            END                 
    	ELSE 'NM'   
	END)::varchar(255) AS campaign_email_open_region
FROM {{ ref('tmp_f_activity_email_eloqua') }} AS aux_eloqua                                                                     
LEFT OUTER JOIN {{ var('schema') }}.account_raw AS account ON aux_eloqua.account_id = account.id
LEFT OUTER JOIN {{ var('schema') }}.country_settings_raw AS JJ_Country_settings ON account.country_JJ__c = jj_country_settings.jj_country_iso_code__c
JOIN (
	select
		 campaign_id || '-' || account_id || '-' || campaign_email_name as Campaign_open_Id
		,MAX(campaign_email_open_counter) AS Campaign_email_open_counter
		,SUM(campaign_email_open_counter) AS Campaign_email_total_open_counter
	from (
		select campaign_id, account_id, campaign_email_name, LEFT(date, 10), campaign_email_open_counter
		from {{ ref('tmp_f_activity_email_eloqua') }}
		where LOWER(campaign_email_status) != 'emailopen'
		group by campaign_id, account_id, campaign_email_name, LEFT(date, 10), campaign_email_open_counter
		)
	group by campaign_id, account_id, campaign_email_name
) aggr on aggr.Campaign_open_Id = aux_eloqua.campaign_id || '-' || aux_eloqua.account_id || '-' || aux_eloqua.campaign_email_name
--For EmailOpen group on full date
LEFT OUTER JOIN (
	select
		 campaign_id || '-' || account_id || '-' || campaign_email_name as Campaign_open_Id
		,MAX(campaign_email_open_counter) AS Campaign_email_open_counter
		,SUM(campaign_email_open_counter) AS Campaign_email_total_open_counter
	from {{ ref('tmp_f_activity_email_eloqua') }}
	where LOWER(campaign_email_status) = 'emailopen'
	group by campaign_id, account_id, campaign_email_name
) aggr2 on aggr2.Campaign_open_Id = aux_eloqua.campaign_id || '-' || aux_eloqua.account_id || '-' || aux_eloqua.campaign_email_name
WHERE LEN(aux_eloqua.campaign_email_open_counter) > 0 AND LEN(aux_eloqua.account_id) > 0
)
GROUP BY
Campaign_open_id,
campaign_email_open_counter,
campaign_email_total_open_counter,
campaign_email_open_region