{{ config(
	post_hook="grant all privileges on {{ this }} to {{ var('snowflake_user_grant_privileges') }}")
}}

SELECT 
Campaign_Clickthrough_Id, 
campaign_email_clickthrough_counter, 
campaign_email_total_clickthrough_counter, 
Campaign_email_clickthrough_region 
--,Asset_Id 
FROM
(
SELECT
	 (aux_eloqua.campaign_id || '-' || aux_eloqua.account_id || '-' || aux_eloqua.assetid)::varchar(255) AS Campaign_Clickthrough_Id
	,COALESCE(aggr2.campaign_email_clickthrough_counter, aggr.campaign_email_clickthrough_counter, 0)::numeric(10, 0) as campaign_email_clickthrough_counter
	,COALESCE(aggr2.campaign_email_total_clickthrough_counter, aggr.campaign_email_total_clickthrough_counter, 0)::numeric(10, 0) as campaign_email_total_clickthrough_counter
	,(CASE 
		WHEN aux_eloqua.account_id = account.id
    	THEN CASE
          		WHEN account.country_JJ__c = jj_country_settings.jj_Country_ISO_code__c
          		THEN jj_country_settings.jj_region__c
           		ELSE 'NM'
          	END                 
    	ELSE 'NM'
	END)::varchar(255) AS Campaign_email_clickthrough_region,
	aux_eloqua.assetid::varchar(255) AS Asset_Id
FROM {{ ref('tmp_f_activity_email_eloqua') }} AS aux_eloqua      
LEFT OUTER JOIN {{ var('schema') }}.account_raw AS account ON aux_eloqua.account_id = account.id
LEFT OUTER JOIN {{ var('schema') }}.country_settings_raw AS JJ_Country_settings ON account.country_JJ__c = jj_country_settings.jj_country_iso_code__c
JOIN (
	select
		 campaign_id || '-' || account_id || '-' || assetid as Campaign_clickthrough_Id
		,MAX(campaign_email_clickthrough_counter) AS Campaign_email_clickthrough_counter
		,SUM(campaign_email_clickthrough_counter) AS Campaign_email_total_clickthrough_counter
	from (
		select campaign_id, account_id, assetid , LEFT(date, 10), campaign_email_clickthrough_counter
		from {{ ref('tmp_f_activity_email_eloqua') }}
		group by campaign_id, account_id, assetid , LEFT(date, 10), campaign_email_clickthrough_counter
		)
	group by campaign_id, account_id, assetid 
) aggr on aggr.Campaign_clickthrough_Id = aux_eloqua.campaign_id || '-' || aux_eloqua.account_id || '-' || aux_eloqua.assetid 
--For EmailOpen & Clickthrough group on full date
LEFT OUTER JOIN (
	select
		 campaign_id || '-' || account_id || '-' || assetid as Campaign_clickthrough_Id
		,MAX(campaign_email_clickthrough_counter) AS Campaign_email_clickthrough_counter
		,SUM(campaign_email_clickthrough_counter) AS Campaign_email_total_clickthrough_counter
	from {{ ref('tmp_f_activity_email_eloqua') }}
	where LOWER(campaign_email_status) IN ('emailopen', 'emailclickthrough')
	group by campaign_id, account_id, assetid 
) aggr2 on aggr2.Campaign_clickthrough_Id = aux_eloqua.campaign_id || '-' || aux_eloqua.account_id || '-' || aux_eloqua.assetid 
WHERE LEN(aux_eloqua.campaign_email_clickthrough_counter) > 0 AND LEN(aux_eloqua.account_id) > 0
)
GROUP BY
Campaign_Clickthrough_Id, 
campaign_email_clickthrough_counter, 
campaign_email_total_clickthrough_counter, 
Campaign_email_clickthrough_region
--,Asset_Id