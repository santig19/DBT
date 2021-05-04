{{ config(
	post_hook="grant all privileges on {{ this }} to {{ var('snowflake_user_grant_privileges') }}")
}}

SELECT
	ELOQUA.ActivityType AS Campaign_Email_Status,
	ELOQUA.AssetId AS AssetId,
	ELOQUA.ActivityDate AS Date,
	COALESCE(c.id, 'NM') AS Campaign_Id,
	a.id AS Account_Id,
     CASE WHEN LOWER(ELOQUA.ActivityType) = 'emailsend' THEN 1 ELSE NULL END::integer AS Campaign_Email_Send_Counter,
	CASE WHEN LOWER(ELOQUA.ActivityType) = 'emailclickthrough' THEN 1 ELSE NULL END::integer AS Campaign_Email_ClickThrough_Counter,
	CASE WHEN LOWER(ELOQUA.ActivityType) = 'emailopen' THEN 1 ELSE NULL END::integer AS Campaign_Email_Open_Counter,
	CASE WHEN LOWER(ELOQUA.ActivityType) = 'bounceback' THEN 1 ELSE NULL END::integer AS Campaign_Email_Bounced_Counter,
	ELOQUA.NumberPageViews::integer AS Campaign_Pageview_Counter,
	'Email Campaign' AS Origin,
	ELOQUA.EmailClickedThruLink AS EmailClickedThroughLink,
	
	--convert hours into seconds
	CASE
		WHEN ELOQUA.Duration LIKE '%H%' 
		THEN CASE WHEN SUBSTRING(ELOQUA.Duration, CHARINDEX('T',ELOQUA.Duration)+1, CHARINDEX('H',ELOQUA.Duration)-CHARINDEX('T',ELOQUA.Duration)-1) like '^[0-9]+$' 
				  THEN SUBSTRING(ELOQUA.Duration, CHARINDEX('T',ELOQUA.Duration)+1, CHARINDEX('H',ELOQUA.Duration)-CHARINDEX('T',ELOQUA.Duration)-1)::int * 3600 ELSE 0 END
		ELSE 0 
	END +
	--add minutes (converted into seconds)
	CASE 
		WHEN ELOQUA.Duration LIKE '%M%' 
		THEN CASE 
				--when there are hours, take everything between H and M
				WHEN ELOQUA.Duration LIKE '%H%'
				THEN CASE WHEN SUBSTRING(ELOQUA.Duration, CHARINDEX('H',ELOQUA.Duration)+1, CHARINDEX('M',ELOQUA.Duration)-CHARINDEX('H',ELOQUA.Duration)-1) like '^[0-9]+$'
						  THEN SUBSTRING(ELOQUA.Duration, CHARINDEX('H',ELOQUA.Duration)+1, CHARINDEX('M',ELOQUA.Duration)-CHARINDEX('H',ELOQUA.Duration)-1)::int * 60 ELSE 0 END
				--when there are No hours, take everything between T and M
				ELSE CASE WHEN SUBSTRING(ELOQUA.Duration, CHARINDEX('T',ELOQUA.Duration)+1, CHARINDEX('M',ELOQUA.Duration)-CHARINDEX('T',ELOQUA.Duration)-1) like '^[0-9]+$'
						  THEN SUBSTRING(ELOQUA.Duration, CHARINDEX('T',ELOQUA.Duration)+1, CHARINDEX('M',ELOQUA.Duration)-CHARINDEX('T',ELOQUA.Duration)-1)::int * 60 ELSE 0 END
				END
		--no minutes present, add 0
		ELSE 0
	END + 
	--add seconds
	CASE 
		WHEN ELOQUA.Duration LIKE '%S%' 
		THEN CASE
				--when there are minutes, take everything between M and S
				WHEN ELOQUA.Duration LIKE '%M%'
				THEN CASE WHEN SUBSTRING(ELOQUA.Duration, CHARINDEX('M',ELOQUA.Duration)+1, CHARINDEX('S',ELOQUA.Duration)-CHARINDEX('M',ELOQUA.Duration)-1) like '^[0-9]+$'
						  THEN SUBSTRING(ELOQUA.Duration, CHARINDEX('M',ELOQUA.Duration)+1, CHARINDEX('S',ELOQUA.Duration)-CHARINDEX('M',ELOQUA.Duration)-1)::int ELSE 0 END
				ELSE CASE 
						  --when there are hours, take everything between H and S'
						  WHEN ELOQUA.Duration LIKE '%H%'
						  THEN CASE WHEN SUBSTRING(ELOQUA.Duration, CHARINDEX('H',ELOQUA.Duration)+1, CHARINDEX('S',ELOQUA.Duration)-CHARINDEX('H',ELOQUA.Duration)-1) like '^[0-9]+$'
						  			THEN SUBSTRING(ELOQUA.Duration, CHARINDEX('H',ELOQUA.Duration)+1, CHARINDEX('S',ELOQUA.Duration)-CHARINDEX('H',ELOQUA.Duration)-1)::int ELSE 0 END
						  --when there are No hours, take everything between T and S'
						  ELSE CASE WHEN SUBSTRING(ELOQUA.Duration, CHARINDEX('T',ELOQUA.Duration)+1, CHARINDEX('S',ELOQUA.Duration)-CHARINDEX('T',ELOQUA.Duration)-1) like '^[0-9]+$'
						  			THEN SUBSTRING(ELOQUA.Duration, CHARINDEX('T',ELOQUA.Duration)+1, CHARINDEX('S',ELOQUA.Duration)-CHARINDEX('T',ELOQUA.Duration)-1)::int ELSE 0 END
					END 
				END
		--no seconds present, add 0
		ELSE 0 
	END::numeric(10,0) AS Campaign_Pageview_Duration,
	
	split_part(ELOQUA.url, '?', 1)::varchar(1000) AS Campaign_pageview_url,
	split_part(ELOQUA.firstpageviewUrl, '?', 1)::varchar(1000) AS Campaign_WebSite_Url,

	CASE WHEN LOWER(ELOQUA.ActivityType) = 'formsubmit' THEN ELOQUA.AssetName ELSE NULL END::varchar(255) AS Campaign_Form_Name,
	CASE WHEN LOWER(ELOQUA.ActivityType) IN ('emailopen', 'emailclickthrough', 'emailsend') THEN ELOQUA.AssetName ELSE NULL END::varchar(255) AS Campaign_Email_Name,
	CASE WHEN LOWER(ELOQUA.ActivityType) = 'formsubmit' THEN 1 ELSE NULL END::integer AS Campaign_Form_Counter
	
FROM (
    	SELECT
		ActivityId,
		ActivityType,
		ActivityDate,
		AssetId,
		AssetName,
		CampaignId AS Campaign_Id,
		CampaignCrmId AS Campaign_CrmId,
		ContactId,
		Contact_Id,
		Contactsfdcid,
		EmailClickedThruLink,
		EmailAddress,
		NULL AS WebVisitId,
		NULL AS Url,
		NULL AS NumberPageViews,
		NULL AS Duration,
		NULL AS FirstPageViewUrl
	FROM (
		SELECT DISTINCT ActivityId, ActivityType, ActivityDate, AssetId, AssetName, CampaignId, CampaignCrmId, ContactId, 
		'CJANS' || left('00000000000000000',12-length(ContactId)) || ContactId as Contact_Id, null as EmailClickedThruLink, null as EmailAddress, Contactsfdcid
		FROM {{ var('schema') }}.eloqua_emailopen_raw
		
		UNION
		
		SELECT DISTINCT ActivityId, ActivityType, ActivityDate, AssetId, AssetName, CampaignId, CampaignCrmId, null as ContactId, 
		null as Contact_Id, null as EmailClickedThruLink, EmailAddress, Contactsfdcid
		FROM {{ var('schema') }}.eloqua_bounceback_raw
		
		UNION
		
		SELECT DISTINCT ActivityId, ActivityType, ActivityDate, AssetId, AssetName, CampaignId, CampaignCrmId, ContactId, 
		'CJANS' || left('00000000000000000',12-length(ContactId)) || ContactId as Contact_Id, EmailClickedThruLink, null as EmailAddress, Contactsfdcid
		FROM {{ var('schema') }}.eloqua_emailclickthrough_raw
		
		UNION
		
		SELECT DISTINCT ActivityId, ActivityType, ActivityDate, null as assetid, AssetName, CampaignId, CampaignCrmId, ContactId, 
		'CJANS' || left('00000000000000000',12-length(ContactId)) || ContactId as Contact_Id, null as EmailClickedThruLink, null as EmailAddress, Contactsfdcid
		FROM {{ var('schema') }}.eloqua_emailsend_raw
		
		UNION
	
		SELECT DISTINCT ActivityId, ActivityType, ActivityDate, null as assetid, AssetName, CampaignId, CampaignCrmId, null as ContactId,	
		'CJANS' || left('00000000000000000',12-length(ContactId)) || ContactId as Contact_Id, null as EmailClickedThruLink, null as EmailAddress, Contactsfdcid
		FROM {{ var('schema') }}.eloqua_formsubmit_raw
		)
	
	UNION
	
	SELECT 
		ActivityId,
		ActivityType,
		ActivityDate,
		NULL AS AssetId,
		NULL AS AssetName,
		CampaignId,
		CampaignCrmId,
		ContactId,
		Contact_Id,
		Contactsfdcid,
		NULL AS EmailClickedThruLink,
		NULL AS EmailAdress,
		WebVisitId,
		Url,
		NumberPageViews,
		Duration,
		FirstPageViewUrl
	FROM (
		SELECT DISTINCT 
			pv.ActivityId,
			pv.ActivityType,
			pv.ActivityDate,
			pv.CampaignId,
			pv.CampaignCrmId,
			pv.ContactId,
			'CJANS' || left('00000000000000000',12-length(pv.ContactId)) || pv.ContactId as Contact_Id, 
			pv.Contactsfdcid,
			pv.WebVisitId,
			pv.Url,
			NPV.NumberPageViews,
			wv.Duration,
			wv.FirstPageViewUrl
		FROM {{ var('schema') }}.eloqua_pageview_raw pv
		LEFT OUTER JOIN {{ var('schema') }}.eloqua_webvisit_raw wv ON wv.ActivityId = pv.WebVisitId
		LEFT OUTER JOIN (
			SELECT COUNT(DISTINCT(pvw.ActivityId)) as NumberPageViews, pvw.WebVisitId as WVID  
			FROM {{ var('schema') }}.eloqua_pageview_raw pvw
			GROUP BY pvw.WebVisitId
		) AS NPV ON NPV.WVID = pv.WebVisitId
	)
) AS ELOQUA

LEFT OUTER JOIN ( select * from (
select * , ROW_NUMBER() OVER(PARTITION BY ID ORDER BY SYSTEMMODSTAMP DESC) AS row1
  from {{ var('schema') }}.campaign_raw ) a
 where a.row1 = 1 ) c ON ELOQUA.Campaign_CrmId = c.id 

-- us-48166
-- edit left outer join because of multiple countrys associated to the same campaign_id 

LEFT OUTER JOIN {{ var('schema') }}.account_raw a ON ELOQUA.Contactsfdcid = a.id
