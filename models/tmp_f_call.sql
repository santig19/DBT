{{ config(
	post_hook="grant all privileges on {{ this }} to {{ var('snowflake_user_grant_privileges') }}")
}}

SELECT 
c.JJ_Account_Plan__c As Account_Plan_Id,
c.JJ_Therapeutical_Area__c As Call_Therapeutical_Area,
concat(c.Account_vod__c,'_',c.OwnerId,'_',case when c.OwnerId = ut.UserId then ut.TerritoryId else 'NM' end,'_',c.Id) As Call_Counter_And_Expenses_Id,
c.Account_vod__c As Account_Id,
concat(case when Len(a.Country_JJ__c)>0 Then a.Country_JJ__c else a.JJ_Country__c end,'_',case when Len(a.Brick_Number_JJ__c)>0 Then a.Brick_Number_JJ__c else 'No Value' end) As Brick_Code,
c.Account_vod__c As Assignment_Id,
c.Territory_vod__c As Call_Territory,
CASE 
	WHEN c.Account_vod__c = a.Id AND LEN(a.CODS_external_id__c)>0 THEN (CASE WHEN a.Country_jj__C IS NULL THEN a.jj_country__C ELSE a.Country_jj__C END)
	WHEN US.id = c.OwnerId THEN cs.country_iso_code
	ELSE 'NM' 
END::varchar(2) AS Country_Code, 
c.Attendee_Type_vod__c As Call_Attendee_Type,
c.Attendees_vod__c As Call_Attendees,																							
c.Call_Comments_vod__c As Call_Comments,
case when (c.Call_Datetime_vod__c != '') then to_char(to_date(c.Call_Datetime_vod__c,'YYYYMMDD HH24:MI:SS'),'dd-mm-yyyy')::varchar(10) else null end As Date,
case when (c.Call_Datetime_vod__c != '') then to_char(to_date(c.Call_Datetime_vod__c,'YYYYMMDD HH24:MI:SS'),'dd-mm-yyyy')::varchar(10) else null end As Call_Date,
case when (c.Call_Datetime_vod__c != '') then to_char(to_timestamp(c.Call_Datetime_vod__c,'YYYYMMDD HH24:MI:SS'),'yyyy-mm-dd HH24:MI:SS')::varchar(19) else null end As Call_Timestamp,
case when (c.Call_Objective__c = '') then 'No Value' else c.Call_Objective__c end::varchar(255) As Call_Objective,
case when (c.Call_Type_jj__c = '') then 'No Value' else c.Call_Type_jj__c end::varchar(255) As Call_Channel,
case when (c.Call_Type_vod__c = '') then 'No Value' else c.Call_Type_vod__c end::varchar(255) As Call_Type,
case when (c.CLM_vod__c = 1) then 'Yes' else case when (c.CLM_vod__c = 0) then 'No' else 'No Value' end end::varchar(8) As Call_CLM,
c.CurrencyIsoCode::varchar(3) As Call_Currency_Code,
'EUR' As Call_Group_Currency,
c.Duration_vod__c As Call_Duration,
c.Id As Call_Id,
c.Is_Parent_Call_vod__c As is_parent_call,
c.JJ_Attendee_Status__c As Call_Attendee_Status,
case when( c.jj_Call_Status__c = '') then 'No Value' else c.jj_Call_Status__c end::varchar(255) As Call_Status,
case when (c.JJ_Customer_Request__c <> '') then c.JJ_Customer_Request__c else '' end::varchar(255) As Call_Customer_Request_Number,
('Link<url>' || 'https://iconnect-emea--uat.cs9.my.salesforce.com/'|| case when (c.JJ_Customer_Request__c <> '') then c.JJ_Customer_Request__c else '' end)::varchar(255) As Call_Customer_Request_Veeva_Link,
case when c.Account_vod__c = a.Id then case when a.IsPersonAccount = '1' then 'Yes' else 'No' end ELSE 'NM' end::varchar(3) As Call_IspersonAccount,
ce.Type_vod__c As Call_Expense_Type,
case when( c.Last_Device_vod__c = '') then 'No Value' else c.Last_Device_vod__c end::varchar(255) As Call_Device,
c.Name As Call,
case when ch.cont_child=0 and case when (c.Account_vod__c = a.Id) then case when (a.IsPersonAccount = '1') then 'Yes' else 'No' end else 'NM' end = 'No' 
	then 'Yes' 
	else 'No' 
end::varchar(3) as Single_Call_to_Hospital,
c.Next_Call_Notes_vod__c As Next_Call_Notes,
c.Pre_Call_Notes_vod__c As Pre_Call_Notes,
case when(c.Proactive__c = '') then 'No Value' else c.Proactive__c end::varchar(255) As Call_Proactive,
case when(Len(rtl.Value)>0) then rtl.Value else rt.Name end::varchar(1000) As Call_Record_Type, 
CASE 
	WHEN c.jj_Call_Status__c = 'Submitted' AND (CASE WHEN(LEN(rtl.Value)>0) THEN rtl.Value ELSE rt.Name END) = 'Medical Call' 
		THEN 'Medical Interaction'
	WHEN c.jj_Call_Status__c = 'Submitted' AND (CASE WHEN(LEN(rtl.Value)>0) THEN rtl.Value ELSE rt.Name END) = 'Promotional Call'
		THEN 'Promotional Interaction'
	WHEN c.jj_Call_Status__c = 'Submitted' AND (CASE WHEN(LEN(rtl.Value)>0) THEN rtl.Value ELSE rt.Name END) = 'Other Call'
		THEN 'Other Interaction'
ELSE Null END::varchar(255) As Activity,
CASE 
	WHEN c.jj_Call_Status__c = 'Submitted' AND CASE WHEN(LEN(rtl.Value)>0) THEN rtl.Value ELSE rt.Name END IN ('Medical Call', 'Promotional Call', 'Other Call')
		THEN c.Call_Type_jj__c
ELSE Null END::varchar(255) As Activity_Type,
case when( c.JJ_Sub_Type__c = '') then 'No Value' else c.JJ_Sub_Type__c end::varchar(255) As Call_Subtype,
Concat('Visited','_',
case when Len(a.Country_JJ__c)>0 
	then case when (c.Account_vod__c = a.Id) then a.Country_JJ__c else a.JJ_Country__c end
	else case when (c.OwnerId=Us.Id and Us.jj_User_Country__c=cs.Name) then cs.jj_Country_ISO_Code__c end
end)::varchar(255) As Account_Visited_NonVisited_Technical,
'Call' As Origin,
c.OwnerId As Aux_Owner_Id,
c.User_vod__c As Aux_User_Id,
c.Parent_Call_vod__c As Aux_Parent_Call_Id,
c.VEEVA_REMOTE_MEETING_ID_VOD__C As veeva_remote_meeting_id,
c.jj_proactive_reactive_sales__c As call_proactive_reactive_flag,
case when c.JJ_AVERAGE_CALL_EXPENSES__C = '' then null else c.JJ_AVERAGE_CALL_EXPENSES__C end::decimal(28,2) as average_call_expenses_lc,
case when c.JJ_TOTAL_CALL_EXPENSES__C = '' then null else c.JJ_TOTAL_CALL_EXPENSES__C end::decimal(28,2) as total_call_expenses_lc,
case when c.ZVOD_ATTACHMENTS_VOD__C = '' then null else c.ZVOD_ATTACHMENTS_VOD__C end::integer as call_attachments
FROM {{ var('schema') }}.call_raw c 
LEFT JOIN (
	SELECT ut_uniq.id, ut_uniq.userid, ut.territory2id as territoryid
	FROM (
		SELECT min(id) as id, userid
		FROM (
			SELECT ut.Id, ut.UserId
			FROM {{ var('schema') }}.user_territory_association_raw ut
			JOIN {{ var('schema') }}.user_raw u on u.id = ut.userid AND u.isactive != 0
		)
		GROUP BY userid
	) ut_uniq 
	JOIN {{ var('schema') }}.user_territory_association_raw ut on ut.id = ut_uniq.id
) ut on c.OwnerId = ut.UserId
LEFT JOIN {{ var('schema') }}.user_raw us on c.OwnerId = us.Id
LEFT JOIN {{ var('schema') }}.account_raw a on c.Account_vod__c = a.Id
LEFT JOIN {{ var('schema') }}.record_type_raw rt on c.RecordTypeId = rt.Id
LEFT JOIN (
	SELECT ROW_NUMBER() OVER (PARTITION BY IsoCode ORDER BY lastmodifieddate DESC) as rowid,IsoCode 
	FROM {{ var('schema') }}.currency_type_raw
) cu on c.CurrencyIsoCode = cu.IsoCode and cu.rowid=1
LEFT JOIN (SELECT ROW_NUMBER() OVER (PARTITION BY parentid ORDER BY lastmodifieddate DESC) as rowid,parentid,value,Language 
	FROM {{ var('schema') }}.record_type_localization_raw 
	WHERE Language = 'en_US'
) rtl on rt.id=rtl.parentid and rtl.rowid=1
LEFT JOIN (
	SELECT ROW_NUMBER() OVER (PARTITION BY Name ORDER BY lastmodifieddate DESC) as rowid,name,country_iso_code,jj_Country_ISO_Code__c 
	FROM {{ var('schema') }}.country_settings_raw
) cs on Us.jj_User_Country__c=cs.Name and cs.rowid=1
LEFT JOIN (
	SELECT ROW_NUMBER() OVER ( partition by Account_vod__c,User_vod__c,Call2_vod__c order by lastmodifieddate asc) as rowid, Type_vod__c, Account_vod__c, User_vod__c, Call2_vod__c 
	FROM {{ var('schema') }}.call_expenses_raw 
) ce on concat(c.Account_vod__c,'_',c.User_vod__c,'_',c.Id) = concat(ce.Account_vod__c,'_',ce.User_vod__c,'_',ce.Call2_vod__c) and ce.rowid=1
LEFT JOIN (
	SELECT count(DISTINCT Child_Call) as cont_child, Parent_Call
	FROM {{ ref('tmp_call_hierarchy') }}
	GROUP BY Parent_Call
) ch on c.Name = ch.Parent_Call
WHERE case when c.Call_Datetime_vod__c != '' then to_char(to_date(c.Call_Datetime_vod__c,'yyyymmdd hh24:mi:ss'),'YYYY')>=to_char(add_months(CURRENT_DATE,-36),'YYYY') end