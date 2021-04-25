{{ config(
	post_hook="grant all privileges on {{ this }} to {{ var('snowflake_user_grant_privileges') }}")
}}

SELECT
	 t.ID as territory_id
	,t.NAME as territory
	,CASE 
		WHEN t1.NAME LIKE 'L1%' THEN t1.NAME
		WHEN t.NAME LIKE 'L1%' THEN t.NAME
	END as territory_EMEA
	,CASE
		WHEN t1.NAME LIKE 'L1%' OR t.NAME LIKE 'L1%' THEN
			CASE 
				WHEN t2.NAME LIKE 'L2%' THEN t2.NAME
				WHEN t.NAME NOT LIKE 'L1%' THEN t.NAME
			END
		ELSE NULL
	 END as territory_parent_region
	,CASE
		WHEN	(t1.NAME LIKE 'L1%' OR t.NAME LIKE 'L1%') AND
				(t2.NAME LIKE 'L2%' OR t.NAME LIKE 'L2%') THEN
			CASE 
				WHEN t3.NAME LIKE 'L3%' THEN t3.NAME
				WHEN t.NAME NOT LIKE 'L2%' THEN t.NAME
			END
		ELSE NULL
	END as territory_region
	,CASE
		WHEN	(t1.NAME LIKE 'L1%' OR t.NAME LIKE 'L1%') AND
				(t2.NAME LIKE 'L2%' OR t.NAME LIKE 'L2%') AND
				(t3.NAME LIKE 'L3%' OR t.NAME LIKE 'L3%') THEN
			CASE 
				WHEN t4.NAME LIKE 'L4%' THEN t4.NAME
				WHEN t.NAME NOT LIKE 'L3%' THEN t.NAME
			END
		ELSE NULL
	END as territory_loc
	,CASE
		WHEN	(t1.NAME LIKE 'L1%' OR t.NAME LIKE 'L1%') AND
				(t2.NAME LIKE 'L2%' OR t.NAME LIKE 'L2%') AND
				(t3.NAME LIKE 'L3%' OR t.NAME LIKE 'L3%') AND
				(t4.NAME LIKE 'L4%' OR t.NAME LIKE 'L4%') THEN
			CASE 
				WHEN t5.NAME LIKE 'L5%' THEN t5.NAME
				WHEN t.NAME NOT LIKE 'L4%' THEN t.NAME
			END
		ELSE NULL
	END as territory_function
	,CASE
		WHEN	(t1.NAME LIKE 'L1%' OR t.NAME LIKE 'L1%') AND
				(t2.NAME LIKE 'L2%' OR t.NAME LIKE 'L2%') AND
				(t3.NAME LIKE 'L3%' OR t.NAME LIKE 'L3%') AND
				(t4.NAME LIKE 'L4%' OR t.NAME LIKE 'L4%') AND
				(t5.NAME LIKE 'L5%' OR t.NAME LIKE 'L5%') THEN
			CASE 
				WHEN t6.NAME LIKE 'L6%' THEN t6.NAME
				WHEN t.NAME NOT LIKE 'L5%' THEN t.NAME
			END
		ELSE NULL
	 END as territory_franchise
	,CASE
		WHEN	(t1.NAME LIKE 'L1%' OR t.NAME LIKE 'L1%') AND
				(t2.NAME LIKE 'L2%' OR t.NAME LIKE 'L2%') AND
				(t3.NAME LIKE 'L3%' OR t.NAME LIKE 'L3%') AND
				(t4.NAME LIKE 'L4%' OR t.NAME LIKE 'L4%') AND
				(t5.NAME LIKE 'L5%' OR t.NAME LIKE 'L5%') AND
				(t6.NAME LIKE 'L6%' OR t.NAME LIKE 'L6%') THEN
			CASE 
				WHEN t7.NAME LIKE 'L7%' THEN t7.NAME
				WHEN t.NAME NOT LIKE 'L6%' THEN t.NAME
			END
		ELSE NULL
	 END as territory_f_district
	,CASE
		WHEN	(t1.NAME LIKE 'L1%' OR t.NAME LIKE 'L1%') AND
				(t2.NAME LIKE 'L2%' OR t.NAME LIKE 'L2%') AND
				(t3.NAME LIKE 'L3%' OR t.NAME LIKE 'L3%') AND
				(t4.NAME LIKE 'L4%' OR t.NAME LIKE 'L4%') AND
				(t5.NAME LIKE 'L5%' OR t.NAME LIKE 'L5%') AND
				(t6.NAME LIKE 'L6%' OR t.NAME LIKE 'L6%') AND
				(t7.NAME LIKE 'L7%' OR t.NAME LIKE 'L7%') THEN
			CASE 
				WHEN t8.NAME LIKE 'L8%' THEN t8.NAME
				WHEN t.NAME NOT LIKE 'L7%' THEN t.NAME
			END 
		ELSE NULL
	END as territory_f_position
	,t.PARENTTERRITORY2ID as territory_parent_id
	,RIGHT(t3.NAME, LEN(t3.NAME)-3) as territory_distribution_region
	,t.JJ_TERRITORY1ID__C as Territory1_id
	,t.JJ_PARENTTERRITORY1ID__C as parentterritory1_id
	,t.country_iso_code as country_iso_code	
	,case when t.lastmodifieddate = '' then null else to_date(t.lastmodifieddate,'YYYYMMDD HH24:MI:SS') end::datetime as last_modified_date	
	,t.developername as developer_name
	,t.description as description
FROM {{ var('schema') }}.territory_raw t
LEFT OUTER JOIN (
	SELECT ID, NAME, PARENTTERRITORY2ID
	FROM {{ var('schema') }}.territory_raw
	WHERE NAME LIKE 'L8%') t8 ON t8.ID = t.ID
LEFT OUTER JOIN (
	SELECT ID, NAME, PARENTTERRITORY2ID
	FROM {{ var('schema') }}.territory_raw
	WHERE NAME LIKE 'L7%') t7 ON COALESCE(t8.PARENTTERRITORY2ID, t.ID) = t7.ID 
LEFT OUTER JOIN (
	SELECT ID, NAME, PARENTTERRITORY2ID
	FROM {{ var('schema') }}.territory_raw
	WHERE NAME LIKE 'L6%') t6 ON COALESCE(t7.PARENTTERRITORY2ID, t.ID) = t6.ID 
LEFT OUTER JOIN (
	SELECT ID, NAME, PARENTTERRITORY2ID
	FROM {{ var('schema') }}.territory_raw
	WHERE NAME LIKE 'L5%') t5 ON COALESCE(t6.PARENTTERRITORY2ID, t.ID) = t5.ID 
LEFT OUTER JOIN (
	SELECT ID, NAME, PARENTTERRITORY2ID
	FROM {{ var('schema') }}.territory_raw
	WHERE NAME LIKE 'L4%') t4 ON COALESCE(t5.PARENTTERRITORY2ID, t.ID) = t4.ID
LEFT OUTER JOIN (
	SELECT ID, NAME, PARENTTERRITORY2ID
	FROM {{ var('schema') }}.territory_raw
	WHERE NAME LIKE 'L3%') t3 ON COALESCE(t4.PARENTTERRITORY2ID, t.ID) = t3.ID
LEFT OUTER JOIN (
	SELECT ID, NAME, PARENTTERRITORY2ID
	FROM {{ var('schema') }}.territory_raw
	WHERE NAME LIKE 'L2%') t2 ON COALESCE(t3.PARENTTERRITORY2ID, t.ID) = t2.ID
LEFT OUTER JOIN (
	SELECT ID, NAME, PARENTTERRITORY2ID
	FROM {{ var('schema') }}.territory_raw
	WHERE NAME LIKE 'L1%') t1 ON COALESCE(t2.PARENTTERRITORY2ID, t.ID) = t1.ID 
WHERE (UPPER(t1.NAME) = 'L1-EMEA' OR UPPER(t.NAME) = 'L1-EMEA')
AND t.ID not in(
	select id from {{ var('schema') }}.territory_raw where name like '%L4%Consumer%'
union
	select id from {{ var('schema') }}.territory_raw where parentterritory2id in
		(select id from {{ var('schema') }}.territory_raw where name like '%L4%Consumer%')
union
	select id from {{ var('schema') }}.territory_raw where parentterritory2id in
		(select id from {{ var('schema') }}.territory_raw where parentterritory2id in
			(select id from {{ var('schema') }}.territory_raw where name like '%L4%Consumer%'))
union
	select id from {{ var('schema') }}.territory_raw where parentterritory2id in
		(select id from {{ var('schema') }}.territory_raw where parentterritory2id in
			(select id from {{ var('schema') }}.territory_raw where parentterritory2id in
				(select id from {{ var('schema') }}.territory_raw where name like '%L4%Consumer%')))
union
	select id from {{ var('schema') }}.territory_raw where parentterritory2id in
		(select id from {{ var('schema') }}.territory_raw where parentterritory2id in
			(select id from {{ var('schema') }}.territory_raw where parentterritory2id in
				(select id from {{ var('schema') }}.territory_raw where parentterritory2id in
					(select id from {{ var('schema') }}.territory_raw where name like '%L4%Consumer%'))))
union
	select id from {{ var('schema') }}.territory_raw where parentterritory2id in
		(select id from {{ var('schema') }}.territory_raw where parentterritory2id in
			(select id from {{ var('schema') }}.territory_raw where parentterritory2id in
				(select id from {{ var('schema') }}.territory_raw where parentterritory2id in
					(select id from {{ var('schema') }}.territory_raw where parentterritory2id in
						(select id from {{ var('schema') }}.territory_raw where name like '%L4%Consumer%'))))))