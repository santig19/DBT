
---Validates if all countries have calls

select * FROM (
SELECT count(distinct ID) as number, country_iso_code
FROM "PRD_POC_DB"."RAW_SCHEMA"."CALL_RAW" 
Group by country_iso_code) where number =0
