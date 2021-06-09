---Validates if the length of the account_id is 18 otherwise the test fails.
SELECT *
FROM "PRD_POC_DB"."RAW_SCHEMA"."ACCOUNT_RAW"
WHERE LEN(ID) != 18