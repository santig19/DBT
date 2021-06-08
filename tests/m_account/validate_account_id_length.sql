--Validates if the length of the account_id is 18 otherwise the test fails.
SELECT *
FROM "PRD_POC_DB"."BUSINESS_SCHEMA"."BUW_CUSTOMER_M_ACCOUNT"
WHERE LEN(ACCOUNT_ID) != 18