{{ config(
	post_hook="grant all privileges on {{ this }} to {{ var('snowflake_user_grant_privileges') }}")
}}

SELECT DISTINCT
        an.id             AS product_analytic_group_id,
        an.name           AS product_analytic_group,
        pg.product_vod__c AS product_id
FROM {{ source('raw', 'analytics_product_group') }} an
JOIN {{ source('raw', 'product_group_map') }} pg ON an.id=pg.analytics_product_group_vod__c
WHERE pg.product_vod__c IS NOT NULL AND pg.product_vod__c <> ''
