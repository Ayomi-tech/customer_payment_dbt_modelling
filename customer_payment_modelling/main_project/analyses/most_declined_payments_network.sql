
{{ config(materialized='table') }}

WITH declined_payments AS (
    SELECT 
        processor_name AS network,
        COUNT(*) AS declined_count
    FROM {{ ref('fact_payments') }}
    WHERE payment_status = 'DECLINED'
    GROUP BY processor_name
)

SELECT *
FROM declined_payments
ORDER BY declined_count DESC
LIMIT 1 ;
