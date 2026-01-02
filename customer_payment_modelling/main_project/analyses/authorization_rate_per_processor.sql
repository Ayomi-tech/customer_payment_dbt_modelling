
{{ config(materialized='table') }}

WITH all_payments AS (
    SELECT *
    FROM {{ ref('fact_payments') }}
    WHERE payment_status != 'PENDING' -- Excluding PENDING payments
),

authorized_payments AS (
    SELECT 
        processor_name,
        COUNT(*) AS authorized_count
    FROM all_payments
    WHERE payment_status IN ('AUTHORIZED', 'CANCELLED', 'SETTLING', 'SETTLED', 'PARTIALLY_SETTLED')
    GROUP BY processor_name
),
total_non_pending_payments AS (
    SELECT
        processor_name,
        COUNT(*) AS total_count
    FROM all_payments
    GROUP BY processor_name
)
SELECT
    t.processor_name,
    t.total_count AS total_payments,  -- All payments where status is not equal PENDING
    a.authorized_count AS authorized_payments,  -- Only successfully authorized payments
    ROUND((a.authorized_count * 1.0 / NULLIF(t.total_count, 0)), 1) AS authorization_rate
FROM total_non_pending_payments t
LEFT JOIN authorized_payments a 
ON t.processor_name = a.processor_name 
ORDER BY authorization_rate DESC ;


{{ config(materialized='table') }}

WITH all_payments AS (
    SELECT *
    FROM {{ ref('fact_payments') }}
    WHERE payment_status != 'PENDING' -- Excluding PENDING payments
),

authorized_payments AS (
    SELECT 
        processor_name,
        COUNT(*) AS authorized_count
    FROM all_payments
    WHERE payment_status IN ('AUTHORIZED', 'CANCELLED', 'SETTLING', 'SETTLED', 'PARTIALLY_SETTLED')
    GROUP BY processor_name
),
total_non_pending_payments AS (
    SELECT
        processor_name,
        COUNT(*) AS total_count
    FROM all_payments
    GROUP BY processor_name
)
SELECT
    t.processor_name,
    t.total_count AS total_payments,  -- All payments where status is not equal PENDING
    a.authorized_count AS authorized_payments,  -- Only successfully authorized payments
    ROUND((a.authorized_count * 1.0 / NULLIF(t.total_count, 0)), 1) AS authorization_rate
FROM total_non_pending_payments t
LEFT JOIN authorized_payments a 
ON t.processor_name = a.processor_name 
ORDER BY authorization_rate DESC ;

