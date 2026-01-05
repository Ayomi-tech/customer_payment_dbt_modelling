{{ config(materialized='table') }}

WITH merchants_with_metadata AS (
    SELECT
        pr.payment_request_id,
        pr.primer_account_id,
        pa.company_name
    FROM {{ ref('fact_payment_requests') }} pr
    JOIN {{ ref('dim_payment_primer_accounts') }} pa
    ON pr.primer_account_id = pa.primer_account_id
    WHERE 
           pr.payment_service_provider IS NOT NULL 
        OR pr.product_id IS NOT NULL
        OR pr.merchant_id IS NOT NULL
        OR pr.capture_status IS NOT NULL
        OR pr.transaction_country_code IS NOT NULL
)

SELECT DISTINCT company_name
FROM merchants_with_metadata
WHERE company_name IS NOT NULL
ORDER BY company_name ;
