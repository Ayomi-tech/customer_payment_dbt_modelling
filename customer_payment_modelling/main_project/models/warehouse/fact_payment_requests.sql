{{ config(materialized='incremental', unique_key='payment_request_id') }}

WITH staging_payment_request AS (
    SELECT * 
    FROM {{ ref('stg_payment_request') }}
),

final_payments AS (
SELECT
    payment_request_id::VARCHAR AS payment_request_id,  --  Primary key
    currency_code::VARCHAR AS currency_code,
    created_at::TIMESTAMP AS transaction_creation_date,
    payment_request_type::VARCHAR AS payment_request_type,
    payment_instrument_vault_intention::VARCHAR AS vault_intention,
    payment_id::VARCHAR AS payment_id,  -- Foreign key 
    primer_account_id::VARCHAR AS primer_account_id,  -- Foreign key
    payment_instrument_token_id::VARCHAR AS payment_instrument_token_id,  -- Foreign key
    amount::INTEGER AS transaction_amount, 
    merchant_request_id::VARCHAR AS merchant_request_id,

    -- Extracted metadata fields with standardized names
    json_extract_string(cleaned_metadata, '$.psp')::VARCHAR AS payment_service_provider,
    json_extract_string(cleaned_metadata, '$.productId')::VARCHAR AS product_id,
    json_extract_string(cleaned_metadata, '$.merchantId')::VARCHAR AS merchant_id,
    json_extract_string(cleaned_metadata, '$.capture')::VARCHAR AS capture_status,
    json_extract_string(cleaned_metadata, '$.country')::VARCHAR AS transaction_country_code,

    NOW()::TIMESTAMP AS ingestion_timestamp,
    ROW_NUMBER() OVER (PARTITION BY payment_id ORDER BY created_at DESC) AS row_num --- Row number for deduplication

FROM staging_payment_request
),

deduplicated_payments AS (
    SELECT *
    FROM final_payments
    WHERE row_num = 1 --- keeping only the most recent record
)

SELECT * FROM deduplicated_payments 

{% if is_incremental() %}
WHERE transaction_creation_date IS NOT NULL  
AND transaction_creation_date > COALESCE((SELECT MAX(transaction_creation_date) FROM {{ this }}), '1900-01-01')
{% endif %}
