
{{ config(materialized='table') }}

WITH source AS (
    SELECT * 
    FROM {{ source('raw_data', 'payment_data') }}
),

cleaned_data AS (
    SELECT 
        payment_id::VARCHAR AS payment_id,
        currency_code::VARCHAR AS currency_code,
        statement_descriptor::VARCHAR AS statement_descriptor,
        created_at::TIMESTAMP AS created_at, 
        updated_at::TIMESTAMP AS updated_at,
        token_id::VARCHAR AS token_id,
        vaulted_token_id::VARCHAR AS vaulted_token_id,
        merchant_payment_id::VARCHAR AS merchant_payment_id,
        primer_account_id::VARCHAR AS primer_account_id,
        amount::INTEGER AS amount, 
        status::VARCHAR AS status,
        processor_merchant_id::VARCHAR AS processor_merchant_id,
        processor::VARCHAR AS processor,
        amount_captured::INTEGER AS amount_captured,
        amount_authorized::INTEGER AS amount_authorized,
        amount_refunded::INTEGER AS amount_refunded,
        NOW()::TIMESTAMP AS ingestion_timestamp,

        --- Standardize JSON format
        COALESCE(
            REPLACE(REPLACE(NULLIF(customer_details, ''), '''', '"'), 'None', 'null'),
            '{}'
        ) AS cleaned_customer_details

    FROM source
)

SELECT * FROM cleaned_data

{% if is_incremental() %}
WHERE created_at IS NOT NULL 
AND created_at > COALESCE((SELECT MAX(created_at) FROM {{ this }}), '1900-01-01')
{% endif %}
