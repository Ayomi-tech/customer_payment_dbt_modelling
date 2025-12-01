
{{ config(materialized='table') }}

WITH raw_data AS (
    SELECT * 
    FROM read_csv_auto('seeds/payment_request_data.csv', HEADER=TRUE, QUOTE='"', ESCAPE='"')
),

cleaned_data AS (
    SELECT
        CAST(payment_request_id AS VARCHAR) AS payment_request_id,  
        CAST(currency_code AS VARCHAR) AS currency_code,

         -- Ensure valid hour format
         CASE 
            -- Convert hours that exceed 23 into valid timestamps
            WHEN CAST(SPLIT_PART(created_at, ':', 1) AS INTEGER) >= 24 
            THEN STRPTIME(CONCAT(CURRENT_DATE, ' ', 
                                 (CAST(SPLIT_PART(created_at, ':', 1) AS INTEGER) % 24), 
                                 ':', SPLIT_PART(created_at, ':', 2)), 
                         '%Y-%m-%d %H:%M.%S')
            
            -- Normal case: Convert valid HH:MM.S format
            ELSE STRPTIME(CONCAT(CURRENT_DATE, ' ', created_at), '%Y-%m-%d %H:%M.%S') 
        END AS created_at,
  
        CAST(payment_request_type AS VARCHAR) AS payment_request_type,
        CAST(payment_instrument_vault_intention AS VARCHAR) AS payment_instrument_vault_intention,  
        CAST(payment_id AS VARCHAR) AS payment_id,  
        CAST(primer_account_id AS VARCHAR) AS primer_account_id,  
        CAST(payment_instrument_token_id AS VARCHAR) AS payment_instrument_token_id, 
        CAST(amount AS INTEGER) AS amount,  
        CAST(merchant_request_id AS VARCHAR) AS merchant_request_id,

        -- Standardize JSON format
        REPLACE(metadata, '''', '"') AS fixed_quotes_metadata,

        -- -- Fix Python-style boolean values (True → true, False → false)
        REGEXP_REPLACE(fixed_quotes_metadata, '\bTrue\b', 'true') AS fixed_true_metadata,
        REGEXP_REPLACE(fixed_true_metadata, '\bFalse\b', 'false') AS fixed_false_metadata,

        -- Remove trailing commas before closing braces{} {"key2": "value1",} → { "key1": "value1"}
        REGEXP_REPLACE(fixed_false_metadata, ',\s*}', '}') AS fixed_commas_metadata,

        -- Remove trailing double quotes from values like {"country": "US""} → {"country": "US"}
        REGEXP_REPLACE(fixed_commas_metadata, '":\s*"([^"]+)""', '": "\1"') AS cleaned_metadata


    FROM raw_data
)

SELECT
    payment_request_id,
    currency_code,
    created_at,
    payment_request_type,
    payment_instrument_vault_intention,
    payment_id,
    primer_account_id,
    payment_instrument_token_id,
    amount,
    merchant_request_id,
    NOW() AS ingestion_timestamp,
    cleaned_metadata
FROM cleaned_data

{% if is_incremental() %}
WHERE created_at IS NOT NULL 
AND created_at > (SELECT MAX(created_at) FROM {{ this }})
{% endif %}
