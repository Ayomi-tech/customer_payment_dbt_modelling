
{{ config(materialized='incremental', unique_key='token_id') }}

WITH staged_payment_instrument_token AS (
    SELECT * FROM {{ ref('stg_payment_instrument_token') }}
),

parsed_data AS (
    SELECT
        token_id::VARCHAR  AS payment_instrument_token_id, --- Primary key
        token_type::VARCHAR AS authorization_token_type,
        payment_instrument_type::VARCHAR AS  payment_instrument_type,
        network::VARCHAR AS payment_instrument_network ,

        --- Ensuring `cleaned_auth` is valid JSON before extracting fields
        json_extract_string(cleaned_auth, '$.reason_code')::VARCHAR AS authorization_code,
        json_extract_string(cleaned_auth, '$.reason_text')::VARCHAR AS authorization_reason,
        json_extract_string(cleaned_auth, '$.response_code')::VARCHAR AS authorization_response_code,
        json_extract_string(cleaned_auth, '$.challenge_issued')::VARCHAR AS authentication_challenge_issued,
        json_extract_string(cleaned_auth, '$.protocol_version')::VARCHAR AS transaction_protocol_version,

        NOW()::TIMESTAMP AS ingestion_timestamp

    FROM staged_payment_instrument_token
)

SELECT *
FROM parsed_data

{% if is_incremental() %}
WHERE ingestion_timestamp IS NOT NULL  
AND ingestion_timestamp > COALESCE((SELECT MAX(ingestion_timestamp) FROM {{ this }}), '1900-01-01')
{% endif %}
