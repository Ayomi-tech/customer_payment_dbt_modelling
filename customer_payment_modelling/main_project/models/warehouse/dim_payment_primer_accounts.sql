
{{ config(materialized='incremental', unique_key='primer_account_id') }}

WITH staging_data AS (
    SELECT *
    FROM {{ ref('stg_payment_primer_account') }}
)

SELECT
    primer_account_id::VARCHAR AS primer_account_id , ---  Primary key
    company_name::VARCHAR AS company_name ,
    created_at::TIMESTAMP AS company_creation_date,
    NOW()::TIMESTAMP AS ingestion_timestamp
FROM staging_data

{% if is_incremental() %}
WHERE company_creation_date IS NOT NULL  
AND company_creation_date > COALESCE((SELECT MAX(company_creation_date) FROM {{ this }}), '1900-01-01')
{% endif %}
