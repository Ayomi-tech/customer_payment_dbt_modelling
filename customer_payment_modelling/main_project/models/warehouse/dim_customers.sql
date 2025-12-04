
{{ config(materialized='incremental', unique_key='customer_identifier') }}

WITH staged_payments AS (
    SELECT * FROM {{ ref('stg_payment') }}
),

cleaned_data AS (
    SELECT
        payment_id,
        created_at,
        updated_at,
        COALESCE(
            REPLACE(REPLACE(NULLIF(cleaned_customer_details, ''), '''', '"'), 'None', 'null'),
            '{}'
        ) AS new_cleaned_customer_details
    FROM staged_payments
),

parsed_data AS (
    SELECT 
        COALESCE(
            json_extract_string(c.new_cleaned_customer_details, '$.customer_id'), 
            json_extract_string(c.new_cleaned_customer_details, '$.email_address'),
            'UNKNOWN' --- Default value if both are NULL
        ) AS customer_identifier,
        json_extract_string(c.new_cleaned_customer_details, '$.customer_id') AS customer_id,
        json_extract_string(c.new_cleaned_customer_details, '$.first_name') AS first_name,
        json_extract_string(c.new_cleaned_customer_details, '$.last_name') AS last_name,
        json_extract_string(c.new_cleaned_customer_details, '$.email_address') AS email,
        json_extract_string(c.new_cleaned_customer_details, '$.phone_number') AS phone_number,
        json_extract_string(c.new_cleaned_customer_details, '$.title') AS customer_title,
        c.created_at,
        ROW_NUMBER() OVER (
            PARTITION BY COALESCE(
                json_extract_string(c.new_cleaned_customer_details, '$.customer_id'), 
                json_extract_string(c.new_cleaned_customer_details, '$.email_address')
            ) ORDER BY c.created_at DESC
        ) AS customer_row_number
    FROM cleaned_data c
    WHERE json_valid(c.new_cleaned_customer_details)
),

final_customers AS ( 
    SELECT *
    FROM parsed_data
    WHERE customer_row_number = 1
)

SELECT 
    customer_identifier::VARCHAR AS customer_identifier,  --- customer_identifier explicitly VARCHAR
    customer_id::VARCHAR AS customer_id ,
    first_name::VARCHAR AS first_name,
    last_name::VARCHAR AS last_name,
    email::VARCHAR AS customer_email,  
    phone_number::VARCHAR AS phone_number,
    customer_title::VARCHAR AS customer_title,
    created_at::TIMESTAMP AS customer_creation_date,
    NOW()::TIMESTAMP AS ingestion_timestamp
FROM final_customers

{% if is_incremental() %}
WHERE customer_creation_date IS NOT NULL 
AND customer_creation_date > COALESCE((SELECT MAX(customer_creation_date) FROM {{ this }}), '1900-01-01')
{% endif %}
