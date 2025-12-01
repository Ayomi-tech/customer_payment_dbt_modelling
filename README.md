# payment_customer_dbt_modelling

This dbt project transforms, models, and analyzes payment and customer data to generate insights. It utilizes DuckDB as the data warehouse.

## Project Overview

This project follows a standard dbt workflow, progressing from raw CSV data to a curated data warehouse ready for analysis. The project structure is organized into seeds, staging, warehouse, and analysis folders, each serving a specific purpose.

## Data Sources

4 CVS files were loaded into the `seeds` folder for initial ingestion into DuckDB.

## Data Loading and Transformation

1.  **Seeds:** The CSV files were initially loaded into DuckDB using dbt seeds. However, the `payment_request_data.csv` file encountered issues during loading due to inconsistencies in the JSON format within the `metadata` column.

2.  **Staging:** To address the JSON format issues in `payment_request_data.csv`, transformations were performed in the `staging` folder. This involved cleaning and standardizing the JSON data within the `metadata` column, enabling successful loading into DuckDB.  Additionally, minor transformations were applied to other columns as needed.  This staging layer prepares the data for integration into the data warehouse.

3.  **Warehouse:** The `warehouse` folder contains the core logic for building the final data warehouse tables. Here, data types are explicitly defined, and final transformations are applied to prepare the data for analysis. This is where the fact table and dimension tables are built and additional two tables from 'customer details' field.  Deduplication logic is implemented in this layer to ensure data quality.

4.  **Analyses:** The `analyses` folder contains SQL queries used to generate insights based on the business questions. These queries leverage the curated data in the warehouse to answer specific analytical questions.

## Key Challenges and Solutions

*   **JSON Formatting Issues:** The `payment_request_data.csv` file contained inconsistent JSON formatting in the `metadata` column, preventing a direct load into DuckDB. This was addressed by implementing transformations in the `staging` layer to clean and standardize the JSON data.
*   **Missing Relationships:** During data validation, it was discovered that some `payment_id` values in the `fact_payment_requests` table did not exist in the `fact_payments` table (608 instances), and some `payment_instrument_token_id` values were missing from the `dim_payment_instrument_tokens` table (857 instances).  This indicates a referential integrity issue.

    *   **Potential Causes:** This could be due to several factors, including data loading order, data quality issues in the source data, or the timing of data arrival.  It is also possible, though less likely, that the relationships are optional.
    *   **Proposed Solutions:**
        *   **Data Quality Checks:**  Data quality checks should be implemented at the source to prevent such inconsistencies.
        *   **Data Staging and Reconciliation:** A staging and reconciliation process could be implemented to identify, manage, and decide how to handle these orphaned records. This might involve backfilling missing data, rejecting invalid records, or accepting `NULL`s if the relationship is optional.
        *   **Business Rules:** The best approach will depend on the business rules and whether the relationships are truly required or optional.

## Data Dictionary

The `schema.yml` file provides a detailed description of the data, including column definitions, data types, and tests.  It serves as a data dictionary for the project.

## Project Configuration

The `dbt_project.yml` file contains the project configuration, including the logic for seeds, staging, and warehouse models. It defines the project structure and dependencies.

## Running the Project

To run this dbt project, you will need to:

1.  Install dbt and DuckDB.
2.  Configure your `profiles.yml` file to connect to your DuckDB instance.  
3.  Navigate to the project directory in your terminal.
4.  Run the following dbt commands:

    ```bash
    dbt seed  # Load the CSV data into DuckDB
    dbt run   # Run the dbt models
    dbt test  # Run the dbt tests
    ```

## Conclusion

This dbt project provides a comprehensive analysis of the payment data. The project addresses data quality challenges, implements best practices for data warehousing, and delivers insights based on the provided business questions. The missing relationships issue highlights the importance of data quality and referential integrity and provides an opportunity to further investigate and improve the data pipeline.

