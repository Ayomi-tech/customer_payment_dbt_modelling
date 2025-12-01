# payment_customer_dbt_modelling

This dbt project transforms, models, and analyzes payment and customer data to generate insights. It utilizes DuckDB as the data warehouse.

## Project Overview

This project follows a standard dbt workflow, progressing from raw CSV data to a curated data warehouse ready for analysis. The project structure is organized into seeds, staging, warehouse, and analysis folders, each serving a specific purpose.

## Data Sources

4 CVS files were loaded into the `seeds` folder for initial ingestion into DuckDB.

## Data Loading and Transformation

1.  **Seeds:** The CSV files were initially loaded into DuckDB using dbt seeds. However, the `payment_request_data.csv` file encountered issues during loading due to inconsistencies in the JSON format within the `metadata` column.

2.  **Staging:** To address the JSON format issues in `payment_request_data.csv`, transformations were performed in the `staging` folder. This involved cleaning and standardizing the JSON data within the `metadata` column, enabling successful loading into DuckDB.  Additionally, minor transformations were applied to other columns as needed.  This staging layer prepares the data for integration into the data warehouse.
