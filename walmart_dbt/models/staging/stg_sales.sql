{{ config(materialized='view') }}

-- stg_sales pre-built via ingestion/generate_unpivot_sql.py
-- Using view to avoid duplicating stg_sales_unpivot in BigQuery
-- create view in order to use {{ ref('stg_sales') }} similar to other staging data that come from dbt
SELECT * FROM `walmart_forecasting.stg_sales_unpivot`