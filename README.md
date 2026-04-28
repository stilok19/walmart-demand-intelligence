# walmart-demand-intelligence
End-to-end grocery demand forecasting platform built on M5 Walmart dataset — GCS, BigQuery, dbt, LightGBM, PyTorch TFT

### Note on stg_sales

`stg_sales` is created directly in BigQuery via 
`ingestion/generate_unpivot_sql.py` (not dbt) because:

- The M5 dataset has 1,941 day columns requiring UNPIVOT
- Writing 1,941 column names in a dbt model is impractical
- The resulting table has ~59M rows (30,490 items × 1,941 days)

Run once:
    python ingestion/generate_unpivot_sql.py
    # Copy output SQL → paste into BigQuery console → Run
